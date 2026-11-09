# SQL result exports

Exports run a validated project-scoped SQL query asynchronously and stream its result to private S3-compatible storage. Clients receive download links only after the entire query succeeds. Exports use the same SQL grammar, parameters, row policies, current revisions, and deletion rules as [interactive analytics](analytics.md), with separate operator budgets. Select `person_id` to include the resolved user and `person_properties` for the latest projected profile.

Native [ClickHouse S3 cold storage](storage.md) and result exports are separate. Export objects use their own private prefix and credentials. Recurring [project pipelines](pipelines.md) configure their own scoped
destination and schedule. This one-off export API still has no caller-selected
destination and does not create a recurring job.

## API operations

Every operation requires an API-resource OAuth access token and `events:read`
for the path's project. Use [client credentials](oauth.md) to obtain that token.
Static API Bearer keys apply only to legacy mode. Any authorized project reader can list jobs, read status, and obtain successful file links. Only the creator credential or an instance administrator can cancel a job.

| Operation ID | Endpoint | Success |
| --- | --- | --- |
| `create-event-export` | `POST /api/v1/projects/{id}/exports` | `202` |
| `list-event-exports` | `GET /api/v1/projects/{id}/exports` | `200` |
| `get-event-export` | `GET /api/v1/projects/{id}/exports/{export_id}` | `200` |
| `cancel-event-export` | `DELETE /api/v1/projects/{id}/exports/{export_id}` | `202` |
| `list-event-export-files` | `GET /api/v1/projects/{id}/exports/{export_id}/files` | `200` |

Job and file lists accept `limit`, default 20 and maximum 100, and a nonnegative `offset`. Responses contain `limit`, `offset`, and optional `next_offset`. The job list also reports `enabled` and the effective operator `limits`. When exports are disabled, creation returns `503`; no API call can configure storage or raise budgets.

### Create and poll

Save this request as `export.json`:

```json
{
	"idempotency_key": "nov-export-1",
	"sql": "SELECT toDate(event_time) AS day, JSONExtractString(properties,'client') AS client, count() AS conversations FROM events WHERE event={event:String} AND JSONExtractString(properties,'client')={client:String} GROUP BY day, client ORDER BY day, client",
	"parameters": {"event": "conversation_started", "client": "acme"},
	"format": "jsonl"
}
```

`format` is `jsonl` by default, or `csv`. SQL remains limited to 16 KiB and the existing typed scalar parameter contract. No implicit SQL `LIMIT`, pagination, or query splitting changes the result. Use `ORDER BY` when output order matters.

Submit and inspect the returned job ID:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports" \
	-H "Authorization: Bearer $ACCESS_TOKEN" \
	-H 'Content-Type: application/json' --data-binary @export.json

curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports/$EXPORT_ID" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

The examples assume `PORCUPINE_URL`, an authorized `ACCESS_TOKEN`, an existing `PROJECT_ID`, and `EXPORT_ID` from creation. A `202` confirms durable SQLite job creation. States are `queued`, `running`, `succeeded`, `failed`, `canceled`, and `expired`. Queue waiting is normal while delivery or interactive queries take priority.

An idempotency key contains 1 through 200 bytes and cannot be blank. Its scope is project and creator credential. An exact canonical SQL, parameters, and format retry returns the same job, including after terminal completion. Reusing the key for a different request returns `409`. SQLite retains the key hash, not the raw key.

Job metadata includes `id`, `project_id`, `state`, `format`, row and byte progress, `file_count`, `attempts`, timestamps, optional generic `error`, `columns`, and `consistency`. It never returns SQL, parameters, actor IDs, internal object keys, or credentials. Columns describe result names and ClickHouse types. Optional `totals`, `extremes`, and `rows_before_limit_at_least` describe supported result extras without adding them to exported data rows.

### Download or cancel

After `succeeded`, list files:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports/$EXPORT_ID/files?limit=20" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

Each item contains `index`, `name`, `rows`, compressed `bytes`, `decoded_bytes`, `sha256`, `url`, and `expires_at`. The checksum covers the compressed object. No file URLs are available while queued, running, failed, or canceled; those requests return `409`. Expired exports return `410` for files. Fresh links last at most five minutes, bounded by remaining job retention.

A presigned URL grants access without a Porcupine credential. Revoking a key does not revoke an already issued URL. Do not put links in public logs or messages. Previously downloaded files and successful exports are independent copies; later source-event edits or deletions do not rewrite them.

Cancel a queued or running job with no body:

```sh
curl --fail-with-body -X DELETE "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports/$EXPORT_ID" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

Cancellation of a running job is asynchronous. Poll until a terminal state. A terminal job returns `409` on cancellation. A different project reader receives `403` unless it is an instance administrator.

### MCP examples

Use `porcupine_docs` with an operation ID to obtain its contract. All five export operations use the existing `porcupine_call` tool; no additional MCP tool is required.

```json
{
	"operation_id": "create-event-export",
	"path": {"id": "PROJECT_ID"},
	"body": {
		"idempotency_key": "agent-export-2026-11-08-1",
		"sql": "SELECT uuid, event, event_time, properties FROM events WHERE event={event:String} ORDER BY id",
		"parameters": {"event": "conversation_started"},
		"format": "csv"
	}
}
```

Poll or cancel with the returned ID:

```json
{"operation_id":"get-event-export","path":{"id":"PROJECT_ID","export_id":"EXPORT_ID"}}
```

```json
{"operation_id":"cancel-event-export","path":{"id":"PROJECT_ID","export_id":"EXPORT_ID"}}
```

`list-event-export-files` uses the same path map plus optional string pagination parameters. Download links point to the operator's configured S3 endpoint, which must be reachable by the client.

## Formats and publication

Files are gzip-compressed JSONL or CSV, split on row boundaries at a default 256 MiB of decoded output. A single encoded row or header can exceed a configured chunk target. The row-value and total-output budgets still apply. Multipart uploads are sequential with 8 MiB parts. At most 10,000 files are produced for one attempt.

JSONL contains one object per result row, with column names as keys and raw ClickHouse JSON values. A String `properties` column remains a JSON-encoded string, rather than an expanded object. Quoted 64-bit integers remain strings. Duplicate result column names fail validation.

Each CSV file repeats its header. String cells are decoded; other values use their JSON representation. A null becomes an empty CSV cell, so null and an empty string are not distinguishable in that format. Nested values remain JSON text within a cell. CSV quoting preserves cell content rather than applying spreadsheet formula sanitization.

The service streams ClickHouse `JSONCompact` without buffering the full result. It validates column metadata, row widths and counts, supported extras, and complete end-of-stream. A late ClickHouse error, malformed envelope, oversized row, failed compression, or incomplete upload prevents success. Earlier chunk objects may already exist privately, but the API publishes no URLs until the entire attempt is validated and committed as succeeded.

## Consistency, retries, and revocation

Creation records accepted-event and mutation watermarks in SQLite. Execution waits for that project's accepted work through those watermarks to leave its delivery queues. The reported consistency is `delivery_barrier_nontransactional`.

This barrier is not a point-in-time snapshot. Later events and corrections can appear, and the query does not add an upper watermark filter. A whole-query retry can observe newer data. There is no cross-database transaction or exactly-once result guarantee.

One export runs at a time. It starts only while delivery is healthy, total pending originals and mutations are at or below the configured threshold, and the shared two-slot interactive query pool is idle. It then holds one slot. During execution, delivery failure or pending work above four times the threshold interrupts the attempt so ingestion can recover.

Creator authorization is checked before execution, polled every second while running, and checked again in the transaction that publishes success. Revocation or loss of project read authority stops publication. Cancellation and authorization checks do not revoke links already issued for a prior successful job.

After interruption or process restart, incomplete attempt objects are cleaned up and the whole query reruns, with at most three attempts. The worker does not resume at a row offset or combine files from different attempts. SQL and parameters leave job metadata after terminal completion; rows are streamed to S3, not retained as a local export file. SQLite holds job and file metadata. ClickHouse can spill bounded aggregation or sort work to its own local disk.

## Operator configuration

Exports require configured ClickHouse and S3 storage. Use [compose.exports.yaml](../compose.exports.yaml) alongside the base Compose file and private environment values. It can coexist with the cold-storage override. Export credentials go to the Go process; native ClickHouse disk credentials remain separate. Clients choose SQL and format, never a bucket, prefix, endpoint, credential, or resource budget.

| Environment variable | Default | Meaning |
| --- | --- | --- |
| `PORCUPINE_EXPORT_S3_BUCKET` | Empty, disabled | Existing private export bucket |
| `PORCUPINE_EXPORT_S3_PREFIX` | Required when enabled | Dedicated export prefix |
| `PORCUPINE_EXPORT_S3_REGION` | `AWS_REGION`, then `us-east-1` | Bucket region; Compose defaults to `us-east-1` |
| `PORCUPINE_EXPORT_S3_ENDPOINT` | AWS service endpoint | Optional base URL for an S3-compatible service |
| `PORCUPINE_EXPORT_S3_PATH_STYLE` | `false` | Path-style addressing for a compatible service |
| `PORCUPINE_EXPORT_S3_ACCESS_KEY_ID` | Credential provider chain | Optional static access key |
| `PORCUPINE_EXPORT_S3_SECRET_ACCESS_KEY` | Credential provider chain | Required with a static access key |
| `PORCUPINE_EXPORT_S3_SESSION_TOKEN` | Empty | Optional token for static temporary credentials |
| `PORCUPINE_EXPORT_MAX_EXECUTION_SECONDS` | `86400` | Whole-job execution deadline, 1 through 604,800 seconds |
| `PORCUPINE_EXPORT_MAX_READ_BYTES` | `1099511627776` | ClickHouse read budget, 1 byte through 1 PiB |
| `PORCUPINE_EXPORT_MAX_OUTPUT_BYTES` | `1099511627776` | Decoded output budget including CSV headers, 1 byte through 1 PiB |
| `PORCUPINE_EXPORT_FILE_BYTES` | `268435456` | Decoded chunk target, 1 KiB through 1 GiB |
| `PORCUPINE_EXPORT_MAX_ROW_BYTES` | `8388608` | Source JSON row-value bound, 1 KiB through 64 MiB |
| `PORCUPINE_EXPORT_MEMORY_BYTES` | `536870912` | ClickHouse query memory, 64 MiB through 2 GiB |
| `PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND` | `5242880` | Query and compressed upload bandwidth, 64 KiB/s through 100 MiB/s |
| `PORCUPINE_EXPORT_TEMPORARY_BYTES` | `1073741824` | ClickHouse temporary-disk budget, 1 MiB through 10 GiB |
| `PORCUPINE_EXPORT_QUEUE_THRESHOLD` | `1000` | Start threshold for total pending work, 1 through 100,000 |
| `PORCUPINE_EXPORT_MAX_QUEUED_JOBS` | `100` | Maximum queued and running jobs combined, 1 through 100 |
| `PORCUPINE_EXPORT_RETENTION_HOURS` | `168` | Availability after success, 1 through 720 hours |

The export reader uses one ClickHouse thread, fixed read-only settings, project row policies, and column grants excluding private originals and tombstone internals. Default budgets are 512 MiB memory, 5 MiB/s bandwidth, 24 hours execution, 1 TiB read and decoded output, and 1 GiB temporary disk. Interactive query limits remain unchanged. Client SQL cannot raise either set of limits.

The S3 endpoint cannot contain credentials, query parameters, a fragment, or a bucket path. Public endpoints require HTTPS; HTTP is limited to supported local or private destinations. Prefixes contain safe path segments and must not overlap the native ClickHouse disk prefix. Endpoint and credential configuration never appear in job metadata. Download URLs necessarily identify their destination and carry signed authorization parameters.

## Bucket permissions, lifecycle, and cleanup

Limit service credentials to the dedicated export object prefix. The service uses S3 multipart create, upload, and complete operations, covered by `s3:PutObject`, plus `s3:AbortMultipartUpload`, `s3:GetObject` for downloads, and `s3:DeleteObject` for cleanup. It does not list bucket contents or multipart parts. It needs no bucket-creation or lifecycle-management authority. Bucket policy, encryption, public-access blocking, and lifecycle rules are operator responsibilities.

The worker expires successful jobs after the configured retention, default seven days after success, and retries object cleanup. Failed, canceled, expired, and interrupted attempts also require cleanup. API expiry is immediate at the recorded deadline; physical object cleanup can lag or fail while storage is unavailable. Job metadata is retained separately.

Configure lifecycle rules as a required independent backstop for the export prefix. Object expiration must exceed retention plus maximum execution time and cleanup slack. For default budgets, 14-day object expiration and a two-day incomplete-multipart abort rule leave room for a 24-hour query and seven-day availability after success. Keep the incomplete-upload interval longer than maximum execution if a query may aggregate for hours before producing its first row. Recalculate both intervals when changing budgets.

Lifecycle age starts at object or multipart creation, not job success. A strict seven-day object expiry can remove a file before its API retention ends. Versioned buckets also need an explicit noncurrent-version policy if physical removal is required. These export-prefix lifecycle rules must never target native ClickHouse disk objects.

## Verification status

Unit tests, the complete race suite, real HTTP smoke checks, and all six real ClickHouse integration tests pass. The S3 suite also checks actual multipart upload/download against MinIO, multi-file exports, types, SQL semantics, cold data and mutations.
