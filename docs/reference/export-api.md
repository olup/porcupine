# SQL result exports

For CLI steps, see [Export a result](../query-events.md#export-a-result). Use a [pipeline](../pipelines.md) to send results on a schedule.

Exports stream project-scoped SQL results to private, operator-configured S3-compatible storage. They share the [interactive SQL dialect and project isolation](analytics.md) with separate execution and output budgets. A job sees revisions, deletions, `person_id`, and `person_properties` as of its [profile's analytical read](#consistency-retries-and-revocation).

## API operations

Each operation requires `events:read` for the project. HTTP and the CLI accept an API-audience [OAuth token](oauth.md) or [service key](service-keys.md). Use the MCP audience for MCP calls. Any authorized project reader can list jobs, read status and retrieve links for completed files. The creator credential or an instance administrator can cancel a job.

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
	"sql": "SELECT CAST(event_time AS DATE) AS day, json_extract_string(properties,'$.client') AS client, count(*) AS conversations FROM events WHERE event=$event AND json_extract_string(properties,'$.client')=$client GROUP BY day, client ORDER BY day, client",
	"parameters": {"event": "conversation_started", "client": "acme"},
	"format": "jsonl"
}
```

`format` is `jsonl` by default, or `csv`. SQL remains limited to 16 KiB and the running backend's bound parameter contract. No implicit SQL `LIMIT`, pagination, or query splitting changes the result. Use `ORDER BY` when output order matters.

Submit and inspect the returned job ID:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports" \
	-H "Authorization: Bearer $ACCESS_TOKEN" \
	-H 'Content-Type: application/json' --data-binary @export.json

curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports/$EXPORT_ID" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

The examples assume `PORCUPINE_URL`, an authorized `ACCESS_TOKEN`, an existing `PROJECT_ID`, and `EXPORT_ID` from creation. A `202` confirms that the job committed to SQLite in standalone or to shared PostgreSQL in hosted. States are `queued`, `running`, `succeeded`, `failed`, `canceled`, and `expired`. Queue waiting is normal while delivery or interactive queries take priority.

An idempotency key contains 1 through 200 bytes and cannot be blank. Its scope is project and creator credential. An exact canonical SQL, parameters, and format retry returns the same job, including after terminal completion. Reusing the key for a different request returns `409`. The job store retains the key hash.

Job metadata includes `id`, `project_id`, `state`, `format`, row and byte progress, `file_count`, `attempts`, timestamps, optional generic `error`, `columns`, and `consistency`. It never returns SQL, parameters, actor IDs, internal object keys, or credentials. Columns describe result names and database types.

### Download or cancel

After `succeeded`, list files:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/exports/$EXPORT_ID/files?limit=20" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

Each item contains `index`, `name`, `rows`, compressed `bytes`, `decoded_bytes`, `sha256`, `url`, and `expires_at`. The checksum covers the compressed object. File links are available only after the whole attempt succeeds. Queued, running, failed, and canceled jobs return `409` for files. Expired jobs return `410`. Authorized readers can request fresh links, which last at most five minutes and expire by the job's retention deadline.

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
		"sql": "SELECT uuid, event, event_time, properties FROM events WHERE event=$event ORDER BY id",
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

JSONL contains one object per result row, with column names as keys and the backend's JSON values. A String `properties` column remains a JSON-encoded string, rather than an expanded object. Quoted 64-bit integers remain strings. Duplicate result column names fail validation.

Each CSV file repeats its header. String cells are decoded; other values use their JSON representation. A null becomes an empty CSV cell, so null and an empty string are not distinguishable in that format. Nested values remain JSON text within a cell. CSV quoting preserves cell content rather than applying spreadsheet formula sanitization.

The service streams native DuckDB rows without buffering the full result. A query error, malformed stream, oversized row, failed compression, or incomplete upload prevents success. Earlier chunk objects may already exist privately, but the API publishes no URLs until the entire attempt is validated and committed as succeeded.

## Consistency, retries, and revocation

**Standalone.** Creation records accepted-event and mutation watermarks in SQLite. Execution waits for work through both watermarks to leave the project's delivery queues. The reported consistency is `delivery_barrier_nontransactional`. The query can include later events and corrections. A retry can read newer data.

**Hosted.** Creation settles accepted mutations in the project's write session and records a retained DuckLake snapshot in PostgreSQL with the job. The reported consistency is `retained_analytical_snapshot`. Attempts read the same snapshot even if the job moves to another worker. Live events still in worker memory at the snapshot can be absent. If the snapshot is unavailable, the job fails with `snapshot_unavailable`. Include export reads in the [hosted retention settings](../design/hosted-architecture.md#recovery-and-retention).

Exports and scheduled S3 queries share one background slot per server process. The job starts when the local analytical pool is idle and occupies one place. In standalone, delivery must also be healthy and total pending originals and mutations at or below the configured threshold. If delivery fails or the backlog rises above four times that threshold, the attempt stops so ingestion can recover.

The worker checks creator authorization before execution, every second during execution, and again in the transaction that publishes success. Revocation or loss of project read authority stops publication.

After interruption, the worker cleans up incomplete objects and retries the whole query, up to three attempts. A hosted worker can reclaim a job after its attempt lease expires. Each attempt writes its own files. The job store clears SQL and parameters after terminal completion and retains file metadata.

## Operator configuration

Exports require a configured analytical backend and S3 result storage. Set `export.s3` in the private [server configuration](configuration.md). Export credentials go to the Go process and remain separate from analytical storage credentials. Clients choose SQL and format, never a bucket, prefix, endpoint, credential, or resource budget.

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
| `PORCUPINE_EXPORT_MAX_OUTPUT_BYTES` | `1099511627776` | Decoded output budget including CSV headers, 1 byte through 1 PiB |
| `PORCUPINE_EXPORT_FILE_BYTES` | `268435456` | Decoded chunk target, 1 KiB through 1 GiB |
| `PORCUPINE_EXPORT_MAX_ROW_BYTES` | `8388608` | Source JSON row-value bound, 1 KiB through 64 MiB |
| `PORCUPINE_EXPORT_BANDWIDTH_BYTES_PER_SECOND` | `5242880` | Query and compressed upload bandwidth, 64 KiB/s through 100 MiB/s |
| `PORCUPINE_EXPORT_QUEUE_THRESHOLD` | `1000` | Standalone start threshold for total pending work, 1 through 100,000 |
| `PORCUPINE_EXPORT_MAX_QUEUED_JOBS` | `100` | Maximum queued and running jobs combined, 1 through 100 |
| `PORCUPINE_EXPORT_RETENTION_HOURS` | `168` | Availability after success, 1 through 720 hours |

Native exports use the project's isolated engine and public view with the same AST guard as interactive queries. They share its configured memory and fixed temporary directory budget. Physical read-byte and per-query temporary-disk quotas are not implemented. The reported `memory_bytes` is the shared engine setting, not a per-job or process RSS cap.

The S3 endpoint cannot contain credentials, query parameters, a fragment, or a bucket path. Public endpoints require HTTPS; HTTP is limited to supported local or private destinations. Prefixes contain safe path segments and must not overlap the analytical data prefix. Endpoint and credential configuration never appear in job metadata. Download URLs necessarily identify their destination and carry signed authorization parameters.

## Bucket permissions, lifecycle, and cleanup

Limit service credentials to the dedicated export object prefix. The service uses S3 multipart create, upload, and complete operations, covered by `s3:PutObject`, plus `s3:AbortMultipartUpload`, `s3:GetObject` for downloads, and `s3:DeleteObject` for cleanup. It does not list bucket contents or multipart parts. It needs no bucket-creation or lifecycle-management authority. Bucket policy, encryption, public-access blocking, and lifecycle rules are operator responsibilities.

The worker expires successful jobs after the configured retention, default seven days after success, and retries object cleanup. Failed, canceled, expired, and interrupted attempts also require cleanup. API expiry is immediate at the recorded deadline; physical object cleanup can lag or fail while storage is unavailable. Job metadata is retained separately.

Deleting a project removes its one-time export jobs. Porcupine keeps object keys and known multipart upload IDs in its operational store until S3 confirms cleanup. A previously issued download URL may still work until the object is removed or the URL expires. Keep the deployment's export bucket and prefix configured through cleanup. S3 lifecycle rules cover upload IDs lost before checkpointing and noncurrent object versions. Copies already downloaded or delivered to a customer's pipeline destination remain with the recipient.

Configure lifecycle rules as a required independent backstop for the export prefix. Object expiration must exceed retention plus maximum execution time and cleanup slack. For default budgets, 14-day object expiration and a two-day incomplete-multipart abort rule leave room for a 24-hour query and seven-day availability after success. Keep the incomplete-upload interval longer than maximum execution if a query may aggregate for hours before producing its first row. Recalculate both intervals when changing budgets.

Lifecycle age starts at object or multipart creation, not job success. A strict seven-day object expiry can remove a file before its API retention ends. Versioned buckets also need an explicit noncurrent-version policy if physical removal is required. These export-prefix lifecycle rules must never target analytical data objects.

## Verification status

Run the native test and S3 integration targets for storage, streaming export, private downloads, project cleanup, cancellation, and publication-revocation boundaries. These use isolated local data and S3-compatible services, not production resources.
