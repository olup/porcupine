# Project delivery pipelines

Pipelines send project data to a webhook or an S3-compatible destination. They run in the existing Go process with durable SQLite state. They require explicit egress permissions and an operator encryption key; neither Redis nor a message broker is required.

## Separate storage and delivery products

| Capability | Purpose | Configuration | Query or delivery model |
| --- | --- | --- | --- |
| [Native cold storage](storage.md) | Move ClickHouse parts to S3 while retaining analytical history | Deployment operator | Same `events` table and queries; MOVE TTL |
| [One-off SQL export](exports.md) | Download the result of a particular SQL query | Operator export storage; authorized client submits SQL | Asynchronous private gzip files and expiring download links |
| Project pipeline | Deliver data repeatedly to a destination | Project-scoped pipeline writer; operator enables and bounds runtime | Webhook delivery or scheduled S3 runs with persisted progress |

A pipeline is not a backup or a way to change the instance's ClickHouse storage policy. Destination credentials are write-only API inputs. Reads return metadata and nonsecret rules without destination endpoint URLs,
credentials, or event payloads. S3 rules include their SQL and destination
bucket, region, prefix, and addressing mode, so authorized readers can inspect
what a scheduled run does.

## Enable the runtime

| Environment variable | Meaning |
| --- | --- |
| `PORCUPINE_PIPELINE_KEY` | Standard base64 encoding of exactly 32 random bytes |
| `PORCUPINE_PIPELINE_KEY_FILE` | File containing that base64 key; use instead of the direct environment value |
| `PORCUPINE_PIPELINE_S3_ENDPOINTS` | Optional comma-separated exact HTTP or HTTPS origins allowed for custom S3 endpoints |

Without a key, pipeline creation is disabled. Configure exactly one key source. The key file is bounded to 256 bytes, including surrounding whitespace. Its path
is local to the Go process; mount it explicitly when running in a container. Generate the value once with `openssl rand -base64 32` and preserve it through
the deployment's private secret storage. Do not commit it, return it to an agent, or generate a replacement at each restart.

The runtime encrypts destination credentials with AES-256-GCM, using a fresh nonce and project-and-pipeline binding. Webhook endpoints and headers are encrypted too. SQLite keeps only the key fingerprint, not the encryption key. If destinations already exist, startup rejects a missing or different key. There is no encryption-key rotation endpoint. Preserve the original key separately from database backups.

Custom S3 origins match scheme, hostname, and port. They cannot contain paths, credentials, query strings, fragments, or wildcards. Custom origins use HTTPS, with HTTP available for local S3-compatible stores. An empty allowlist permits the normal AWS endpoint selection but no custom endpoint. Granting an origin is an operator decision; project credentials cannot expand the allowlist. The webhook destination rules are separate and do not inherit this exception.

## Permissions and API

Use an API-resource [OAuth access token](oauth.md) for HTTP, or an MCP-resource token through `porcupine_call`. All operations apply to one authorized project.

- Reads require both `pipelines:read` and `events:read`.
- Creation, pause, resume, deletion, credential replacement, and manual retry require both `pipelines:write` and `events:read`.
- `events:read` alone cannot configure or inspect pipeline delivery.

Grant these permissions to an OAuth client only when it may send project data to external destinations. Credential delegation retains the usual anti-escalation checks. Workers also check that the creating principal remains active and retains `pipelines:write` plus `events:read` for the project. Expiry of the short-lived token used to create a pipeline does not itself delete the destination.

| Operation ID | Endpoint | Success |
| --- | --- | --- |
| `create-pipeline` | `POST /api/v1/projects/{id}/pipelines` | `201` |
| `list-pipelines` | `GET /api/v1/projects/{id}/pipelines` | `200` |
| `get-pipeline` | `GET /api/v1/projects/{id}/pipelines/{pipeline_id}` | `200` |
| `pause-pipeline` | `POST /api/v1/projects/{id}/pipelines/{pipeline_id}/pause` | `200` |
| `resume-pipeline` | `POST /api/v1/projects/{id}/pipelines/{pipeline_id}/resume` | `200` |
| `delete-pipeline` | `DELETE /api/v1/projects/{id}/pipelines/{pipeline_id}` | `204` |
| `replace-pipeline-credentials` | `PUT /api/v1/projects/{id}/pipelines/{pipeline_id}/credentials` | `200` |
| `list-pipeline-runs` | `GET /api/v1/projects/{id}/pipelines/{pipeline_id}/runs` | `200` |
| `get-pipeline-run` | `GET /api/v1/projects/{id}/pipelines/{pipeline_id}/runs/{run_id}` | `200` |
| `retry-pipeline-run` | `POST /api/v1/projects/{id}/pipelines/{pipeline_id}/runs/{run_id}/retry` | `202` |

Lists default to 20 items, accept at most 100, and use nonnegative offsets. They return `next_offset` when another page exists. Pipeline lists contain only `id`, `project_id`, `kind`, `name`, `state`, and
`created_at_ms`. Fetch `get-pipeline` for the nonsecret `webhook` or `s3` rule.
Webhook rules expose filters and delivery timing; S3 rules expose SQL, parameters,
format, schedule, and the destination details listed above. Keeping full rules
out of list pages bounds API and MCP response size. State is `active` or `paused`. Creation admits at most 64 pipelines per project and 512 instance-wide.
Creation has no idempotency-key field; inspect the project list after an
uncertain response before creating another destination.

The credential-replacement endpoint accepts `{"credentials":{...}}` without returning or requiring recovery of the old destination. Supply complete replacement credentials. A webhook replacement may also supply its full same-kind specification to change the endpoint, retaining exactly the same event filters. Pending deliveries use the replacement destination. The S3 specification, scheduling, and webhook filters cannot change through credential rotation. Delete and recreate a pipeline to change an immutable specification.

## Webhook event and batch modes

Webhooks deliver newly accepted original events after cookieless identity
resolution, without waiting for ClickHouse projection. Private identity controls are excluded. A scheduled S3 query can select `person_id` and `person_properties` on ordinary events after identity projection. Batch mode groups those
raw accepted events; it does not execute an analytical SQL query. They do not backfill older events or emit a correction stream.
`include_events` and `exclude_events` match exact event names, with exclusions
taking precedence. Each list accepts up to 100 names. An empty inclusion list
matches all names not excluded.

Create an event-mode webhook with this body:

```json
{
	"name": "Conversation notifications",
	"kind": "webhook",
	"webhook": {
		"endpoint": "https://receiver.example.com/porcupine",
		"delivery_mode": "event",
		"include_events": ["conversation_started"],
		"exclude_events": []
	},
	"credentials": {"bearer_token": "DESTINATION_TOKEN"}
}
```

Use a private JSON file for real destination secrets, then submit it:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/pipelines" \
	-H "Authorization: Bearer $ACCESS_TOKEN" \
	-H 'Content-Type: application/json' --data-binary @pipeline.json
```

Event mode is the default. Its request body contains `delivery_id`,
`pipeline_id`, `project_id`, and one `event` object. The object contains the
accepted event and receipt metadata. Each request sends
`X-Porcupine-Delivery-ID` and `Idempotency-Key` with the same delivery ID.

For deliveries that trigger trusted business actions, use a `server_only`
project and an authenticated backend writer. A `mixed` project's public
ingestion key permits its holder to submit supported event names. Destination
authentication does not add provenance to those captured events.

Batch mode collects events in closed UTC receipt-time windows. Configure its
webhook rule as follows:

```json
{
	"endpoint": "https://receiver.example.com/porcupine",
	"delivery_mode": "batch",
	"interval_seconds": 300,
	"include_events": ["conversation_started"]
}
```

Intervals range from 60 through 86,400 seconds. Optional `start_at` is an
interval-aligned UTC RFC3339 boundary. Without it, collection begins when the
pipeline is created, so the first window can be partial. A future start skips
events before that boundary. Windows use server receipt time, not the client's
event timestamp.

A window can contain multiple HTTP chunks. Each chunk targets at most 100
events and about 1 MiB; one previously accepted large event can occupy a chunk
alone. Requests begin only after the window closes. A batch body has the same
identifiers as event mode, the UTC boundaries `window_start_ms` and
`window_end_ms`, and an `events` array. Its delivery
ID and final payload remain stable across retries.

For a five-minute window ending at 12:05 UTC, two chunks might use IDs
`DELIVERY_A` and `DELIVERY_B`. If the receiver accepts `DELIVERY_A` but the
acknowledgment is lost, the retry uses `DELIVERY_A` and the same body, not a new
window or delivery ID. The receiver must deduplicate that ID before performing
its own side effects. A chunk boundary is an HTTP delivery boundary, not a
transaction spanning every chunk in the window.

### Acknowledgments, retries, and limits

Success requires a 2xx response with a complete response body no larger than
64 KiB. The default attempt deadline is ten seconds. Network errors and HTTP
408, 425, 429, and 5xx responses retry with exponential backoff, up to eight
attempts. Other rejections become failed runs. An uncertain response or restart
can redeliver a request already accepted by the receiver. Delivery is at least
once. The receiver should record its delivery ID before applying side effects.

One worker sends webhook requests sequentially. A transient failure delays
further attempts to that destination, starting at one second and increasing
up to one hour. The worker can deliver to other destinations during that delay.

Pending and failed payloads remain durable and count against queue limits.
Defaults are 10,000 queued events and 64 MiB instance-wide, with 1,000 events
and 16 MiB per project. Fanout consumes capacity for each matching destination.
If capture cannot enqueue every matching webhook delivery, the entire capture
batch rolls back and returns `503`; it does not acknowledge partial acceptance.
Pausing stops new enqueueing for that destination but does not free its retained
failed payloads. Deliver, cancel through event deletion, or delete the pipeline
to release retained capacity as appropriate.

Pausing skips new webhook enqueueing and holds existing queued work. Resume
releases pending work and collects future events; it does not backfill the
paused period. A manual retry requeues a retained failed run with its original
delivery ID and payload. Successful and canceled payloads leave the queue. Their
metadata is retained for up to seven days, bounded to 1,000 rows per project and
10,000 instance-wide.

Deleting an event removes it from an unattempted queued batch, keeping that
batch's unsent delivery ID. After any attempt, the old body remains immutable:
the service cancels that delivery and queues surviving members under a fresh
ID. No unrelated member is discarded. The old request may already have reached
the receiver, so deduplicate individual events by project and event UUID across
delivery IDs as well as deduplicating whole requests. Delivered events and
requests already in flight cannot be recalled. Corrections do not update a
receiver's previous copy.

### Destination protection

Webhook destinations must be public HTTPS URLs. The transport checks all DNS
answers immediately before connection, rejects private and reserved networks,
and connects to validated numeric addresses while verifying TLS for the URL
host. It uses neither environment proxies nor redirects. A custom S3 origin
allowlist does not relax webhook rules.

Credentials can contain a Bearer token or permitted custom `X-` headers.
Reserved transport and Porcupine headers cannot be overridden. At most 16
headers totaling 8 KiB are accepted. Credentials and endpoint URLs are write-only
and encrypted. No built-in webhook signature or consumer-specific adapter is
claimed; use the documented HTTP contract and receiver-side deduplication.

## Scheduled S3 query windows

S3 pipelines run the restricted analytical SQL subset over successive closed
receipt-time windows. `interval_seconds` ranges from 60 through 86,400. Required
`start_at` is a nonnegative UTC RFC3339 boundary, with whole-second precision,
aligned to the interval since Unix epoch. It is the lower bound of the first
window. A historical start processes available history one window at a time;
a future start waits until its first complete window closes.

The service restricts every `events` source, including nested queries and joins,
to `received_at_ms >= window_start` and `< window_end`. This is deliberate
windowing, unlike a one-off export that runs the submitted query without adding
window filters. Event timestamps do not choose the delivery window. SQL can
reference `{window_start:Int64}` and `{window_end:Int64}`, but callers cannot
supply those names in `parameters`; the scheduler binds them.

Create a scheduled query with this body, replacing the bucket, credentials, and
`start_at` with the intended destination and aligned first boundary:

```json
{
	"name": "Hourly conversation totals",
	"kind": "s3",
	"s3": {
		"sql": "SELECT {window_start:Int64} AS window_start_ms, event, count() AS total FROM events WHERE event={event:String} GROUP BY event ORDER BY event",
		"parameters": {"event": "conversation_started"},
		"format": "jsonl",
		"interval_seconds": 3600,
		"start_at": "2026-11-08T00:00:00Z",
		"destination": {
			"bucket": "your-private-pipeline-bucket",
			"region": "eu-west-3",
			"prefix": "porcupine/project-delivery"
		}
	},
	"credentials": {
		"access_key_id": "DESTINATION_ACCESS_KEY",
		"secret_access_key": "DESTINATION_SECRET_KEY"
	}
}
```

A session token is optional. Explicit destination access-key and secret-key
credentials are required; a pipeline does not inherit the host's AWS role.
For a custom endpoint, set `endpoint` to an origin on the operator's allowlist.
Porcupine uses path-style addressing so requests stay on that origin. The prefix defaults to `porcupine`. Choose a directory that does not
overlap native ClickHouse objects or one-off exports.

### Data files and committed manifests

Data uses the same gzip JSONL or CSV encoding as [one-off exports](exports.md).
Default chunk size is 256 MiB decoded with sequential 8 MiB multipart uploads.
The runtime validates the complete ClickHouse stream before publishing a
manifest. A partial collection of data objects is not a completed run.

Object paths under the configured prefix follow this layout:

```text
PIPELINE_ID/WINDOW_START_MS/ATTEMPT_ID/part-000001.jsonl.gz
PIPELINE_ID/WINDOW_START_MS/manifest.json
```

CSV uses `.csv.gz`. The manifest contains version, project, pipeline and run
IDs, window bounds, format, total rows, column names and types, and per-file
keys, row counts, compressed and decoded bytes, and compressed SHA-256 checksums.
Each file `key` is the complete bucket-relative S3 object key, including the
configured destination prefix; consumers can pass it directly to S3 GetObject.
An empty query result can publish a zero-row manifest with no data files.
Consumers must use committed manifests, rather than treating each new part
object as a completed delivery.

The manifest key is stable for a pipeline and window. Publication uses a
conditional create and reads the manifest back before advancing the SQLite
cursor and marking success in one transaction. A lost acknowledgment after
manifest publication can recover the same recorded manifest without rerunning
the query. A different or unexpected existing manifest fails closed; it is not
silently overwritten.

Before a manifest checkpoint exists, retries clean uncommitted attempt objects
and rerun the whole window query. A run allows three automatic attempts, then
requires explicit retry after failure. The cursor advances only after verified
publication, so a failed window holds later windows for that pipeline. Retrying
is not a snapshot: it can see newer corrections. Completed windows do not
automatically reopen for later corrections or deletions, and previously
delivered objects remain independent external copies.

Pausing holds the cursor and interrupts current work. Resume catches up closed
S3 windows from that cursor. This differs from webhooks, which skip events
accepted while paused. Deleting an S3 pipeline pauses it and cleans uncommitted
attempts; it can return `503` while a running attempt stops or cleanup remains
unavailable. Successfully committed destination files are not deleted.

### Admission, budgets, and destination permissions

Scheduled work waits for its recorded original-event and mutation delivery
barriers and healthy ClickHouse delivery. This reduces delivery lag; it is not a
transactional snapshot. S3 pipelines and one-off exports share one heavy-query
slot, leaving the other interactive slot available. Work starts only while the
interactive pool is idle and pending input is at or below the operator threshold.
It yields if delivery fails or input grows beyond four times that threshold.

The resource-related `PORCUPINE_EXPORT_*` settings also bound scheduled S3 work.
Defaults are one ClickHouse thread, 512 MiB query memory, 5 MiB/s bandwidth,
24 hours per attempt, 1 TiB reads and output, 1 GiB temporary disk, and an
8 MiB source-row bound. These are operator controls, not pipeline request
fields. The one-off export bucket, job queue, retention, and presigned-link
settings do not become the scheduled destination or its retention policy.

Use private destination credentials limited to the pipeline prefix. The writer
needs multipart writes, abort, cleanup deletion, and manifest reads plus
conditional writes. On AWS, the core object permissions are `s3:PutObject`,
`s3:GetObject`, `s3:AbortMultipartUpload`, and `s3:DeleteObject`. Manifest preflight
also needs `s3:ListBucket` authority that lets a missing manifest return `404`;
without it AWS can return `403` for a missing key. Verify absence detection with
the actual bucket policy. See [AWS GetObject permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html). The operator
creates the bucket and sets encryption, public-access policy, lifecycle, and any
provider-specific permissions. A custom provider must support the conditional
manifest contract; allowing its origin does not establish compatibility.

Successful pipeline objects have no automatic seven-day expiry. SQLite keeps
the newest 1,000 successful run summaries per pipeline and clears their full
manifest checkpoints after commit. Failed and active runs remain available;
the cursor preserves the next uncompleted window. The receiver or operator
owns object retention; deleting the pipeline removes its local run metadata
while preserving committed destination objects. Configure incomplete-multipart cleanup for
crash windows, with a duration longer than permitted work, and protect committed
manifests and referenced files from premature lifecycle deletion. The one-off
export expiry policy must not be applied blindly to recurring delivery data.

## Inspect, pause, replace credentials, and retry

With an API-resource access token, inspect retained run metadata:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/pipelines/$PIPELINE_ID/runs?limit=20" \
	-H "Authorization: Bearer $ACCESS_TOKEN"

curl --fail-with-body -X POST "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/pipelines/$PIPELINE_ID/pause" \
	-H "Authorization: Bearer $ACCESS_TOKEN"
```

Runs expose IDs, state, attempts, creation and completion milliseconds, and a
sanitized error code. Webhook states are `queued`, `sending`, `delivered`,
`failed`, and `canceled`; S3 states are `queued`, `running`, `succeeded`, and
`failed`. Webhook status can include the HTTP status; batch runs
include window bounds and event count. No event body or remote response text is
returned. A retry response contains `requeued`; terminal success is not retried.

Use these arguments with `porcupine_call`, after reading each operation's
contract through `porcupine_docs`:

```json
{
	"operation_id": "replace-pipeline-credentials",
	"path": {"id": "PROJECT_ID", "pipeline_id": "PIPELINE_ID"},
	"body": {"credentials": {"bearer_token": "REPLACEMENT_DESTINATION_TOKEN"}}
}
```

```json
{
	"operation_id": "retry-pipeline-run",
	"path": {"id": "PROJECT_ID", "pipeline_id": "PIPELINE_ID", "run_id": "RUN_ID"}
}
```

Use the destination-appropriate credential object for S3 replacement.
`resume-pipeline` uses the pipeline path map without a body. A retained failed
run can be requeued while its pipeline is paused, but no attempt sends until the
pipeline is active and creator grants still allow it. Rotation changes secrets,
not the identity of the creator principal.

## Restore and external side effects

Supported restores pause all restored destinations before the restored application can send data. Review destinations, creator grants, receiver deduplication, and stored progress, then resume explicitly. Preserve the encryption key out of band. Follow the [online restore procedure](online-backup.md); a raw database copy is not a substitute for its outbound-delivery fence.

A restored checkpoint can precede a webhook acknowledgment or an S3 write that already reached its destination. External effects are outside the SQLite recovery transaction. Receiver deduplication and stable object/run identifiers remain necessary, and no mode promises true exactly once. For webhook batches, retain project-and-event-UUID deduplication across replacement delivery IDs. Pausing or deleting a pipeline cannot recall a request already in flight or erase an external copy.

## Testing a destination

Run `make integration-s3` to exercise a scheduled query against ClickHouse and an
isolated S3-compatible store. The test downloads the compressed files listed in
the manifest, verifies their hashes and checks the rows across two projects and
multiple windows.

Run `go test -race ./internal/porcupine -run TestPipelineBatchHTTPAndMCPContract`
to exercise webhook setup, capture, retries, and run inspection. That test uses a
synthetic HTTPS receiver. When connecting your own receiver, verify that it
deduplicates repeated delivery IDs before enabling the pipeline for real events.
