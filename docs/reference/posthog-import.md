# Historical upload protocol

The HTTP upload protocol accepts native, PostHog, and Segment JSONL. For file and directory imports with the CLI, see [Import event history](../imports.md).

## Manifest

`POST /api/v1/projects/{id}/imports` requires `imports:write` and `events:write` on the selected project, using an API-audience credential. The body selects one format for the entire job:

```json
{
  "format": "native",
  "idempotency_key": "migration-2026-01",
  "files": [
    {"path":"events/part-1.jsonl","kind":"events","size_bytes":1234,"sha256":"SHA256_OF_UNCOMPRESSED_BYTES"}
  ]
}
```

`format` is `native`, `posthog`, or `segment`; omission selects PostHog. Each file has a clean relative label, kind, byte count, and 64-character hexadecimal SHA-256. These labels are not server filesystem paths. `persons` files are supported only for PostHog. Persons files precede events; the remaining order is lexical by label.

The idempotency key is scoped to its project and creator. Reusing it with a different format or manifest returns `409`. The format and authenticated source binding are frozen in the job. Uploading with an administrator's credential does not change them.

## Operations

Prefix each HTTP path with `/api/v1/projects/{id}`. Every operation checks both import permissions and project scope. Writes to an existing job additionally require its creator or an instance administrator.

| Operation ID | Request | Result |
| --- | --- | --- |
| `create-import` | `POST /imports` | Create or recover the manifest. |
| `list-imports` | `GET /imports` | Bounded progress summaries. |
| `get-import` | `GET /imports/{import_id}` | Authoritative file offsets, counts, safe row samples, and analytical delivery status. |
| `upload-import-chunk` | `PUT /imports/{import_id}/files/{file_id}` | Upload uncompressed JSONL bytes with exact offset and checksum. |
| `complete-import-file` | `POST /imports/{import_id}/files/{file_id}/complete` | Check the file digest and process its final unterminated line. |
| `complete-import` | `POST /imports/{import_id}/complete` | Finalize after every file completes. Analytical delivery remains asynchronous. |
| `cancel-import` | `DELETE /imports/{import_id}` | Stop future acceptance and clear private partial rows. Previously accepted records remain. |

These file operations use HTTP and the CLI; they are not exposed as MCP calls. An agent with a local environment can use the standalone CLI. Project-scoped discovery through `get-event-input-contract` remains available through MCP.

## Chunk checkpoints

A chunk uses `Content-Type: application/octet-stream`, `Upload-Offset`, and `X-Chunk-SHA256`. Send at most the returned `limits.max_chunk_bytes` bytes and `limits.max_rows_per_chunk` completed rows. Current maxima are 256 KiB and 1,000 rows. A row can span chunks, up to 1 MiB.

File offsets count the exact uncompressed bytes, including line endings. Each checkpoint commits accepted rows and its offset in the same SQLite transaction. A lost response permits an identical retry of the last chunk. An offset conflict returns `409`; reload the job before continuing. Respect queue pressure responses without advancing the cursor.

The final file digest covers all uncompressed bytes. A mismatch fails the job without undoing earlier accepted rows. Cancellation also retains earlier rows. The server keeps only a bounded incomplete row and resumable hash state, then clears those buffers on completion or cancellation.

## Historical semantics and delivery

Native and Segment records pass through the same historical acceptance rules as PostHog events. They require stable producer IDs and original timestamps. Explicit historical identity evidence cannot replace live-owned profiles or conflicting links. Incremental trait updates are not replayed as current state. PostHog persons snapshots use their separate ownership and version checks. [PostHog row semantics](posthog-import-format.md)

Success means durable acceptance. `get-import` includes `delivery.enabled`, `delivery.pending`, `delivery.quarantined`, and `delivery.ready`. Pending is bounded by a stored project acceptance cursor, so continuous later ingestion does not keep an import waiting forever. It includes earlier undelivered work and quarantine. List summaries omit this detailed delivery report.

The configured analytical store determines where imported data is written: local DuckLake files or [analytical S3](ducklake-s3.md). Imports do not enqueue webhooks or enter automatic receipt-window S3 pipelines.
