# Import a PostHog file or directory

The HTTP API accepts PostHog events and persons in JSON Lines format. An agent with local file access can upload a file or enumerate a directory. It needs an API-resource token or a private [API service key](service-keys.md) with `imports:write` and `events:write` for the destination project. [Import format](posthog-import-format.md)

For each file, record its relative path and model (events or persons), decompress gzip locally, and calculate the SHA-256 digest and byte length of the uncompressed content. Keep the files unchanged until completion. Create the import with a stable idempotency key, then follow the HTTP protocol below. After interruption, fetch the job and seek to each file’s confirmed offset in its uncompressed bytes. The server never receives a local filesystem path to open.

The final JSON includes file offsets, counts, and bounded samples of row numbers with error reasons. A completed import can contain invalid or unresolved rows. Compare event totals by day and name with PostHog, and use a fresh query (`"use_cache": false`) after delivery catches up. Historical data stays queryable. Importing alone does not enqueue webhooks or send the rows to scheduled S3 query pipelines.

## HTTP upload

All paths are under `/api/v1/projects/{project_id}/imports`. Use the API audience. Every operation checks the project and both import permissions; writes to an existing job additionally require the creator or an instance administrator.

| Request | Result |
| --- | --- |
| `POST /imports` | Create or recover a job using `idempotency_key` and a `files` manifest. Each entry has a relative `path`, `kind` (`events` or `persons`), numeric `size_bytes`, and hex `sha256`. |
| `GET /imports/{id}` | Read states, counts, file IDs, processing order and committed offsets. |
| `PUT /imports/{id}/files/{file_id}` | Send raw JSON Lines bytes with `Content-Type: application/octet-stream`, `Upload-Offset`, and `X-Chunk-SHA256`. |
| `POST /imports/{id}/files/{file_id}/complete` | Process any last line and validate the byte count and checksum. |
| `POST /imports/{id}/complete` | Finalize once every file is complete. |
| `DELETE /imports/{id}` | Cancel further uploads and discard buffered partial rows. Rows already accepted remain. |

Upload files in the order returned by the server. Persons files come first. For each chunk, send at most 256 KiB and at most 1,000 line endings. Keep a stable event UUID across live capture and import and select the same logical source. A lost response can be recovered by repeating the last identical chunk or checking the job's offset. Each committed chunk advances the cursor in the same SQLite transaction as accepting its rows. A final digest mismatch fails the job; earlier committed rows are retained. An import consumes the same bounded queue as live ingestion. Retry backpressure responses without advancing the offset.

The server stages at most a partial 1 MiB row per active job. ClickHouse storage is separate: with a hot-to-S3 policy based on receipt time, historical rows initially consume hot storage and become eligible to move according to the configured policy. Budget hot space or import smaller date ranges on a constrained host.
