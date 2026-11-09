# Import a PostHog file or directory

The import API and CLI in this source build accept PostHog event and person exports in JSON Lines format. They run on the existing Porcupine server. No public file URL, shared filesystem, or bucket is required. See [PostHog export fields and identity behavior](posthog-import-format.md).

## Upload from your computer

1. Export events as JSON Lines from PostHog. Include the persons model if you need profile data. Download the files to your computer. Keep the original event UUIDs and timestamps. [PostHog S3 export models](https://posthog.com/docs/data-pipelines/batch-exports/s3)
2. Give a technical client `imports:write` and `events:write` for the destination project. Set `PORCUPINE_URL`, `PORCUPINE_CLIENT_ID` and `PORCUPINE_CLIENT_SECRET` in the CLI environment. The CLI exchanges the credentials for API access tokens and renews them as needed. Alternatively, set `PORCUPINE_ACCESS_TOKEN` to an API-resource token, or a suitably authorized management key in a legacy deployment. Keep secrets out of command arguments.
3. Build the CLI from the same source as the server and run it on the local path:

```sh
go build -o ./porcupine ./cmd/porcupine
./porcupine import posthog ./export --project "$PROJECT_ID"
```

A single file such as `events.jsonl.gz` works in place of the directory. The CLI recognizes `.jsonl`, `.ndjson`, `.jsonlines` and line-oriented `.json`, optionally compressed with gzip. A directory is traversed recursively; hidden entries are skipped and symbolic links are rejected. The CLI infers persons versus events from PostHog fields or a `persons` path component. Use `--kind persons` or `--kind events` to force the model for a homogeneous input. For CSV, Parquet or zstd exports, convert to JSON Lines or request a JSON Lines export first.

The CLI prints the import ID to stderr. Rerun the same command with the same files after an interruption, or provide `--resume IMPORT_ID`. The CLI verifies the manifest, reads the server's confirmed offsets, and continues. Gzip offsets and checksums refer to the **decompressed** bytes. Resuming a gzip file can reread its local prefix. Keep source files unchanged for the whole import.

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
