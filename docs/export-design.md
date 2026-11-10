# SQL export design

Exports stream a validated query into private gzip files without staging the
full result on the application host. See [the API and configuration](exports.md).

## Usage

`POST /api/v1/projects/{id}/exports` accepts the same authorized SELECT and typed
parameters as interactive queries, plus `format: jsonl|csv` and an idempotency key.
It returns a durable job. Clients poll its status and fetch paginated, temporary
links to gzip files after success. Use the operation IDs from HTTP, the standalone CLI or MCP. The MCP client discovers the contract with `porcupine_docs` and calls it with `porcupine_call`.

## Existing boundaries

Huma operation metadata enforces `events:read` and project scope and also generates
MCP discovery/dispatch. SQL validation returns one canonical SELECT. Project views
and restricted ClickHouse readers enforce RLS, public columns and mandatory FINAL.
Interactive HTTP buffers 2 MiB and its reader fixes 5 seconds/256 MiB/1,000 rows;
exports must not weaken that profile. SQLite WAL/FULL uses one connection, so no
rows or transaction may remain open across ClickHouse/S3 network work. Workers
persist creator key IDs and reload active authorization, never persist tokens.

## Shape

`exports_types.go` defines public jobs/files, private stored jobs and deployment
budgets. `exports_jobs.go` and `exports_api.go` own SQLite and API state transitions.
`exports_worker.go` owns the single worker, admission, progress, cleanup and final
publication. `export_reader.go` provisions a distinct project reader and streaming
transport; `export_stream.go` validates the complete JSONCompact envelope.
`internal/exportstore` owns official AWS SDK multipart IO and presigning.

One canonical SELECT executes once. Rows stream through bounded decoding, gzip
and sequential 8-MiB S3 multipart buffers. File rotation divides output, never SQL.
SQLite records attempt keys and multipart IDs. Earlier completed files stay private
and unavailable through the API until the entire response closes successfully,
its row footer matches, and publication commits against current creator grants,
current attempt and cancellation state. Late HTTP-200 errors fail the whole job.

Jobs record a delivery watermark for accepted original and mutation events and
wait for that prefix of the queue before execution. This is a delivery barrier,
not an injected SQL filter or transactional snapshot. Corrected data is read using
ClickHouse's execution-time semantics. Retries execute SQL afresh, never append
rows from another attempt to an old result.

## Streaming and recovery

Direct multipart streaming avoids requiring enough application-host SSD space
for the full export. Local staging would simplify retries of uploads, but would
also impose a dataset-sized disk requirement. The worker instead validates the
complete query response and fences publication against cancellation, attempt
identity, and current permissions.

File rotation bounds multipart part counts and individual downloads. A small
sequence-based delivery barrier includes already accepted queued events. The
API supports submission, listing, status, cancellation, and file links. Budgets
and S3 configuration remain deployment-only. The worker needs no separate
service or staging filesystem.

## Limits and tradeoffs

One export runs at a time and waits when ingestion is behind or delivery unhealthy.
Its separate ClickHouse reader uses one processing thread, 512 MiB memory,
5 MiB/s network/remote-read ceilings, bounded disk spill and a longer deadline.
Thread limits are not strict CPU percentage isolation. Bounded Go/S3 buffers avoid
holding the dataset on SSD or in RAM. Individual rows, total output, pending jobs,
attempts and artifact lifetime remain bounded; crossing a limit fails explicitly.

A crash can require rerunning the whole SQL query. Unknown multipart uploads in
the narrow create-before-checkpoint window require an S3 lifecycle abort rule.
Completed orphan objects are removed using their persisted attempt keys. Cleanup
is durable and retries. Already issued presigned URLs remain valid briefly after
key revocation. No query result is silently truncated and reported successful.

## Verification

Unit/race tests cover SQLite transitions, permissions, idempotency, streaming
completion, oversized values, late errors, abort/delete and publication races.
Real ClickHouse and MinIO checks exercise large results, types, aggregates,
multiple files, cancellation, restart, cold data and object integrity. Use the [load-testing guide](load-testing.md) to measure ingestion and interactive latency while an export is active.

Provisioning locks are context-aware. Export account/schema setup has a separate
five-second deadline so a slow administrative request cannot hold interactive
callers beyond their own deadline.
