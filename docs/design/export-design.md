# SQL export design

> Implementation reference for the standalone export domain and the unreleased hosted provider. See [the export guide](../exports.md) for user operations.

Exports stream one validated DuckDB query into private gzip files. They reuse project isolation and the native SQL policy while keeping a separate durable job lifecycle. See [the API and configuration](../exports.md) for requests and operator settings.

## Module ownership

`internal/output.ExportJobs` owns the complete SQLite export lifecycle. The service constructs it through `NewSQLiteExports` and starts its worker. The HTTP and MCP handlers retain route registration, project authorization, and request validation. The domain invokes native SQL validation when it admits the job. They call the same domain operations for submission, status, pagination, cancellation, and download links.

The domain stores creator-scoped idempotency, delivery watermarks, claims, retry outcomes, progress, multipart checkpoints, publication, retention, and cleanup. `RunOne` performs a bounded worker cycle. Successful publication and `usage.Recorder.AddTx` share the same SQLite transaction and the same winning attempt predicate.

`ExportDependencies` supplies the journal and control databases, clock, usage recorder, external issuer, and a native `ExportReader`. Its function fields check current credential or caller authority, active project and shared owner fences, project activity, and shared analytical admission. These functions do not update export jobs. The native reader retains its query permit until `Export` returns, and the domain retains local admission and project activity until execution returns.

The domain derives finite external execution metadata from the current verified caller. It retains the original tenant, issuer, and expiry without storing the token. Reusing an idempotency key cannot renew this metadata. Download signing separately uses the current caller's expiry and the retained object's deadline.

Project deletion calls `StageExportProjectCleanupTx` inside the journal removal transaction. It copies namespace and upload ownership before deleting export rows. `CleanupProject` retains the control inventory before remote deletion and checkpoint removal. Offline restore calls `ExportRestoreArtifacts` before reset, then `ResetRestoredExportsTx` after preserving source artifact holds. Reset does not contact the source destination.

## Hosted export provider

The staged PostgreSQL provider uses `NewPostgresExports` and stores job state, finite external authority, upload checkpoints, and successful usage in shared control. Admission takes a durable activity ticket before touching the analytical catalog. The hosted startup adapter supplies a project write session and settles accepted mutations. The provider captures the current DuckLake catalog identity and version within that session and commits the job on its PostgreSQL connection. Every retry uses the same version for events and identity tables. Hosted compaction preserves snapshot history used by queued and running jobs.

A worker claims a job with a renewable PostgreSQL lease and a unique attempt ID. Expired claims become eligible for cleanup and a new attempt. The prior worker keeps its activity ticket until its native and upload calls return. Artifact writes, progress, and publication verify the current attempt. Publication rechecks project and creator authority, and posts export usage in the same transaction as success.

The configured pending-job cap applies across the hosted instance. Up to 100 jobs per project, 400 per tenant, and 1,000 per instance can hold capacity for work or S3 artifacts. Queued and running jobs, available downloads, and pending cleanup all count. Total job metadata is capped at 1,000 per project, 2,000 per tenant, and 10,000 per instance. Capacity refusal preserves the existing ownership records. A terminal job remains available for idempotent replay for 30 days after its artifact cleanup completes. Successful jobs first complete their configured download retention. Once retirement is due, the worker removes job metadata only after cleanup has cleared the file checkpoints. The managed ownership records used for project purge remain durable.

## Query and publication boundaries

For a journal-backed job, the worker waits for accepted original and mutation work through its recorded watermarks. This is a delivery barrier, not an injected SQL filter or a transaction spanning SQLite and DuckLake. Execution reads the analytical state available to its native connection.

One canonical query runs once per attempt. File rotation splits output files rather than dividing SQL into pages. Rows pass through bounded row encoding, gzip, and sequential S3 multipart buffers. The full result is not staged on the application host.

SQLite stores job, attempt, file, and multipart metadata. The worker does not hold a SQLite transaction while executing the native query or performing remote uploads. Every file remains private until the attempt finishes and publication rechecks its current identity, cancellation state, and creator authority.

Scheduled pipelines additionally restrict the connection's temporary `events` view to their closed receipt-time window and exclude historical imports. Cleanup removes that view before returning the connection. A failed cleanup discards the connection rather than exposing the restricted or stale view to another caller.

## Recovery and cancellation

A failed or interrupted attempt cleans up its private objects and reruns the whole SQL query. It never appends rows from another attempt. Native cancellation uses the running query context. Publication remains fenced even if cancellation races a query or upload already in progress.

Unknown multipart uploads in the create-before-checkpoint interval need an S3 lifecycle abort rule. Persisted attempt keys support later object cleanup. An issued presigned URL can remain valid briefly after a credential is revoked. Successful downloads and customer copies remain external effects.

## Resource scope

Each worker runs one background analytical query at a time, shared by one-off exports and scheduled pipelines. Journal-backed admission also considers queue pressure and delivery health. Interactive queries keep their separate admission pool.

The worker enforces output, row, file, deadline, sink-bandwidth, pending-job, and retention budgets. Query execution shares the project's engine memory setting with its other work. This is not a per-export memory reservation or a hard process RSS limit. Physical scan quotas and per-query temporary-disk quotas are not implemented.

Large aggregations can consume compute before producing their first output row. A longer export deadline does not provide CPU isolation from ingestion. Size the deployment and container budget for the whole workload.

## Verification

Native tests cover project isolation, parameter binding, connection cleanup, streaming, cancellation, current revisions, delivery windows, permission revocation before publication, and restart. Object-storage tests cover private file integrity and cleanup. The [load-testing guide](load-testing.md) describes workload measurement; its results are specific to the tested deployment.

## Extraction map

| Original source | Current owner |
| --- | --- |
| `internal/porcupine/exports_types.go` and `exports_config.go` | `internal/output/export_types.go` and `export_config.go` |
| Job persistence and finite authority in `exports_jobs.go` | `internal/output/export_sqlite.go` |
| API-independent operations in `exports_api.go` | `internal/output/exports.go` |
| `exports_worker.go` and `exports_sink.go` | `internal/output/export_worker.go` and `export_sink.go` |
| `project_export_cleanup.go` and export row deletion in `project_purge_sqlite.go` | `internal/output/export_cleanup.go` |
| Export artifact enumeration and reset in `native_restore_artifacts.go` and `native_restore.go` | `internal/output/export_artifacts.go` |
| Export construction and worker lifecycle in `service.go` | `internal/porcupine/output_dependencies.go` wires `ExportJobs`; the domain owns the worker and storage lifecycle |

The sink, worker, storage-binding, and project cleanup tests moved with their implementation. Their isolated fixture uses real SQLite, the journal's baseline schema, a real DuckLake reader, and a local S3 protocol fixture. Service integration tests retain HTTP and MCP authorization, actor-scoped idempotency, queue limits, external expiry, download pagination, and native reader coverage. Domain tests also verify usage rollback and publication replay, plus durable cancellation before signaling the active attempt.
