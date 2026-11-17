# SQL export design

> Historical record: this page documents a past design or validation and is not a guide to the current release.

Exports stream one validated DuckDB query into private gzip files. They reuse project isolation and the native SQL policy while keeping a separate durable job lifecycle. See [the API and configuration](../exports.md) for requests and operator settings.

## Query and publication boundaries

The worker waits for accepted original and mutation work through its recorded watermarks. This is a delivery barrier, not an injected SQL filter or a transaction spanning SQLite and DuckLake. Execution reads the analytical state available to its native connection.

One canonical query runs once per attempt. File rotation splits output files rather than dividing SQL into pages. Rows pass through bounded row encoding, gzip, and sequential S3 multipart buffers. The full result is not staged on the application host.

SQLite stores job, attempt, file, and multipart metadata. The worker does not hold a SQLite transaction while executing the native query or performing remote uploads. Every file remains private until the attempt finishes and publication rechecks its current identity, cancellation state, and creator authority.

Scheduled pipelines additionally restrict the connection's temporary `events` view to their closed receipt-time window and exclude historical imports. Cleanup removes that view before returning the connection. A failed cleanup discards the connection rather than exposing the restricted or stale view to another caller.

## Recovery and cancellation

A failed or interrupted attempt cleans up its private objects and reruns the whole SQL query. It never appends rows from another attempt. Native cancellation uses the running query context. Publication remains fenced even if cancellation races a query or upload already in progress.

Unknown multipart uploads in the create-before-checkpoint interval need an S3 lifecycle abort rule. Persisted attempt keys support later object cleanup. An issued presigned URL can remain valid briefly after a credential is revoked. Successful downloads and customer copies remain external effects.

## Resource scope

One background analytical query runs at a time, shared by one-off exports and scheduled pipelines. Admission also considers queue pressure and delivery health. Interactive queries keep their separate admission pool.

The worker enforces output, row, file, deadline, sink-bandwidth, pending-job, and retention budgets. Query execution shares the project's engine memory setting with its other work. This is not a per-export memory reservation or a hard process RSS limit. Physical scan quotas and per-query temporary-disk quotas are not implemented.

Large aggregations can consume compute before producing their first output row. A longer export deadline does not provide CPU isolation from ingestion. Size the deployment and container budget for the whole workload.

## Verification

Native tests cover project isolation, parameter binding, connection cleanup, streaming, cancellation, current revisions, delivery windows, permission revocation before publication, and restart. Object-storage tests cover private file integrity and cleanup. The [load-testing guide](load-testing.md) describes workload measurement; its results are specific to the tested deployment.
