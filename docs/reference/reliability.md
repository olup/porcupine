# Durability and recovery

This guide covers SQLite-journal deployments. The opt-in [hosted profile](../design/hosted-profile-composition.md) acknowledges admission to bounded memory.

In a SQLite-journal deployment, the server durably accepts events into SQLite, then projects them into DuckLake transactions over local or configured S3 data files. The storage device must honor sync operations. A single local disk provides no replication or automatic host-loss recovery.

## Acceptance and queue limits

SQLite uses WAL mode and `synchronous=FULL`. The server acknowledges capture only after the acceptance transaction commits. A batch validates and commits atomically. Queue event-count and payload-byte budgets are separate from the request-size limit. A full queue rejects new capture rather than discarding accepted work.

Retries need a stable PostHog UUID or native/Segment producer message ID in the same project and source. A repeat returns the original event ID and preserves the original accepted payload. Without a stable producer ID, separate submissions can create separate events. [Receipt contract](native-and-segment-ingestion.md#receipts-and-logical-sources)

A control-storage failure while validating a credential returns retryable `503` with `Retry-After`, before acceptance writes. API, MCP, native ingestion, PostHog, and Segment preserve that distinction. A genuinely invalid, expired, or revoked credential returns `401`. Retry the same stable-ID batch after service recovery; do not replace its IDs.

## Analytical delivery

The worker projects bounded batches into DuckLake. A transaction maintains current events and monotonic revision heads. Only after that transaction commits does SQLite atomically record receipts, remove delivered queue payloads, and record delivery success.

A process can stop after the analytical commit and before SQLite acknowledgment. The queued batch retries after restart with the same IDs and revisions. Projection rejects stale revisions and preserves one logical current event. This is at-least-once processing with idempotent projection, not a distributed exactly-once transaction.

SQLite-journal deployments keep project-and-UUID receipts indefinitely. A fresh SQLite-journal deployment can therefore accept new events and deduplicate retries during an analytical outage. Optional receipt pruning introduces an analytical dependency. See the persistent pruning marker in [receipt retention](receipts.md#bounded-receipts). Delivered payloads leave the SQLite queue, but freed database pages do not automatically shrink the file. Receipts, revision heads, catalog snapshots, and history have separate storage costs.

The server schedules native DuckLake flush and compaction operations. DuckLake owns file rewriting and transactional visibility. Maintenance still consumes CPU, memory, disk bandwidth, and catalog work. Deployment sizing must cover ingestion, queries, and maintenance together. [Maintenance measurements](../design/ducklake-maintenance.md)

Porcupine leases only the project currently undergoing maintenance. Projects waiting for their turn remain evictable, and maintenance skips candidates evicted or deleted in the meantime. This prevents a maintenance pass from occupying every resident-engine slot and rejecting an otherwise admissible cold-project query. These changes are not in v0.12.1.

## Identity, edits, and deletion

Supported identity controls commit authoritative SQLite changes and their queued projections in the capture transaction. Analytical views resolve project-local identities and profile versions after projection. A capture acknowledgment can precede identity visibility in SQL. [Identity contract](../data-and-identity.md)

Edits and deletions commit a revision head, audit metadata, and pending projection before returning `202`. Revision checks prevent lost updates, and each event can have one pending mutation. The bounded mutation outbox retains its exact payload until analytical acknowledgment, then removes it in the transaction that marks the audit applied. Audit metadata retains no before-and-after values.

A deleted event returns `410` from the current-event API after acceptance. SQL excludes it after its tombstone projects. A late original or stale correction cannot resurrect it. Tombstones remain for retry protection. This is logical deletion: historical DuckLake snapshots, SQLite free pages or WAL, backups, and external copies can retain old payloads. Porcupine schedules snapshot expiration and obsolete-file cleanup, but does not promise physical erasure within 30 days. [Mutation API](../events.md) · [Storage](../storage.md)

## Whole-project deletion

Instance administrators can delete a project through HTTP, CLI, or MCP. Porcupine cancels its active work, removes its SQLite state, and checkpoints cleanup of its managed analytical directory and export objects. A storage outage can leave cleanup pending after the project disappears from listings. Startup and periodic work retry cleanup. Repeating deletion returns `204` once cleanup finishes. Other projects remain separate.

Backups and copies already delivered to external destinations have their own retention. [Project management](projects.md)

## Status and recovery

`/healthz` checks process liveness. `/readyz` checks SQLite readiness. Neither proves every project catalog is healthy or delivery is caught up. Inspect [instance status](instance-status.md), the project's pending original-event age, pending mutations, and generic delivery error. Use synthetic capture and SQL checks when validating a deployment.

Back up the complete SQLite and DuckLake state from one stopped writer. Both stores are authoritative for different parts of recovery; neither reconstructs the other. Keep the same internal paths, software version, configuration, and encryption keys. A live file copy or export of query results is not a consistent instance backup. [Stopped-volume procedure](restore.md)

Manual full-volume restoration also restores credentials and destination state. Isolate the recovered instance, pause destination egress, reconcile later revocations and deletions, and verify the checkpoint before allowing clients. There is no server-side restore API. [Coordinated native backups and offline recovery](native-backup-recovery.md) provide a checkpoint and fresh-volume recovery workflow.

## Destination delivery

Webhook enqueueing commits with event acceptance. A lost receiver acknowledgment can redeliver the same stable delivery ID, so receivers must deduplicate before applying side effects. Batch retries preserve each attempted payload.

Scheduled S3 queries publish a conditional manifest after every output file succeeds. SQLite advances the window cursor only after reading that manifest back. Interrupted attempts can rerun a whole query. A conflicting commit marker fails closed. Successful files are independent external copies and do not change after source edits or deletions. [Pipeline contract](../pipelines.md)

## Verification

`make test-race` exercises real native storage, query restrictions, HTTP and MCP journeys, identity, revisions, retries, project isolation, cancellation, and restart under the race detector. The stopped-volume restore test covers matching paths and queued work. `make ducklake-smoke` runs the compiled native server and standalone CLI. Capacity measurements and their limits are linked from the [DuckLake validation report](../design/ducklake-validation.md).

`make check` exercises the native Go contracts. Native race, S3, compiled CLI, SDK compatibility, and restore checks cover their respective boundaries. The server requires a C/C++ toolchain for DuckDB; the standalone CLI remains CGO-free.
