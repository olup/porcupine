# Durability and recovery

In standalone, the server accepts events into SQLite, then projects them into DuckLake. The storage device must honor sync operations. A single local disk provides no replication or automatic host-loss recovery. See [storage and acknowledgment by deployment profile](../storage.md#deployment-profiles).

## Acceptance and queue limits

SQLite uses WAL mode and `synchronous=FULL`. The server acknowledges capture after the acceptance transaction commits. A batch validates and commits atomically. The standalone queue defaults to 100,000 pending original events and 256 MiB of charged payload. Queue budgets are separate from the request-size limit and total disk space. A full queue rejects new capture and preserves accepted work.

Retries need a stable PostHog UUID or native/Segment producer message ID in the same project and source. A repeat returns the original event ID and preserves the original accepted payload. Without a stable producer ID, separate submissions can create separate events. [Receipt contract](native-and-segment-ingestion.md#receipts-and-logical-sources)

A control-storage failure while validating a credential returns retryable `503` with `Retry-After`, before acceptance writes. API, MCP, native ingestion, PostHog, and Segment preserve that distinction. A genuinely invalid, expired, or revoked credential returns `401`. Retry the same stable-ID batch after service recovery; do not replace its IDs.

## Analytical delivery

The worker projects bounded batches into DuckLake. A transaction maintains current events and monotonic revision heads. Only after that transaction commits does SQLite atomically record receipts, remove delivered queue payloads, and record delivery success.

A process can stop after the analytical commit and before SQLite acknowledgment. The queued batch retries after restart with the same IDs and revisions. Projection rejects stale revisions and preserves one logical current event. This is at-least-once processing with idempotent projection, not a distributed exactly-once transaction.

A project that has never pruned receipts can accept new events and deduplicate retries during an analytical outage. See [receipt retention](receipts.md#bounded-receipts). SQLite can reuse freed queue pages without shrinking the database file.

The server schedules native DuckLake flush and compaction operations. DuckLake owns file rewriting and transactional visibility. Maintenance still consumes CPU, memory, disk bandwidth, and catalog work. Deployment sizing must cover ingestion, queries, and maintenance together. [Maintenance measurements](../design/ducklake-maintenance.md)

Porcupine leases only the project currently undergoing maintenance. Projects waiting for their turn remain evictable, and maintenance skips candidates evicted or deleted in the meantime. This prevents a maintenance pass from occupying every resident-engine slot and rejecting an otherwise admissible cold-project query.

## Identity, edits, and deletion

Supported identity controls commit authoritative SQLite changes and their queued projections in the capture transaction. Analytical views resolve project-local identities and profile versions after projection. A capture acknowledgment can precede identity visibility in SQL. [Identity contract](../data-and-identity.md)

Edits and deletions commit a revision head, audit metadata, and pending projection before returning `202`. Revision checks prevent lost updates, and each event can have one pending mutation. The bounded mutation outbox retains its exact payload until analytical acknowledgment, then removes it in the transaction that marks the audit applied. Audit metadata retains no before-and-after values.

A deleted event returns `410` from the current-event API after acceptance. SQL excludes it after its tombstone projects. The tombstone prevents a late original or stale correction from resurrecting it. See the [mutation API](../events.md) and [retention of deleted payloads](../storage.md#retention-and-deletion).

## Whole-project deletion

Porcupine removes the project's SQLite state and checkpoints cleanup of its managed analytical directory and export objects. A storage outage can leave cleanup pending after the project disappears from listings. See [project deletion permissions and retries](projects.md#delete-a-project).

## Status and recovery

Use [instance status and the delivery checks](instance-status.md#verify-delivery-before-routing-traffic) when validating a deployment.

Keep SQLite and DuckLake from the same recovery point. SQLite holds credentials and accepted work, while DuckLake holds the analytical archive. See the [backup and restore guide](../backup-and-restore.md) for recovery methods and isolation steps.

## Destination delivery

Webhook enqueueing commits with event acceptance. A lost receiver acknowledgment can redeliver the same stable delivery ID, so receivers must deduplicate before applying side effects. Batch retries preserve each attempted payload.

Scheduled S3 queries publish a conditional manifest after every output file succeeds. SQLite advances the window cursor only after reading that manifest back. Interrupted attempts can rerun a whole query. A conflicting commit marker fails closed. Successful files are independent external copies and do not change after source edits or deletions. [Pipeline contract](../pipelines.md)

## Verification

`make test-race` exercises real native storage, query restrictions, HTTP and MCP journeys, identity, revisions, retries, project isolation, cancellation, and restart under the race detector. The stopped-volume restore test covers matching paths and queued work. `make ducklake-smoke` runs the compiled native server and standalone CLI. Capacity measurements and their limits are linked from the [DuckLake validation report](../design/ducklake-validation.md).

`make check` exercises the native Go contracts. Native race, S3, compiled CLI, SDK compatibility, and restore checks cover their respective boundaries. The server requires a C/C++ toolchain for DuckDB; the standalone CLI remains CGO-free.
