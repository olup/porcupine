# Durability and recovery

Porcupine durably accepts events into SQLite, then delivers them to ClickHouse at least once. This contract depends on the deployment's storage honoring sync operations. A single local disk provides no replication or host-loss recovery.

## Acceptance and queue limits

SQLite uses WAL mode and `synchronous=FULL`. A successful ingestion response follows the transaction commit. A batch validates and commits atomically. The original-event queue enforces both count and byte budgets. Requests have a separate 1 MiB limit. A full queue returns an explicit failure rather than discarding accepted events.

A stable project and UUID keeps the first accepted event. A retry with different content cannot overwrite it. Missing UUIDs receive a generated ID, so a client retry without a stable UUID cannot deduplicate reliably.

## Delivery and acknowledgments

The worker selects at most 100 queued events per batch. Its estimated encoding budget is 4 MiB, with a single large event allowed to exceed that soft bound so size alone cannot block the queue. It sends synchronous `JSONEachRow` inserts with asynchronous insertion disabled. Newly created ClickHouse tables enable `fsync_after_insert` and `fsync_part_directory`. Startup uses `CREATE TABLE IF NOT EXISTS`; it does not alter an existing table to add those settings. Operators reusing a table must check its settings.

Only after ClickHouse acknowledgment does a SQLite transaction insert delivery receipts, delete payloads, and record project delivery success. A failure before that transaction commits leaves the payloads available for retry. The worker retries errors with exponential backoff from one to 30 seconds.

A process can fail after ClickHouse accepts a batch but before SQLite records completion. The retry can create duplicate physical rows. This is at-least-once delivery, not exactly once.

The ClickHouse `events_v2` table uses `ReplacingMergeTree(revision,is_deleted)`,
ordered by project and UUID and partitioned by the UTC month of original receipt.
Originals use revision zero. Accepted corrections and deletions use positive
SQLite audit sequence numbers, so higher revisions win. Project views apply
`FINAL` and readers enforce final reads, making current-state selection
independent of merge timing. Retries preserve revision, event ID, receipt time,
and partition. A late original cannot replace a newer correction or tombstone.

By default SQLite keeps a minimal project-and-UUID receipt forever after delivery. [Bounded receipt mode](receipts.md) lets the operator cap this ledger while preserving exact duplicate recognition through authoritative ClickHouse history. Deleted payloads free SQLite pages for reuse; the database file does not automatically shrink. There is no receipt expiry or automatic history deletion. Optional S3 tiering
uses a MOVE TTL, never a DELETE TTL.

## Quarantined original events

The original-event worker isolates two narrowly classified ClickHouse failures:
`321` (`VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`) and `469`
(`VIOLATED_CONSTRAINT`). These are row-value and row-constraint errors in the
[ClickHouse error-code definitions](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp).
Authentication, schema, general parsing, memory, network, unknown acknowledgment,
and storage failures remain retryable worker errors. A matching code on an HTTP
rate-limit or unavailable response is not enough to quarantine data.

After a classified batch failure, the worker splits the batch and retries its
parts. A singleton must fail twice with the same classified code before entering
quarantine. A successful part follows the normal receipt-and-delete transaction.
Isolation uses at most 32 insert attempts per selected batch, including the first
attempt, and a ten-second deadline after the first classified rejection. Work
left after that budget remains accepted and eligible for a later cycle.

Quarantine adds metadata without moving or changing the payload. The `events`
row retains its original ID, project, UUID, properties, and receipt time.
`delivery_quarantine` stores only the event ID, numeric error code, and quarantine
time. It does not store server messages or rejected values. A trigger-maintained
`delivery_ready` index excludes quarantined rows from automatic retries, including
after restart. Startup backfills this index for older databases without deleting
accepted history.

A durable scan cursor rotates past failed batches, including unclassified failures,
so a bad prefix cannot indefinitely hide later healthy events in the same project
or another project. Ordinary selection uses the ready-ID index rather than a scan
through quarantined payloads. A mutation failure no longer suppresses the original
delivery attempt. When originals progress despite that failure, the next cycle
waits one second. Mutation-to-mutation isolation remains unsupported: one rejected
mutation can still delay other queued mutations.

Quarantined originals remain included in `pending`, ingestion deduplication,
queue count and byte limits, and export delivery barriers. Status also reports a
project-scoped `quarantined` count and a generic repair message. Healthy projects
can drain their accepted originals, but a full shared queue still rejects new
ingestion. Quarantine does not create an unlimited second payload store or silently
free capacity. Operators must resolve retained records when they fill the queue.

An acknowledged correction preserves the original evidence in its private
ClickHouse snapshot, then atomically receipts and removes a quarantined original.
An accepted deletion removes the queued original through the existing deletion
transaction. Both paths clear derived quarantine/index metadata. Stable positive
revisions prevent a late revision-zero delivery from replacing either result.

### Inspect and retry a retained original

First diagnose and fix the rejecting constraint or deployment configuration. If
the accepted event itself needs correction or deletion, use the
[event mutation API](events.md), which preserves its audit and revision contract.
Do not edit accepted payload columns or remove their deduplication receipts.

Use `list-quarantined-events` through HTTP or MCP to inspect safe metadata. The
HTTP operation requires `events:read` and returns event IDs, UUIDs, receipt and
quarantine times, and numeric error codes. It omits payloads and server messages.
Pagination uses `after_event_id` and a `limit` from 1 through 1,000. Event IDs are
decimal strings so clients do not lose integer precision.

```sh
curl --fail-with-body \
  "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/quarantined-events?limit=100" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

After fixing the cause, use `retry-quarantined-event`, which requires
`events:edit` for that project. Set `EVENT_ID` to an ID returned by the list:

```sh
curl --fail-with-body --request POST \
  "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/quarantined-events/$EVENT_ID/retry" \
  -H "Authorization: Bearer $ACCESS_TOKEN"
```

Use an OAuth access token for the API resource with the required project grants.
The `202` response contains `event_id` and `requeued`. `requeued: true` means the
retained original became eligible again, not that delivery succeeded. Repeating
the request while that original is still queued returns `requeued: false`; an
unknown or already acknowledged event returns `404`. A different project's event
is also `404` after the caller passes the route's project authorization.

The same retry through `porcupine_call` is:

```json
{
  "operation_id": "retry-quarantined-event",
  "path": {"id": "PROJECT_ID", "event_id": "EVENT_ID"}
}
```

Check that project's `pending` and `quarantined` counts after the retry. A persistent
classified rejection quarantines the same original again. A network failure
retains it for normal retry. Successful delivery frees its payload capacity and
preserves its UUID for deduplication. Retrying does not change or delete the payload.

## Mutation acceptance and cleanup

[Corrections and deletions](events.md) commit a mutation head, audit metadata, and
projection payload to SQLite before returning `202`. Acceptance uses expected
revision checks and allows one pending mutation per event. The outbox is bounded
by the smaller of the queue event limit and 1,000 entries, plus 64 MiB of payloads.

The worker retries the exact accepted projection. After ClickHouse acknowledgment,
one SQLite transaction deletes the outbox payload, marks audit state applied,
and clears the pending head. Audit metadata retains no before-and-after payloads.
Private original snapshots survive corrections in ClickHouse. Deletion's newest
projection blanks both original and current payloads and retains a tombstone.

Deleting a queued original atomically removes its payload and adds a receipt.
Late revision-zero inserts cannot resurrect the event. Tombstones are retained
indefinitely and `FINAL CLEANUP` is not used. Old parts, S3 versions, SQLite WAL
and free pages, and backups clean up separately. Logical deletion is not an
immediate physical-erasure guarantee.

## Visibility and delivery status

Schema, catalog, and query responses include project delivery state: `enabled`, `pending`, `quarantined`, `pending_mutations`, optional `last_success`, and an
optional generic worker error. The error appears only while the requested project
has pending original events or mutations. It does not expose another project's error details or queue count.

Delivery is eventual. An event can already be queryable after a lost acknowledgment while still counted as pending. A query and its delivery status use separate systems without a shared snapshot. `last_success` records acknowledged delivery for the project; it does not prove that the queue is empty.

`/healthz` checks the process and `/readyz` checks SQLite. Neither proves that ClickHouse is healthy or that delivery is caught up. Queue age, rejected request counts, and richer delivery metrics remain operational work.

## Storage and backups

SQLite contains authoritative credentials, projects, daily cookieless keys, the query-reader derivation secret, receipts, mutation heads and audit, pending payloads, and delivery metadata. ClickHouse facts cannot reconstruct that state. Back up SQLite through a supported online backup method or a coordinated stopped copy. Copying only the database file while WAL writes continue is not a complete backup method.

ClickHouse needs its own backup and restore plan for delivered history. Restore must preserve a consistent recovery point across both stores: restoring old queue state can retry deliveries, while losing ClickHouse facts after their SQLite payloads were deleted requires a ClickHouse backup. The [restore drill](restore.md) exercises a coordinated stopped copy of SQLite, ClickHouse and native cold objects on disposable storage. For a running instance use the [online checkpoint and restore tool](online-backup.md). The operator schedules checkpoints and manages off-host copies.

Native S3 cold storage, project delivery pipelines, and instance backups have
different purposes. Cold parts still depend on persistent ClickHouse disk
metadata and configuration. S3 objects alone do not establish a restore
procedure, and neither cold storage nor an export is a SQLite backup.
See [storage](storage.md) for migration, MOVE TTL changes, and recovery boundaries.

## Verification

`make integration-s3` adds isolated native ClickHouse S3 storage with MinIO and
mutation tests across hot and cold parts. It is opt-in and does not contact AWS.
The [storage reference](storage.md) describes the deployment boundaries.

`make integration` starts an isolated ClickHouse 26.8.36.3 container and runs the opt-in real-storage tests with the Go race detector. The script removes its own container afterward. The suite exercises analytical HTTP and real Go SDK MCP calls, schema and catalog discovery, UUID pointers, direct row-policy bypass attempts, read-only enforcement, quotas, outages, queue backpressure, and restart.

A lost-acknowledgment test stops background merges, delivers twice, and checks two physical rows against one logical row. This verifies deduplication before merges. Normal `make check` skips real ClickHouse tests unless their test environment is configured.

A test forces SQLite to return SQLITE_FULL and checks batch rollback, preservation of previously accepted events, capacity counters and recovery on retry.
Further release work includes operating-system ENOSPC injection, additional safely classified
row failures, mutation isolation, large historical backup validation, and arbitrary partial delivery
failures. Future identity projections need race and out-of-order replay tests.
Pipeline failure and receiver-idempotence checks are described in
[the delivery contract](pipelines.md). Benchmarks require
recorded hardware, workload, event sizes, and durability settings before publication.

## Pipelines and identity

[Webhook and scheduled S3 pipelines](pipelines.md) persist queued work and
checkpoints in SQLite. Webhook capture and enqueueing commit together. A lost
receiver acknowledgment can redeliver the same stable ID. Batch mode retries
the whole HTTP chunk; receivers must deduplicate before their own side effects.

Scheduled S3 runs publish a conditional manifest after validating every output
file and the full query result. SQLite advances its window cursor only after
reading that manifest back. An interrupted attempt can retry a whole query; an
unexpected or conflicting commit marker fails closed. This remains an
at-least-once external-delivery contract rather than a distributed transaction.

Supported restores pause every destination. Review the receiver or bucket,
creator grants, restored checkpoints, and effects that may already exist before
resuming. Preserve the original pipeline encryption key outside the backup.
Follow [online recovery](online-backup.md) for the outbound-delivery fence.

Identity controls commit the authoritative SQLite changes and their queued projections atomically. Stable event UUIDs deduplicate capture retries. ClickHouse uses accepted versions to resolve current profiles despite replay or out-of-order delivery.
