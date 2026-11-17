# Event IDs and durable receipts

Porcupine's SQLite-journal mode keeps a minimal local receipt for each delivered event. Receipt retention defaults to `0`, which keeps every receipt, as the published v0.14.0 release does. New events and retries on a fresh installation therefore do not require DuckLake to be available at acceptance. Queue capacity, ingestion permissions, and SQLite durability still apply.

Journal deployments also support optional age-based pruning through `queue.receipt_retention` or `PORCUPINE_RECEIPT_RETENTION`. Leave it unset or use `"0"` for local-only receipt checks. A positive duration such as `"720h"` opts into pruning after 30 days and the analytical dependency described below. The duration must be at least `24h`. Queue payloads and receipts have separate storage lifecycles.

For native and Segment inputs, a stable producer message ID identifies one message within its authenticated project and logical source. A retry returns the original server event ID and retains the first accepted payload. Unbound inputs have separate source namespaces. Supported PostHog inputs use their UUID namespace unless the credential binds an explicit logical source. See [producer IDs and source namespaces](native-event-contract.md#ids-and-retry-semantics).

## Before and after delivery

Before delivery, the accepted original in SQLite prevents duplicate acceptance. Product identity changes and any webhook work commit with that original. The server acknowledges only after the transaction commits.

A worker applies the original to DuckLake, then records its receipt and removes the queued payload in one SQLite transaction. A failure between the analytical commit and SQLite acknowledgment leaves the original available for replay. Native revision and event-ID checks make that replay converge on one logical event.

A duplicate cannot change properties, update traits a second time, enqueue another webhook, or acquire a new numeric ID. Corrections use the [event management API](../events.md).

## Bounded receipts

Pruning is disabled by default. Enabling it trades local receipt growth for an analytical lookup before future acceptance in that project.

The maintenance worker checks each candidate against the project's DuckLake heads before pruning it. Pending queue payloads and mutations keep their receipts. Each project's durable marker and receipt deletion commit in the same SQLite transaction. The worker rechecks candidates after the analytical lookup. Project gates serialize lookups and pruning within one project; remote reads never hold the instance-wide policy/commit lock. Each maintenance batch has a five-second deadline.

Once a project has pruned any receipts, live ingestion and historical import chunks check producer IDs against DuckLake before acceptance. Producer timestamps cannot select this check: a retry may omit or change its timestamp while keeping its message ID. A failed analytical lookup rejects the whole batch or import chunk for retry. Projects that have never pruned receipts retain the SQLite-only acceptance path. Without DuckLake, receipts are never pruned.

The pruning marker survives restart and setting retention back to `0`. Zero stops future pruning but cannot recreate deleted receipts or remove the existing analytical dependency. The default is a guarantee for fresh installations and projects that have never pruned, not a repair of already-pruned history.

## Deletion and recovery

Mutation heads and payload-free analytical tombstones retain the facts needed to reject stale originals. Deleting an event does not make its producer ID available for reuse. Deleting the entire project removes its receipts and managed analytical history.

Back up SQLite with the matching DuckLake catalogs and data files. The analytical archive cannot reconstruct credentials, accepted pending work, or the complete receipt ledger. [Backups and recovery](native-backup-recovery.md)

With retention `0`, receipts consume SQLite space as the number of unique events grows. Pruned and delivered rows free pages for reuse; the database file does not automatically shrink. Do not delete receipts by hand: use the retention setting, which is paired with the DuckLake check.
