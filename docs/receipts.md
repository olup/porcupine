# Event IDs and durable receipts

Porcupine keeps a minimal SQLite receipt for each delivered event. Unreleased: receipts older than `PORCUPINE_RECEIPT_RETENTION` (default `720h`, 30 days) are pruned hourly; DuckLake keeps the identity of every projected event. Set it to `0` to keep every receipt, as v0.14.0 does. Queue payloads and analytical data have separate storage lifecycles.

For native and Segment inputs, a stable producer message ID identifies one message within its authenticated project and logical source. A retry returns the original server event ID and retains the first accepted payload. Unbound inputs have separate source namespaces. Supported PostHog inputs use their UUID namespace unless the credential binds an explicit logical source. See [producer IDs and source namespaces](native-event-contract.md#ids-and-retry-semantics).

## Before and after delivery

Before delivery, the accepted original in SQLite prevents duplicate acceptance. Product identity changes and any webhook work commit with that original. The server acknowledges only after the transaction commits.

A worker applies the original to DuckLake, then records its receipt and removes the queued payload in one SQLite transaction. A failure between the analytical commit and SQLite acknowledgment leaves the original available for replay. Native revision and event-ID checks make that replay converge on one logical event.

A duplicate cannot change properties, update traits a second time, enqueue another webhook, or acquire a new numeric ID. Corrections use the [event management API](events.md).

## Bounded receipts

Retries normally arrive within minutes. Once a receipt is pruned, an incoming event whose occurrence time is older than the retention window minus seven days is checked against the project's DuckLake event heads before acceptance. A match is a duplicate and returns the original event ID. Historical imports use the same check for each chunk. The seven-day margin covers clients whose clocks run ahead.

A retry whose occurrence time is recent but whose original was received more than the retention window earlier would not be recognized. That requires a client clock ahead by more than seven days; keep `0` if your producers cannot rule it out. Without DuckLake, receipts are never pruned.

## Deletion and recovery

Mutation heads and payload-free analytical tombstones retain the facts needed to reject stale originals. Deleting an event does not make its producer ID available for reuse. Deleting the entire project removes its receipts and managed analytical history.

Back up SQLite with the matching DuckLake catalogs and data files. The analytical archive cannot reconstruct credentials, accepted pending work, or the complete receipt ledger. [Backups and recovery](native-backup-recovery.md)

With retention `0`, receipts consume SQLite space as the number of unique events grows. Pruned and delivered rows free pages for reuse; the database file does not automatically shrink. Do not delete receipts by hand: use the retention setting, which is paired with the DuckLake check.
