# Event IDs and durable receipts

Porcupine keeps a minimal SQLite receipt for each delivered event for the lifetime of its project. There is no expiry window or finite local receipt mode. Queue payloads and analytical data have separate storage lifecycles.

For native and Segment inputs, a stable producer message ID identifies one message within its authenticated project and logical source. A retry returns the original server event ID and retains the first accepted payload. Unbound inputs have separate source namespaces. Supported PostHog inputs use their UUID namespace unless the credential binds an explicit logical source. See [producer IDs and source namespaces](native-event-contract.md#ids-and-retry-semantics).

## Before and after delivery

Before delivery, the accepted original in SQLite prevents duplicate acceptance. Product identity changes and any webhook work commit with that original. The server acknowledges only after the transaction commits.

A worker applies the original to DuckLake, then records its receipt and removes the queued payload in one SQLite transaction. A failure between the analytical commit and SQLite acknowledgment leaves the original available for replay. Native revision and event-ID checks make that replay converge on one logical event.

A duplicate cannot change properties, update traits a second time, enqueue another webhook, or acquire a new numeric ID. Corrections use the [event management API](events.md).

## Deletion and recovery

Mutation heads and payload-free analytical tombstones retain the facts needed to reject stale originals. Deleting an event does not make its producer ID available for reuse. Deleting the entire project removes its receipts and managed analytical history.

Back up SQLite with the matching DuckLake catalogs and data files. The analytical archive cannot reconstruct credentials, accepted pending work, or the complete receipt ledger. [Backups and recovery](native-backup-recovery.md)

Permanent receipts consume SQLite space as the number of unique events grows. Monitor the acceptance database and reserve space for both this ledger and pending payloads. Deleted queue payloads free pages for reuse; the database file does not automatically shrink. Do not delete receipts to control size, because that can accept an old retry as a new event.
