# Exact deduplication with a finite local receipt cache

Porcupine retains accepted `(project_id, uuid)` receipts for the lifetime of the project. [Deleting the project](management-api.md#operations) removes its receipts and analytical history. The default
`PORCUPINE_HOT_RECEIPT_LIMIT=0` keeps delivered-event receipts in SQLite. There is
no UUID expiry window.

Set `PORCUPINE_HOT_RECEIPT_LIMIT` to a positive integer to limit the total hot
`event_receipts` rows across the deployment. This mode requires ClickHouse. It
trades SQLite growth for indexed historical lookups and a dependency on the
availability of authoritative ClickHouse history. It does not limit pending
payloads, quarantine payloads, mutation heads, or the size of ClickHouse history.
Queue count and byte budgets still apply separately.

## What happens after eviction

An original event leaves the queue only after a synchronous ClickHouse ACK.
SQLite atomically records that receipt's source provenance, removes the payload,
and evicts eligible receipts down to the configured budget. Each transaction
removes at most 256 receipts. Mutation heads permanently retain their UUIDs,
including pending deletions; quarantined originals retain their queued payload
and identity. Those local records also prevent duplicate acceptance.

Before the first eviction, ingestion checks the complete local ledger without a
ClickHouse lookup. After eviction, each capture batch checks local state and
looks up remaining UUIDs in one ClickHouse query. A UUID found in **any physical
row**, including an old revision, a tombstone, or a cold receipt partition, is a
duplicate. The incoming replacement payload is never accepted. There is no time
filter on historical membership.

Lookup leases protect the requested UUIDs against concurrent receipt eviction
until the final SQLite write transaction. The final transaction also catches a
first eviction that raced a local-only preflight. Unrelated delivery churn does
not force repeated generation-wide retries. Leases expire after 30 seconds;
expired proofs must be retried. Two historical capture probes can run at once,
independently of interactive query slots. Each has a five-second deadline,
64 MiB ClickHouse memory limit, 1 GiB read limit, and 2 MiB response limit. Probe
overload, outages, exhausted limits, and incomplete responses return retryable
HTTP 503 rather than accepting an unknown UUID. Complete local duplicates can
still succeed during a ClickHouse outage.

Historical probes select only project IDs and UUIDs with a project/UUID primary
key predicate. They do not read event properties or expose history through an
agent API. Their SQL marker is `/* porcupine:receipts */` for operator query-log
measurement. The result must be a complete JSON envelope; read, aggregation,
set, result, and timeout limits must throw rather than return partial facts.

## Upgrading an existing ledger

The schema migration is additive. Existing receipts have no assumed ClickHouse
coverage. The delivery worker reconciles at most 256 unproven receipts per pass,
marks only identities actually found in the pinned source, and then evicts
eligible rows. Reconciliation continues across projects using a durable cursor.

If all required history is present, an oversized ledger converges to the budget.
If missing legacy history prevents convergence, those receipts stay in SQLite.
New unique captures return HTTP 503 while the ledger exceeds its configured
budget; local duplicate retries remain safe. Delivery retains unacknowledged
payloads. Restore the authoritative history or increase the limit (or set it to
zero); never delete legacy receipts to force convergence. A finite limit cannot
safely bound an arbitrarily large set of receipts whose historical coverage is
unknown.

## Protecting authoritative history

The first verified source pins both its ClickHouse table UUID and a fingerprint
of the configured URL, database, and operator user. The password is excluded.
Startup verifies this identity and source safety whenever a pin exists, even if
the hot limit has subsequently been set to zero. A missing source, changed
configuration/table identity, or unavailable history blocks admission. Disabling
eviction does not erase the requirement to consult history already evicted.

Porcupine requires its physical `ReplacingMergeTree` keyed by `(project_id,
uuid)`, cleanup disabled, and no deleting TTL. Only the deployment's recognized
MOVE TTL is accepted. Physical reads explicitly disable `FINAL`, including when
the operator profile enables it by default. Restrictive row policies are
rejected. If any policies exist, the configured operator needs an explicit
unrestricted policy. The administrative principal must have authoritative access
to table and row-policy metadata, as required by the backup tool as well.

Do not run external `DELETE`, `TRUNCATE`, partition drops, deleting TTLs, or
`OPTIMIZE ... FINAL CLEANUP` against authoritative event history. Do not install
restrictive row policies or replace/partially restore the table out of band.
The table UUID cannot detect prior destructive changes made in place. These
operator actions invalidate the completeness assumption and can destroy exact
deduplication. Cold storage must remain accessible; its objects are part of the
same authoritative history.

Use the coordinated backup/restore tool to preserve evicted history. It verifies
the native archive and logical event summary before explicitly rebinding the
copied SQLite source pin and receipt provenance to the newly restored table
UUID. Startup must never be admitted against a partial archive. Ordinary copied
SQLite databases cannot be rebound merely by editing metadata.

After the first eviction or omission of a redundant mutation-head receipt, an
additive SQLite trigger rejects legacy-shaped event INSERTs that omit the receipt-aware protocol marker. Rolling back to a binary
without historical lookup support is unsupported: it cannot safely ingest into
this database. Keep the pinned source and use a receipt-aware binary, or restore
a coordinated pre-eviction checkpoint of both stores.

## Measured cost

The [local benchmark summary](benchmarks.md) includes a ten-minute run with 1,000 hot receipts, exact final event counts, and duplicate retries against native cold storage. ClickHouse reads consume compute even when the local SQLite ledger stays flat. Use your expected UUID distribution, number of historical partitions, event rate and query mix to size a deployment.
