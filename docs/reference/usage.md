# Numerical usage

`list-usage` exposes technical consumption to the instance operator. It is available through HTTP, the standalone CLI and MCP. Tenant administrators and project readers cannot read this deployment-wide report; `get-tenant-usage` remains their monthly ingestion-budget view.

Porcupine records measurements. It does not define prices, subscriptions, invoices or rules for charging incomplete work. An external control plane reads this authenticated API rather than Porcupine's database.

## Read a period

```sh
porcupine list-usage --start 2026-11-01T00:00:00Z \
  --end 2026-12-01T00:00:00Z --granularity month \
  --tenant-id TENANT_ID --group-by project --limit 100
```

HTTP: `GET /api/v1/usage?start=2026-11-01T00:00:00Z&end=2026-12-01T00:00:00Z&granularity=month&tenant_id=TENANT_ID`.

An agent discovers `list-usage` with `porcupine_docs` and invokes it with `porcupine_call`. The operation requires `instance:admin` on every page. Never give a customer this credential merely to display their bill; the external application should provide its own restricted view.

`start` is inclusive and `end` is exclusive, both UTC midnights. Defaults cover the current month through tomorrow. Omit `tenant_id` to include all tenants. Choose `day` or `month` buckets and `project` rows or disjoint `tenant` aggregates. Tenant aggregates sum projects; they are not extra rows to add to project totals.

Follow `next_cursor` without changing filters or the page size. Filters can be omitted on subsequent pages:

```sh
porcupine list-usage --cursor CURSOR_FROM_PREVIOUS_PAGE
```

A cursor reads the same stored numerical snapshot, including from another worker using the same control database. Replaying it does not repeat the underlying operations or add consumption. Snapshots last 15 minutes. After `410 usage_snapshot_expired`, restart the read. Default pages contain 100 rows; the maximum is 1,000. A snapshot allows at most 366 days, 100,000 rows and 32 MiB of encoded row data. Oversized requests return an error asking for a shorter period or one tenant, rather than silently dropping rows. Snapshot capacity is reserved before construction; a full store or concurrent construction returns a retryable `503`.

## Understand the numbers

| Field | Meaning |
| --- | --- |
| `accepted_live_events` | Newly retained canonical live rows. Standalone counts the SQLite journal commit. Hosted counts published DuckLake commit evidence. Stable-ID retries add nothing. |
| `accepted_import_events` | Newly retained historical rows, counted at chunk acceptance time, separately from live traffic. Original event dates remain unchanged. |
| `interactive_query_count` | Durably admitted backend attempts for SQL, catalogue discovery and event profiles, including failures and cancellations. |
| `interactive_query_seconds` | Observed wall time of those backend invocations, with six fractional digits. It is not CPU time or response serialization time. |
| `published_export_bytes` | Encoded, compressed data-object bytes of successfully published export jobs. Failed/cancelled attempts and repeat downloads add nothing. |
| `published_pipeline_bytes` | Encoded data-file bytes committed by an S3 pipeline's successful cursor publication. Excludes its manifest and failed uploads. |
| `webhook_deliveries_succeeded` | Unique delivery IDs committed as delivered after successful HTTP acknowledgment. A batched delivery counts once; retries do not multiply it. |
| `storage.current_parquet_bytes` | Latest sampled sum of current referenced data/delete Parquet files, excluding obsolete files, metadata/catalog databases, backups and output objects. |

Counts and bytes are decimal strings. Correcting or deleting events does not refund accepted usage. Current Parquet bytes are a **gauge**: do not sum daily samples to obtain a cumulative byte count or a storage bill. Data inlined into a local catalogue is not Parquet storage; a zero Parquet sample does not mean zero events.

Accepted-event accounting matches the existing ingestion quota: every newly retained UUID counts, including identity controls and SDK no-ops that retain a canonical row even when the analytical `events` view hides that control. Retrying the same UUID adds nothing; an invalid atomic batch adds nothing. `COUNT(*) FROM events` therefore cannot reconcile these acceptance counters or a consumer's bill by itself.

The response labels the accounting boundary `acceptance_basis: durable_commit`. Standalone counts inside its committed SQLite acceptance transaction. In hosted mode, HTTP acknowledgment confirms memory admission. Usage increases when the system publishes durable DuckLake commit evidence. Both hosted budget enforcement and `get-tenant-usage` read the same monthly counter. The admission cache can briefly lag the current count or budget.

Query counts commit before dispatch. A query that never returns remains `lower_bound`, with `unresolved_executions` and `last_observed_at`. Its duration is observed cumulatively every 10 seconds and after actual backend return through the common maintenance loop. A process crash may lose an unobserved tail, including the complete duration of a short query, but not its durable admitted count. A crash between admission and dispatch may leave a counted attempt with no backend work. HTTP timeout and permit expiry do not fabricate a final duration. Porcupine marks a hosted execution lost after five minutes without an observation. It preserves the last observed lower bound and clears the lost flag if a real observation resumes.

Query time belongs to its start bucket, even if it finishes after midnight. Other counters use durable acceptance/publication time, not the event's historical date. Past buckets can still increase when pending outcomes are published. `publication_status: unknown` means there is no global sealed-through-time guarantee across journals and the control store; callers must replay recent periods and replace previous totals, not add every polling response together.

Storage sampling runs approximately hourly. Standalone samples its local catalogs. Hosted workers read current Parquet metadata in PostgreSQL and share the latest sample. A sample's cutoff is the earlier of bucket end and report creation. `observed_at`, `oldest_observed_at`, `sample_age_seconds`, and measured/expected project counts describe its coverage. After two hours it is `stale`, with the last measured value retained and identified as stale. Missing samples remain unknown. A partially measured tenant has `current_parquet_bytes: null`; Porcupine never fills the gaps with zero. Only the latest sample per project/UTC day is retained; v1 exposes no byte-time integral or hourly history.

## History and recovery

`available_since` records when instrumentation began. `counters` and `query_duration` are absent for a bucket entirely before that point, and `partial` flags a bucket that overlaps it. Editable analytical events and old combined tenant-budget counters cannot reconstruct past live/import/project usage.

The default numerical retention is 365 days. Deleted tenants/projects retain opaque IDs and numbers under this policy, with deletion flags; no names, email addresses, SQL, credentials or event payload are stored in this usage history. Opaque IDs remain linkable identifiers, not a claim of anonymization.

The offline restore operation changes `usage_epoch` and the journal source incarnation. A consumer must treat a changed epoch as a recovery boundary and reconcile its own historical baseline; adding restored counters to previously collected totals would double count work. Old snapshot cursors are rejected. Before restoring a hosted PostgreSQL control database, stop/fence all writers and reconcile pending sources, then rotate the usage epoch under operator control before resuming. Automatic PostgreSQL rewind detection and a complete distributed recovery tool are not implemented; do not restore a running hosted deployment and continue billing as if its counters were monotone.

## Configure retention and snapshot capacity

```json
{
  "usage": {
    "retention_days": 365,
    "max_snapshots": 16,
    "max_snapshot_bytes": 134217728
  }
}
```

The matching server variables are `PORCUPINE_USAGE_RETENTION_DAYS`, `PORCUPINE_USAGE_MAX_SNAPSHOTS`, and `PORCUPINE_USAGE_MAX_SNAPSHOT_BYTES`. The capacity limits encoded numerical snapshot data, not total process memory or database-file size. One constructor per control database is admitted before materialization, with a local construction guard and a recoverable, fenced reservation. Reservation expiry during a partition is not a global memory-limit guarantee.

Standalone journals keep cumulative counters and durable query starts. Hosted publishes cumulative live and import counters from durable DuckLake commit evidence into PostgreSQL. The control store applies cumulative revisions idempotently and stores report snapshots. Live memory admission alone does not count in hosted. See the [usage design and causal boundaries](../design/hosted-architecture.md#usage).
