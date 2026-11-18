# Hosted architecture

Porcupine v0.15.0 ships two profiles. [Standalone](../architecture.md) acknowledges live events after its SQLite journal commits. Hosted acknowledges validated admission to bounded worker memory. An acknowledged but uncommitted hosted event can be lost on a crash or incomplete shutdown. The sections below describe [v0.15.0](https://github.com/olup/porcupine/tree/v0.15.0) unless marked [after v0.15.0](#changes-after-v0150).

Hosted workers share PostgreSQL for control, coordination, operational jobs, and DuckLake metadata. S3 stores Parquet and immutable job inputs and outputs. Workers are replaceable behind an ingress and use disposable scratch space. PostgreSQL holds object references and job state. Event bodies, import parser tails, and mutation bodies live in memory before their respective commit or staging boundary and in S3 when persisted. DuckLake data inlining is disabled. Protect the catalog as sensitive data because its statistics can contain derived event values.

## Configuration

Run `porcupine init --hosted` and merge the following settings into the generated private configuration. Keep the bootstrap and pipeline secrets. Use a fresh PostgreSQL database and a dedicated analytical S3 prefix.

```json
{
  "profile": "hosted",
  "acceptance": {"mode": "buffered"},
  "shared": {"control_database_url_file": "/run/secrets/porcupine-postgres"},
  "ducklake": {
    "s3": {"bucket": "example-analytics", "prefix": "porcupine", "region": "eu-west-1"},
    "cleanup_grace": "240h",
    "restore_window": "168h",
    "restore_max_reader_time": "1h",
    "restore_safety_margin": "24h",
    "restore_floor_file": "/run/porcupine/restore-floor"
  }
}
```

An explicit `ducklake.postgres_url` must match the shared PostgreSQL URL. Keep configuration and the pipeline encryption key consistent across replicas. Give each worker private writable scratch space. Set `storage.ducklake` only if you need a specific disposable directory. The hosted profile rejects local journal/control paths, receipt retention, owner routing settings, and local native backups.

Run `porcupine config check --config porcupine.json`, then start each worker with `porcupine --config porcupine.json`. Set the listener and HTTPS ingress for your deployment. Apply infrastructure rate limits at the gateway using verified tenant or credential identity. Exercise PostgreSQL and S3 recovery before admitting traffic. [Hosting guide](../hosting.md)

## Live admission and shutdown

Native, PostHog, and Segment live adapters share validation, credential-bound project scope, and UUID normalization. The worker admits the entire request or rejects it. Native HTTP 200 includes `durability: "buffered"` and ordered receipts with `message_id` and `uuid`. The original event ID and duplicate outcome are unknown at admission. The native schema and authenticated input contract advertise the profile's acknowledgment. [Event contract](../reference/native-event-contract.md#unreleased-acknowledgment-metadata)

The buffer owns the pending event and parsed identity input. It resolves cookieless pseudonyms before enqueueing using shared daily keys and keeps raw peer data out of pending batches. The default two-second flush timer or 5 MiB project threshold triggers dispatch. The buffer accounts for pending and in-flight work against worker and project limits, including per-row and per-project overhead. Defaults are 64 MiB and 100,000 rows per worker, 8 MiB and 10,000 rows per project, 1,024 active projects, and four concurrent flushes. Process RSS also includes handlers, native memory, and caches. During an outage the oldest admitted event can remain pending beyond the flush interval. A query on another worker cannot report that buffer's state. [Hosted runtime reference](../reference/shared-deployment.md)

A full, draining, or flush-failed worker returns HTTP 503 with `Retry-After`. At the tag, one project's failed batch can block new admissions on that worker. Failed batches retain their IDs and retry with backoff until they succeed or a deletion fence rejects them. The shutdown path allows ten seconds for HTTP shutdown and 30 seconds for the buffer drain. At the tag, an unfinished analytical call can abort shutdown before the buffer drain starts. Allow termination grace for HTTP shutdown, draining, and cleanup.

## Flush and publication

A PostgreSQL advisory lock ties each project write session to one connection and serializes hosted writers. A native fence checks authority inside DuckLake. Live writes, imports, mutations, maintenance, and sealing participate in this ordering. Query readers use snapshots and separate permits.

1. Recheck the tenant/project fences. Deduplicate within the batch and against authoritative DuckLake heads, including tombstones. Only newly retained UUIDs update identity or schedule live effects.
2. Stage immutable output references on S3. Commit native events, heads, identity changes, cumulative usage, and recoverable effect evidence together in DuckLake.
3. Reconcile the committed proof into PostgreSQL. Publish output jobs and cumulative usage idempotently, then acknowledge reconciliation so another worker can distinguish pending from settled native proofs. At the tag, native fence advancement, data commit, and reconciliation acknowledgment use separate DuckLake transactions.

The native proof and PostgreSQL publication checkpoint let a retry resume at the right boundary after a lost response. PostgreSQL and DuckLake do not share an application transaction. Receiver delivery is at least once within the retry policy. At the tag, reservations charge instance, tenant, and project output capacity. The stable delivery ID lets a receiver deduplicate a delivery whose response was lost. Webhook content comes from immutable S3 data and is independent of subsequent edits or deletes.

## Imports and mutations

Historical upload requests authorize against the shared project and job state. S3 holds originals, parser state, and serialized rolling-hash state. PostgreSQL stores digests, offsets, and references. Serialized SHA-256 state can contain raw input, so treat it as payload. Commit each import chunk's stable ID and original outcome beside its native changes before advancing its PostgreSQL checkpoint. A retry returns the original outcome rather than reclassifying its committed rows as duplicates. Historical imports skip live webhooks and scheduled receipt-window deliveries.

Explicit event mutations persist a durable intent before success. Immutable S3 objects hold the mutation input. The project writer applies the change once against the expected revision and reconciles the intent after commit. The deletion fence still applies to resumed work. [Correct or delete events](../events.md)

## Query permits and output jobs

Hosted SQL, catalog, and profile requests use worker-local slots and renewable PostgreSQL permits. The keeper renews until actual native return, independently of HTTP deadlines. A stuck call or failed permit renewal quarantines that project's interactive engine on the worker. The process stays up and other projects can execute within their limits. A lost heartbeat can free a permit before a partitioned native call returns. Physical concurrency can exceed the configured limit during that window. [Hosted runtime reference](../reference/shared-deployment.md#native-timeouts-and-worker-supervision)

Shared output jobs recheck creator authority and destination revision. Scheduled SQL-to-S3 delivery pins a reconciled snapshot under the project write session and releases the session before streaming. A stable window key, immutable manifest, and conditional cursor update make a retry recoverable after a lost commit. Live events still in memory when the snapshot is pinned can miss that receipt window. Cleanup uses the attempt's original destination credentials. Published customer outputs stay in the customer's bucket. [Pipelines](../pipelines.md)

## Usage

Usage counts the actual durable transition. Buffered acknowledgment alone adds no hosted usage. Native evidence supplies cumulative accepted live/import counts split by project, UTC day, and source. The control store applies revisions idempotently. Publication and admission policy can observe different points in time. The durable hosted allowance uses `tenant_usage`, which combines retained live and import rows, while reporting preserves the split. Deletion gives no refund.

Interactive query starts persist before dispatch. Duration measures monotonic time through actual native return. An incomplete execution keeps its last recorded lower bound and unresolved count. Output meters count a unique durable delivery acknowledgment or the final published compressed bytes. The operator report retains bounded numerical snapshots for stable pagination and uses an epoch to identify incompatible restored history. Current Parquet usage is a sample of referenced current files, including delete files. At v0.15.0 scheduled Parquet sampling is standalone-only. Missing samples are unknown. [Usage metrics and API](../reference/usage.md)

## Deletion

`delete-tenant` requires `instance:admin`. It commits tenant and project fences before returning HTTP 202. `get-tenant-deletion` reads the same durable status, even after the tenant row is gone. Recovery retries unfinished project cleanup. The fence applies to native credentials and external JWTs. Tenant-owned authority is revoked while access to unrelated tenants is preserved.

Hosted project cleanup seals the native writer, waits for conflicting activity, settles retained proofs, and inventories managed objects and multipart uploads. It keeps published customer results and uses exact ownership evidence for unfinished output attempts. Missing destination authority or unresolved activity keeps cleanup pending. At v0.15.0, an activity ticket from a dead worker can block cleanup indefinitely. `deleting` means work remains. `logically_deleted` reports completed logical cleanup, with `physical_purge: pending_verification`. A tenant tombstone does not prove historical backups or noncurrent object versions are gone. Minimal numerical usage history and opaque deletion identifiers remain.

## Recovery and retention

Hosted recovery uses PostgreSQL point-in-time recovery (PITR) and S3. Keep control state and catalog metadata in one PostgreSQL recovery domain. Preserve the deployment configuration and pipeline encryption keys separately. Before restoring, fence old writers and keep the restored service isolated with outbound jobs paused. Verify expected objects, authority, and pending jobs. Reapply revocations and deletion instructions after the target, or rotate recovered access. Rotate the usage epoch before publishing reports from restored history. A receiver can see a replay of a previously delivered job.

Set `cleanup_grace` to at least `restore_window + restore_max_reader_time + restore_safety_margin`. Include exports when establishing the reader limit. Keep all referenced S3 objects readable for the supported restore window. Bucket versioning alone may leave ordinary reads returning 404 under a delete marker. Check provider lifecycle policies and test a real restore against the deployed PostgreSQL and S3 services.

Set `ducklake.restore_floor_file` to an absolute path containing one operator-verified RFC3339 timestamp for the earliest supported recovery point, including secondary backups. Update it atomically across the deployment as recovery points expire. Managed project reclamation waits for both the verified floor and grace since inventory. Missing, invalid, or stale evidence keeps reclamation pending. Hosted maintenance schedules fenced, bounded compaction. At the tag it does not schedule snapshot expiry or obsolete-file cleanup for active hosted catalogs. Compaction alone does not purge old data. [Restore drill](archive/hosted-restore-drill.md)

## Changes after v0.15.0

- [#100](https://github.com/olup/porcupine/pull/100) starts the buffer drain before waiting for canceled analytical calls. After draining, it allows five seconds for analytics and maintenance to return, then reports an error if either remains active. Native resources stay open under running calls until process exit. Other background jobs and an in-flight flush can still delay shutdown.
- [#101](https://github.com/olup/porcupine/pull/101) tolerates encryption-only keys in external JWKS, infers supported signing algorithms when `alg` is absent, and can serve cached signing keys for up to an hour after their refresh deadline when the refresh fails. [External authentication](../reference/external-auth.md)
- [#105](https://github.com/olup/porcupine/pull/105) scopes hosted output reservations to tenant and project capacity. A full tenant backlog leaves another tenant's output budget available.
- [#103](https://github.com/olup/porcupine/pull/103) lets deletion reclaim crashed workers' activity tickets after a 30-second incarnation lease and 45-second grace. Workers renew every five seconds. Lease loss refuses new tickets and exits with status 75 if write-capable work is still running. A read-only query alone never triggers that exit. The native epoch fences stale commits once the sealer advances it.
- [#104](https://github.com/olup/porcupine/pull/104) limits flush backpressure to the affected project. Worker capacity and draining still apply globally. Temporary failures keep the batch ID and retry. A flush proven too large before native commit splits at request boundaries in admission order. If one request cannot fit, the worker discards it and logs the project, batch ID, and event count. Instance administrators can see this worker's counters since startup in `hosted_ingestion.dropped_requests` and `hosted_ingestion.dropped_events` at `GET /api/v1/status`.
- [#106](https://github.com/olup/porcupine/pull/106) samples hosted current-file metadata directly in PostgreSQL. Hosted query metering marks an open execution lost after five minutes without an observation. A resumed observation clears the lost flag. The admitted count, last observed duration, and unresolved count remain intact.

Before upgrading across #103, drain older workers. Tickets from before migration 20 have no incarnation and do not expire. If one remains, block all pre-upgrade executors from restarting and verify provider-side uploads and deliveries have settled. A database operator can remove only the verified ticket by project and activity ID with `incarnation_id IS NULL`. No public endpoint clears tickets.

S3 and webhook requests already accepted by a provider can outlive the local worker. Reclamation relies on their settling within the grace. An object written after reclamation may fall outside the cleanup inventory, but publication still requires a valid native proof or current output claim.

## Standalone journal inventory

The inventory maps versioned standalone journal tables to their hosted equivalents. Exactly once describes a committed logical effect. At least once describes resumable job processing with finite retries. Best effort describes volatile live admission before the native commit.

<!-- journal-inventory:start -->
| Journal table | Hosted location | Recovery | Responsibility |
| --- | --- | --- | --- |
| `access_keys` | Removed | Exactly once | No local credential anchors. The control store owns credentials. |
| `cookieless_keys` | PostgreSQL | Exactly once | Shared daily secret. |
| `delivery_quarantine` | Removed | Best effort | Memory buffer and flush status. |
| `delivery_ready` | Removed | Best effort | Memory flush scheduling. |
| `event_heads` | DuckLake | Exactly once | UUID, revision, original ID, tombstone. |
| `event_receipts` | Removed | Best effort | Hosted receipts acknowledge memory admission. |
| `events` | Removed | Best effort | No SQL inbox. |
| `export_files` | PostgreSQL | At least once | S3 file and upload references. |
| `export_external_authority` | PostgreSQL | Exactly once | Bounded execution authority without secrets. |
| `export_jobs` | PostgreSQL | At least once | Authorization, query, snapshot, attempt. |
| `identity_links` | DuckLake | Exactly once | Versioned links. |
| `identity_profiles` | DuckLake | Exactly once | Profiles commit with links. |
| `import_upload_files` | PostgreSQL | At least once | Offsets, digests, S3 references. |
| `import_upload_samples` | PostgreSQL | Exactly once | Bounded line/reason samples. |
| `import_uploads` | PostgreSQL | At least once | Checkpoint and chunk outcome. |
| `journal_actor_tenants` | Removed | Exactly once | Job audit state replaces local anchors. |
| `metadata` | PostgreSQL | Exactly once | Durable source, schema, and job keys. |
| `mutation_audit` | PostgreSQL | Exactly once | Revision, request hash, actor, state. |
| `mutation_outbox` | PostgreSQL | At least once | Intent and immutable object reference. |
| `pipeline_retired_destinations` | PostgreSQL | At least once | Hosted attempts retain destination authority for cleanup. |
| `pipeline_retired_files` | PostgreSQL | At least once | Hosted attempt records retain ownership evidence. |
| `pipeline_s3_files` | PostgreSQL | At least once | File and upload references. |
| `pipeline_s3_runs` | PostgreSQL | At least once | Pipeline/window attempts and snapshots. |
| `pipeline_s3_state` | PostgreSQL | Exactly once | Conditional cursor update. |
| `pipelines` | PostgreSQL | Exactly once | Definitions, creator, encrypted destination secrets. |
| `posthog_import_person_state` | DuckLake | Exactly once | Mappings commit with profiles and snapshots. |
| `project_deletions` | PostgreSQL | Exactly once | Durable project fence. |
| `projects` | PostgreSQL | Exactly once | Authoritative ownership. |
| `queue_usage` | Removed | Best effort | Worker buffer accounting. |
| `tenant_usage` | PostgreSQL | Exactly once | Durable allowance, no refund. |
| `tenants` | PostgreSQL | Exactly once | Authoritative tenant policy. |
| `webhook_cooldowns` | PostgreSQL | At least once | Shared retry state. |
| `webhook_deliveries` | PostgreSQL | At least once | Stable delivery ID and body reference. |
| `webhook_members` | PostgreSQL | Exactly once | Event/revision membership. |
| `webhook_queue_usage` | PostgreSQL | Exactly once | Pending output capacity. |
| `shared_tenant_cache` | Removed | Best effort | Disposable policy cache. |
| `project_deletion_files` | PostgreSQL | At least once | Retention-gated cleanup inventory. |
| `usage_rollups` | PostgreSQL | Exactly once | Revisioned cumulative counters. |
| `usage_executions` | PostgreSQL | At least once | Admitted starts and actual-return observations. |
<!-- journal-inventory:end -->

The hosted registry in PostgreSQL replaces the standalone analytical `registry.sqlite`. It supports cold project discovery and keeps permanent deletion fences. For earlier designs, fixture limits, and source-to-destination notes, see the [archive](archive/README.md).
