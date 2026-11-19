# Hosted architecture

Hosted acknowledges validated live events after admission to bounded worker memory. An acknowledged event can be lost if the worker crashes or shuts down before the flush commits. Standalone acknowledges live events after its SQLite journal commits. [Standalone architecture](../architecture.md) [Hosting and upgrades](../hosting.md)

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

Native, PostHog, and Segment live adapters share validation, credential-bound project scope, and UUID normalization. The worker admits the entire request or rejects it. Native HTTP 200 includes `durability: "buffered"` and ordered receipts with `message_id` and `uuid`. The original event ID and duplicate outcome are unknown at admission. The native schema and authenticated input contract advertise the profile's acknowledgment. [Event contract](../reference/native-event-contract.md#acknowledgment-metadata)

The buffer owns the pending event and parsed identity input. It resolves cookieless pseudonyms before enqueueing using shared daily keys and keeps raw peer data out of pending batches. The default two-second flush timer or 5 MiB project threshold triggers dispatch. The buffer accounts for pending and in-flight work against worker and project limits, including per-row and per-project overhead. Defaults are 64 MiB and 100,000 rows per worker, 8 MiB and 10,000 rows per project, 1,024 active projects, and four concurrent flushes. Process RSS also includes handlers, native memory, and caches. During an outage the oldest admitted event can remain pending beyond the flush interval. A query on another worker cannot report that buffer's state. [Hosted runtime reference](../reference/shared-deployment.md)

A full or draining worker returns HTTP 503 with `Retry-After`. Live HTTP adapters enforce the 1 MiB request body limit before admission, including after decompression. The buffer also checks that the entire validated request fits its row and byte limits before acknowledging it. A temporary flush failure puts backpressure on its project, retains the batch ID, and retries with backoff.

When the matched webhooks exceed a fixed preparation, manifest, or reservation limit, the worker first splits the flush at request boundaries. If a single request still exceeds a limit, the worker commits its events, deduplication heads, identity changes, and usage. It permanently refuses the matched webhook references for that request as a set. A temporary shortage caused by queued outputs delays a flush and triggers a retry. An artifact-free reservation still protects native proof and usage publication when the webhook budget is full. At most one such reservation can be active per project.

On observing the native commit, the worker emits a structured warning per affected pipeline, with the project, batch ID, refusal reason, and number of declined references. `porcupine_webhook_refused_references_total` counts those references by reason across pipelines. A lost commit response can leave the operational signals missing. The committed UUIDs and proof prevent a replay from scheduling previously declined outputs.

After three consecutive flush failures with the same deterministic error code, the worker quarantines the project and returns HTTP 503 with `Retry-After` for that project's new ingestion. Missing native evidence, an inconsistent immutable pipeline binding, and an invalid native live projection can trigger quarantine. Connection failures, deadlines, stale write sessions, and output capacity still use backoff and retain their inputs.

Quarantine discards the project's buffered copies and frees their flush slots and memory. An accepted event that never reached a native commit is lost. Native commits and their recovery evidence remain intact. The flush log includes the project, batch, reason, and discard counts, and the [worker metrics](../reference/observability.md#read-the-dashboard) count each quarantine and discarded event copy. The worker keeps only a project ID and reason for up to 1,024 quarantines by default. A restart clears that list. At the limit, a new quarantine lets the oldest project try ingestion again. Administrators see the list in the [instance status](../reference/instance-status.md#interpret-the-response).

If PostgreSQL is unavailable, the worker answers with HTTP 503 when it cannot validate or admit a request. Buffer limits continue to apply. If its incarnation lease expires, the worker reports HTTP 503 on `/readyz`, cancels in-flight writes, and pauses write admission. A live flush and its effect publication have a 30-second deadline. DuckLake also applies its PostgreSQL metadata and S3 request timeouts. A native provider call can take longer to return if it ignores cancellation. The advisory session remains held until its callback returns when the physical connection survives, and DuckLake's epoch and nonce fence rejects a stale writer after takeover.

The worker keeps acknowledged batches and their IDs in memory throughout the outage. When PostgreSQL returns, the worker acquires a fresh incarnation and retries pending batches. Native proofs and effect checkpoints make retries idempotent. The outage alone does not discard the buffer. A process crash or termination before flush can still lose its pending events.

Shutdown allows ten seconds for HTTP shutdown and 30 seconds for the buffer drain. The worker drains before waiting up to five seconds for canceled analytics and maintenance. If they stay active, it reports a shutdown error and leaves their native resources open until process exit. Other background jobs and in-flight flushes can still affect shutdown time. Allow termination grace for HTTP shutdown, draining, and cleanup.

## Flush and publication

A PostgreSQL advisory lock ties each project write session to one connection and serializes hosted writers. Blocking acquisition has a five-second default deadline across the local slot, connection, and advisory lock. Live writes, imports, mutations, maintenance, and sealing also use a native fence. Query readers use snapshots and separate permits.

An ordinary live flush follows this sequence:

1. Deduplicate the batch against authoritative DuckLake heads, including tombstones. Only newly retained UUIDs update identity or schedule live effects.
2. Commit the output reservation in PostgreSQL before the first S3 PUT. The reservation records the attempt's ownership and charges tenant and project capacity. Then stage immutable output objects on S3.
3. Recheck the tenant and project deletion fences in a short PostgreSQL transaction. Release those row locks before DuckLake COMMIT. The writer keeps the advisory session and activity ticket through native return. Deletion intent can fence new work during the commit. Sealing waits for active work and settles its outcome.
4. In one DuckLake snapshot, advance the native writer fence, acknowledge the previous committed PostgreSQL publication checkpoint, and commit the new events, heads, identity changes, usage evidence, and proof.
5. Publish output jobs and cumulative usage idempotently in a PostgreSQL transaction that verifies and settles the original reservation. The fresh path carries its committed binding from staging. It verifies the manifest and uses the uploads' S3 checksum validation for webhook bodies already written by this attempt.

The two PostgreSQL transactions serve different recovery points. A worker that dies after PUT leaves a durable reservation identifying the abandoned prefix. A worker that dies after the native commit leaves the proof and reservation its successor needs for publication. Cold recovery reads the durable binding and verifies the immutable bodies, including their size, checksum, and version when present. Before retiring an orphan reservation, it commits a successor fence and checks native history for proof or absence. Cold recovery, imports, mutations, and deletion can still use separate native fence and acknowledgment commits.

Delivery and assembly verify the stored bodies they consume. Delivery retries can reach a receiver more than once. Use the stable delivery ID to deduplicate a request whose response was lost. The scheduled body's contents remain fixed after later edits or deletes.

## Imports and mutations

Historical upload requests authorize against the shared project and job state. S3 holds originals, parser state, and serialized rolling-hash state. PostgreSQL stores digests, offsets, and references. Serialized SHA-256 state can contain raw input, so treat it as payload. Commit each import chunk's stable ID and original outcome beside its native changes before advancing its PostgreSQL checkpoint. A retry returns the original outcome rather than reclassifying its committed rows as duplicates. Historical imports skip live webhooks and scheduled receipt-window deliveries.

Explicit event mutations persist a durable intent before success. Immutable S3 objects hold the mutation input. The project writer applies the change once against the expected revision and reconciles the intent after commit. The deletion fence still applies to resumed work. [Correct or delete events](../events.md)

## Query permits and output jobs

Hosted SQL, catalog, and profile requests use worker-local slots and renewable PostgreSQL permits. The keeper renews until actual native return, independently of HTTP deadlines. A stuck call or failed permit renewal quarantines that project's interactive engine on the worker. The process stays up and other projects can execute within their limits. A lost heartbeat can free a permit before a partitioned native call returns. Physical concurrency can exceed the configured limit during that window. [Hosted runtime reference](../reference/shared-deployment.md#native-timeouts-and-worker-supervision)

Exports and scheduled SQL pipelines share worker capacity with interactive queries. Output jobs recheck creator authority and destination revision. Scheduled SQL-to-S3 delivery pins a reconciled snapshot under the project write session and releases the session before streaming. A stable window key, immutable manifest, and conditional cursor update make a retry recoverable after a lost commit. Live events still in memory when the snapshot is pinned can miss that receipt window. Cleanup uses the attempt's original destination credentials. Published customer outputs stay in the customer's bucket. [Pipelines](../pipelines.md)

## Usage

Usage counts the actual durable transition. Buffered acknowledgment alone adds no hosted usage. Native evidence supplies cumulative accepted live/import counts split by project, UTC day, and source. The control store applies revisions idempotently. Hosted live ingestion checks its tenant allowance before buffering. Cached admission policy can lag the durable `tenant_usage` ledger, which accounts for retained live and import rows at publication. A later budget change leaves already admitted events eligible to flush, subject to deletion fences. The operator report separates live and import usage. Deletion gives no refund.

Interactive query starts persist before dispatch. Duration measures monotonic time through actual native return. An incomplete execution keeps its last recorded lower bound and unresolved count. Five minutes without an observation marks a hosted execution lost. A resumed observation clears the flag. Output meters count a unique durable delivery acknowledgment or the final published compressed bytes. The operator report retains bounded numerical snapshots for stable pagination and uses an epoch to identify incompatible restored history. Current Parquet usage is a sample of referenced current files, including delete files. Hosted workers sample current-file metadata directly in PostgreSQL. Missing samples are unknown. [Usage metrics and API](../reference/usage.md)

## Deletion

`delete-tenant` requires `instance:admin`. It commits tenant and project fences before returning HTTP 202. `get-tenant-deletion` reads the same durable status, even after the tenant row is gone. Recovery retries unfinished project cleanup. The fence applies to native credentials and external JWTs. Tenant-owned authority is revoked while access to unrelated tenants is preserved. Operator-issued keys and OAuth clients scoped to one tenant record cleanup ownership so deletion can revoke and remove them. Older credentials can be assigned at deletion when their remaining project scope identifies one tenant. [Tenant credential cleanup](../reference/tenants.md#delete-a-tenant)

Hosted project cleanup seals the native writer, waits for conflicting activity, settles retained proofs, and inventories managed objects and multipart uploads. It keeps published customer results and uses exact ownership evidence for unfinished output attempts. Missing destination authority or unresolved activity keeps cleanup pending. Cleanup can reclaim an old incarnation’s tickets after its 30-second lease and 45-second cancellation grace expire. Workers attempt to renew or reacquire their lease every five seconds. `deleting` means work remains. `logically_deleted` reports completed logical cleanup, with `physical_purge: pending_verification`. A tenant tombstone does not prove historical backups or noncurrent object versions are gone. Minimal numerical usage history and opaque deletion identifiers remain.

## Recovery and retention

Background reconciliation selects projects from an indexed PostgreSQL work queue. Output reservations, ready mutations, import preflight, and reclaimed worker tickets add work. The worker confirms native effect acknowledgements before removing the entry. It yields to a project writer that already holds the lock. Once recovery settles, an idle project causes no DuckLake commits or S3 writes from reconciliation. An upgrade from control schema 21 queues a one-time check of existing effect streams because those native acknowledgements were recorded only in DuckLake.

Hosted recovery uses PostgreSQL point-in-time recovery (PITR) and S3. Keep control state and catalog metadata in one PostgreSQL recovery domain. Preserve the deployment configuration and pipeline encryption keys separately. Before restoring, fence old writers and keep the restored service isolated with outbound jobs paused. Verify expected objects, authority, and pending jobs. Reapply revocations and deletion instructions after the target, or rotate recovered access. Rotate the usage epoch before publishing reports from restored history. A receiver can see a replay of a previously delivered job.

Set `cleanup_grace` to at least `restore_window + restore_max_reader_time + restore_safety_margin`. Include exports when establishing the reader limit. Keep all referenced S3 objects readable for the supported restore window. Bucket versioning alone may leave ordinary reads returning 404 under a delete marker. Check provider lifecycle policies and test a real restore against the deployed PostgreSQL and S3 services.

Set `ducklake.restore_floor_file` to an absolute path containing one operator-verified RFC3339 timestamp for the earliest supported recovery point, including secondary backups. Update it atomically across the deployment as recovery points expire. Managed project reclamation waits for both the verified floor and grace since inventory. Missing, invalid, or stale evidence keeps reclamation pending. Hosted maintenance schedules compaction and history cleanup under the project write fence. Cleanup rewrites files with deleted rows, expires eligible snapshots, and reclaims obsolete files after the committed deletion schedule passes the restore floor and configured grace. [Cleanup bounds and status](../reference/ducklake-cleanup.md#hosted-profile) [Restore drill](archive/hosted-restore-drill.md)

S3 and webhook requests already accepted by a provider can outlive the local worker. Reclamation relies on their settling within the grace. An object written after reclamation may fall outside the cleanup inventory. Publication still requires a valid native proof or current output claim.

For earlier measurements, see the [flush cost record](archive/hosted-flush-cost.md). The [changelog](../../CHANGELOG.md) records differences between releases.

## Upgrade worker activity tickets

Drain older workers before upgrading from v0.15.0. Tickets created before migration 20 have a NULL incarnation and never expire automatically. If a ticket remains, block all pre-upgrade executors from restarting and verify that provider-side uploads and deliveries have settled. A database operator can remove the verified ticket by project and activity ID with `incarnation_id IS NULL`. No public endpoint clears tickets. [Upgrade guide](../hosting.md#upgrade-from-v0150)

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
