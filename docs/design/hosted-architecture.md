# Hosted architecture

Hosted can acknowledge a SQLite commit on the worker volume or use bounded memory admission. Memory admission can lose acknowledged events during a worker crash. Standalone acknowledges live events after its SQLite journal commits. [Standalone architecture](../architecture.md) [Hosting and upgrades](../hosting.md)

Hosted workers share PostgreSQL for control, coordination, operational jobs, and DuckLake metadata. S3 stores Parquet and immutable job inputs and outputs. Workers are replaceable behind an ingress and use disposable scratch space. PostgreSQL holds object references and job state. Event bodies, import parser tails, and mutation bodies live in memory before their respective commit or staging boundary and in S3 when persisted. DuckLake data inlining is disabled. Protect the catalog as sensitive data because its statistics can contain derived event values.

## Configuration

See [Configure hosted workers](../reference/shared-deployment.md#configuration).

## Live admission and shutdown

Native, PostHog, and Segment live adapters share validation, credential-bound project scope, and UUID normalization. The worker admits the entire request or rejects it. Native HTTP 200 reports the configured durability and includes ordered receipts with `message_id` and `uuid`. The original event ID and duplicate outcome are unknown at admission. The native schema and authenticated input contract advertise the profile's acknowledgment. [Event contract](../reference/native-event-contract.md#acknowledgment-metadata)

The buffer owns the pending event and parsed identity input. It resolves cookieless pseudonyms before enqueueing using shared daily keys and keeps raw peer data out of pending batches. It accounts for pending and in-flight work against worker and project limits, including per-row and per-project overhead. A temporary flush failure puts backpressure on its project, retains the batch ID, and retries with backoff. [Buffer limits and live delivery](../reference/shared-deployment.md#live-ingestion-and-delivery)

When the matched webhooks exceed a fixed preparation, manifest, or reservation limit, the worker first splits the flush at request boundaries. If a single request still exceeds a limit, the worker commits its events, deduplication heads, identity changes, and usage. It permanently refuses the matched webhook references for that request as a set. A temporary shortage caused by queued outputs delays a flush and triggers a retry. An artifact-free reservation still protects native proof and usage publication when the webhook budget is full. At most one such reservation can be active per project.

On observing the native commit, the worker emits a structured warning per affected pipeline, with the project, batch ID, refusal reason, and number of declined references. `porcupine_webhook_refused_references_total` counts those references by reason across pipelines. A lost commit response can leave the operational signals missing. The committed UUIDs and proof prevent a replay from scheduling previously declined outputs.

Live admission checks the DuckLake projection before acknowledging a request. An invalid projection returns HTTP 400 with `invalid_live_projection`. The reserved event ID and materialized identity snapshot are checked at flush because they depend on the project write session and the current identity state. If the native writer proves that a projection failed before commit, the buffer splits the flush at request boundaries and continues with the other requests. Buffered mode discards the isolated request. Durable mode retains it in the journal and quarantines the project on that worker. The error log includes the project, batch ID, and reason. The `porcupine_flush_dropped_events_total{reason="invalid_live_projection"}` metric counts its discarded event copies.

Missing native evidence or a conflicting immutable pipeline binding quarantines a project. The worker records the reason in PostgreSQL's `hosted_recovery_pending` table. New ingestion for that project returns HTTP 503 with `code: "project_ingestion_quarantined"` and `Retry-After: 1` on every worker once the marker commits. Other projects continue within the worker limits. The affected worker retains its accepted requests within the existing project and worker budgets. It releases its flush slot between attempts and retries with exponential backoff up to five seconds by default. Successful recovery or a successful flush clears the shared quarantine. Administrators can see the reasons in the [instance status](../reference/instance-status.md#interpret-the-response). A worker crash or a shutdown that exhausts the drain deadline can still lose buffered events that never reached native commit.

Starting with hosted control schema 26, a failed scheduled recovery attempt delays the next one for that project by 30 seconds. All workers read the deadline from PostgreSQL. A new recovery signal makes the project eligible immediately. Buffered flushes can recover on their own retry schedule.

If PostgreSQL is unavailable, the worker answers with HTTP 503 when it cannot validate or admit a request. Buffer limits continue to apply. If its incarnation lease expires, the worker reports HTTP 503 on `/readyz`, cancels in-flight writes, and pauses write admission. A live flush and its effect publication have a 30-second deadline. DuckLake also applies its PostgreSQL metadata and S3 request timeouts. A native provider call can take longer to return if it ignores cancellation. The advisory session remains held until its callback returns when the physical connection survives, and DuckLake's epoch and nonce fence rejects a stale writer after takeover.

The worker keeps acknowledged batches and their IDs throughout the outage. In durable mode it also journals each admitted request, seals batch IDs before dispatch, and removes journal entries after publication. When PostgreSQL returns, the worker acquires a fresh incarnation and retries pending batches. Native proofs and effect checkpoints make retries idempotent. The outage alone does not discard the buffer. In buffered mode a process crash or termination before flush can still lose its pending events.

Shutdown drains the buffer before waiting for canceled analytics and maintenance. If they stay active past the deadline, the worker reports a shutdown error and leaves their native resources open until process exit. [Drain a worker](../reference/shared-deployment.md#drain-a-worker)

## Flush and publication

A PostgreSQL advisory lock ties each project write session to one connection and serializes hosted writers. Blocking acquisition has a five-second default deadline across the local slot, connection, and advisory lock. Live writes, imports, mutations, maintenance, and sealing also use a native fence. Query readers use snapshots and separate permits.

Before acquiring that lock, the worker caps the PostgreSQL backend's supported TCP keepalive settings at 15 seconds idle, 5 seconds between probes, and 3 probes. It caps the Linux TCP user timeout at 30 seconds so unacknowledged data can also trigger disconnect detection. The worker restores the prior socket settings before returning the connection to the control pool. If acquisition or cleanup leaves ownership uncertain, the worker discards the connection. Takeover still depends on PostgreSQL detecting the lost peer and releasing the session lock, and on the successor acquiring the lock and advancing the native fence.

An ordinary live flush follows this sequence:

1. Deduplicate the batch against authoritative DuckLake heads, including tombstones. Only newly retained UUIDs update identity or schedule live effects.
2. Commit the output reservation in PostgreSQL before the first S3 PUT. The reservation records the attempt's ownership and charges tenant and project capacity. Then stage immutable output objects on S3.
3. Recheck the tenant and project deletion fences in a short PostgreSQL transaction. Release those row locks before DuckLake COMMIT. The writer keeps the advisory session and activity ticket through native return. Deletion intent can fence new work during the commit. Sealing waits for active work and settles its outcome.
4. In one DuckLake snapshot, advance the native writer fence, acknowledge the previous committed PostgreSQL publication checkpoint, and commit the new events, heads, identity changes, usage evidence, and proof.
5. Publish output jobs and cumulative usage idempotently in a PostgreSQL transaction that verifies and settles the original reservation. The fresh path carries its committed binding from staging. It verifies the manifest and uses the uploads' S3 checksum validation for webhook bodies already written by this attempt.

The two PostgreSQL transactions serve different recovery points. A worker that dies after PUT leaves a durable reservation identifying the abandoned prefix. A worker that dies after the native commit leaves the proof and reservation its successor needs for publication. Cold recovery reads the durable binding and verifies the immutable bodies, including their size, checksum, and version when present. Before retiring an orphan reservation, it commits a successor fence and checks native history for proof or absence. Cold recovery, imports, mutations, and deletion can still use separate native fence and acknowledgment commits.

With verified DuckLake versions, a worker caches the last snapshot whose proof sequences passed a full audit. Across up to 64 snapshots, it checks PostgreSQL catalog metadata and validates newly appended proof sequences. A cold engine, missing metadata, proof deletion or compaction, and a project seal require a full audit. Cached validation assumes the retained Parquet objects stay immutable.

Delivery and assembly verify the stored bodies they consume. Delivery retries can reach a receiver more than once. Use the stable delivery ID to deduplicate a request whose response was lost. The scheduled body's contents remain fixed after later edits or deletes.

## Imports and mutations

Historical upload requests authorize against the shared project and job state. S3 holds originals, parser state, and serialized rolling-hash state. PostgreSQL stores digests, offsets, and references. Serialized SHA-256 state can contain raw input, so treat it as payload. Commit each import chunk's stable ID and original outcome beside its native changes before advancing its PostgreSQL checkpoint. A retry returns the original outcome rather than reclassifying its committed rows as duplicates. Historical imports skip live webhooks and scheduled receipt-window deliveries.

Explicit event mutations persist a durable intent before success. Immutable S3 objects hold the mutation input. The project writer applies the change once against the expected revision and reconciles the intent after commit. The deletion fence still applies to resumed work. [Correct or delete events](../events.md)

A query deletion stores its selected UUIDs and revisions in one immutable intent. The project writer checks all selected revisions and commits the tombstones and their proof together. Publication cancels pending webhook work for those UUIDs and settles the intent in one PostgreSQL transaction.

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

Background reconciliation finds unpublished live reservations and explicit work queued by ready mutations, import preflight, and reclaimed worker tickets. It yields to a project writer that already holds the lock and rechecks for work after acquiring the lock. Successful live publication removes the reservation in the same PostgreSQL transaction as its checkpoint. A native acknowledgement can lag a durable publication. The next live commit acknowledges that checkpoint in DuckLake, and deletion sealing explicitly acknowledges the final checkpoint. Recovery settles pending work before removing its queue entry. Once recovery settles, an idle project causes no DuckLake commits or S3 writes from reconciliation. An upgrade from control schema 21 queues a one-time check of existing effect streams because those native acknowledgements were recorded only in DuckLake.

Hosted maintenance schedules compaction and history cleanup under the project write fence. Cleanup rewrites files with deleted rows, expires eligible snapshots, and reclaims obsolete files after the committed deletion schedule passes the restore floor and configured grace. S3 and webhook requests accepted by a provider can outlive the worker. An object written after reclamation may fall outside the cleanup inventory. Publication requires a valid native proof or current output claim. [Recovery runbook](../reference/shared-deployment.md#recovery-and-retention) · [Cleanup bounds and status](../reference/ducklake-cleanup.md#hosted-profile)

For earlier measurements, see the [flush cost record](archive/hosted-flush-cost.md). The [changelog](../../CHANGELOG.md) records differences between releases.

## Upgrade worker activity tickets

Drain older workers before upgrading from v0.15.0. Tickets created before migration 20 have a NULL incarnation and never expire automatically. If a ticket remains, block all pre-upgrade executors from restarting and verify that provider-side uploads and deliveries have settled. A database operator can remove the verified ticket by project and activity ID with `incarnation_id IS NULL`. No public endpoint clears tickets. [Upgrade guide](../upgrades.md#upgrade-from-v0150)

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
| `event_deletions` | PostgreSQL | Exactly once | Project-scoped idempotency, match count, and state in `hosted_mutation_intents`. |
| `pipeline_retired_destinations` | PostgreSQL | At least once | Hosted attempts retain destination authority for cleanup. |
| `pipeline_retired_files` | PostgreSQL | At least once | Hosted attempt records retain ownership evidence. |
| `pipeline_s3_files` | PostgreSQL | At least once | File and upload references. |
| `pipeline_s3_runs` | PostgreSQL | At least once | Pipeline/window attempts and snapshots. |
| `pipeline_s3_state` | PostgreSQL | Exactly once | Conditional cursor update. |
| `pipelines` | PostgreSQL | Exactly once | Definitions, creator, encrypted destination secrets. |
| `posthog_import_person_state` | DuckLake | Exactly once | Mappings commit with profiles and snapshots. |
| `project_deletions` | PostgreSQL | Exactly once | Durable project fence. |
| `project_queue_usage` | Removed | Best effort | Hosted worker buffer tracks pending events and bytes per project. Standalone journal table added in v0.19.3. |
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
