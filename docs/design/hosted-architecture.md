# Stateless hosted architecture

Decision record, 17 November 2026. **Target, not implemented.** The published release is v0.14.0. Current shared deployments still require owner-local SQLite volumes. This design replaces the hosted proposal in [the earlier SaaS notes](saas-and-compute-plans.md#future-saas-architecture-to-evaluate); it does not change the standalone durability contract.

## Two acceptance contracts

| Contract | Standalone | Hosted target |
| --- | --- | --- |
| Live success response | SQLite journal committed | Validated and admitted to bounded worker memory |
| Durable event data | Journal, then DuckLake | DuckLake commit, with Parquet on S3 |
| Crash before analytical commit | Recover accepted work from the journal | Buffered accepted events can be lost |
| Retry response | Stable original `event_id` | No synchronous promise of an original `event_id` |
| Deduplication and identity | Journal transaction | Serialized DuckLake flush transaction |
| Historical import checkpoint | Journal transaction | PostgreSQL checkpoint after durable DuckLake commit |
| Persistent worker volume | Required | Not required; caches and scratch space are disposable |

Normalization, validation, project authorization, canonical event types, SQL policy and the `lake` executor remain shared. The commercial service remains an API client of the same open-source server. No pricing rules enter the event executor.

The [replaceable built-in and control-plane contracts](control-plane-contracts.md) define the complementary tenant-deletion, external-authentication and usage work. Built-in self-hosted components remain available; the commercial application is separate.

### D1: Workers without fixed project owners

Any hosted worker can ingest, query or maintain any permitted project. PostgreSQL owns control state and DuckLake catalogs; S3 owns event data. Remove owner routing, volume registrations and owner-local operational state only after their replacements pass the inventory below. A replica count change must not require moving a journal or reassigning projects.

### D2: Bounded memory acceptance

Proposed flush thresholds are two seconds or 5 MiB per project batch, whichever occurs first. These values are proposed defaults, not published guarantees. Also bound total worker buffer bytes/events and simultaneous flushes: a per-project threshold alone does not bound total process memory.

Validate the complete request and reserve its memory capacity before acknowledging it. A full, draining or failed-flush worker refuses new ingestion with `503` and `Retry-After`. Retain failed batches in bounded memory and retry with backoff; do not copy them to PostgreSQL as an event queue. Report pending bytes, oldest age, flush duration/failures, backpressure and commit conflicts.

The flush timer does **not** establish a maximum loss window. Scheduling and commit latency add to it; a storage outage can leave already acknowledged events pending indefinitely. Memory capacity bounds the outstanding volume, not its age. Describe normal-operation freshness separately from loss during outages. Never advertise a two-second worst-case loss guarantee based only on a two-second timer.

On SIGTERM, stop admission, drain in-flight handlers and flush buffered work before exit. The orchestrator's grace period must exceed the configured drain deadline plus cleanup time. A forced kill or grace-period expiry can lose buffered work. Rolling-deployment zero-loss is a tested operating condition with healthy dependencies and adequate grace, not an unconditional property.

### D3: Deduplication and effects at flush

Under the project commit guard, deduplicate within the batch and against authoritative `heads`, including deleted heads. Retain first-write-wins and tombstones. Identity changes, usage and webhook scheduling occur only for newly retained UUIDs. Hosted mode has no synchronous receipt ledger.

DuckLake and the application's PostgreSQL connection do not automatically share one transaction. A crash after a DuckLake commit but before usage/outbox publication must not silently lose billing counts or webhooks. Before phase 4, prove a recoverable commit/effect protocol: retain a durable commit identifier and its newly accepted count/effect references with the DuckLake commit, then publish PostgreSQL jobs/usage idempotently and mark reconciliation. Scan unfinished commits after worker loss. Do not rely on an in-memory list of post-commit callbacks.

Operational jobs contain identifiers, status and object references. Existing webhook bodies, import tails and mutation snapshots are event payloads; moving their current SQL rows verbatim into PostgreSQL would violate the no-event-payload requirement. Persist needed immutable bodies on S3 and reference them, or reconstruct from a retained immutable analytical version. Test that edits and snapshot cleanup cannot change an already scheduled webhook's body. Keep Postgres DuckLake data inlining disabled; catalog statistics can still contain derived values and belong within the data-security boundary.

The owner selected the same native route, with an explicit `durability: "durable" | "buffered"` response. Standalone retains its current response and adds only `durability: "durable"`. Each buffered receipt contains only `message_id` and `uuid`: omit both `event_id` and the existing per-receipt `duplicate` boolean. Omit the aggregate duplicate count or report it as unknown, never as a fabricated zero. Make `event_id` optional in the schema and describe each instance's acknowledgment in `get-event-input-contract` and its published native schema. Add contract tests for both modes; PostHog and Segment need separate compatibility response tests.

### D4: One project commit guard

Use a short PostgreSQL advisory lock on a dedicated session as the initial candidate for serializing hosted writes. Inserts, identity changes, mutations, project deletion and maintenance must follow the same ordering. Read queries use stable snapshots and reader protection, not the write lock for their full duration.

Compare this with native optimistic commits plus conflict retry before choosing the final mechanism. Measure contention, wait time and retries. If the session or a renewable lease is lost, stale work must not publish effects. A lease timeout alone does not stop a native call; retain native transaction conflict checks and a fencing design. Do not replace fixed project ownership with an unfenced expiring boolean.

### D5: PostgreSQL recovery and S3 retention

Hosted backup uses managed PostgreSQL recovery and S3, rather than the standalone bundle tool. Control and DuckLake metadata can use different connection URLs today: require a single recovery-consistent PostgreSQL cluster/timepoint initially, or prove a coordinated recovery barrier for separate clusters. Retain the deployment configuration and pipeline encryption-key versions separately in the deployment secret source; PostgreSQL/S3 data alone cannot decrypt destinations after a lost key.

Require obsolete-file grace to cover the PostgreSQL restore window, maximum reader duration and a safety margin. Validate configured policy relationships at startup and verify effective provider lifecycle/PITR policies. Apply protection to every deletion path, including project removal and orphan cleanup, and every referenced immutable object: Parquet, mutation bodies, upload/parser state and webhook bodies.

[DuckLake cleanup](https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_of_files) ages files from when they were scheduled for deletion. Snapshot expiration alone does not remove them. Retaining those objects readable for the recovery window is the simplest candidate for restoring PostgreSQL against an unchanged bucket.

[S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeleteMarker.html) preserves versions but a current delete marker still makes an unversioned GET return `404`. A versioning-based recovery therefore needs explicit version/delete-marker restoration unless all referenced keys remain readable. Test the actual selected provider and pinned DuckLake version, not just catalog SQL.

Restore PostgreSQL to a pre-mutation time after real edits, deletion and cleanup, leave the bucket unchanged, and query the restored data. Fence old writers and pause outbound jobs before recovery validation. A job already delivered after T may reappear after restoring T; preserve receiver idempotency IDs and do not claim recovery can undo an external send. Reapply subsequent credential revocations and deletion instructions, or rotate/revoke recovered access before serving traffic. Also test isolation, project deletion and preserved encryption keys. This is an acceptance test, not an existing guarantee.

### D6: Renew query execution permits without process exit

Replace the deliberate exit-70 watchdog with context interruption, a renewable execution permit and project-engine health. Renewal continues independently of the HTTP deadline until the native call returns. Release only on actual return; reject new work on an engine that stays blocked while allowing healthy projects to continue.

The owner selected **automatic expiry**, accepting physical concurrency overshoot if a worker loses PostgreSQL while its native call keeps running. That overshoot can last until the native call returns or its executor terminates; the lease TTL does not bound it. Renew while connectivity is healthy, cancel and quarantine after renewal failure, and let the token-scoped lease expire when it cannot be renewed. Never describe expiration as proof that native work stopped. Apply this policy to admission, deletion and shutdown consistently. Readiness/status identifies affected project engines while healthy projects can continue; the orchestrator chooses a restart.

### D7: Limits

Keep tenant budgets, interactive time/row/byte limits and worker memory bounds. Enforce infrastructure rate limits at a gateway using the authenticated tenant/key identity. Hosted usage is counted after retained flush commits; cached budget decisions can overshoot across workers. The stopped SQLite project-counter experiment is not a hosted dependency.

### D8: Roles later

Keep one server binary for now. Separate acceptance, analytical execution and operational job responsibilities in code; do not implement deployment-role switches or choose a serverless provider in this phase. Budget DuckDB memory, Go buffers, cache and container headroom together. DuckDB's setting is not a hard process-RSS cap.

### D9: Standalone keeps its journal

Keep durable SQLite acknowledgment, stable receipts and existing transaction owners. Phase 2 is merged on main in PR #56, unreleased: the default receipt retention is zero. Explicit nonzero pruning remains optional and describes its historical DuckLake dependency. Disabling retention cannot recreate receipts already pruned. Compact-receipt and queue-counter experiments are preserved separately, not shipped or required for this target.

### D10: Repository contracts

Keep standalone and hosted acceptance guarantees explicit in `AGENTS.md`, API discovery, CLI/MCP descriptions and user documentation. A hosted design document does not make buffered acceptance available. No event staging in PostgreSQL, DuckLake data inlining, broker, new service, authentication-mode removal or CGO requirement for the standalone CLI is introduced by this plan.

## Complete journal disposition

Source: `internal/porcupine/journal_schema.sql` and all numbered `journalSchema.steps`. Targets below describe hosted mode only. Guarantees concern recovery after a successful durable commit; live memory admission before that point remains best effort. Each row below has a recovery class:

- **Exactly once:** one committed logical state/effect per authoritative key and guarded transition. This does not add request idempotency to every management endpoint.
- **At least once:** resumable job/attempt processing with stable keys and idempotent checkpoints/publication. Finite retry policies can end in an explicit terminal failure; this is not unconditional successful delivery to an unavailable receiver.
- **Best effort:** only bounded volatile work exists before the native commit. Its outstanding volume is bounded, not its age during an outage.

<!-- journal-inventory:start -->
| Journal table | Hosted target | Recovery class | State and recovery rule |
| --- | --- | --- | --- |
| `access_keys` | PostgreSQL | Exactly once | Authoritative credential state; remove the local shadow. Revocation remains live. |
| `cookieless_keys` | PostgreSQL | Exactly once | One durable daily secret shared by workers. |
| `delivery_quarantine` | Removed | Best effort | Failed live flushes stay bounded in memory, with diagnostics and the stated loss contract. |
| `delivery_ready` | Removed | Best effort | Memory admission/flush scheduling replaces this local index. |
| `event_heads` | DuckLake | Exactly once | Authoritative UUID, revision, original ID and tombstone under the project guard. |
| `event_receipts` | Removed | Best effort | No synchronous hosted receipt; post-commit deduplication uses heads. |
| `events` | Removed | Best effort | No hosted SQL inbox; memory until durable DuckLake/S3 commit. |
| `export_files` | PostgreSQL | At least once | Multipart/object metadata references S3 output; idempotent publication/cleanup. |
| `export_jobs` | PostgreSQL | At least once | Durable authorization, query, analytical snapshot and attempt state. |
| `identity_links` | DuckLake | Exactly once | Versioned links per retained UUID in the guarded commit. |
| `identity_profiles` | DuckLake | Exactly once | Same transaction as links; duplicates have no identity effect. |
| `import_upload_files` | PostgreSQL | At least once | Durable offsets/checksums; bytes, parser tails and serialized rolling hashes on S3. |
| `import_upload_samples` | PostgreSQL | Exactly once | Bounded line/reason records keyed by job/file/row, without event bodies. |
| `import_uploads` | PostgreSQL | At least once | Stable chunk identity and original outcomes survive native commit/checkpoint retries. |
| `metadata` | PostgreSQL | Exactly once | Durable source/schema/job keys; remove local routing/cache keys after classification. |
| `mutation_audit` | PostgreSQL | Exactly once | Durable mutation ID, expected revision, request hash, actor and application state. |
| `mutation_outbox` | PostgreSQL | At least once | Durable intent before ACK; immutable S3 body reference, then replay-safe application. |
| `pipeline_s3_files` | PostgreSQL | At least once | Durable object/upload references and cleanup state. |
| `pipeline_s3_runs` | PostgreSQL | At least once | Unique pipeline/window attempts with retained analytical snapshots. |
| `pipeline_s3_state` | PostgreSQL | Exactly once | Cursor advances conditionally after successful publication. |
| `pipelines` | PostgreSQL | Exactly once | Authoritative definition state, scoped creator and encrypted destination secrets. |
| `posthog_import_person_state` | DuckLake | Exactly once | Mapping/profile versions commit with snapshots; PG holds the import checkpoint. |
| `project_deletions` | PostgreSQL | Exactly once | Durable global deletion fence; cleanup retries cannot recreate a catalog. |
| `projects` | PostgreSQL | Exactly once | Authoritative tenant ownership; remove local mirrors. |
| `queue_usage` | Removed | Best effort | Worker-memory accounting, never reconstructed from analytical totals. |
| `tenant_usage` | PostgreSQL | Exactly once | One posting per retained commit; preserved periods, no refund on deletion. |
| `tenants` | PostgreSQL | Exactly once | Authoritative budgets/permissions; remove local mirrors. |
| `webhook_cooldowns` | PostgreSQL | At least once | Shared retry/backoff state across attempts. |
| `webhook_deliveries` | PostgreSQL | At least once | Proposed external contract: stable ID, immutable body reference, bounded attempts and explicit failure. |
| `webhook_members` | PostgreSQL | Exactly once | Event/revision membership prevents double scheduling and supports cancellation. |
| `webhook_queue_usage` | PostgreSQL | Exactly once | Transactional pending output-job counts, distinct from memory acceptance. |
| `shared_tenant_cache` | Removed | Best effort | Disposable worker cache, never durable truth or revocation authority. |
| `project_deletion_files` | PostgreSQL | At least once | Durable object/upload cleanup inventory respecting restore retention. |
<!-- journal-inventory:end -->

SQLite's generated `sqlite_sequence` disappears with the journal; it is not an application table. Hosted event identity allocation and exported positions must be independent of worker-local integer sequences.

Import checkpointing must recover the **original** chunk outcome, not reclassify its prior accepted rows as duplicates after a crash. Commit a stable chunk identity (job/file/offset/content hash), accepted/duplicate/ignored/unresolved/invalid counts and bounded sample references alongside the native changes; then compare-and-set the PostgreSQL offset/checkpoint. Retain this evidence until publication is reconciled, including project deletion. Test crashes after native commit, concurrent retry and changed chunk content. Historical imports remain excluded from live webhooks and automatic receipt-window S3 pipeline delivery.

Serialized SHA-256 state contains a partial raw input block, not just a digest. Keep it with immutable upload/parser objects on S3 or recompute it from retained chunks. Only final digests, offsets and references belong in PostgreSQL.

### Other durable local state

The analytical `registry.sqlite` is also authoritative today. Move its `source`, `catalogs`, `deleting_projects`, `cleanup_progress` and `storage_configuration` responsibilities to PostgreSQL, reusing existing `pc_native_sources`/`pc_native_projects` where suitable. Local registry inode checks cannot be a hosted durability requirement.

Keep shared browser sessions, email budgets, credentials and query permits in PostgreSQL. Retire `shared_project_owners`, persistent-volume registrations, routing nonces and worker-local cumulative usage only when their consumers have been replaced. Temporary worker identifiers can remain for diagnostics and leases; they must not impose project ownership. Replace export/pipeline SQLite event and mutation watermarks with retained analytical snapshots.

## Phases and evidence

1. **Phase 0, this record.** Complete disposition and explicit unresolved decisions. Update repository rules and the roadmap; no buffered runtime ships here.
2. **Phase 1, independent.** Remove exit 70 with the selected automatic-expiry policy. Simulate native code that ignores cancellation: process survives, healthy projects run, a healthy heartbeat keeps the permit until actual return, and missing heartbeats permit expiry with the stated overshoot risk.
3. **Phase 2, complete on main, unreleased.** PR #56 defaults standalone receipt retention to zero, retains documented opt-in purge, and tests fresh and duplicate events with analytics unavailable.
4. **Phase 3.** Two acceptance implementations behind the shared validated-batch boundary, selected explicitly by configuration. Pin native/vendor response contracts before adding the hosted implementation.
5. **Phase 4.** Flush under the project guard, replay-safe identity/usage/output scheduling, bounds, metrics and graceful drain. Kill at every commit/effect boundary; prove retries do not duplicate effects.
6. **Phase 5.** Remove hosted local state using the complete inventory. Imports, management, jobs, mutations and cleanup work on any worker without a persistent volume.
7. **Phase 6.** Validate restore-window settings and exercise PostgreSQL point-in-time recovery against the real bucket after deletion/cleanup.
8. **Phase 7.** Measure multi-worker/tenant real-S3 load, kill/restart and rolling replacement, memory, freshness, maintenance query latency, requests/Parquet sizes/compaction and cost per million events. Explicitly pass/fail the requested healthy-load crash-loss-age target against the flush interval; a failure requires revising the advertised SLO with the owner. Separately report outage loss volume and oldest age, for which the timer is not a hard bound.

Every phase has a focused PR with before/after evidence and remaining limits. Compare performance only under stated hardware/cache/load conditions. Role-specific deployment remains deferred.

## Owner decisions before dependent code

- Native route and response are decided above; pin the HTTP status and complete schemas in phase 3 contract tests.
- Public freshness/loss wording and flush/drain defaults. Two seconds/5 MiB are proposed operational thresholds, not hard loss bounds.
- Hosted webhook at-least-once guarantee and acceptable delay; immutable output bodies must outlive their job.
- Explicit edits/deletes are decided: durable intent before acknowledgment, then replay until application or an explicit terminal error. Stage immutable edit bodies on S3 and persist their reference before acknowledging.
- Native permit recovery is decided: automatic expiry with accepted physical-overshoot risk during a PostgreSQL partition.

## Documentation fact ledger

| Source | Destination | Preserved or replaced fact |
| --- | --- | --- |
| Earlier SaaS notes, future architecture | This record, D1–D10 and disposition | Replaces owner-local hosted journals with the new target; preserves the old design as historical evidence. |
| `AGENTS.md`, durable acceptance rule | Mode-specific rule plus this record | Keeps every current/standalone durable guarantee; future hosted admission has a separate explicit contract. |
| Roadmap, hosted unit and operational protections | Roadmap link to this record | Marks compact receipts/project counters as stopped experiments and keeps load, fairness and recovery validation. |
| User architecture note | This record, decisions and owner questions | Preserves the decisions, adds the complete source-table inventory, and separates implementable guarantees from unproven time/lease/restore claims. |

No reference guide is removed. This is a design target, not a release or capacity announcement.
