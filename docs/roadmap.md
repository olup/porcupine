# Scope and roadmap

## Unreleased

- Tenant ownership for projects, with one default tenant on fresh installations. Explicit project grants may span tenants; there is no new tenant administrator role. Tenant operations, aggregate usage reads, and project creation require `instance:admin`. [Tenant guide](tenants.md)
- Monthly ingestion allowances shared by a tenant's projects. Counts commit atomically with newly accepted canonical rows, including imports and identity controls. Duplicate, invalid, ignored, and rolled-back rows consume nothing; deletion does not refund usage. Periods are UTC calendar months, with preserved history and no manual reset endpoint.
- Shared tenant limits for interactive query concurrency, execution time, result rows, and result bytes. Effective limits cannot exceed deployment ceilings and are discoverable through the project schema. [Query limits](analytics.md#query-limits)
- Operator ingestion suspension and resumable import checkpoints when an allowance is exhausted. These technical controls contain no pricing or billing logic.
- A fresh data directory is required. Pre-tenant SQLite databases are rejected; no legacy migration or compatibility layer is included.

These changes target the existing single-process SQLite deployment. PostgreSQL coordination, distributed quota refresh, hosted billing, and separate analytical compute remain future work below. They are not included in the published v0.14.0 release.

## Available in v0.14.0

- **Capture.** Native JSON, supported PostHog SDK profiles and Segment ingestion. Native, PostHog, and Segment history can be imported from JSONL through one resumable CLI command. PostHog persons files provide profile snapshots. Stable producer IDs support retries. [Inputs](collect-events.md)
- **Analytics.** Project-scoped DuckDB SQL through API, CLI and MCP. Discover event names, properties, activity, recent examples and sampled property coverage with event profiles. Identify links anonymous activity to a stable actor. [Queries](query-events.md) · [Event profiles](event-discovery.md)
- **Delivery.** One-off query exports, individual or batched event webhooks, and scheduled SQL-to-S3 pipelines. [Pipelines](pipelines.md)
- **Management.** Projects, scoped credentials, human email/OIDC sign-in and service accounts, through the shared API contract and agent tools. [Projects](projects.md) · [Permissions](security-and-interfaces.md)
- **Operations.** Local DuckLake by default, optional [analytical S3](ducklake-s3.md) for fresh deployments, native history cleanup and administrator-visible status. [Scheduled backups](native-backups.md) support local data files; [independent S3 backups and offline recovery](native-backup-recovery.md) support both local and analytical S3 data.

## Added in v0.13.0

- Analytical S3 storage with local metadata, pinned storage identity, project prefixes and scoped native credentials. No automatic migration from an existing local archive or age-based native tiering.
- Event profiles with occurrence-based activity, separate receipt times, bounded examples and sampled property statistics.
- Native maintenance no longer pins every resident project at once. Cold project admission and true resident-budget exhaustion have separate error handling.
- Scheduled snapshot expiration, deleted-row rewrites and obsolete-file cleanup, including cold catalogs and reader protection.
- Optional coordinated local recovery bundles, verified inventory, configurable retention, Parquet reuse and a tested isolated restore.
- Release publication depends on the validation workflow and tests the actual native images against HTTPS object storage without runtime extension downloads.

## Added in v0.14.0

- One-command historical imports for native, PostHog and Segment files or directories, including gzip streaming, automatic resume, original occurrence times and stable-ID deduplication. [Import guide](imports.md)
- Published native schemas and `get-event-input-contract` through API, CLI and MCP. [Event contract](native-event-contract.md)
- Smaller analytical transactions after a typed native out-of-memory error. Unfinished work stays queued. [Large-event delivery](ducklake-large-batches.md)
- Complete backups to an independent S3 destination and offline recovery through the standalone CLI. [Backup and recovery](native-backup-recovery.md)
- DuckDB/DuckLake as the only analytical runtime. Explicit acceptance/import/webhook transaction owners, an independent SQL-policy module, and offline CLI contract generation. [Architecture boundaries](native-only-architecture.md)

## Other planned improvements

- Editable documentation for event definitions and their properties, scoped to a
  project and available through API, CLI, MCP, and existing discovery responses.
  Record the author and last modification time. Documentation remains optional
  and never gates ingestion. See the deferred automation ideas below.
- Expand historical adapter coverage from real-world export variants while preserving the common acceptance rules.
- Define a separate native historical person-snapshot encoding against the [event contract](native-event-contract.md). Whether additional official SDK connectors or a plugin framework are useful remains an open question.
- Scheduled SQL results delivered to webhooks, with optional alert conditions, cooldowns and repeat suppression. Reuse durable runs and destination credentials.
- Operational metrics and measured hosting profiles. Add an external queue or multiple writers only when a deployment need justifies the shared state and failure-model changes.

## Future SaaS architecture to evaluate

This is a design candidate, not an implementation commitment. No SaaS work starts
with this roadmap entry. Keep the standalone deployment unchanged.

The candidate shares workers across customer teams instead of running one permanent
instance per customer. A tenant represents a team and contains projects, members,
service accounts, grants, and usage limits. Resolve tenant and project scope from
authenticated access, never from an untrusted event payload.

Proposed layout:

- Each ingestion worker owns a bounded, durable SQLite queue on its own persistent
  volume. Acknowledge capture only after that queue commits. Workers deliver batches
  to DuckLake and resume unfinished work after restart.
- Shared PostgreSQL holds accounts, grants, usage records, DuckLake catalogs, and
  the coordination needed across workers. Evaluate a separate logical DuckLake
  catalog per project in the shared PostgreSQL service.
- Parquet files live on S3, separated by tenant and project prefixes. Prefixes
  organize storage; authorization and SQL restrictions must enforce isolation.
- Shared state also covers cross-worker deduplication, import and pipeline progress,
  maintenance ownership, and mutation rules. A queued event must not resurrect an
  event or project deleted through another worker.

### Usage budgets and hosted control

Keep the technical budget mechanisms in the open-source server and reuse the same
published server in hosted deployments without private patches. Standalone
operators configure their own budgets. The private hosted service owns prices,
subscriptions, invoices, and spending-cap calculations. It reads usage and sets
budgets or explicit suspensions through an authenticated operator API, with no
direct access to Porcupine's database. That API may run on multiple workers behind
the same load balancer. Tenant administrators cannot raise operator-set limits.

The proposed default scope is the tenant, aggregating all its projects and workers.
Creating another project or credential must not multiply the tenant's allowance.
Optional project ceilings may subdivide that allowance later. Worker and deployment
resource ceilings remain independent safety limits.

The unreleased single-process implementation counts new canonical rows at durable
acceptance. Preserve that accounting definition when evaluating distributed workers.
Keep shared accounting overhead off the synchronous capture path where possible,
using idempotent batch publication without losing accepted usage after a crash.
Track received, pending, stored, and discarded events separately; delivery metrics
must not charge accepted events a second time. Workers compare cached usage with
event budgets, without consulting the private service on each capture.
An event consumes period allowance once; a concurrent query instead reserves a
place and releases it on completion. Coordinate query admission atomically across
workers, with tested crash recovery. Retain usage history when opening a new period.

Use a fixed, configurable cache refresh interval rather than adapting it to each
tenant's remaining budget. Start evaluation at 10 seconds. Batch reads for active
tenants on each worker and stagger refreshes across workers. Counter publication
must be frequent enough to make those reads useful. No per-worker invalidation
endpoint is required initially. This cache policy applies to usage budgets, not
credential revocation.

Accept soft event-quota overshoot from accounting and cache delays. Combine this
with a tenant-wide rate limit across workers. The discussed example of 100 events
per second is illustrative, not a committed free-plan allowance. Measure bursts
and total enforcement lag before making a bound or spending-cap promise. Bound
queue bytes and admission rates independently of commercial quotas. An unavailable
or full queue must not return success for events it cannot retain. Continue
processing events already durably accepted when new ingestion is suspended.

### Separate analytical compute to validate later

Keep this as an open architecture question, not an implementation task. Prefer
bounded interactive queries and queued asynchronous queries over speculative
pre-execution cost prediction. Interactive execution needs deadlines that cancel
the computation, concurrency limits, and bounds on both output rows and bytes.
Deferred queries may use larger budgets, with tenant fairness and durable results.

Validate how the same analytical executor can serve standalone installations and
the future hosted platform without private patches. A common job contract could
carry the query, parameters, authorized project, resource limits, and result
destination. Keep provider-specific compute provisioning in the hosted scheduler.
Do not choose Lambda, a container platform, or a VM platform at this stage.

Questions to resolve:

- What local execution and isolation can the standalone deployment support without
  requiring external services? Local writable catalogs cannot be assumed to support
  arbitrary access from separate processes.
- How does a remote executor access the DuckLake catalog and managed files?
  Parquet on S3 alone does not provide remote catalog access.
- Which responsibilities belong to the shared executor, task coordination, and
  infrastructure scheduler? Keep tenant authorization, retries, cancellation, and
  result publication consistent across deployment modes.
- How are ingestion and interactive queries protected from deferred CPU, memory,
  and I/O demand? Separate compute still shares catalog and object-store capacity.

Before selecting an architecture, run the same task locally and on an external
worker with the same analytical code. Compare results and test project isolation,
revocation, cancellation, retries, crash recovery, and concurrent edits or deletes.
Existing asynchronous exports provide a starting point, not proof of separate
compute support. Leave the implementation and deployment choices for later work.

### Limits and open decisions

Adding workers distributes new traffic without moving existing Parquet files.
Removing a worker requires draining its queue or transferring its volume with
exclusive ownership. A worker crash delays pending events until its volume is
recovered. Permanent volume loss loses acknowledged events not yet delivered.
This design does not provide replicated acceptance or immediate failover for a
local queue. Stronger durability would require replicated storage or a replicated
queue before acknowledgment.

PostgreSQL becomes a shared dependency. Catalog contention, small batches, S3
requests, cache misses, and maintenance may limit throughput or increase cost.
More workers increase concurrent capacity; they do not automatically distribute
one DuckDB query across machines. Shared native processes also need resource
budgets and protection against noisy tenants.

### Evidence required before implementation or a service guarantee

- [ ] Measure sustained ingestion, queue growth, end-to-end freshness, and recovery
  with concurrent queries and maintenance. Include many quiet tenants and one busy
  tenant. Report hardware, batch sizes, resource use, and cost per volume ingested.
- [ ] Retry identical events through different workers, including crashes between
  queue commit, DuckLake commit, usage recording, and queue acknowledgment. Verify
  stable IDs, deduplication, and no double accounting.
- [ ] Test concurrent identify, edits, deletes, and project deletion while older
  events remain queued elsewhere. Verify ordering rules and no resurrection.
- [ ] Kill workers, interrupt PostgreSQL and S3, and restart with pending work.
  Exercise drain, volume transfer, and lost-volume scenarios. Verify exclusive
  ownership and document the resulting delay or data loss.
- [ ] Verify tenant and project isolation across SQL, discovery, credentials,
  caches, imports, pipelines, backups, and object access.
- [ ] Test quota overshoot, backpressure, per-tenant fairness, and queue capacity.
  Verify that delayed metering cannot exhaust shared resources.
- [ ] Measure the proposed 10-second grouped refresh with many active tenants and
  workers, including counter-publication lag and synchronized refresh bursts.
- [ ] Verify that multiple projects, credentials, and workers share the same tenant
  allowance. Test query-slot recovery and separation of operator and tenant rights.
- [ ] Exercise concurrent catalog access, maintenance ownership, snapshot cleanup,
  and coordinated recovery of PostgreSQL metadata and S3 data.

## Deferred

Automatic documentation from SDK payloads, competing-description consolidation,
codebase-driven documentation refresh, and proactive documentation reminders remain
on hold. The editable event and property documentation above is separate planned work.

Saved parameterized SQL queries and report collections remain on hold. Agents write SQL and use tested [DuckDB recipes](duckdb-recipes.md). A server-side graph renderer is not a current priority; the client agent can generate graphs where its tools support it.

General aliasing, merging identified users, unlinking, remote DuckLake catalogs, native result caching and finite native receipt history remain outside this release. The [compatibility matrix](../compat/matrix.json) records the exact supported SDK behaviors. Feature flags and replay are outside the current product scope.
