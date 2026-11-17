# Scope and roadmap

> Hosted direction superseded on 17 November 2026: use the [stateless hosted architecture](hosted-architecture.md) for new work. The owner-local queue proposal below records the previous design and the current unreleased implementation; it is not the hosted target. Published v0.14.0 capabilities remain identified separately.

## Unreleased

- Tenant ownership for projects, with one default tenant on fresh installations. Tenant administrators manage their team's projects, memberships, and independent service credentials. Members can receive read-only or finer grants on selected projects, or tenant-local access including future projects. Instance operators retain hard budgets and deployment controls. [Team access](../reference/tenant-access.md), [tenant guide](../reference/tenants.md)
- Email OTP and optional trusted Google/OIDC sign-in authenticate the same invited humans. Operator-only identity recovery lists and unlinks provider bindings while revoking prior interactive connections and browser proofs. [Identity recovery](../reference/identity-recovery.md)
- Monthly ingestion allowances shared by a tenant's projects. Counts commit atomically with newly accepted canonical rows, including imports and identity controls. Duplicate, invalid, ignored, and rolled-back rows consume nothing; deletion does not refund usage. Periods are UTC calendar months, with preserved history and no manual reset endpoint.
- Shared tenant limits for interactive query concurrency, execution time, result rows, and result bytes. Effective limits cannot exceed deployment ceilings and are discoverable through the project schema. [Query limits](../reference/analytics.md#query-limits)
- Operator ingestion suspension and resumable import checkpoints when an allowance is exhausted. These technical controls contain no pricing or billing logic.
- Optional [shared PostgreSQL deployment](../reference/shared-deployment.md): shared control metadata and DuckLake catalogs, one persistent SQLite volume per worker, and S3 Parquet. Public entry points forward owner-bound requests internally; delivered SQL can run on any worker. Event payloads do not enter operational PostgreSQL, and PostgreSQL DuckLake data inlining is disabled.
- Cumulative usage publication and cached shared ingestion policies on a ten-second cycle, with fail-closed stale policies and soft quota overshoot across owners. Tenant interactive query places are reserved immediately in PostgreSQL, with per-process protection and fixed safe expiry after a failed release.
- Bounded native PostgreSQL metadata settings and schema-visible I/O allowances. Shared interactive queries use a fail-stop watchdog if native work remains after cancellation and its safety grace: exit `70`, followed by supervisor restart on the same persistent volume. Worker status distinguishes shared control and registration checks. This is not a guarantee that every SQL timeout returns an HTTP error immediately.
- Stable project-owner/volume identity, owner-down `503` responses, and explicit `delivery.pending_unknown` on non-owner query responses. Shared cleanup preserves at least eight days of snapshots and obsolete-file grace. Automatic owner failover and native whole-instance backup/restore are not supported in shared mode.
- Read-only MCP tools `porcupine_schema` and `porcupine_query` for analysis. Clients can approve them without approving the generic `porcupine_call`. [MCP](../reference/mcp.md)
- Removed inert ClickHouse-era API fields: `use_cache` in `query-events` (now rejected), `sql.result_cache` and the zero scan-quota limits in `get-event-schema`, and the zero read and temporary-disk limits in exports.
- Versioned schema migrations for the journal, local control and shared control databases. Newer or unknown databases are refused before any change, and restores accept older versions. [Schema versions](../storage.md#unreleased-schema-versions)
- A fresh data directory is required. Pre-tenant SQLite databases are rejected; no legacy migration or compatibility layer is included.

The default remains a single-process SQLite deployment. Optional shared mode is implementation work, not a released hosting guarantee. Hosted billing, tenant-wide rate limiting, replicated acceptance, automatic owner failover, and separately provisioned analytical compute remain future work below. None of these unreleased changes is included in the published v0.14.0 release.

## Available in v0.14.0

- **Capture.** Native JSON, supported PostHog SDK profiles and Segment ingestion. Native, PostHog, and Segment history can be imported from JSONL through one resumable CLI command. PostHog persons files provide profile snapshots. Stable producer IDs support retries. [Inputs](../collect-events.md)
- **Analytics.** Project-scoped DuckDB SQL through API, CLI and MCP. Discover event names, properties, activity, recent examples and sampled property coverage with event profiles. Identify links anonymous activity to a stable actor. [Queries](../query-events.md) · [Event profiles](../reference/event-discovery.md)
- **Delivery.** One-off query exports, individual or batched event webhooks, and scheduled SQL-to-S3 pipelines. [Pipelines](../pipelines.md)
- **Management.** Projects, scoped credentials, human email/OIDC sign-in and service accounts, through the shared API contract and agent tools. [Projects](../reference/projects.md) · [Permissions](../reference/security-and-interfaces.md)
- **Operations.** Local DuckLake by default, optional [analytical S3](../reference/ducklake-s3.md) for fresh deployments, native history cleanup and administrator-visible status. [Scheduled backups](../reference/native-backups.md) support local data files; [independent S3 backups and offline recovery](../reference/native-backup-recovery.md) support both local and analytical S3 data.

## Added in v0.13.0

- Analytical S3 storage with local metadata, pinned storage identity, project prefixes and scoped native credentials. No automatic migration from an existing local archive or age-based native tiering.
- Event profiles with occurrence-based activity, separate receipt times, bounded examples and sampled property statistics.
- Native maintenance no longer pins every resident project at once. Cold project admission and true resident-budget exhaustion have separate error handling.
- Scheduled snapshot expiration, deleted-row rewrites and obsolete-file cleanup, including cold catalogs and reader protection.
- Optional coordinated local recovery bundles, verified inventory, configurable retention, Parquet reuse and a tested isolated restore.
- Release publication depends on the validation workflow and tests the actual native images against HTTPS object storage without runtime extension downloads.

## Added in v0.14.0

- One-command historical imports for native, PostHog and Segment files or directories, including gzip streaming, automatic resume, original occurrence times and stable-ID deduplication. [Import guide](../imports.md)
- Published native schemas and `get-event-input-contract` through API, CLI and MCP. [Event contract](../reference/native-event-contract.md)
- Smaller analytical transactions after a typed native out-of-memory error. Unfinished work stays queued. [Large-event delivery](ducklake-large-batches.md)
- Complete backups to an independent S3 destination and offline recovery through the standalone CLI. [Backup and recovery](../reference/native-backup-recovery.md)
- DuckDB/DuckLake as the only analytical runtime. Explicit acceptance/import/webhook transaction owners, an independent SQL-policy module, and offline CLI contract generation. [Architecture boundaries](native-only-architecture.md)

## Next validation: manage a team through the public API

After the local control/journal split and tenant membership work are verified,
exercise the complete hosted-team journey with test data. This is a planned
acceptance scenario, not a claim that tenant administration has shipped.

- [ ] An instance operator creates a tenant, assigns budgets, and invites its
  first tenant administrator.
- [ ] That administrator connects an agent or the CLI through Porcupine OAuth,
  creates projects, and invites a colleague with read-only project permissions.
- [ ] A team-owned service credential ingests events independently of the human
  who created it. The colleague discovers the data and runs SQL.
- [ ] The team can read its consumption and effective limits. Exhausted budgets
  return documented errors without granting a way to raise operator ceilings.
- [ ] Removing a membership or reducing its permissions takes effect on existing
  tool connections. Another tenant cannot access the team's data or management.
- [ ] Run the journey against standalone SQLite and shared PostgreSQL/S3, including
  worker restart and retry. No step requires SSH or direct database access.

Audit API coverage alongside this journey. A future administration frontend must
use the same authenticated HTTP operations as agents and the CLI. Inventory each
management action, its authorization scope, contract, generated command, and
tests. Include memberships, service credentials, projects, budgets, consumption,
imports and uploads, exports, and pipelines. Identify missing operations explicitly;
tenant deletion and replacement of an existing pipeline's configuration need a
product decision rather than an assumption that CRUD coverage is complete.

Deployment secrets, database connections, storage configuration, and recovery
remain operator-controlled setup unless an explicit management API is designed.
They do not become tenant-editable settings to support a frontend. Require every
registered route to declare its protection or intentional public exposure.

Once this journey passes, validate a separate private commercial service that
provisions tenants, assigns budgets, and reads usage through Porcupine's operator
API. Reuse the published open-source binary without private patches or direct
database writes. Payment integration follows that contract validation.

## Other planned improvements

- Editable documentation for event definitions and their properties, scoped to a
  project and available through API, CLI, MCP, and existing discovery responses.
  Record the author and last modification time. Documentation remains optional
  and never gates ingestion. See the deferred automation ideas below.
- Expand historical adapter coverage from real-world export variants while preserving the common acceptance rules.
- Define a separate native historical person-snapshot encoding against the [event contract](../reference/native-event-contract.md). Whether additional official SDK connectors or a plugin framework are useful remains an open question.
- Scheduled SQL results delivered to webhooks, with optional alert conditions, cooldowns and repeat suppression. Reuse durable runs and destination credentials.
- Operational metrics and measured hosting profiles. Add an external queue or multiple writers only when a deployment need justifies the shared state and failure-model changes.

## Future SaaS architecture to evaluate

The commercial service remains a design candidate, not an implementation
commitment. Its shared-storage foundation is being implemented in the unreleased
open-source [shared deployment](../reference/shared-deployment.md). Keep default standalone
deployment simple; no billing service or hosted availability promise follows from
that implementation.

The candidate shares workers across customer teams instead of running one permanent
instance per customer. A tenant represents a team and contains projects, members,
service accounts, grants, and usage limits. Resolve tenant and project scope from
authenticated access, never from an untrusted event payload.

Shared-deployment foundation and remaining hosted questions:

- Each ingestion worker owns a bounded, durable SQLite queue on its own persistent
  volume. Acknowledge capture only after that queue commits. Workers deliver batches
  to DuckLake and resume unfinished work after restart.
- Shared PostgreSQL holds accounts, grants, usage records, per-project DuckLake
  catalogs, and coordination across workers. It holds no event-payload inbox.
  PostgreSQL-backed DuckLake data inlining is disabled.
- Parquet files live on S3, separated by tenant and project prefixes. Prefixes
  organize storage; authorization and SQL restrictions must enforce isolation.
- Stable project ownership and forwarding make retries from different public
  workers reach the same SQLite receipts. Identity state, import and pipeline
  progress, and jobs remain owner-local. PostgreSQL coordinates analytical writes,
  maintenance, query admission, and project deletion. A queued event must not
  resurrect an event or project deleted through another worker. Automatic owner
  reassignment without recovering its volume remains outside the implementation.

### Usage budgets and hosted control

Keep the technical budget mechanisms in the open-source server and reuse the same
published server in hosted deployments without private patches. Standalone
operators configure their own budgets. The private hosted service owns prices,
subscriptions, invoices, and spending-cap calculations. It reads usage and sets
budgets or explicit suspensions through an authenticated operator API, with no
direct access to Porcupine's database. That API may run on multiple workers behind
the same load balancer. Tenant administrators cannot raise operator-set limits.

The budget scope is the tenant, aggregating all its projects and workers.
Creating another project or credential must not multiply the tenant's allowance.
Optional project ceilings may subdivide that allowance later. Worker and deployment
resource ceilings remain independent safety limits.

The unreleased implementations count new canonical rows at durable acceptance.
Standalone mode enforces allowance exactly in SQLite. Shared mode records cumulative
owner-local counters in that same transaction and publishes them idempotently to
PostgreSQL. Keep shared accounting overhead off the synchronous capture path where
possible without losing accepted usage after a crash.
Track received, pending, stored, and discarded events separately; delivery metrics
must not charge accepted events a second time. Workers compare cached usage with
event budgets, without consulting the private service on each capture.
An event consumes period allowance once; a concurrent query instead reserves a
place and releases it on completion. Shared query admission reserves places
atomically in PostgreSQL; fixed safe expiry recovers reservations after a failed
release. Validate crash recovery before making a hosted guarantee. Retain usage
history when opening a new period.

The unreleased shared implementation uses a fixed ten-second publication and policy
refresh cycle, with an expired-cache refusal when refresh fails. It includes each
owner's unpublished consumption in local decisions. Evaluate configurable intervals,
batch-read costs and staggered refreshes under larger deployments rather than
adapting frequency to each tenant's remaining budget. No per-worker invalidation
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

### Evidence required before a hosted service guarantee

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

Saved parameterized SQL queries and report collections remain on hold. Agents write SQL and use tested [DuckDB recipes](../reference/duckdb-recipes.md). A server-side graph renderer is not a current priority; the client agent can generate graphs where its tools support it.

General aliasing, merging identified users, unlinking, native result caching and finite native receipt history remain deferred. PostgreSQL-backed remote DuckLake catalogs are part of unreleased shared-deployment work, not v0.14.0. The [compatibility matrix](../../compat/matrix.json) records the exact supported SDK behaviors. Feature flags and replay are outside the current product scope.
