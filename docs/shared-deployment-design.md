# Shared metadata and owner-local event queues

**Implementation in progress, unreleased.** PostgreSQL stores control metadata and
DuckLake metadata. Event payloads go from the owning worker's SQLite queue directly
to DuckLake and its S3 data files. No PostgreSQL event inbox is introduced.

Two designs were compared. A recoverable PostgreSQL payload handoff was rejected
because it puts events into PostgreSQL before DuckLake. The selected design uses
durable project ownership and internal forwarding. It accepts owner affinity and
the need to recover that owner's volume after failure. It does not promise
automatic failover of an acknowledged but undelivered event.

## Problem and caller usage

Keep the SQLite queue as the real acceptance authority. A shared deployment has many
Porcupine workers, but exactly one durable owner for each project. Any public worker
can receive a request. Project operations that depend on accepted-but-undelivered
state are forwarded to that owner; delivered SQL can run on any worker.

Local deployment remains:

```text
PORCUPINE_DB_PATH=/data/porcupine.db
PORCUPINE_DUCKLAKE_DIRECTORY=/data/lake
```

Shared deployment adds:

```text
PORCUPINE_CONTROL_DATABASE_URL=postgres://.../porcupine
PORCUPINE_WORKER_ID=eu-1
PORCUPINE_WORKER_PUBLIC_URL=https://worker-1.internal
PORCUPINE_CLUSTER_AUTH_FILE=/run/secrets/cluster-routing-key
PORCUPINE_DUCKLAKE_CATALOG_URL=postgres://.../analytics
PORCUPINE_DUCKLAKE_S3_BUCKET=...
```

The worker ID is bound to its durable SQLite volume, not to a replaceable process.
Restarting that worker reuses its volume. Adding a worker immediately admits new
projects there. Clients still send their existing HTTP, MCP, CLI or SDK requests to
one load-balanced public address. No public ownership parameter is accepted.

## Shape and core signatures

```go
type ControlStore interface {
    ResolvePrincipal(ctx context.Context, credential Credential) (actor, error)
    Project(ctx context.Context, id string) (ProjectAccess, error)
    Route(ctx context.Context, project string) (ProjectOwner, error)
    NewProjects(ctx context.Context, spec ProjectSpec) (Project, error)
    TenantPolicy(ctx context.Context, tenant string) (TenantPolicy, error)
    PublishUsage(ctx context.Context, report UsageReport) error
    AcquireQuery(ctx context.Context, scope QueryScope, limits QueryLimits) (QueryLease, error)
    RenewQuery(ctx context.Context, lease QueryLease) error
    ReleaseQuery(ctx context.Context, lease QueryLease) error
    BeginProjectDeletion(ctx context.Context, project string) (DeletionFence, error)
    // Human, credential, OAuth and short-lived browser session operations.
}

type ProjectOwner struct {
    WorkerID string
    RouteURL string
    Generation uint64
    VolumeIdentity string
}

type LocalProjects struct {
    DB *sql.DB // Always SQLite. Owns queue, identities, receipts, imports and jobs.
    WorkerID string
}

type UsageReport struct {
    WorkerID string
    PeriodStart time.Time
    Totals []TenantCumulativeUsage // Monotonic per worker, tenant and period.
}

type ProjectDispatcher interface {
    ServeOwner(w http.ResponseWriter, r *http.Request, owner ProjectOwner)
}
```

Control storage has SQLite and PostgreSQL implementations of domain operations.
SQL does not escape those implementations. Existing authorization remains pure.
There is no generic runtime dialect converter. Local queue/identity/import helpers
can retain SQLite transactions because only the durable project owner calls them.
The dispatcher hides routing, authenticated forwarding and owner-generation checks.
It does not make callers decide whether a transaction is local or shared.

## Ownership map

Shared control owns tenants, policies, projects and immutable owner bindings, human
and service principals, credentials, OAuth tokens/grants, browser OTP/CSRF sessions,
daily cookieless key material, worker registration, usage reports and query leases.

Project-owner SQLite owns accepted originals, receipts, identity state, identity
versioning, mutation CAS/audit, import checkpoints and local upload bytes, webhook
memberships/delivery state, exports and recurring pipeline execution state. Pipeline
configuration may live with these owner-bound jobs in this candidate; CRUD is still
available from every public worker through forwarding. It need not pretend to be
stateless management data.

PostgreSQL DuckLake metadata plus S3 owns delivered analytical state. The source UUID
and project catalog UUID live in shared control, never independently generated in
local registries. Catalog metadata schemas and S3 prefixes are derived from trusted
project identity. Default standalone mode keeps its local DuckDB catalog files.

## Request and durability paths

1. Any worker authenticates against shared control. The credential determines project
   access. Routing comes from trusted project metadata, never the request body.
2. A project-dependent operation resolves its durable owner. An owner-local request
   executes directly. A non-owner forwards the original bounded request to the trusted
   internal owner URL over authenticated transport, retaining the client credential
   for revalidation. External routing headers are stripped. The owner validates the
   internal route token and ownership generation before processing.
3. Owner acceptance is the existing atomic SQLite transaction: deduplication, ID,
   identity controls, event and webhook membership, and durable usage outbox together.
   Only the SQLite commit permits an acknowledgment.
4. The response streams back without changing the public contract. Thus a retry
   arriving at another edge worker reaches exactly the same permanent receipt ledger
   and returns exactly the same event_id.

Globally unique event IDs come from PostgreSQL-reserved ID blocks. A worker writes its
current reservation and next ID to SQLite before using the block, with bounds checked
inside each acceptance transaction. Reserving another block is infrequent. Its owner
executes one project serially, so event.ID remains a valid strictly increasing identity
version within a project. Crashed reservations merely waste numbers. An empty/replaced
volume cannot silently reuse an existing worker identity.

The local default keeps the same allocator interface backed by its SQLite sequence.
IDs remain global BIGINTs; no UUID-as-int hash or probabilistic collision assumption.

## Usage and limits

Each acceptance transaction increments a local cumulative usage value for its tenant
and UTC acceptance month. The publisher sends a bounded report at the configured
interval, initially ten seconds. PostgreSQL locks the report row, compares previous
cumulative usage and applies only a positive delta to aggregate tenant_usage. It
persists report watermark and aggregate increment in one transaction. Retrying after
an ambiguous commit cannot double count. Month history is retained. Event/project
deletion does not remove the worker/tenant report or refund usage.

The owner refreshes policy and aggregated usage in one bounded query. It adds local
unpublished consumption to its cached shared value, avoiding the worker's own
underestimate. Several owners serving one tenant may overshoot within the permitted
report/cache delay. This is the previously accepted soft ingestion budget model.
Policies expire: beyond a configured bounded stale interval new acceptance fails,
while already accepted queue delivery continues. Revocation and deletion fences do
not use this eventual budget cache.

Query admission is shared and immediate. PostgreSQL acquires a row-scoped tenant lock,
checks live leases and inserts an unguessable lease/fencing token with DB-clock expiry.
A heartbeat renews only the same live token. Cancellation or renewal failure interrupts
the native query. Release is conditional on token and idempotent. A query deadline must
end before its reservation can be reused; renewal must never extend an already expired
lease. Per-process engine capacity remains an additional hard ceiling. Local mode uses
the same admission contract with an in-process semaphore.

A shared query/reader lease also protects maintenance cleanup. A separate shared
project writer lock serializes current delete+insert projection, maintenance and
mutation projection. Relying on process-local p.write would permit duplicate heads.
Lock ownership must be held through the native transaction's completion; generation
checks fence stale deletion/project ownership. No expiring lock may be reassigned while
a native operation can still commit under its old generation.

## OAuth, jobs and lifecycle

Browser session/OTP state moves into control storage with compare-and-consume changes.
Thus authorization can start on worker A and finish on B. Hashed OTPs, CSRF and refresh
family reuse semantics remain intact; no session affinity is required for authentication.

Imports, exports, pipelines, quarantine repair and pending event listing are forwarded
to the project owner. This preserves their current local transaction boundaries and
upload checkpoints. The owner-down response is an explicit temporary 503, not a fake
missing job or a new empty import. A public worker is interchangeable as an entry point,
but durable owners are intentionally not interchangeable as storage.

Project deletion first writes a shared deleting fence and removes admission/grants in
control storage. Every worker must check the fence before opening native reads; the
project owner drains local work and purges its SQLite project state and analytical/S3
objects. Shared cleanup state remains until completion. Repeated deletion converges.
A project owner that is offline cannot accept/project until it refreshes ownership and
fences after restart. If its volume is permanently lost, operator-directed cleanup can
remove the project after fencing that identity permanently.

## Failure behavior and accepted costs

This is a valid PostgreSQL deployment, with multi-worker ingestion across projects and
multi-worker queries. It is not automatic failover of a project whose sole durable
SQLite queue volume is unavailable. That limitation follows directly from retaining
only local copies of acknowledged-but-undelivered events. An automatic owner switch
without restoring the old volume would violate the durable acceptance promise.

A worker process may restart freely with the same volume. It does not lose receipts,
identity state, import checkpoints, usage outbox or pending deliveries. A process crash
releases native PostgreSQL locks; expired query reservations become reclaimable.
A worker-volume loss requires restoring the project-owner checkpoint or explicitly
accepting data loss. New projects can use another worker while the old one is down.

We accept an internal request hop and stable owner placement in exchange for avoiding
a PostgreSQL canonical-event payload handoff and keeping the existing strong SQLite
acceptance transaction. A single hot project still writes through one owner. Adding
workers scales the number of projects and analytical reads, not that one writer.

Whole-instance local backup/restore cannot represent this deployment. Reject that mode
when shared control/catalogs are configured. Document coordinated PostgreSQL backup,
S3 retention and per-owner volume checkpoint recovery. Do not let restoring one worker
revoke shared credentials or overwrite cluster metadata.

## Structurally distinct alternatives

A staged PostgreSQL canonical handoff makes any worker accept the same project and
recover another worker's interrupted handoff. It removes owner routing and permits
more flexible writer placement, but it changes the acceptance transaction fundamentally:
canonical identity changes and receipts need recoverable shared prepared records,
local materialization and confirmation. It adds at least one shared transaction to
every batch and temporary shared payload retention. Prefer it if arbitrary-worker
acceptance/failover is a requirement, not merely horizontal capacity across projects.

Moving every queue and job into PostgreSQL simplifies shared state but abandons the
per-worker SQLite queue architecture and increases PostgreSQL write/storage load.
It should not be described as an adapter-only implementation.

Mirroring whole SQLite snapshots under PostgreSQL leases or rewriting SQLite SQL at
runtime is rejected: it hides ownership, creates stale reads and makes distributed
transaction correctness depend on a string adapter.

## File/module map and commit chain

1. Commit design and contracts in docs/shared-deployment.md; introduce config validation,
   control.Store and worker/volume identity contract. Fail invalid partial shared config.
2. Add SQLite and PostgreSQL control stores, explicit schemas and transactional domain
   methods for auth/projects/tenants/humans/tokens, with parameterized cross-dialect tests.
   Move OAuth browser state into control.Store; bootstrap once under an installation lock.
3. Add PostgreSQL owner registry and internal authenticated project dispatcher. Route all
   project operation entry points, including ingestion paths, uploads, MCP dispatch and
   job cancellation. Test owner down, stale generation, unauthorized forwarding and retries.
4. Add reserved global IDs and cumulative per-worker usage publication. Refactor acceptance
   to allocator + local usage outbox in the same SQLite transaction. Prove crash/retry and
   month boundaries with two owners under one tenant.
5. Add shared query/reader/write admission and shared DuckLake catalogs with S3. Preserve
   native cancellation; test two engines' conflicting writes/reads and cleanup behavior.
6. Fence project deletion across nodes, add coordinated owner startup/recovery checks,
   explicit shared backup restrictions, and CLI/system status discovery of these limits.
7. Add real two-process PostgreSQL/S3 CI journeys, docs and default local regression suite.

The first implementation step is the storage/owner contract, not transplanting existing
SQLite SQL into PostgreSQL. Owner routing is a meaningful deployment contract that must
be chosen explicitly; it cannot be slipped in behind a configuration variable.
