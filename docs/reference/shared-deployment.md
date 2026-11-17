# Shared PostgreSQL deployment

**Unreleased.** The published v0.14.0 server does not support this configuration. Use the matching development server and CLI. This guide describes the implemented deployment contract, not a production availability guarantee. For a single-server setup, compare the [SQLite and PostgreSQL modes](../hosting.md#choose-local-sqlite-or-shared-postgresql-unreleased) first.

The same server binary has two modes. The default keeps control metadata in one local SQLite file and accepted events and jobs in a separate SQLite journal, with local DuckLake catalogs. Shared mode moves control metadata and DuckLake catalogs to PostgreSQL and analytical files to S3. Every worker still needs its own persistent SQLite volume.

Complete canonical events commit to the owner's SQLite queue, then project directly into S3 Parquet. No PostgreSQL event queue or staging table is introduced. DuckLake data inlining is disabled for PostgreSQL catalogs. Those catalogs still contain metadata and derived column statistics, which may include event-property, context, or trait values such as minima and maxima. Treat the catalog as part of the analytical data when choosing hosting, access, backups, and deletion policies. Control PostgreSQL holds accounts, grants, policies, counters, and coordination records.

## Where data lives

| State | Default deployment | Shared deployment |
| --- | --- | --- |
| Tenants, projects, people, keys, OAuth state | Local SQLite | Shared control PostgreSQL |
| Budget policy and aggregate usage | Local SQLite | Shared control PostgreSQL, with owner-local usage awaiting publication |
| Accepted event payloads and retry state | Local SQLite | Project owner's local SQLite |
| Identity state, mutations, imports, uploads, pipeline and export jobs | Local SQLite | Project owner's local SQLite and local managed files |
| DuckLake catalog | Local project catalog | Per-project PostgreSQL catalog metadata |
| Delivered analytical payloads | Local Parquet or analytical S3 | S3 Parquet |

The two PostgreSQL connections can address separate databases on one PostgreSQL service. They have distinct responsibilities and credentials. No event queue, event inbox, or canonical event-payload table is added to control PostgreSQL.

## Public requests and project ownership

Clients use one public address behind a load balancer. HTTP, CLI, MCP, and supported ingestion SDKs keep their existing requests. No client supplies an owner or trusted tenant field.

Each project has one durable owner. Any worker can authenticate a request against shared control metadata. An operation that needs pending or operational project state is forwarded internally to its owner. This includes ingestion, identify, event changes, queue inspection, historical uploads, quarantine repair, pipelines, and exports. Forwarding retains the client credential for revalidation and authenticates the internal request separately.

Only the owner's SQLite commit acknowledges accepted events. A retry sent through a different public worker reaches the same receipt ledger and returns the original `event_id`. PostgreSQL-reserved ID ranges provide global event identifiers without using PostgreSQL as an event-payload inbox.

Delivered SQL can execute on any worker against the shared catalog and S3 files. Adding workers increases capacity across projects and analytical requests. It does not split one project's ingestion across owners or distribute one DuckDB query across machines.

A non-owner query response sets `delivery.pending_unknown: true` because that process cannot inspect the owner's SQLite queue. Do not interpret `pending: 0` in such a response as a drained queue. `get-event-schema` is routed to the owner and returns authoritative project delivery status. If that owner is unavailable, owner-bound requests return `503`; they do not invent an empty job or silently move the project to another worker.

## Configure workers

Leave all shared settings unset for the default standalone deployment. If starting from `porcupine init`'s JSON, remove `storage.control` before configuring shared control. It must not select both a local control file and PostgreSQL. JSON fields under `shared` and `ducklake` map to the environment settings below; environment overrides must be set on the actual server/container process. Shared mode requires all four control/routing settings together:

| Setting | Purpose |
| --- | --- |
| `PORCUPINE_CONTROL_DATABASE_URL` | PostgreSQL URL for control metadata |
| `PORCUPINE_WORKER_ID` | Stable identity of this worker's persistent volume |
| `PORCUPINE_WORKER_URL` | Direct internal address reachable by the other workers |
| `PORCUPINE_SHARED_ROUTING_SECRET` | Shared internal request-authentication secret, at least 32 bytes |

For the control URL, routing secret, and `PORCUPINE_DUCKLAKE_POSTGRES_URL`, a `_FILE` variant reads the value from a mounted secret file. Set the value or its `_FILE` option, never both. A host path is insufficient unless the file is mounted inside the container.

With analytics enabled, shared mode also requires `PORCUPINE_DUCKLAKE_POSTGRES_URL` and the existing [analytical S3 configuration](ducklake-s3.md#configure-a-new-deployment). All workers must address the same control deployment, analytical catalog, bucket, and prefix. Each worker has a different local data volume and worker ID.

`PORCUPINE_DUCKLAKE_METADATA_TIMEOUT_SECONDS` controls native PostgreSQL metadata I/O in this shared analytical mode. Its default is 10 seconds; accepted values are 2–60 seconds. Porcupine applies it to native connection, statement, lock, and TCP settings. Do not set it for a local DuckLake catalog. Schema discovery exposes the configured allowance as `limits.metadata_request_timeout_seconds`.

For example, worker `eu-1` could use:

```text
PORCUPINE_DB=/data/porcupine.db
PORCUPINE_DUCKLAKE_DIR=/data/ducklake
PORCUPINE_CONTROL_DATABASE_URL_FILE=/run/secrets/control-postgres-url
PORCUPINE_DUCKLAKE_POSTGRES_URL_FILE=/run/secrets/ducklake-postgres-url
PORCUPINE_WORKER_ID=eu-1
PORCUPINE_WORKER_URL=https://porcupine-eu-1.internal
PORCUPINE_SHARED_ROUTING_SECRET_FILE=/run/secrets/porcupine-routing-secret
PORCUPINE_DUCKLAKE_S3_BUCKET=porcupine-analytics
PORCUPINE_DUCKLAKE_S3_PREFIX=shared-production
PORCUPINE_DUCKLAKE_S3_REGION=eu-west-3
```

The secret files contain actual PostgreSQL URLs and routing secret, not variable names. Configure S3 credentials, endpoint, and TLS as described in the S3 guide. Use private network access and TLS for PostgreSQL, object storage, and worker transport. `PORCUPINE_WORKER_URL` must be an HTTPS origin reachable directly on that worker, rather than back through the public load balancer. Loopback HTTP origins are accepted for local testing.

Workers expose one public OAuth issuer and use compatible authentication, resource, email, and destination-encryption configuration. Shared OAuth browser state lets sign-in start on one worker and finish on another. Keep these deployment secrets and both PostgreSQL credentials outside prompts and project-visible responses.

Worker registration pins an ID to its local volume. It rejects another active process using that ID and rejects replacing its durable identity with an empty volume. Restart a worker with its existing volume and ID. Do not clone a worker volume into another active worker, change IDs to bypass registration, or mount one SQLite volume into several processes.

This mode does not migrate an existing standalone database. Start with fresh shared metadata and fresh worker directories. Preserve old instances and recovery material separately if importing history into the new deployment.

## Shared budgets

[Tenant budgets](tenants.md) have the same API and authority in both modes. A monthly event consumes allowance when its owner durably accepts it, not when it reaches DuckLake. The owner commits cumulative usage with the event in SQLite.

Shared workers publish cumulative counters to PostgreSQL and refresh cached budget/usage state on a fixed ten-second cycle. Publication applies only the increase above each worker's last watermark, so retries do not double-count usage. An owner includes its own unpublished consumption when deciding whether to accept more events. Aggregate usage can lag local acceptance until publication; month history remains available.

Several owners serving one tenant can exceed an allowance during that publication and cache delay. There is no strict cross-worker event-quota bound or hard spending-cap guarantee. An expired policy cache must refresh successfully before accepting new events; unavailable PostgreSQL does not permit unlimited use of stale policy. Already accepted delivery continues. Credential revocation and deletion fences do not use the budget cache.

Interactive query places are reserved immediately in PostgreSQL across workers. Local process capacity remains an additional limit. Each reservation expires after 30 seconds and an independent execution keeper renews active reservations every five seconds. Renewal continues after an HTTP timeout or cancellation until the analytical call actually returns. That return releases the reservation and local place before profile summarization, delivery-status lookups, or response encoding. Response processing retains the original deadline through a separate context.

## Native timeouts and worker supervision

The SQL deadline normally interrupts CPU execution. Native database and network calls are a separate boundary: a blocking PostgreSQL/libpq call or an in-flight object read can outlast context cancellation. Metadata connection, statement, lock, and TCP settings reduce those waits but do not make all native I/O immediately interruptible. A SQL timeout does not guarantee a prompt HTTP error response in every network failure.

If native execution has not returned by the SQL deadline plus both I/O allowances plus 15 seconds, Porcupine quarantines that project's interactive engine on this worker. New SQL, catalog, and profile executions for it return `503` with `analytics_engine_unhealthy`. Other healthy projects can continue while local and tenant capacity remains. The quarantine lasts until worker restart. Porcupine does not terminate the process because of a stuck query.

A failed renewal also cancels the affected project's active requests and quarantines its interactive engine. The keeper continues trying to renew while calls remain alive. Renewal and release are scoped to the original reservation token; neither can revive an expired reservation or change a replacement. A control operation has a two-second deadline.

If reservation loss cancels native work, its result is rejected even when the driver returns no error. The response is `503` with `query_permit_uncertain`, indicating shared-control availability rather than a quota refusal. Ordinary execution deadlines and client cancellation retain their existing error mapping. A failure or delay in delivery-status response processing cannot quarantine an engine whose native call has already returned.

Automatic expiry recovers capacity after a crash or lost heartbeat. It does not prove that native execution stopped. During a PostgreSQL network partition, an uninterruptible call may continue after its reservation expires and another worker receives the place. The tenant's physical concurrency can then exceed its configured limit. This is an accepted failure window, not a strict distributed execution guarantee. Local slots remain occupied until calls return.

On normal shutdown, the worker refuses new shared analytical work, cancels active calls, and keeps renewing during the HTTP shutdown grace. `Service.Close` refuses to close its control connection while an interactive scope is still alive. When the process actually stops, remaining reservations expire without a final release. Run workers under a supervisor with the same worker ID and persistent SQLite volume so durably accepted work can resume. Clients whose responses were interrupted should retry with the same producer IDs. An unacknowledged request may already have committed.

The default local deployment retains its ordinary native query cancellation and process resource controls. It does not use shared reservation renewal or project quarantine.

## Check worker status

The authenticated [instance status report](instance-status.md) includes `checks.shared_control`, `checks.worker_registration`, and `checks.query_execution` in shared mode. A failure degrades the report. Instance administrators also see active executions, uncertain reservations, draining state, and quarantined project IDs. Project-scoped callers receive no execution identities. Local deployments omit these fields. These checks do not execute a new native query against a stuck engine.

`configuration.shared_control` and `configuration.owner_routing` indicate configured functionality, not successful operation. These fields disclose no PostgreSQL connection string, worker URL, routing secret, or other credential. Check each worker as well as the public entry point; a healthy worker does not prove another project's owner is available.

## Maintenance and deletion

Workers share catalog metadata, so local process locks alone cannot protect analytical writes. Shared projection and maintenance coordinate their native transactions through PostgreSQL. Project deletion fences new work and waits for admitted readers to release their reservations or for those reservations to expire before removing the owner's state and managed analytical objects. Under a network partition, expiry is not proof that a native reader stopped; such a reader can fail when cleanup proceeds. An unavailable owner prevents completion; retry after recovering it.

Shared catalogs enforce a minimum of eight days for both snapshot retention and obsolete-file cleanup grace, even if shorter values are configured. This protects remote readers, including background exports whose maximum execution window is seven days. These floors can delay physical cleanup after an event is removed from current query results. Object-store versioning, backups, and external destinations have their own retention policies.

Data inlining is disabled in this mode. Small writes therefore need S3 files rather than PostgreSQL payload rows; batch size, object-request volume, and maintenance need capacity measurements before choosing a hosting profile.

## Failure and recovery boundary

Multiple public entry points do not replicate owner-local accepted events. If a project owner stops, its pending payloads, receipts, identity state, import checkpoints, and jobs remain on its volume. Other workers can continue reading delivered analytical data when shared dependencies are available, but cannot take over that local acceptance state automatically.

Recover the same owner volume and worker identity before resuming its projects. Losing that volume permanently can lose acknowledged events not yet delivered, retry receipts, and jobs. Draining the queue alone does not transfer all operational state. Automatic ownership transfer and replicated acceptance are outside this contract.

The native whole-instance backup scheduler and offline restore are not supported in shared mode. A single worker bundle cannot represent shared control, remote catalogs, and every owner's local state. Recovery needs a coordinated plan for:

- Both PostgreSQL metadata stores and their consistent recovery points.
- S3 data objects, retained versions, and catalog references.
- Every worker's complete SQLite and managed local volume, including upload and job files.
- Deployment identities, keys, credentials, configuration, and the matching server build.

Do not restore one worker bundle over live shared metadata or treat an S3 bucket listing as a complete backup. Validate coordinated recovery in isolation before promising service availability or loss bounds. [Storage boundaries](../storage.md) · [Architecture](../architecture.md)
