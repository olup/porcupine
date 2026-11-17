# Hosted runtime

Use the [hosted setup](../design/hosted-profile-composition.md) for worker configuration, startup, and the recovery floor. The same server serves HTTP, CLI, MCP, and supported ingestion SDKs. Workers share PostgreSQL for control and DuckLake metadata and S3 for analytical data.

## Live ingestion and delivery

Hosted live responses confirm bounded memory admission. The worker flushes to DuckLake and publishes durable effects. A worker crash or incomplete drain can lose acknowledged events that have not committed. Keep stable producer IDs on retries. Durable import and mutation operations have separate, shared checkpoints. [Acknowledgment contract](native-event-contract.md#unreleased-acknowledgment-metadata)

A hosted query can report `delivery.pending_unknown: true` because its worker cannot inspect another worker's memory buffer. Verify expected events in SQL. Do not interpret a zero pending count as a flushed deployment.

The analytical catalog can contain column statistics derived from event values. Protect it with the same access and retention controls as the analytical archive. Hosted workers write event files to S3. DuckLake data inlining is disabled for the hosted PostgreSQL catalog.

## Hosted budgets {#shared-budgets}

Hosted live admission reads a short-lived snapshot of policy and published usage. Usage can lag admission while workers buffer data or publish durable effects. Concurrent workers can exceed the allowance during that interval. Interactive query reservations use shared PostgreSQL and local worker capacity independently. [Tenant limits](tenants.md#interactive-query-limits)

## Native timeouts and worker supervision

The SQL deadline normally interrupts CPU execution. Native database and network calls are a separate boundary: a blocking PostgreSQL/libpq call or an in-flight object read can outlast context cancellation. Metadata connection, statement, lock, and TCP settings reduce those waits but do not make all native I/O immediately interruptible. A SQL timeout does not guarantee a prompt HTTP error response in every network failure.

If native execution has not returned by the SQL deadline plus both I/O allowances plus 15 seconds, Porcupine quarantines that project's interactive engine on this worker. New SQL, catalog, and profile executions for it return `503` with `analytics_engine_unhealthy`. Other healthy projects can continue while local and tenant capacity remains. The quarantine lasts until worker restart. Porcupine does not terminate the process because of a stuck query.

A failed renewal also cancels the affected project's active requests and quarantines its interactive engine. The keeper continues trying to renew while calls remain alive. Renewal and release are scoped to the original reservation token; neither can revive an expired reservation or change a replacement. A control operation has a two-second deadline.

If reservation loss cancels native work, its result is rejected even when the driver returns no error. The response is `503` with `query_permit_uncertain`, indicating shared-control availability rather than a quota refusal. Ordinary execution deadlines and client cancellation retain their existing error mapping. A failure or delay in delivery-status response processing cannot quarantine an engine whose native call has already returned.

Automatic expiry recovers capacity after a crash or lost heartbeat. It does not prove that native execution stopped. During a PostgreSQL network partition, an uninterruptible call may continue after its reservation expires and another worker receives the place. The tenant's physical concurrency can then exceed its configured limit. This is an accepted failure window, not a strict distributed execution guarantee. Local slots remain occupied until calls return.

On normal shutdown, the worker refuses new hosted analytical work, cancels active calls, and keeps renewing during the HTTP shutdown grace. `Service.Close` refuses to close its control connection while an interactive scope is still alive. When the process actually stops, remaining reservations expire without a final release. Run workers under a supervisor and allow time for the live ingress buffer to drain during planned shutdown. Clients whose responses were interrupted should retry with the same producer IDs. An interrupted request may already have committed.

The default local deployment retains its ordinary native query cancellation and process resource controls. It does not use shared reservation renewal or project quarantine.

## Recovery and deletion

The native standalone bundle and offline restore procedure apply to SQLite deployments. Recover hosted by coordinating PostgreSQL recovery points and versioned S3 objects. Restore in isolation and verify access, committed events, and durable jobs before reconnecting clients. [Hosted recovery drill](../design/hosted-restore-drill.md)

Project and tenant deletion fence new work before physical reclamation. Reclamation waits for the configured grace, the verified operator restore floor, and reconciliation of outstanding work. Backups and noncurrent S3 versions have their own retention policies. [Hosted operator requirements](../design/hosted-profile-composition.md)
