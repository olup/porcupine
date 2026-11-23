# Hosted runtime

The hosted profile runs replaceable Porcupine workers behind an ingress. PostgreSQL holds shared control state and DuckLake metadata. S3 holds analytical files and immutable job inputs and outputs. Live ingestion acknowledges admission to bounded worker memory. A worker crash or an incomplete shutdown can lose acknowledged events that have yet to commit. [Storage by profile](../storage.md#deployment-profiles)

## Configuration

1. Provision a fresh PostgreSQL database, a dedicated analytical S3 prefix, and private writable scratch space on each worker. Enable PostgreSQL point-in-time recovery and bucket versioning. Protect catalog metadata as you protect analytical files because statistics can contain values derived from events.
2. Run `porcupine init --hosted --config porcupine.json`. Merge these settings into the generated private configuration and keep its bootstrap and pipeline secrets.

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

   Set the PostgreSQL URL through `shared.control_database_url_file` or `shared.control_database_url`. If you also set `ducklake.postgres_url` or its file variant, it must resolve to the same URL. Use the same deployment configuration and pipeline encryption key on all workers. Set `storage.ducklake` if you need a specific disposable scratch directory. The hosted profile rejects local journal and control paths, receipt retention, and native backup settings.

   `acceptance.mode: "buffered"` explicitly accepts the hosted acknowledgment policy. The `profile` selects the runtime.

3. Configure the listener, public HTTPS ingress, and authentication. Apply gateway rate limits using verified tenant or credential identity. [Server settings](configuration.md) · [Access setup](../access.md)
4. Run `porcupine config check --config porcupine.json`, then start each worker with `porcupine --config porcupine.json`. Exercise [PostgreSQL and S3 recovery](#recovery-and-retention) and [verify readiness and delivery](instance-status.md#verify-delivery-before-routing-traffic) before routing traffic.

## Live ingestion and delivery

The worker admits or rejects each request as a unit. Live requests have a 1 MiB body limit, including after decompression. A full or draining worker returns HTTP 503 with `Retry-After`. Reuse the same producer IDs on retry. [Live acknowledgment contract](native-event-contract.md#acknowledgment-metadata)

| Buffer setting | Default |
| --- | --- |
| Dispatch trigger | 2 seconds or 5 MiB for one project |
| Per worker | 64 MiB, 100,000 rows, and 1,024 active projects |
| Per project | 8 MiB and 10,000 rows |
| Concurrent flushes per worker | 4 |

The buffer limits cover pending and in-flight work, including overhead. Size process memory for handlers, native engines, and caches too. Flush retries can keep events in memory beyond the timer. A query can return `delivery.pending_unknown: true` because its worker cannot inspect other workers' buffers. Verify the expected events in SQL before treating a deployment as drained.

If a worker loses its PostgreSQL lease, `/readyz` returns 503 and the worker pauses write admission. The worker retains pending batches in memory and retries after acquiring a fresh lease. If recovery quarantines one project, all workers reject new ingestion for that project with `project_ingestion_quarantined` and `Retry-After: 1`. A successful recovery or flush clears the quarantine. [Instance status and quarantine reasons](instance-status.md#interpret-the-response)

### Tune the hosted flush timer

In the hosted profile, set `acceptance.flush_interval` or `PORCUPINE_ACCEPTANCE_FLUSH_INTERVAL` to a duration from `1s` through `30s`. For example, use `"acceptance": {"mode": "buffered", "flush_interval": "10s"}`. Omit the setting to keep the 2-second timer. A batch becomes eligible to flush when the oldest pending event reaches that age, the pending batch reaches 5 MiB, or it reaches one-third of the project event capacity (3,333 events by default). The count trigger gives the worker room to accept more events while the first batch commits. Requests stay atomic and traffic bursts can still fill the buffer.

A longer timer can combine more events into each flush and reduce S3 requests. It also keeps accepted events in memory longer, delaying query visibility and increasing the time during which a worker crash can lose them. Busy flush slots, provider latency, and retries can extend that time beyond the timer.

In one hosted test, each case sent 24 HTTP events to one project at one event per second. With a 10-second timer, successful S3 PUTs fell from 55 to 15 without an event webhook and from 94 to 45 with a webhook. The 95th-percentile time from acknowledgment to successful flush rose from 2.147 to 10.148 seconds without a webhook and from 2.172 to 10.172 seconds with one. The test timed flush completion. Query visibility and webhook delivery were outside the measurement. [Measurement run](https://github.com/olup/porcupine/actions/runs/44723693790)

## Drain a worker

Remove the worker from the ingress before planned shutdown. Allow ten seconds for HTTP shutdown, 30 seconds for the buffer drain, and time for cleanup. The worker waits up to five more seconds for canceled analytics and maintenance after draining. In-flight flushes and other jobs can extend shutdown. Inspect the worker's shutdown result. If the drain fails, use stable producer IDs to retry interrupted writes and verify which acknowledged events reached DuckLake.

For a fleet-wide upgrade, pause producers, wait for expected events to become queryable, then drain every worker. [Upgrade procedure](../hosting.md#upgrade)

## Hosted budgets {#shared-budgets}

Hosted live admission checks a short-lived snapshot of policy and usage before buffering. Workers can admit events ahead of the published usage ledger. [Hosted usage accounting](../design/hosted-architecture.md#usage) · [Interactive query limits](tenants.md#interactive-query-limits)

## Native timeouts and worker supervision

The SQL deadline normally interrupts CPU execution. Native database and network calls are a separate boundary: a blocking PostgreSQL/libpq call or an in-flight object read can outlast context cancellation. Metadata connection, statement, lock, and TCP settings reduce those waits but do not make all native I/O immediately interruptible. A SQL timeout does not guarantee a prompt HTTP error response in every network failure.

If native execution has not returned by the SQL deadline plus both I/O allowances plus 15 seconds, Porcupine quarantines that project's interactive engine on this worker. New SQL, catalog, and profile executions for it return `503` with `analytics_engine_unhealthy`. Other healthy projects can continue while local and tenant capacity remains. The quarantine lasts until worker restart. Porcupine does not terminate the process because of a stuck query.

A failed renewal also cancels the affected project's active requests and quarantines its interactive engine. The keeper continues trying to renew while calls remain alive. Renewal and release are scoped to the original reservation token; neither can revive an expired reservation or change a replacement. A control operation has a two-second deadline.

If reservation loss cancels native work, its result is rejected even when the driver returns no error. The response is `503` with `query_permit_uncertain`, indicating shared-control availability rather than a quota refusal. Ordinary execution deadlines and client cancellation retain their existing error mapping. A failure or delay in delivery-status response processing cannot quarantine an engine whose native call has already returned.

Automatic expiry recovers capacity after a crash or lost heartbeat. It does not prove that native execution stopped. During a PostgreSQL network partition, an uninterruptible call may continue after its reservation expires and another worker receives the place. The tenant's physical concurrency can then exceed its configured limit. This is an accepted failure window, not a strict distributed execution guarantee. Local slots remain occupied until calls return.

On normal shutdown, the worker refuses new hosted analytical work, cancels active calls, and keeps renewing during the HTTP shutdown grace. `Service.Close` refuses to close its control connection while an interactive scope is still alive. When the process actually stops, remaining reservations expire without a final release. Run workers under a supervisor and allow time for the live ingress buffer to drain during planned shutdown. Clients whose responses were interrupted should retry with the same producer IDs. An interrupted request may already have committed.

The default local deployment retains its ordinary native query cancellation and process resource controls. It does not use shared reservation renewal or project quarantine.

## Recovery and retention

Keep control state and DuckLake metadata in one PostgreSQL recovery domain. Retain the private deployment configuration, signing material, and pipeline encryption keys separately. Test the deployed PostgreSQL and S3 services together before admitting traffic.

Set `cleanup_grace` to at least `restore_window + restore_max_reader_time + restore_safety_margin`. Include exports in the maximum reader duration. Keep referenced S3 objects readable for the entire restore window. Check the bucket lifecycle and delete markers. Restoring metadata to an earlier point can require restoring an object version before it is readable at its ordinary key.

Set `ducklake.restore_floor_file` to an absolute path containing one operator-verified RFC3339 timestamp for the earliest supported recovery point. Include secondary backups when choosing the floor. Replace the file atomically on each worker as recovery points expire. Keep the same floor across workers and restore at or after the last floor they used. Missing, invalid, or stale evidence leaves reclamation pending. [Cleanup behavior and status](ducklake-cleanup.md#hosted-profile)

### Restore PostgreSQL and S3 together

1. Fence all old writers. Isolate the recovery deployment from client traffic and block destination egress before starting workers. Keep restored outbound jobs paused during verification.
2. Recover PostgreSQL to the chosen point with both control state and DuckLake metadata. Restore access to the S3 objects referenced at that point, including analytical files and pending job inputs. Use the matching server build and retained private configuration.
3. Verify access, representative committed events, and pending job references. Check published checkpoints against retained native proofs. Validate destination decryption before resuming delivery.
4. Reapply credential revocations and deletion instructions recorded after the target. Rotate recovered access when you cannot reconcile credential history. Rotate the [usage epoch](usage.md) under operator control before publishing restored usage reports.
5. Reconnect clients and resume reviewed jobs. Previously delivered work can replay from an older recovery point. Webhook receivers can use stable delivery IDs to deduplicate it.

See the [recovery protocol](../design/hosted-architecture.md#recovery-and-retention) and the [PostgreSQL and S3 restore drill](../design/archive/hosted-restore-drill.md).

<a id="recovery-and-deletion"></a>

### Reclamation after deletion

Project and tenant deletion fence new work before reclamation. Managed project reclamation waits for both the verified restore floor and the grace since object inventory. Provider requests can outlive a worker. Reclamation relies on those calls settling within the grace. [Hosted deletion protocol](../design/hosted-architecture.md#deletion)
