# Shared metadata and owner-local event queues

This pre-launch change supports two configurations of the same published server.
The default keeps separate control and journal SQLite files with local DuckLake catalogs. Shared deployment adds
PostgreSQL control metadata and PostgreSQL DuckLake catalogs with S3 data files.
No existing-data migration, commercial fork or billing service is introduced.
See [shared deployment](shared-deployment.md) for the operator instructions.

## The storage boundary

`Service.db` always owns a local SQLite journal. It contains accepted events,
receipts, identity state, imports, mutations and destination jobs. A producer is
acknowledged only after its SQLite transaction commits.

`Service.control` always uses a separate database. Local mode opens another
SQLite file; shared mode connects to PostgreSQL. It owns projects, tenant policies, cumulative usage, credentials,
humans, OAuth state, worker registration and query permits. It contains no event,
identity-profile, import, export or pipeline-payload tables. PostgreSQL allocates
ID ranges, rather than receiving a row for every event.

The shared DuckLake PostgreSQL database is a separate metadata role and may be a
separate database/server. Each project has its own catalog schema and S3 prefix.
Row inlining is disabled there: event payloads go from SQLite to Parquet on S3.
DuckLake metadata still contains schemas, file references and column statistics.

Common control queries use numbered parameters and explicit conflict clauses.
Schemas and locks are defined for each database. The source conversion tool is
not a runtime SQL translator. Integer flag binding is normalized at the PostgreSQL
driver boundary; transaction semantics remain explicit.

## Durable project ownership

Every project has a stable journal owner. Any public worker can authenticate and
accept an API, CLI, MCP or SDK request, then forward owner-bound operations over an
authenticated internal route. SQL analyses can execute on another worker against
the shared catalog. Signed requests bind their method, URI, credentials and body,
expire, and are single-use. The owner reauthenticates the original credential.

Worker identity is pinned to its persistent SQLite volume. A dedicated PostgreSQL
session lock prevents two active processes from claiming the same worker ID.
Owner registration failure blocks new work. Adding a worker adds capacity for new
projects and analytical reads; it does not automatically rebalance an existing
project's writer. An unavailable owner returns a retryable failure. Restarting its
original volume preserves receipts and import checkpoints. Permanent disk loss
can lose accepted, undelivered events; horizontal replication of processes does
not replicate their journals.

## Budgets and recovery

The acceptance transaction increments a local tenant/month cumulative counter.
Every ten seconds, the worker publishes its high-water marks to PostgreSQL. Only
the positive difference from the previous report updates aggregate usage. Reports
are replayable after a crash; project/event deletion never refunds consumption.
Admission compares cached aggregate usage with the worker's own unpublished
consumption. This allows the agreed small overshoot across owners. Expired policy
fails closed. Authentication and revocation do not use that quota cache.

Interactive SQL, catalog and profile requests reserve a shared tenant place as
well as worker capacity. The permit covers the SQL deadline, S3 and PostgreSQL
metadata I/O allowances, then a thirty-second margin. Release is fenced by its
random token. A native call that still has not returned fifteen seconds after its
deadline and I/O allowances triggers worker exit 70, before permit reuse. Normal
cancellation interrupts execution and releases the place. Local mode has no
shared fail-stop watchdog.

Project deletion fences all workers before cleanup. Local work registers before
materializing foreign-key anchors, so deletion cancels and drains it before
purging the journal. Shared reader permits drain before analytical files are
removed. Permanent control and analytical tombstones block stale recreation.
Native projection updates a guard inside the actual DuckLake transaction and
retries conflicts; a separate advisory lock alone would not fence a stale native
write. Shared cleanup retains an eight-day reader horizon for bounded exports.

## Alternatives and costs

Two complete designs were compared. A recoverable PostgreSQL canonical payload
handoff was rejected after the user clarified that events must not enter
PostgreSQL before DuckLake. Stable project ownership keeps the SQLite acceptance
transaction and requires no shared payload queue or per-event PostgreSQL receipt.
It accepts owner affinity and volume recovery instead of automatic writer failover.

Shared grants and browser sessions use PostgreSQL transactions. Owner-local jobs
retain their SQLite transactions and check current shared grants before external
work. A restored local journal must not replace cluster credentials; the existing
full-instance backup/restore is therefore rejected in shared mode. Coordinated
shared backup and automatic project migration remain separate work.

## Verification

Both configurations need real database tests. Shared journeys cover independent
workers and processes, stable-ID retries through another edge, identify, import
resume, global counters, permissions, OAuth one-use consumption and revocation,
query crash recovery, deletion races, and native PostgreSQL/S3 projection. CI
starts a disposable native PostgreSQL process and an S3 fixture container.
No production deployment is part of this implementation request.

`make shared-postgres-test` needs Go but no Docker. The test runner downloads
PostgreSQL distribution 17.11.1 and its supervisor through the pinned
`embedded-postgres/v2` dependency. Subsequent runs reuse verified binaries in
`porcupine-test-postgres` under the OS user-cache directory. Set
`PORCUPINE_TEST_POSTGRES_CACHE` to choose another cache directory. CI caches this
directory by platform, dependency checksums, and fixture configuration.

Each suite starts a fresh cluster with a random password and a loopback-only
dynamic port. Fsync, synchronous commits, and full-page writes remain enabled.
The runner stops PostgreSQL and removes its data after success, child-command
failure, or cancellation. `make shared-postgres-fixture-test` exercises these
cleanup paths against the real server. Run as a regular non-root user.
`make shared-integration` also exercises the shared PostgreSQL/S3 journeys;
the S3 fixture still needs Docker. This tooling is not part of either distributed
Porcupine binary.
