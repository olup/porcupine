# DuckLake experiment

This branch is an experiment. Released Porcupine still uses ClickHouse. The preserved reference is `reference/clickhouse-before-ducklake`, commit `d869fd6`.

## Work checklist

- [x] Ground the current ingestion, projection, query, mutation, and identity paths.
- [x] Compare embedded and separate-worker designs, then select the smallest safe integration.
- [x] Implement local DuckLake storage and native DuckDB queries through the existing HTTP and MCP contracts.
- [x] Verify stable IDs, retries, restart, identity, edits, deletions, and two-project isolation with real DuckLake.
- [x] Record analytical equivalence, resource observations, unsupported features, and repeatable verification commands.

## Constraints from the current implementation

SQLite is a durable outbox, not an archive. Delivery removes payloads only after the analytical store commits. The replacement must preserve the same event ID on retry, reject stale projection revisions, and keep deletion facts after data cleanup so a late original cannot resurrect an event.

Identity controls contain canonical versioned snapshots. The query engine must expose resolved people without exposing these private snapshots. Arbitrary agent SQL must have an engine isolation boundary in addition to validation. A shared privileged connection with only a project filter is insufficient.

The public API and MCP dispatch already share Huma operations. SQL examples, schema discovery, budgets, exports, and receipt history currently depend on ClickHouse and require explicit treatment. Unsupported experimental features must fail clearly, rather than use a hidden ClickHouse fallback.

## Design comparison

Two independent candidates compared embedding the official DuckDB Go driver with running a separate native worker over a private process protocol. A separate review selected embedding for this local experiment. It keeps the runtime to one process while leaving normal CGO-free builds unchanged. A separate worker would provide better containment of native crashes and memory exhaustion, but would also require process supervision, cancellation forwarding, and another protocol before the storage could be evaluated.

The selected design incorporates the worker candidate's catalog identity checks, uncertain-commit replay tests, cancellation tests, and explicit unsupported-feature reporting. A root directory marker alone cannot detect a replaced project catalog. The implementation checks a marker in each actual catalog and retains source identity in the acceptance database.

Each project has a distinct DuckDB instance and local DuckLake catalog. The default working set holds at most four open project contexts and evicts idle contexts. It does not attach all projects to one privileged query connection. Writes are serialized within each project, and a native transaction applies only strictly newer revisions. Deletion keeps a payload-free head to reject late originals. A batch spanning several projects can commit one project before another fails, then converge on retry.

The native driver is pinned to DuckDB 2.0.0. It requires CGO for this tagged server. The standalone CLI and the normal server still build with `CGO_ENABLED=0`. No Python runtime is required by the DuckLake server or CLI. The optional ClickHouse comparison reuses the existing Python test runner to start and remove a disposable Docker container.

## Run locally

Use a Go and C/C++ toolchain suitable for the official DuckDB Go driver. The first use installs the matching signed DuckLake extension into DuckDB's extension cache and needs access to the official extension repository. Extensions load during trusted initialization. Query contexts then disable automatic extension installation and loading, restrict file access to the managed project directory, and lock configuration.

```sh
make ducklake-build
umask 077
export PORCUPINE_BOOTSTRAP_TOKEN="$(openssl rand -hex 32)"
export PORCUPINE_DB=./data/ducklake-experiment/queue.db
export PORCUPINE_DUCKLAKE_DIR=./data/ducklake-experiment/lake
./bin/porcupine-ducklake
```

Use a fresh acceptance database and persistent directory for the experiment. Do not set `PORCUPINE_CLICKHOUSE_URL`. Configuring both engines is rejected, and opening an existing ClickHouse acceptance database with DuckLake is rejected. Existing deployment data is not migrated.

The default listener is `127.0.0.1:8080`. Keep this experiment local. The ordinary API, CLI, and MCP routes remain available. Schema discovery reports `sql.dialect: duckdb` and `sql.result_cache: false`. Use native DuckDB SQL and `$name` scalar parameters:

```json
{
  "sql": "SELECT event, count(*) AS total FROM events WHERE event=$name GROUP BY event",
  "parameters": {"name": "page_viewed"},
  "use_cache": false
}
```

Both default and explicitly fresh queries execute without a result cache in this experiment. Integer identifiers and other 64-bit integer results remain decimal strings in JSON. Native function discovery lists the actual accepted functions. SQL is parsed through DuckDB's native AST, then validated before execution. Only the public `events` table and local CTEs are query sources. Private storage, metadata, time travel, external readers, table functions, and configuration changes are rejected.

## Maintenance and deletion

Porcupine runs a bounded flush and file-compaction pass every five minutes. Set `PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL` to a duration between `1s` and `24h` to change the cadence. Maintenance failures are recorded with a generic error in the local metadata table. The first implementation maintains open project contexts only.

Event edits and deletion affect the current analytical state once projected. Deletion cannot be undone by a late original or equal-revision retry. Historical snapshots can still contain old payloads. This experiment does not implement snapshot expiration, full physical event purging, or the proposed 30-day erasure deadline. Backups and versioned object storage would also need a retention policy before that deadline could be promised.

Project deletion drains project work, closes its engine, removes its managed local catalog and data directory, and preserves other projects. It remains resumable through the existing project-deletion job.

## What is available

- Native, PostHog-compatible, and Segment ingestion through the existing durable SQLite queue.
- Historical PostHog upload protocol through the same ingestion path.
- Stable receipt IDs, retry-safe projection, identity resolution, event edits and deletion.
- Native DuckDB queries, event and property discovery, and existing HTTP, CLI, and MCP authorization.
- Local catalogs and Parquet files, bounded active project contexts, and local maintenance.
- Existing SQLite-backed webhook destinations.

The first experiment explicitly excludes analytical S3 storage, remote catalogs, finite receipt eviction, result caching, one-time SQL exports, and scheduled SQL-to-S3 pipelines. These need their own implementation and verification. Configured exports and finite receipts are rejected. S3 pipeline creation remains unavailable. Webhook destinations do not require an analytical export worker.

## Resource boundaries

Interactive queries retain the two-query admission limit, five-second execution deadline, 1,000-row output limit, and 1 MiB JSON result limit. Engine contexts default to two threads and a 256 MiB DuckDB memory setting. The memory setting applies per open engine, not to the whole Porcupine process. Physical scan row and byte quotas are not implemented and schema discovery reports them as zero with an explicit unsupported entry.

DuckDB can allocate memory outside its buffer-manager limit. A large scalar result can allocate native memory before the result-size check runs. The guard rejects `repeat`, and result encoding checks sizes before making another JSON copy, but these are not a hard resident-memory ceiling. A production multi-client service accepting arbitrary analytical queries would need process or container resource containment. Native allocations are also absent from Go's benchmark allocation counters.

## Verify

```sh
make check
make ducklake-test
make ducklake-smoke
make ducklake-bench
```

To compare both engines on the same fixtures, install Docker and Python 3, then run `make ducklake-compare`. The existing container fixture supplies temporary ClickHouse credentials to the Go tests. It does not use a running production database.

The smoke command starts the compiled native server on loopback with a private temporary database, calls it through the compiled standalone CLI, and removes its test data when it exits. It does not connect to the existing deployment. The Go tests additionally exercise MCP, two-project scope, revocation, identity, mutations, restart, and failed acknowledgment recovery against real DuckLake.

The analytical fixtures cover event counts, actors, daily grouping, sessions, median, ordered conversion, retention, and custom properties with explicit expected results. The benchmarks separate native HTTP acceptance plus projection from a warm aggregation over 10,000 synthetic rows. They exclude setup and do not establish a hosting capacity or a ClickHouse performance comparison.

The real-engine comparison passes nine scenarios on identical captured fixtures: counts and actors, event volumes, hourly trends, custom dimensions, amount and explicitly defined nearest-rank p95, sessions, an ordered funnel, next-day retention, and a Paris day boundary. It also compares the results after an edit and a deletion. Equivalent SQL is written separately for each dialect; no ClickHouse SQL translation layer is used.

## Initial measurements

On an Apple M1 Pro running macOS arm64, a fixed ten-iteration run reported:

| Fixture | Observed time |
| --- | --- |
| Native HTTP acceptance plus projection, 100 events per batch | 54.64 ms per batch, about 1,830 events/s |
| Warm grouped aggregation over 10,000 synthetic rows | 10.70 ms per query |

An earlier row-by-row projection took about 2.04 seconds per 100 events. The implementation now stages a bound batch and applies it with set-based SQL. It retains revision and event-ID checks inside the same project transaction.

These are local microbenchmarks with setup excluded and ten Go logical processors available. The analytical fixture is seeded using trusted bulk SQL, so its timing does not measure ingestion. Go's allocation metrics exclude native DuckDB allocations. No equivalent ClickHouse throughput run or multi-client hosting-capacity test has been completed.

The local unstripped server binaries built with `-trimpath` were approximately 106 MiB with the native tag and 37 MiB without it. The standalone CLI was about 12 MiB. These are macOS development builds, not release artifact sizes.

## Scope

Local files are the first target. No production ingestion, live infrastructure change, or migration of existing deployment data is authorized by this experiment.
