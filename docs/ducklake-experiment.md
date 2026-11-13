# DuckLake experiment

This branch is an experiment. Released Porcupine still uses ClickHouse. The preserved reference is `reference/clickhouse-before-ducklake`, commit `d869fd6`.

The [product analytics validation report](ducklake-validation.md) records the larger-history, concurrent ingestion/query, project-scope and process-recovery tests. It includes the missed load and maintenance targets as well as successful checks.

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
make ducklake-performance-compare
make ducklake-batching-bench
```

To compare both engines on the same fixtures, install Docker and Python 3, then run `make ducklake-compare`. The existing container fixture supplies temporary ClickHouse credentials to the Go tests. It does not use a running production database.

The smoke command starts the compiled native server on loopback with a private temporary database, calls it through the compiled standalone CLI, and removes its test data when it exits. It does not connect to the existing deployment. The Go tests additionally exercise MCP, two-project scope, revocation, identity, mutations, restart, and failed acknowledgment recovery against real DuckLake.

The analytical fixtures cover event counts, actors, daily grouping, sessions, median, ordered conversion, retention, and custom properties with explicit expected results. `ducklake-bench` measures the native backend alone. `ducklake-performance-compare` runs both backends successively inside a disposable Linux container with one total budget of two CPUs and 2 GiB RAM. Both commands exclude setup and do not establish a hosting capacity.

The real-engine comparison passes nine scenarios on identical captured fixtures: counts and actors, event volumes, hourly trends, custom dimensions, amount and explicitly defined nearest-rank p95, sessions, an ordered funnel, next-day retention, and a Paris day boundary. It also compares the results after an edit and a deletion. Equivalent SQL is written separately for each dialect; no ClickHouse SQL translation layer is used.

## Initial measurements

On an Apple M1 Pro running macOS arm64, a fixed ten-iteration run reported:

| Fixture | Observed time |
| --- | --- |
| Native HTTP acceptance plus projection, 100 events per batch | 54.64 ms per batch, about 1,830 events/s |
| Warm grouped aggregation over 10,000 synthetic rows | 10.70 ms per query |

An earlier row-by-row projection took about 2.04 seconds per 100 events. The implementation now stages a bound batch and applies it with set-based SQL. It retains revision and event-ID checks inside the same project transaction.

These are local microbenchmarks with setup excluded and ten Go logical processors available. The analytical fixture is seeded using trusted bulk SQL, so its timing does not measure ingestion. Go's allocation metrics exclude native DuckDB allocations. Use the bounded Linux comparison below to compare backends under the same CPU and memory budget.

The local unstripped server binaries built with `-trimpath` were approximately 106 MiB with the native tag and 37 MiB without it. The standalone CLI was about 12 MiB. These are macOS development builds, not release artifact sizes.

## Bounded Linux comparison

Run `make ducklake-performance-compare` with Docker. The benchmark image is separate from the released server image. It uses Go 1.26.11, DuckDB 2.0.0 with its matching DuckLake extension, and ClickHouse 26.8.36.3. It mounts a temporary Docker volume for both backends' SQLite and analytical data, publishes no ports, and removes the container and volume afterward. It does not mount credentials or existing event data.

DuckLake runs first with ClickHouse stopped. ClickHouse then runs alongside the Go API process within the same total container budget. `GOMAXPROCS` is two. Interactive queries use the normal two-thread and 256 MiB engine settings. The container's 2 GiB limit includes native allocations and the ClickHouse server. ClickHouse background merge pools have four threads to fit this small container.

The ingestion case sends native events in sequential batches of 100 through Porcupine's HTTP handler, then explicitly drains each batch to the analytical backend. It includes SQLite acceptance, receipt creation, projection, and verification that no events remain pending. The final analytical count must equal the accepted count. Payloads are about 300 bytes per event. This is projected throughput for this batching pattern, not the maximum HTTP acceptance rate.

The query cases use the public HTTP query handler, including authentication, SQL validation, project isolation, and JSON encoding. They share a 10,000-event synthetic fixture inserted through the same ingestion path on each backend. The fixture contains three event names, 100 actors, 100 sessions, timestamps across three hours, and custom JSON properties. The SQL uses each engine's own dialect. Calls set `use_cache: false`; a warm-up runs before timing, so filesystem and engine caches remain warm. Requests use an in-process HTTP recorder; ClickHouse's internal HTTP connection remains part of its measured cost.

Each case runs 30 iterations, repeated three times. Setup, schema creation, extension installation, reader provisioning, fixture ingestion for query cases, and cleanup are excluded. The runner checks analytical counts and expected query result sizes. Existing equivalence tests cover the more involved funnel, retention, mutation, and identity semantics.

On 13 November 2026, Linux arm64 under Docker Desktop on an Apple M1 Pro produced the following results. The Docker VM had ten CPUs and approximately 4 GiB RAM available; the benchmark container was limited to two CPUs and 2 GiB, with no swap allowance beyond that limit. Values are the median of three run averages, followed by the observed range. They are not individual-request latency percentiles. [Raw output](benchmarks/ducklake-clickhouse-arm64.txt) is included.

| Case | DuckLake | ClickHouse |
| --- | --- | --- |
| Accept and project 100 events | 66.61 ms, 65.95–74.25 | 30.90 ms, 30.53–31.28 |
| Equivalent projected event rate | 1,501 events/s | 3,236 events/s |
| Count events and unique actors, 10,000 events | 17.15 ms, 17.13–17.37 | 66.78 ms, 45.20–69.88 |
| Hourly counts and JSON amount sums, 10,000 events | 19.54 ms, 19.40–21.10 | 40.59 ms, 35.52–49.05 |
| Unique sessions from JSON, 10,000 events | 21.54 ms, 19.20–23.89 | 59.12 ms, 58.55–59.17 |

ClickHouse projected these batches about 2.2 times faster. DuckLake served these small API queries about 2.1 to 3.9 times faster. These are measurements of the current Porcupine implementations, including ClickHouse's project-view and internal HTTP overhead, not a direct engine-only benchmark. Query timing begins after fixture ingestion without waiting for ClickHouse background merges to settle. The variation between repetitions, particularly on ClickHouse counts and trends, should remain visible when reporting the result.

This small fixture does not test years of data, S3 reads, mixed ingestion and query contention, many active projects, native crash containment, or sustained hosting capacity. It cannot establish which engine has lower unit costs for a hosted service.

## Batching experiment

`make ducklake-batching-bench` uses the same two-CPU, 2 GiB Linux container with ClickHouse stopped. Each iteration accepts 3,000 synthetic events in 30 HTTP batches of 100, then drains the SQLite queue completely. The measured window includes acceptance and analytical projection. It does not sleep for a second or implement a timer. A window must finish in under one second to keep up with a hypothetical 3,000-event burst every second under these conditions.

The experiment varies the internal delivery row cap between 100, 500, 1,000 and 3,000. The existing 4 MiB conservative byte budget still applies to these four cases, so the actual chunks can be smaller. A fifth case raises the byte budget to 32 MiB and DuckDB's engine budget to 512 MiB to permit one actual 3,000-event write for these small payloads. This budget estimates worst-case JSON expansion, not raw input bytes. Production defaults remain 100 events, 4 MiB per batch and 256 MiB per engine, and there is no new public configuration or batching timer. Successful HTTP acceptance still requires a durable SQLite commit. The native HTTP request limit remains 1,000 events; a worker batch can combine several accepted requests.

Each case runs five windows of 3,000 events, repeated three times with a fresh project and database. That grows each timed fixture to 15,000 events. The runner verifies the final analytical count and reports actual chunks per window and the largest chunk. This workload differs from the earlier 100-event-per-flush microbenchmark, so only compare batching variants within this experiment.

The four 4 MiB cases produced these medians of three run averages on the same Linux arm64 environment as above. [Raw output](benchmarks/ducklake-batching-arm64.txt) includes the individual repetitions.

| Row cap | Largest actual chunk | Writes per 3,000-event window | Window time | Event rate |
| --- | --- | --- | --- | --- |
| 100 | 100 | 30 | 3.410 s | 880 events/s |
| 500 | 500 | 6 | 1.135 s | 2,644 events/s |
| 1,000 | 743 | 5 | 1.043 s | 2,877 events/s |
| 3,000 | 743 | 5 | 1.097 s | 2,734 events/s |

Larger chunks amortize queue reconciliation, analytical transactions and SQLite acknowledgments. Increasing the row cap beyond the byte cap has no further batching benefit; the two 743-row cases perform the same five-write workload and differ within the observed run variation. None of these four cases kept up with a 3,000-event window every second. The control run overlapped local Go checks during part of its execution; these remain development measurements rather than an isolated-machine capacity result.

An actual 3,000-event write with a 32 MiB byte budget [failed at the default 256 MiB engine memory limit](benchmarks/ducklake-batching-256mib-failure.txt). Raising the engine budget to 512 MiB within the same total 2 GiB container allowed it to complete. Across three five-window runs, the median was 778.77 ms per 3,000-event window, or 3,852 events/s. All fifteen measured windows finished below one second; the slowest took 874.0 ms. [Raw output](benchmarks/ducklake-batching-512mib-arm64.txt) includes the result. Run just this case with `PORCUPINE_BENCH_BATCHING=large sh scripts/analytics-benchmark.sh`.

A longer run of 30 windows grew the fixture to 90,000 events. It averaged 907.82 ms per window, or 3,305 events/s. Three windows exceeded one second, with a maximum of 1,265 ms. [Raw output](benchmarks/ducklake-batching-90000-arm64.txt) is included. Reproduce it with `PORCUPINE_BENCH_BATCHING=long sh scripts/analytics-benchmark.sh`. This is a back-to-back burst test, not an arrival-paced concurrent ingestion test. Its average clears 3,000 events/s with little headroom and does not prove that 3,000 events/s is a comfortable sustained rate, particularly with concurrent analytics or larger histories.

The larger-chunk result changes both batch size and the engine memory budget. It does not imply that a 3,000-row chunk fits the original 256 MiB setting. A native recovery test deliberately exhausts a 128 MiB engine with a 3,000-event write, checks that all events remain queued and none become partially visible, then lowers the batch size and verifies complete delivery and a duplicate retry.

A future batching policy should flush at an event or byte threshold, or at most one second after the first waiting event. Resetting a debounce timer on every arrival could defer a continuously active stream indefinitely. Waiting before projection must not weaken SQLite's durable-acceptance contract. Larger byte budgets also need testing with large events and concurrent queries before becoming deployment defaults.

## Scope

Local files are the first target. No production ingestion, live infrastructure change, or migration of existing deployment data is authorized by this experiment.
