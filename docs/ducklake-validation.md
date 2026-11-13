# Product analytics architecture validation

This is the initial prototype validation checkpoint. Its statements about unchanged defaults and deployment apply to that checkpoint. The [sustained maintenance follow-up](ducklake-maintenance-soak.md) records the later policy selected for v0.11.0 and its release-profile checks.
On 13 November 2026, the local DuckLake prototype passed the functional product analytics and process-recovery checks in this report. With a million-event history, its small analytical queries were faster than the current ClickHouse request path. The short 1,000-events/s mixed-load test passed. The 3,000-events/s stress target and the maintenance-under-load target did not pass.

These results support continuing the DuckLake experiment. They do not establish a production SaaS capacity or a lower hosting bill. The main issue measured here is write delay during maintenance. No deployment or default setting changed.

The [maintenance investigation](ducklake-maintenance.md) now separates flush and compaction costs, compares a smaller native pass and records direct transaction-conflict probes.

## Reproduce

```sh
make ducklake-architecture-test
```

Docker is required. The command builds a test image, then runs each scenario in a separate disposable container. It uses no production credentials or event data. Full logs and JSONL observations go into timestamped directories under `benchmark-results/architecture/`. A nonzero exit can indicate a missed performance predicate as well as a failed integrity check; inspect the recorded verdicts.

To select scenarios:

```sh
PORCUPINE_ARCH_ENGINES=ducklake \
PORCUPINE_ARCH_PHASES='history mixed projects guard payloads crash' \
sh scripts/architecture-benchmark.sh
```

After building once, set `PORCUPINE_ARCH_SKIP_BUILD=1` to reuse the image. Rebuild after changing Go code. `PORCUPINE_ARCH_RUN_ID` can name a run directory using letters, digits, underscores and hyphens. Use a new name for each run.

The [validation plan](ducklake-validation-plan.md), [decision trail](benchmarks/ducklake-validation-decisions.tsv), committed Go tests and raw observations make up the evidence. A prior disk-full fixture run and an unequal-block ClickHouse setup attempt are preserved separately and excluded from the comparisons below. Both final historical fixtures use 10,000-row setup blocks.

## Conditions

Each container had two CPU cores, 2 GiB RAM and no additional swap allowance. Tests ran sequentially on Linux arm64 under Docker Desktop on an Apple M1 Pro. The Docker VM had ten logical CPUs and about 4 GiB RAM available. This was a development machine, not an isolated benchmark host.

The versions were Go 1.26.11, DuckDB 2.0.0 with the matching DuckLake extension, and ClickHouse 26.8.36.3. The experimental DuckLake engine budget was 512 MiB per active project with two native threads. The ClickHouse reader retained its existing 256 MiB per-query limit. Both services used the same total container limit and an experimental worker cap of 1,000 events plus the existing 4 MiB conservative byte budget. These are complete application profiles, not identical internal engine limits.

The historical fixture contains pageviews, completed signups and purchases, with stable actors, distinct sessions, country and plan properties, and timestamps spanning several days. At one million events there are 10,000 actors and 333,334 sessions. It contains repeated successful journeys; it is not a worst-case distribution for cardinality or conversion logic. The smaller heterogeneous equivalence fixtures remain part of the functional test suite.

History is seeded through trusted SQL into the analytical stores. This setup exercises real storage but bypasses HTTP ingestion and the historical SQLite receipt ledger. Timed live events use the public HTTP handlers and the real background delivery worker. HTTP requests use an in-process recorder, so latency excludes network, TLS and reverse-proxy costs. All analytical calls bypass result caching. Engine and filesystem caches are warm.

## Analytical queries on one million events

Values are median handler times from five successive calls, not robust tail-latency estimates. Each query checks known fixture results, including actor counts, dimension cells, sessions, funnel conversions and next-day retention.

| Analysis | DuckLake | ClickHouse |
| --- | --- | --- |
| Events and distinct actors | 31 ms | 132 ms |
| Events by day and custom country property | 131 ms | 433 ms |
| Distinct sessions | 83 ms | 537 ms |
| Ordered pageview → signup → purchase funnel | 62 ms | 545 ms |
| Next-day retention | 106 ms | 347 ms |

Both backends passed all five queries at 10,000, 100,000 and 1,000,000 events. SQL uses each engine's own dialect and the public project-scoped query API. These timings include validation, project views, identity handling and result encoding, plus ClickHouse's internal HTTP connection.

The full history scenario peaked at 442 MiB of container memory for DuckLake and 719 MiB for ClickHouse. The recorded managed files occupied about 151 MiB and 224 MiB respectively. These disk figures describe this compressible fixture immediately after setup and lifecycle checks; they exclude a million events' normal acceptance-receipt history and do not predict hosted cost per million events.

Raw results: [DuckLake history](benchmarks/architecture/ducklake-history.jsonl), [ClickHouse history](benchmarks/architecture/clickhouse-history.jsonl).

## Ingestion while querying

Each stage scheduled 100-event requests over ten seconds, then waited up to 30 seconds for the accepted queue to drain. Two readers repeatedly ran session counting and the ordered funnel, pausing 100 ms between calls. This is a closed-loop reader workload: the faster backend completed more queries. It is not an equal-query-rate comparison.

The producer is synchronous. `offered_rate` describes the schedule; handler delays can make the actual arrival period longer. Acceptance latencies measure handler service time, not delay from the scheduled arrival. The run records elapsed arrival time, maximum queue depth, depth at the final request, refusals, query statuses and eventual analytical counts. It does not measure individual-event visibility latency.

| Stage | DuckLake | ClickHouse |
| --- | --- | --- |
| 1,000 events/s, no readers | Passed; 900 pending at stop | Passed; 1,100 pending at stop |
| 1,000 events/s, two readers | Passed; 500 pending at stop | Passed; 1,000 pending at stop |
| 3,000 events/s, two readers | Target missed; 15,094 pending at stop | Passed; 1,665 pending at stop |
| 1,000 events/s, two readers and native maintenance | Target missed; 7,762 pending at stop | Not run |

Every offered event in these stages was accepted and eventually became queryable. There were no HTTP refusals or duplicate live inputs. All reader calls returned 200. The targets require the producer to keep close to its schedule, queue depth below two seconds of offered traffic, complete drain, and successful readers; durable acceptance alone is insufficient.

Transient background projection-error counts were not instrumented. Eventual successful drain does not establish that there were no intermediate retries or delivery failures.

At 1,000 events/s with readers, DuckLake completed 68 queries with a pooled p95 of 302 ms; ClickHouse completed 28 with a pooled p95 of 682 ms. At the 3,000-events/s target they completed 53 and 20 queries respectively. These sample counts matter when interpreting the ingestion tradeoff.

During native maintenance, queries continued to succeed and accepted events were retained, but writes fell behind. The current maintenance path holds the project write lock while flushing inlined data and compacting files. Maintenance scheduling and work per pass need improvement before this profile can promise a stable freshness target under continuous load.

Whole-container peak memory was 915 MiB for the DuckLake mixed scenario and 803 MiB for the ClickHouse mixed scenario. The native scenario includes an extra maintenance stage, so the totals are not identical workloads. Native managed disk usage reached 388 MiB after the live stages and compaction. Old snapshots and replaced files are retained; this prototype still lacks snapshot expiration and full physical purging. A hosted cost model must include that maintenance and retention work.

Raw results: [DuckLake mixed load](benchmarks/architecture/ducklake-mixed.jsonl), [ClickHouse mixed load](benchmarks/architecture/clickhouse-mixed.jsonl).

## Product semantics and operational checks

- On a million-event history, both engines resolved an anonymous pageview after identify, exposed the updated person traits, projected an event edit, deleted that event, and rejected resurrection by a late original retry. DuckLake visibility checks took about 0.13 seconds for identify and 1.1 seconds each for edit and deletion including the background-worker wait.
- Sixteen project-scoped keys were checked against separate datasets. DuckLake retained at most four resident catalogs. Round-robin access across sixteen projects had a p95 of about 209 ms, including catalog reopen work. These are serial isolation and working-set tests, not concurrent noisy-neighbor tests.
- A deliberately excessive join returned 429 after about five seconds on both backends. A subsequent count query succeeded in about 33 ms on DuckLake and 87 ms on ClickHouse. This records resource rejection and subsequent reuse; this probe does not retain the error body to distinguish the precise rejection cause. Separate native cancellation tests check interruption and connection reuse. None of these establishes immunity from every native memory exhaustion path.
- Each backend accepted and projected 1,000 events with custom padding of 0, 2 KiB, 16 KiB and 64 KiB. Larger payloads reduced throughput. The padding is repetitive and highly compressible, so these cases test allocation and queue handling rather than representative long-term storage density.
- Real subprocess kills covered reception before projection and analytical commit before SQLite acknowledgment. After each restart, all 2,000 events became visible. Replaying all 2,000 inputs returned duplicate receipts with every durable event ID unchanged. The after-commit fixture uses 500 events per write to place the kill barrier, injects a SQLite acknowledgment failure, then removes that test trigger on restart. The restarted real worker uses its default 100-event cap. This tests process failure and replay, not power loss, host destruction or replicated failover.

Raw results: [native projects](benchmarks/architecture/ducklake-projects.jsonl), [ClickHouse projects](benchmarks/architecture/clickhouse-projects.jsonl), [native query guard](benchmarks/architecture/ducklake-guard.jsonl), [ClickHouse query guard](benchmarks/architecture/clickhouse-guard.jsonl), [native payloads](benchmarks/architecture/ducklake-payloads.jsonl), [ClickHouse payloads](benchmarks/architecture/clickhouse-payloads.jsonl), [native process recovery](benchmarks/architecture/ducklake-crash.jsonl).

## Decision

Keep DuckLake as an experimental candidate for the simple headless product analytics service. The measured analytical behavior and recovery checks justify further work. The first product priority is maintenance that preserves an ingestion/freshness budget, followed by a longer arrival-paced soak with measured per-event visibility and a larger, less compressible history.

SaaS remains a separate architecture gate. This round does not verify analytical S3 storage, remote catalogs, horizontal writers, query-worker process containment, per-project resource fairness, physical-erasure deadlines, backups or regional disaster recovery. Native allocations share Porcupine's process; the engine memory setting is not a tenant-level hard limit. An EU deployment and an object-storage design can be pursued later, but neither is established by these local SSD tests.

## Attention

Reviewed by GPT-6 Astra. The independent review identified incomplete fixture/lifecycle assertions, an overwritten pilot log, and an incorrectly successful fixture-failure path. The assertions and failure handling were corrected, run directories are now separate, and setup failures are preserved and excluded. The remaining interpretation limits are the short runs, in-process synchronous producer, closed-loop readers, synthetic fixture, serial project checks and absent end-to-end event-freshness measurements.
