# Repeated DuckLake maintenance under product analytics load

This investigation follows the [single-pass maintenance measurements](ducklake-maintenance.md). It tests whether small native compaction passes keep working during continuous ingestion, and whether the compactor can run without Porcupine's event-projection mutex. Normal event projection remains serialized.

## Workload and checks

Each controlled run starts with one million synthetic product events. A synchronous in-process HTTP producer schedules 100-event native batches every 100 ms for 180 seconds, while the real SQLite delivery worker projects them. Two closed-loop API readers run session and ordered-funnel queries, pausing 100 ms between requests. Six additional lifecycle rounds send pages and identify controls, correct one event, delete another, and retry the originals.

The workload uses two CPU cores and 2 GiB container RAM, 512 MiB per native engine, two native threads, a 1,000-event projection cap and a conservative 4 MiB projection byte budget. These are explicit test settings. The projection cap is not a promise that each write contains 1,000 events.

The checks compare every accepted live event's message ID and durable event ID against the final analytical rows. First and last live batches are retried. Lifecycle checks require the returned correction revision, a projected deletion tombstone, resolved identity and traits, the corrected custom amount, and stable retry IDs. A second project must remain unchanged. A separate native race test repeats eight lifecycle rounds while compacting, closes and reopens the archive, and verifies correction, identity and retry preservation.

Queue depth and oldest pending receipt age are sampled after each ingest request. Current-snapshot event and head data-file entries are sampled every ten seconds. Maintenance reports flush/compaction durations, write-mutex time, success and input/output file counts. These counters sum native maintenance work across tables; they are not a count of files scanned by analytical queries.

The predeclared performance predicate requires arrival time within one second of the 180-second schedule, no reader failures, at least three successful maintenance passes with actual compaction progress, all six lifecycle rounds, a peak queue of at most 2,000 events, oldest sampled pending receipt age at most three seconds, and final event-file entries no more than 128 above the initial count. Integrity failure is a test failure regardless of those budgets.

## Why a size filter is needed

The first sustained local run used an 8 MB target and one candidate group per table. It accepted and eventually projected all 180,000 live events with exact durable IDs. However, current event-file entries grew from 100 to 327 and peak queue depth reached 10,873 events, with an oldest sampled receipt age of 10.97 seconds. Maintenance initially compacted files, then repeatedly completed without processing any.

DuckLake documents that an unmergeable candidate still consumes the candidate-group budget. A small limit can therefore stop making progress even when other small files remain. Its streaming maintenance example filters input file sizes. The next policy keeps the 8 MB target and one candidate group, but considers only files smaller than 1 MiB. Porcupine still calls the native compactor; it does not implement its own file merger. [Native compaction options](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files.html).

The initial lifecycle helper incorrectly waited for the entire continuously-fed queue to become empty. That assertion failed and recorded no completed lifecycle rounds. It does not establish a mutation or identity failure. The corrected helper waits for the specific identify, edit and deletion projections, then checks their analytical result. The initial run remains useful evidence of queue/file growth, but is not a successful lifecycle run. [Original observations](benchmarks/maintenance/soak/unfiltered-locked.jsonl) and [full test log](benchmarks/maintenance/soak/unfiltered-locked.log).

A subsequent local filtered run stopped when the Docker VM ran out of disk. Native Parquet writes returned `ENOSPC`, followed by an ingestion 503. This run is excluded from capacity and concurrency conclusions. Its complete [observations](benchmarks/maintenance/soak/excluded-local-filtered-disk-full.jsonl) and [log](benchmarks/maintenance/soak/excluded-local-filtered-disk-full.log) remain available.

Both filtered comparison runs used the same Linux amd64 benchmark image on Lightsail, with disposable volumes, no published ports and the same two-CPU/two-GiB container budget. The existing proxy and ClickHouse services were on that host and received no intentional test traffic. This is a shared-host experiment. The earlier local runs were Linux arm64; their timings are not an architecture-controlled performance comparison with the Lightsail runs.

## Controlled filtered comparison

Both amd64 runs passed their integrity tests. Each preserved all 180,000 live message/event ID pairs, completed six identify/edit/delete/retry rounds, kept the other project unchanged, and drained the queue. Native compaction made progress with no recorded maintenance error. Neither run met the predeclared capacity predicate.

| Measurement | Projection mutex held | Native concurrent maintenance |
| --- | --- | --- |
| Scheduled live events | 180,000 | 180,000 |
| Actual submission duration | 408.35 s | 407.37 s |
| Actual average submission rate | 441 events/s | 442 events/s |
| Completed native maintenance passes | 118 | 125 |
| Input files processed across tables | 739 | 743 |
| Maximum project write-mutex hold | 1,405 ms | 0 ms |
| Initial/final current event-file entries | 100 / 169 | 100 / 170 |
| Final head-file entries | 39 | 40 |
| Peak queued originals | 11,814 | 12,745 |
| Oldest sampled queued receipt | 32.04 s | 33.00 s |
| Reader HTTP 200 / HTTP 429 | 613 / 22 | 609 / 21 |
| Pooled reader p95 | 1,761 ms | 1,741 ms |
| Container peak memory | 944 MiB | 1,068 MiB |

Removing the application mutex did not materially improve total submission time in these runs. It did allow native maintenance to proceed without that mutex while preserving the tested lifecycle semantics. The experiment does not isolate the remaining CPU, storage and projection costs. It does not establish 1,000 events/s on this Lightsail workload.

The controlled runs include occasional lifecycle verification queries in addition to the two continuous readers. Their 429 response bodies were not retained, so their precise cause is unknown. A third caller can encounter the two-query admission limit; memory or other resource limits can also return 429. The status alone does not distinguish them.

Final file counts include the last ingestion drain after maintenance stopped. They describe current-snapshot entries, not erased historical files or a fixed file count independent of data volume. The file-size filter kept compaction working instead of repeatedly selecting an unmergeable candidate.

The controlled image used source `fe7d33e` and native DuckDB 2.0.0 on Linux amd64. Its manifest was `sha256:b9d9207406eb92ede4de7a52381d23f810a61b05edc0e1a0384c0ffce75e92d3`. [Locked observations](benchmarks/maintenance/soak/filtered-locked-amd64.jsonl) and [log](benchmarks/maintenance/soak/filtered-locked-amd64.log); [concurrent observations](benchmarks/maintenance/soak/filtered-concurrent-amd64.jsonl) and [log](benchmarks/maintenance/soak/filtered-concurrent-amd64.log).

## Selected maintenance policy

The selected implementation calls the native compactor with an 8 MB target, input files below 1 MiB and one candidate group per table. It retains serialization between event projections and a separate mutex between maintenance calls, but does not take the event-projection mutex. A timer leaves a two-second gap after each completed pass instead of immediately catching up on expired ticks. The native engine still handles snapshots, file replacement and transactional conflicts. An OOM retains the accepted queue for retry and operator recovery. The existing allocation-failure regression reduces batch size manually; there is no automatic batch-shrinking controller.

A regression test holds the event-projection mutex while calling the production `Maintain` method. Maintenance must finish, reduce native current-file entries and preserve all 500 rows. The native race suite also repeats eight lifecycle rounds during compaction and checks archive reopen/retry behavior. The controlled runs explicitly used 512 MiB and a 1,000-event cap. At that point the constructor and delivery defaults were still 256 MiB and 100 events. The release defaults were subsequently changed as described below.

## Configured release profile

An initial `Open` run with a 100-event delivery cap and an explicit 512 MiB budget could not keep up with 500 offered events/s. At its last recorded window, 87,600 events had been accepted in 267.38 seconds and 55,302 remained queued. A lifecycle readiness assertion had already timed out waiting for its identify control, while the correction and deletion had projected. The run was stopped after that failure; it does not establish a final drain or exact-data result. Its [observations](benchmarks/maintenance/soak/failed-open-100row-500.jsonl) and [unmodified log](benchmarks/maintenance/soak/failed-open-100row-500.log) are preserved.

The selected release defaults are now a 512 MiB native engine budget and a 1,000-event maximum projection batch, retaining the 4 MiB byte budget. ClickHouse delivery retains its 100-event default. The default memory constant is shared by construction and reported query limits.

A final run uses `Open` with those actual release delivery defaults, an explicit 512 MiB native memory budget matching the Lightsail deployment and native default, and a lower offered rate of 500 events/s. It replaces only Open's maintenance timer with the measured scheduler using the same native policy; it keeps Open's real delivery worker and its row/byte limits. Lifecycle controls and mutations run during ingestion, with their view assertions after the two readers stop. This avoids adding a third analytical API caller. Reader results are checked against known session/funnel totals and failed query bodies are recorded. This is a different profile and cannot replace either failed 1,000-events/s capacity verdict above.

Results for that configured profile are pending at this checkpoint.

## Limits

Current-snapshot file entries do not measure physical storage reclamation. Older snapshots and files remain until separate expiration, rewrite and cleanup policies run. This experiment does not establish a physical-deletion deadline or bounded long-term disk use.

Oldest queued receipt age is sampled, not a per-event visibility-latency histogram. The two analytical readers are closed-loop; their completed query counts can differ between policies. HTTP calls run in-process and omit network/TLS latency. Three-minute runs do not establish hosted capacity or indefinite steady state.
