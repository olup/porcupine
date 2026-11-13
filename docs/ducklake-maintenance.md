# DuckLake maintenance investigation

The measured delay came from file compaction while Porcupine held its project write mutex. Flushing inlined data took about 31 ms; compaction took about 7.82 seconds. Receiving events into SQLite and reading existing analytical data do not acquire that mutex. The load run accepted every event and every reader request succeeded, but its pooled timings do not establish which requests finished during the hold.

A smaller native compaction policy reduced the measured mutex hold to 0.71 seconds and peak queued events from 8,100 to 2,300. It did less work in that pass and still missed the experiment's 2,000-event queue budget. This is a useful candidate, not a completed freshness fix. SQLite, the ingestion architecture, deployment and production defaults remain unchanged.

## What the engine already provides

DuckLake implements snapshots, transactional file replacement, inlining, compaction and conflict detection. Porcupine should call those operations rather than implement its own file merger. Scheduling the operations remains an application responsibility: `auto_compact` selects eligible tables when a maintenance call runs; it does not start a background compaction worker. [Native compaction documentation](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files.html).

Our current pass flushes inlined data and then calls `ducklake_merge_adjacent_files` while holding the same project mutex as event projection. The mutex also protects the application's stable-ID and revision semantics. Keeping it across the whole maintenance pass is a conservative prototype choice, not a demonstrated requirement for every native compaction operation.

The default `max_compacted_files => 8` limits candidate file groups considered per table, including candidates that cannot merge. It does not limit the number of input files or wall-clock time. In the baseline, 210 input files became only two outputs, so lowering eight to one would not by itself guarantee shorter work. Target file size matters. The native documentation describes size-based, tiered maintenance for streaming workloads. [Options and tiered compaction](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files.html).

## Measured policies

Both runs used a fresh one-million-event product analytics fixture, two CPU cores, 2 GiB container RAM, a 512 MiB native engine budget, two analytical readers, and 20 seconds of requests scheduled at 1,000 events/s. Maintenance started about two seconds into the run. Each policy ran once. The reader workload is closed-loop, so completed query counts differ.

| Measurement | Baseline policy | Smaller native pass |
| --- | --- | --- |
| Target file size | Engine default | 8 MB |
| Maximum candidate groups per table | 8 | 1 |
| Time waiting for Porcupine's write mutex | 119 ms | 102 ms |
| Time holding that mutex | 7,850 ms | 712 ms |
| Inlined-data flush | 31 ms | 28 ms |
| Compaction | 7,819 ms | 684 ms |
| Input files processed | 210 | 66 |
| Output files created | 2 | 2 |
| Peak queued events | 8,100 | 2,300 |
| Accepted and eventually projected events | 20,000 | 20,000 |
| Completed analytical queries | 150 | 125 |
| Pooled query p95 | 305 ms | 496 ms |

Both passes completed real work, all reader requests returned 200, and no input was refused. Reducing the pass size shortened the write pause; it did not make the same total compaction work eleven times faster. More passes would be needed to keep up with the full file backlog. Query latency was higher with the smaller pass, but these measurements do not isolate its cause or record the number of visible files scanned. Neither run met the strict peak-queue predicate.

The measurements now record the flush and compaction durations, mutex wait and hold durations, rows flushed, input/output file counts, and completion status separately. The default `Maintain` call still considers at most eight candidate groups per table. The 8 MB / one-group policy is configured only inside the experiment.

Raw results: [baseline](benchmarks/maintenance/baseline.jsonl), [smaller pass](benchmarks/maintenance/bounded.jsonl).

Reproduce the two policies separately:

```sh
PORCUPINE_ARCH_ENGINES=ducklake PORCUPINE_ARCH_PHASES=maintenance \
PORCUPINE_MAINT_POLICY=baseline sh scripts/architecture-benchmark.sh

PORCUPINE_ARCH_ENGINES=ducklake PORCUPINE_ARCH_PHASES=maintenance \
PORCUPINE_MAINT_POLICY=bounded sh scripts/architecture-benchmark.sh
```

The tagged Go test `TestDuckLakeNativeWriteConflictSemantics` runs the direct transaction probes without the load fixture. Run it with `CGO_ENABLED=1 go test -tags ducklake ./internal/porcupine -run '^TestDuckLakeNativeWriteConflictSemantics$' -count=1 -v`.

## Native upserts and concurrent transactions

DuckLake supports `MERGE INTO` for upserts and does not provide primary-key constraints. That makes `MERGE` a possible SQL simplification, but not a replacement for every domain-level stable-ID, revision or deduplication rule. [Native upsert documentation](https://ducklake.select/docs/stable/duckdb/usage/upserting.html).

The engine handles compatible concurrent commits internally. Some logical conflicts abort the transaction and require a caller retry, including interactions among deletions, insertions and compaction. [Conflict rules](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution.html).

An open report filed on 1 November 2026 describes table-level `DELETE`/`INSERT` conflicts on DuckDB 2.0, including disjoint data. A historical report describes expensive compaction on a much larger partitioned lake using an older version; that issue is closed and was converted to a discussion. These show relevant operating concerns, not proof that either report diagnoses our workload. [Issue #1895](https://github.com/duckdb/ducklake/issues/1895), [historical issue #687](https://github.com/duckdb/ducklake/issues/687).

We tested native transaction behavior directly on the pinned DuckDB 2.0.0 engine, outside Porcupine's write mutex. One transaction worked on an existing Parquet-backed table while another committed a new row first. The table initially contained 100 even-numbered IDs; the unmatched source ID was an odd value inside that file's min/max range.

| First transaction | Outcome after the other insertion committed |
| --- | --- |
| Delete an existing row | Commit rejected with a conflict |
| Delete using an incoming-key table, with no matching row | Commit succeeded |
| Insert a new row | Commit succeeded |
| `MERGE` with update/insert clauses, unmatched source | Commit rejected with a conflict |
| `MERGE` updating an existing row | Commit rejected with a conflict |

Every case checked every final ID and value, including the concurrently committed row. Conflicting transactions left no partial changes. The probe runs on macOS arm64 with a 128 MiB engine budget and one native thread; it is a transaction-semantics test, not a performance comparison with the Linux load runs. [Raw native outcomes](benchmarks/maintenance/native-conflicts.jsonl).

This result argues against replacing the current write path with `MERGE` solely on the assumption that it will eliminate conflicts. It does not prove that the current path is optimal, or validate compaction without the application mutex. The `VERIFIED` labels in this probe refer to atomic outcomes, including clean transaction rejection.

## Next gate

Use the native compactor's size and per-pass controls to find a sustainable maintenance budget. Measure repeated passes, file backlog, analytical latency and event freshness together. Before relaxing the mutex, exercise concurrent compaction alongside original ingestion, identify, event edits and deletion, with both maintenance progress and exact final data checked. An approach that merely causes maintenance to abort indefinitely would not be a fix.

No extra ingestion service, queue system or storage engine is required by this finding.

## Attention

Reviewed by GPT-6 Astra. The review prompted more precise candidate-group terminology, narrower claims about pooled query timings and exact-row assertions in the transaction probe.

These are short, single-run maintenance comparisons on a synthetic local fixture. The smaller policy processes fewer files, still misses the peak queue budget and has higher query latency in this run. Native conflict observations apply to the tested build and statements. They do not establish that all compaction concurrency is unsafe or that all `MERGE` workloads conflict.
