# Hosted flush cost

The baseline uses the production source at `v0.15.0` (`664f12cd1779789e6f7fb63aa734ae78db912fc7`). The measurement adds a test to that source tree. It runs a single hosted project on native PostgreSQL, DuckDB, and the Silo S3-compatible fixture on the same Linux runner.

## Repeat the measurement

Run the existing fixture launchers with the test enabled. The command runs one test, without the race detector, and emits a `hosted-flush-cost` JSON record per phase.

```sh
PORCUPINE_MEASURE_HOSTED_FLUSH=1 CGO_ENABLED=0 \
  go run ./internal/cmd/shared-postgres-integration -- \
  go run ./internal/cmd/ducklake-s3-integration -suite shared -race=false \
  -timeout=15m -run '^TestDuckLakeS3SharedPostgresHostedFlushCost$'
```

The defaults are 20 measured flushes per phase and 100 unique events per flush. The test first measures with the webhook paused, then with a webhook that selects every event. It builds a history of 200 published flushes using single-event flushes, then repeats both phases. After one explicit compaction pass it measures another 20 flushes with the webhook paused. The test raises queue limits in the disposable control database because the sender is stopped and the jobs remain available for counting. Set `PORCUPINE_MEASURE_HOSTED_FLUSH_N`, `PORCUPINE_MEASURE_HOSTED_FLUSH_EVENTS`, or `PORCUPINE_MEASURE_HOSTED_FLUSH_HISTORY` to change the workload. The history setting is a comma-separated list of minimum starting sequence numbers. Use phase order and the reported actual sequence when comparing runs.

The timer covers the service-configured `FlushLive` call from project lock acquisition through publication and the native acknowledgement attempt. It excludes HTTP admission, validation, waiting in the buffer, and sending webhooks. The S3 proxy counts actual requests by method and response status. It records multipart operations separately. To compare PUTs, use `PUT/200` and check for other PUT statuses or multipart operations. The proxy also groups Parquet requests by table. The test reads the change in the highest DuckLake snapshot ID outside the timed interval. The loopback fixture measures protocol and local engine costs. It does not estimate managed storage or cross-zone latency.

## v0.15.0 baseline

Each row reports 20 flushes of 100 events. PUTs, GETs, and snapshots are per flush. [GitHub Actions run and full JSON records](https://github.com/olup/porcupine/actions/runs/44267301943).

| Phase | Webhook | Starting sequence | p50 (ms) | p95 (ms) | S3 PUTs | S3 GETs | Guard Parquet PUTs | DuckLake snapshots |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Before compaction | Paused | 0 | 193.8 | 221.9 | 7 | 5.90 | 3 | 3 |
| Before compaction | On | 20 | 536.5 | 547.4 | 109 | 108.00 | 3 | 3 |
| Before compaction | Paused | 200 | 334.9 | 345.3 | 7 | 5.95 | 3 | 3 |
| Before compaction | On | 220 | 659.2 | 674.7 | 109 | 108.00 | 3 | 3 |
| After compaction | Paused | 240 | 249.5 | 255.6 | 7 | 6.05 | 3 | 3 |

The webhook phases added 102 PUTs and 102 whole-object GETs per flush for 100 bodies, a plan, and a manifest. Every flush wrote three `hosted_write_guard` Parquet files. The paused-webhook samples at sequence 200 and after compaction differ in catalog layout as well as sequence, so those timings do not isolate the cost of scanning proof history.

## Source size

At `v0.15.0`, the scoped total is **16,100 physical lines**. It includes whitespace, comments, and SQL embedded in Go. The selection uses filename prefixes so it can be repeated across revisions.

| Files | Lines |
| --- | ---: |
| `internal/porcupine/hosted*.go` | 4,093 |
| `internal/lake/hosted*.go` | 1,501 |
| `internal/lake/native*.go` | 1,660 |
| `internal/output/*.go` | 8,846 |

```sh
git ls-files 'internal/porcupine/hosted*.go' 'internal/lake/hosted*.go' \
  'internal/lake/native*.go' 'internal/output/*.go' |
  rg -v '_test\.go$' | xargs wc -l
```

## Flush operations

The baseline inventory counts 20 responsibilities on the live flush path with a webhook. Count once per responsibility, even if the code repeats it during recovery or uses multiple calls to carry it out. Count each independent checkpoint or reservation separately. Optional operations are listed where the code supports them.

1. Acquire and release the PostgreSQL project writer session.
2. Register and finish hosted project activity.
3. Resolve the existing native project and bind the flush to its tenant and stream.
4. Persist the successor native fence.
5. Identify an existing commit by batch ID and input fingerprint.
6. Reconcile the PostgreSQL checkpoint, pending native proofs, and old reservations.
7. Settle any ready mutation encountered during reconciliation.
8. Freeze the output plan against pipeline revisions and creator authority.
9. Reserve stable event IDs.
10. Validate the native proof history and allocate the next effect sequence.
11. Deduplicate UUIDs against event heads.
12. Apply identity commands and prepare canonical event projections.
13. Match webhook rules and assemble delivery bodies or fragments.
14. Reserve output capacity and bind the plan to the flush.
15. Write immutable webhook bodies, the plan, and the manifest.
16. Record native source totals, event data and heads, and the commit proof.
17. Recheck project admission while committing the DuckLake transaction.
18. Verify staged output objects against the native proof and reservation.
19. Publish webhook jobs, advance the PostgreSQL checkpoint, and account for usage in one transaction.
20. Acknowledge published effects in DuckLake.
