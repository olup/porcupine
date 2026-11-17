# Hosted flush economics fixture

This experiment measures the object traffic of the hosted live flusher, query
executor, and scoped compactor. It uses the proposed two-second batch window in
the [hosted architecture](hosted-architecture.md#d2-bounded-memory-acceptance).
The fixture starts PostgreSQL 17 and Silo locally, then uses the native
DuckDB/DuckLake engine. A local Docker daemon runs Silo. No cloud account
is required.

```sh
PORCUPINE_TEST_HOSTED_ECONOMICS=1 CGO_ENABLED=0 \
  go run ./internal/cmd/shared-postgres-integration -- \
  go run ./internal/cmd/ducklake-s3-integration \
    -suite shared -race=false -timeout=600s \
    -run '^TestDuckLakeS3SharedPostgresHostedEconomics$'
```

On a shared development machine, reserve the native execution slot while the
command runs. The benchmark is opt-in. The fixture may download its local
PostgreSQL, Silo, and DuckDB extension dependencies on a cold machine.

## Workload and counters

The two workers share PostgreSQL data and catalogs and use the same bucket.
Each allows two resident project engines and sets a 128 MiB DuckDB memory
limit and one DuckDB thread per project engine. Both workers run in one
process and alternate quiet-project writes. The first worker flushes the
noisy project, and the second queries it through the store executor. Each
query checks the retained event count.

| Cohort | Projects | Events per project | Nonempty flushes per project | Modeled time per project |
| --- | ---: | ---: | ---: | ---: |
| One event per minute | 12 | 4 | 4 | 4 minutes |
| One event per second | 12 | 8 | 4 | 8 seconds |
| Noisy tenant, 250 events per second | 1 | 2,000 | 4 | 8 seconds |

The test models arrivals with timestamps and runs the flushes without wall-clock
sleeps. The one-per-minute cohort flushes one event in each occupied window.
Empty windows issue no calls in this fixture. Events use a small repeated page
payload and ordinary distinct IDs. The noisy project belongs to a separate
tenant. The fixture configures pipeline proof capacity and runs without
output destinations. Its filtered count query runs before, alongside, and after maintenance.
One project from each quiet cohort gets a maintenance pass and a read-back
check.

Lines prefixed with `hosted-economics` carry JSON. Each phase reports request
attempts by operation and response status and the HTTP body bytes forwarded in
both directions. Header and TCP/TLS bytes are excluded. The inventory lists
current objects under the experiment's disposable prefix. It includes
Parquet files that remain in the bucket after compaction. Inventory reads use
the fixture client directly and are excluded from the counted workload.
Parquet files may contain event data or supporting DuckLake tables.

## How to price the measurements

For the illustrative US East (N. Virginia) S3 Standard request model,
multiply write-class requests by $0.005/1,000 and read-class requests by
$0.0004/1,000. The write class includes PUT, LIST, and multipart initiation,
part upload, and completion. The read class includes GET and HEAD, including ordinary missing
object probes. DELETE is free in this model. Request counts and response status
come from Silo. The rates and response billing rules come from
[AWS S3 pricing](https://aws.amazon.com/s3/pricing/) and
[the AWS S3 error billing rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ErrorCodeBilling.html).

Normalize the flush phase request subtotal by its events and multiply by one
million. For storage, subtract the preceding inventory from the cohort's
inventory. Multiply incremental bytes per event by one million, divide by
2^30, and multiply by the assumed $0.023 per GiB-month. That is the cost of
holding that cohort's new objects for one full month at its sample density.
A recurring storage projection needs a retention policy and a measured file
lifetime distribution. Setup, queries, compaction, and reaping have separate
cadences.

This model covers object storage only. PostgreSQL, worker compute, transfer,
outputs, historical imports, backup/version retention, and real-provider
retries need their own measurements. The fixture calls the flusher directly
and leaves automatic ingestion and maintenance scheduling out of the run.
Production capacity, latency, durability, and crash-loss SLOs still require
the real-provider phase 7 run.

## Measured fixture run

The 17 November run used an Apple M1 Pro with 16 GiB of RAM, Go 1.26.11,
DuckDB v2.0.0 with DuckLake extension 417a9117f5, and PostgreSQL 17.11.
The focused test passed in 52 seconds on base commit `6e16c51`.
The [run data](benchmarks/hosted/economics.jsonl) records each phase, with
cumulative inventory and per-phase traffic. The table uses inventory deltas.
Dollar columns apply the pricing model above to the measured flushes.

| Cohort | Events | Flushes | PUT | Range GET | Missing HEAD | New Parquet objects / bytes | Request estimate per million events | One-month storage per million events |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| One per minute | 48 | 48 | 480 | 492 | 960 | 480 / 926,712 | $62.100 | $0.414 |
| One per second | 96 | 48 | 480 | 522 | 960 | 480 / 941,436 | $31.175 | $0.210 |
| Noisy tenant | 2,000 | 4 | 40 | 35 | 80 | 40 / 376,975 | $0.123 | $0.004 |

All three cohorts wrote ten Parquet objects per flush. The average object
was about 1.9 KiB for either quiet cohort and 9.2 KiB for the noisy cohort.
At these rates, the same request mix over a 30-day month would cost about
$2.68 per one-event-per-minute project and $80.81 per one-event-per-second
project for flush requests alone. This extrapolates four nonempty flushes per
project at the measured batch sizes. Continued file growth, cache residency,
real-provider response behavior, and the quiets' actual arrival patterns can
change the request mix.

Setup retained 150 Parquet objects (105,400 bytes) across 25 projects.
The noisy maintenance and concurrent query phase made 5 PUTs, 17 range GETs,
and 10 missing HEADs. The bucket grew by 317,123 bytes across five objects.
The two quiet maintenance passes together added 30,500 bytes across ten
objects. Old objects remain in this inventory because reaping is outside the
experiment. DuckLake reported 12 files processed and three created by the
compaction procedures per project; the proxy counts all S3 activity during
the maintenance phase.

The noisy filtered count returned 2,000 before, during, and after maintenance.
Those calls took 183 ms cold, 12 ms warm alongside maintenance, and 16 ms
warm after maintenance. The middle query overlapped about 12 ms of the
maintenance call, including its setup. Each timing represents a single query
and the cold and warm calls have different cache conditions. The isolated
before and after queries used five and one range GETs, respectively.

## Legacy guard rewrite experiment

The counting proxy also groups Parquet requests by data directory. For the
one-event-per-minute cohort, AWS Emulator 1.0.497 confirmed the original
480 PUTs across 48 nonempty flushes. Each flush wrote these files.

| Native operation | Parquet files written per flush |
| --- | --- |
| Advance the recovery fence | 1 `hosted_write_guard`, 1 `project_write_guard` |
| Commit the live batch | 1 each for `hosted_write_guard`, `project_write_guard`, `current_events`, `heads`, `native_usage_totals`, and `native_commit_proofs` |
| Acknowledge external effects | 1 `hosted_write_guard`, 1 `project_write_guard` |

`current_events` has year and month partitions. `native_usage_totals`
retains the physical directory name `live_usage_totals` from before the
catalog rename.

After hosted cutover, the hosted guard orders native writes and seals.
Hosted transactions still check the legacy guard's deletion marker.
Removing its generation updates took three PUTs and six missing HEADs out
of every flush in this cohort.

| One-event-per-minute cohort, 48 flushes | Baseline | Legacy guard read only |
| --- | ---: | ---: |
| Parquet PUT | 480 | 336 |
| Missing HEAD | 960 | 672 |
| Range GET | 492 | 396 |
| Modeled flush request cost per million events | $62.10 | $43.90 |
| Modeled flush request cost per project per 30 days | $2.68 | $1.90 |

The comparison uses the completed sparse phase from both emulator runs.
The baseline later reached the emulator's 1,000-object bucket limit in the
one-event-per-second phase. The updated flusher completed all three cohorts
with seven PUTs and 14 missing HEADs per flush. The table models request cost
using the published rates for both runs.
