# Local benchmark reference

This historical reference measures the optional ClickHouse backend. For the default native architecture, read [DuckLake validation](ducklake-validation.md) and [maintenance measurements](ducklake-maintenance.md).

A synthetic ten-minute run accepted and reconciled 120,000 events at 200 events/s.
It reported zero ingestion errors and zero skipped request slots. Hot receipts
stayed within the configured 1,000-row limit in every successful sample.
SQLite, WAL, and shared-memory files were about 5.05 MiB at drain, with no measured
growth during the latter half of the run.

This is a local reference, not a maximum capacity or sizing guarantee. The API,
ClickHouse, MinIO, and load generator shared one Docker host using loopback HTTP.
The API had a one-CPU/512-MiB cap; ClickHouse had two CPUs/2 GiB; MinIO had
one CPU/512 MiB. The host was arm64 macOS with Docker Server 28.3.3 and
ClickHouse 26.8.36.3. Other local test work overlapped some measurements.

| Measurement | Result |
| --- | ---: |
| Accepted / SQL-visible events | 120,000 / 120,000 |
| Average accepted throughput | 199.996 events/s |
| Ingestion p95 | 50.617 ms |
| Concurrent count queries | 561 |
| Count-query p95 | 111.764 ms |
| Final count reconciliation and queue drain | 0.733 s |
| Sampled maximum pending originals | 240 |
| Sampled maximum oldest pending age | 1.040 s |
| Sampled maximum API / ClickHouse memory | 19.95 / 602.40 MiB |
| Sampled maximum API / ClickHouse CPU | 10.47% / 60.86% |
| Historical UUID-fact queries | 3,000 |
| Logical ClickHouse fact-query reads | 12.78 GiB |

Percentiles are approximately two-percent histogram upper bounds. HTTP latency
excludes OAuth token exchange. Samples were taken every five seconds and are not
continuous peak measurements. SQL reconciliation checks aggregate cardinality,
not every stored field.

Each event carried a UUID, visitor ID, timestamp, run marker, and 512 bytes of
synthetic property padding. The reader issued a parameterized count about once
per second. Separate writer and reader clients used 60-second OAuth tokens and
renewed during the run. The queue budget was 100,000 events and 256 MiB.

After drain, the test explicitly moved a receipt partition to native S3 storage
in isolated MinIO. An early batch of 20 UUIDs, absent from SQLite's queue, hot
receipts, and mutation heads, returned zero newly accepted events and 20
duplicates on retry. The SQL count remained 120,000. This checks native cold
placement and historical lookup, not elapsed TTL movement or external S3
performance. Filesystem and object caches may have served reads.

## Reproduce

Use Docker, Python 3, and the repository's Go toolchain. The script creates
fresh projects, OAuth clients, containers, storage, and loopback ports, then
removes its disposable resources. Reports contain synthetic run metadata and
aggregate measurements. Keep generated reports out of version control.

```sh
python3 -m unittest discover -s scripts -p 'test_soak_metrics.py' -v
python3 scripts/benchmark_oauth_local.py \
  --seconds 600 --rps 5 --batch 40 --concurrency 8 \
  --sample-interval 5 --query-interval 1 --drain-timeout 120 \
  --hot-receipt-limit 1000 \
  --history-query-substring '/* porcupine:receipts */' \
  --history-fact-substring ' PREWHERE ' --s3 \
  --output-dir benchmark-results/finite-receipts
```

The first ten seconds use ten 20-event requests per second; the remainder uses
five 40-event requests per second. The script requires complete dispatch, zero
request errors, matching accepted and SQL counts, empty delivery queues, UUID
deduplication, token renewal, and adequate successful metric samples.

The repetitive workload does not cover realistic event cardinality, TLS/WAN
costs, complex analytics, concurrent exports, corrections/deletions, sustained
cold-history ingestion, or long-term storage behavior. Test those separately
with the [load generator](load-testing.md) and the reusable recovery drills.
