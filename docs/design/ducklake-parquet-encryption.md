# DuckLake Parquet encryption

Porcupine opts in per analytical source. The SQLite native registry records the mode in step 2. PostgreSQL records it in native registry step 6. Existing initialized sources retain plaintext mode. Workers must agree with the registry, and each managed attach also supplies the expected `ENCRYPTED` mode. DuckLake checks it against the project catalog.

## Catalog creation and migration

The pinned driver is `duckdb-go/v2` v2.20000.0. The local probe used DuckDB v2.0.0 and DuckLake `417a9117f5`, which created catalog format 1.1. `ENCRYPTED` sets the mode at catalog creation. Reattach with the opposite value and DuckLake rejects the attach. Omit the option and the catalog supplies its recorded mode. A plaintext catalog keeps writing plaintext, including during compaction. Enabling encryption for existing data requires a rebuild in a fresh catalog. [Pinned attach tests](https://github.com/duckdb/ducklake/blob/417a9117f5/test/sql/encryption/encryption_attach_mismatch.test)

For an existing standalone source, restore a native backup into a fresh target with encryption enabled. Restore reconstructs current rows and identity state, and DuckLake writes new encrypted files. The original files and backup retain their original protection. Offline restore requires encryption when the archive catalog is encrypted. It rebuilds current contents and starts a new physical snapshot history. [Offline restore](../reference/native-backup-recovery.md#restore-with-parquet-encryption)

To verify a rebuild, compare scoped event counts, stable IDs, revisions, and identity results with the source. Inspect the target catalog from an operator session. In each PostgreSQL project metadata schema or local DuckLake catalog database, both counts should be zero. `end_snapshot IS NULL` selects the current files. If the cutover must cover retained snapshot history as well, repeat the checks without that predicate. [Pinned file and key tests](https://github.com/duckdb/ducklake/blob/417a9117f5/test/sql/list_files/list_files_encrypted.test)

```sql
SELECT 'data' AS file_kind, count(*) AS files_without_key
FROM ducklake_data_file
WHERE end_snapshot IS NULL AND (encryption_key IS NULL OR encryption_key = '')
UNION ALL
SELECT 'delete', count(*)
FROM ducklake_delete_file
WHERE end_snapshot IS NULL AND (encryption_key IS NULL OR encryption_key = '');
```

Also check that the inventory contains the expected objects and that a sample `read_parquet` without the catalog fails. Keep key values out of reports and logs.

## Inlining, snapshots and recovery

An encrypted data or delete Parquet file uses a key generated on write. Files from one write can share a key. The catalog holds the keys as well as plaintext metadata such as column statistics. In a local probe, an inlined value was readable in the catalog. Hosted attaches keep inlining off, and encrypted standalone catalogs also disable it. [Pinned inlining test](https://github.com/duckdb/ducklake/blob/417a9117f5/test/sql/data_inlining/data_inlining_encryption.test)

Current reads and historical reads worked across deletes, compaction, and reopen in the local probe. The same hosted attach statement, tested with local PostgreSQL 17 metadata and local files, produced encrypted data and delete Parquet with catalog keys. Both modes returned the same current and historical results after reattach. A read-only attach of the relocated checkpoint catalog inferred encryption from the catalog and read the archived files. The Porcupine backup/restore tests also reconstruct encrypted targets and open them independently of the archive. Managed project deletion removes its catalog and files. Backup retention and S3 version retention can preserve earlier copies.

A native bundle contains the project catalogs with their keys, encrypted Parquet files, the SQLite journal, and control data. Access to the complete bundle permits recovery of the data. Hosted recovery likewise needs a PostgreSQL recovery point and the corresponding object versions. Other objects in the same bucket, such as pipeline bodies, follow their own storage policies. Scheduled S3 query deliveries and one-time exports keep their existing readable formats for downstream consumers. [Storage and backups](../storage.md#encrypt-analytical-parquet-files)

## Small local measurements

Darwin arm64, DuckDB v2.0.0, DuckLake `417a9117f5`, one engine thread, 512 MiB engine limit, local catalog and files. Each fixture used four equal batches with inlining off. The flush measures the hosted native head lookup and canonical event writes through DuckLake commit. It excludes PostgreSQL, S3, hosted coordination, and effect publication. This is a local measurement of the native portion of hosted flush latency.

Each cell shows median wall / process CPU in milliseconds across three paired runs with alternating mode order. Flush time is the sum for four batches. Queries have one untimed warm pass. The test checks exact result parity and reads again after compaction.

| Operation | 4,000 plain | 4,000 encrypted | 40,000 plain | 40,000 encrypted |
| --- | ---: | ---: | ---: | ---: |
| Native flush | 196.8 / 204.5 | 163.5 / 168.1 | 784.8 / 808.2 | 667.5 / 690.8 |
| Count by day | 13.2 / 13.5 | 11.5 / 11.6 | 17.2 / 17.6 | 15.8 / 15.9 |
| Event profile | 33.7 / 38.0 | 34.0 / 36.6 | 46.7 / 46.7 | 38.9 / 47.7 |
| Event catalog | 13.0 / 13.3 | 12.7 / 13.6 | 18.6 / 18.8 | 15.9 / 17.7 |
| Adjacent-file compaction | 39.2 / 39.2 | 101.2 / 101.3 | 46.8 / 47.0 | 94.3 / 94.5 |

| Events | Phase | Active files | Plain bytes | Encrypted bytes |
| --- | --- | ---: | ---: | ---: |
| 4,000 | Flushed | 8 | 826,882 | 833,794 |
| 4,000 | Compacted | 2 | 829,028 | 831,034 |
| 40,000 | Flushed | 8 | 7,905,959 | 7,912,835 |
| 40,000 | Compacted | 5 | 8,062,915 | 8,068,812 |

Active Parquet for `current_events` and `heads` grew by at most 0.84%. Encrypted compaction took about 2.0–2.6 times as long and was slower in every pair. Treat the other timing differences as observations from this fixture. The runs are too small to estimate hosted latency.

```sh
PORCUPINE_TEST_ENCRYPTION_BENCH=1 CGO_ENABLED=1 \
  go test ./internal/lake -run '^TestDuckLakeParquetEncryptionBenchmark$' -v -count=1
```

Set `PORCUPINE_TEST_ENCRYPTION_BENCH_ROWS=400000` on that command for 40 local batches of 10,000 events. The separate hosted fixture measures full flush latency, snapshot changes, and S3 requests. To compare its two modes with the same workload, run the following on an S3 test runner. This run is outside the local results above. [Hosted fixture and workload controls](archive/hosted-flush-cost.md#repeat-the-measurement)

```sh
for encrypted in false true; do
  PORCUPINE_MEASURE_HOSTED_FLUSH=1 \
    PORCUPINE_MEASURE_HOSTED_FLUSH_ENCRYPTED="$encrypted" CGO_ENABLED=0 \
    go run ./internal/cmd/shared-postgres-integration -- \
    go run ./internal/cmd/ducklake-s3-integration -suite shared -race=false \
    -timeout=15m -run '^TestDuckLakeS3SharedPostgresHostedFlushCost$' || exit $?
done
```

## Rollout recommendation

Keep encryption opt-in. Test the hosted flush and compaction costs with S3 before making it the default for fresh Cloud sources. All workers for a new encrypted source must support the setting before accepting traffic.

Keep existing Cloud projects on their current plaintext source until a hosted migration can rebuild them in a fresh encrypted source. Drain and fence the old writer, then copy current analytical state and carry over the hosted identity, commit-proof, usage, and publication authorities. Verify file keys, project-scoped results, stable-ID retries, and the new recovery point before routing writes to the target. Retain the old source for the rollback window and retire its plaintext objects and backup versions according to retention policy. The hosted cutover requires its own migration procedure.
