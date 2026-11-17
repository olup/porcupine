# Hosted maintenance fence

Unit A2, unreleased. `DuckLakeStore.MaintainHostedProject` adds an explicit
compaction entry point for an already-held project coordinator session. No
background caller is enabled. Legacy `Maintain`, `CleanupDue`, and arbitrary
`MaintenanceCommand` calls still refuse hosted v4 catalogs. Standalone and
shared-v2 maintenance keep their existing behavior.

## Native snapshot fence

DuckLake cannot commit the ordinary guard-row update and compaction together.
The verified extension's
[`WriteSnapshotChanges`](https://github.com/duckdb/ducklake/blob/417a9117f590e8141742ac0579d02f6a90162e37/src/storage/ducklake_transaction_state.cpp#L421-L423)
rejects compaction combined with ordinary changes, even across different tables.
A separate guard update followed by a CALL is unsafe. Reading the guard in an
explicit transaction is also insufficient with default native commit retries.
Compatible concurrent snapshots can be retried against newer metadata without
revalidating the application's epoch read.

The guarded path uses the same authoritative epoch, nonce, stream, open state,
and identity-ready record as ordinary writes. Each maintenance attempt reads that
record and the project-deletion guard inside an explicit native transaction.
It records `ducklake_current_snapshot('lake')` after the guard read and verifies
that the CALL retains that snapshot before COMMIT. Native commit retries are zero
on this dedicated engine.

In the pinned source,
[`GetLatestSnapshot`](https://github.com/duckdb/ducklake/blob/417a9117f590e8141742ac0579d02f6a90162e37/src/storage/ducklake_transaction.cpp#L1747-L1762)
caches the transaction's snapshot. The
[first commit attempt](https://github.com/duckdb/ducklake/blob/417a9117f590e8141742ac0579d02f6a90162e37/src/storage/ducklake_transaction_state.cpp#L2651-L2736)
inserts snapshot `captured + 1`. A successor fence commit occupies that ID before
it can advance further. The stale maintenance commit then fails the native
snapshot primary key constraint. With zero retries, it cannot refresh the
snapshot and silently preserve the old guard decision. Hosted snapshot/history
expiration remains disabled; deleting intervening snapshot records would require
another proof.

External retries are bounded by the existing 16-attempt policy. Every attempt
rechecks the physical coordinator session through `RunNative`, begins a fresh
native transaction, and rereads the original fence. It never adopts another
acquisition's epoch. The outer `RunNative` includes engine opening, attempts,
rollback, and engine close, so `Release` joins actual returns and cleanup.
Control loss requests cancellation. A delayed CALL can still write its temporary
S3 output; the guarantee concerns its committed native file set after successor
fencing. Native rollback removes its own uncommitted outputs.

This contract is enabled only for the verified DuckDB `v2.0.0` and DuckLake
`417a9117f5`. Another engine build fails explicitly on the new path. The official
[configuration](https://ducklake.select/docs/stable/duckdb/usage/configuration)
and [conflict rules](https://ducklake.select/docs/stable/duckdb/advanced_features/conflict_resolution)
describe the supported retry setting and snapshot primary key mechanism. Source
inspection and the delayed-call tests establish the narrower pinned contract.

## Resource and scheduling bounds

The caller acquires the existing project session. The method discovers the
existing stream and advances its fence, without a second coordinator acquisition.
A resident existing-only project lease protects the store's lifetime. The method
opens one extra existing-only maintenance engine, with the configured memory and
thread limits and one SQL connection. It sets `ducklake_max_retry_count=0` before
applying the existing user-query configuration seal. Ordinary engines retain
their retry policy. No configuration permission is added to user SQL.

The store's cleanup guard permits at most one such extra engine per store,
including while opening and closing it. This is a fixed allowance above
`MaxOpenProjects`. Engine close precedes resident-lease release and callback
return, including failure paths. Opening failure closes its partial engine.

One call runs a merge pass with a 1 MiB file-size filter, then a partial-delete
rewrite pass. Both consider at most one candidate group per table. The method
requests a 30-second context deadline; cancellation does not prove native work
has ended. Native memory/thread limits and metadata/S3 timeouts still apply.
As the [compaction documentation](https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files)
explains, candidate-group limits do not bound the number of input files or the
operation's duration.

No scheduler invokes this method automatically. Its eventual caller must choose
when a project is due and has useful work. Reopening a dedicated engine adds
startup, PostgreSQL, and S3 requests. The correctness fixtures do not measure that
overhead, sparse-project economics, throughput, or a suitable polling cadence.

## Evidence and reproduction

Actual fixtures use PostgreSQL 17.11, two independent native DuckLake engines,
and an isolated S3-compatible server. The race fixture pauses one actual native
Parquet output PUT. Its callback deliberately uses a bounded context without
session cancellation, modeling an executor that remains active after control
loss. This does not claim that the driver naturally ignores cancellation.

| Candidate | Result after control backend loss, successor fence, and seal |
| --- | --- |
| Guard update plus real CALL in one transaction | CALL writes output; COMMIT rejects mixed changes and compaction. Committed guard and file paths stay unchanged. |
| Guard committed before autocommit CALL | Old merge and rewrite both commit after seal. |
| Guard read plus CALL, default native retries | Captured snapshot stays fixed through CALL, but COMMIT retries and commits after seal. |
| Guard read plus CALL, native retries zero | Captured snapshot stays fixed. Old COMMIT fails at its original snapshot ID. Committed paths and exact row count survive; the attempt's own new output is removed. |
| Fresh valid owner, native retries zero | Merge and rewrite commit successfully. |

The tests exercise the actual production opener and attempt as well as the native
comparison. The public entry point also proves useful compaction, fresh external
retry after an intervening native commit, unchanged ordinary engine retry
settings, seal refusal, active-store close refusal, and lease/engine cleanup.
The fixtures use 128 MiB and one native thread per engine. A pass is correctness
evidence, not a load or performance result.

Run the normal focused tests with the existing fixture runners. Add the opt-in
comparison to reproduce the unsafe alternatives and delayed executor model:

```sh
PORCUPINE_TEST_HOSTED_MAINTENANCE_EXPERIMENT=1 CGO_ENABLED=0 \
  go run ./internal/cmd/shared-postgres-integration -- \
  go run ./internal/cmd/ducklake-s3-integration \
  -suite shared \
  -run '^TestDuckLakeS3SharedPostgresHostedMaintenance' \
  -race=true -timeout=180s
```

The opt-in comparison bypasses the legacy gate only on disposable fixtures. It
seeds synthetic native rows and one partial deletion. It runs no physical purge,
project drop, snapshot expiration, or proof/history pruning.

## Remaining lifecycle boundaries

Compaction changes physical file layout while preserving the existing logical
history. It supplies no event/effect publication, restore-retention policy,
physical reader/drop drain, catalog removal, or orphan purge guarantee. Those
remain separate units. No control or native migration is added here.

## Documentation fact ledger

The recovery document's previous "no guarded compaction implementation" status
is superseded by this explicit component. Its legacy activation gate, reader
drain, and physical deletion limits remain there. This page adds the pinned
source contract, resource allowance, and rerunnable comparison. No reference
guide or remaining lifecycle requirement is removed.
