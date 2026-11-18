# Usage measurement contract

Standalone and hosted deployments share this numerical usage contract. Standalone counts committed SQLite journal rows. Hosted counts retained rows after durable DuckLake commit evidence is published to PostgreSQL.

The [operator reference](../reference/usage.md) defines every metric, unit, accounting date, API parameter, resource bound and recovery limitation. Prices, invoices, subscription rules and decisions about charging incomplete observations remain outside Porcupine. A self-hosted operator gets the same measurements and configuration as the commercial deployment.

## Causal accounting

An immutable source incarnation, tenant ID, project ID and UTC day identify a cumulative bucket. Month reports aggregate days. The journal obtains live/import ownership from its authoritative project anchor. An owner-free analytical worker obtains tenant/project scope from the control store and records its own source; it must not create a local ingestion owner or require an owner journal merely to count a read.

One cumulative update joins each existing successful domain transaction:

| Transaction owner | Metered transition |
| --- | --- |
| `durableAcceptance.Accept` | Newly accepted canonical live rows, once per batch, beside acceptance, receipts, identity changes, allowance accounting and webhook outbox scheduling. |
| `importManager.commitImportChunk` | Accepted-row delta for this committed chunk, beside its durable parser offset, original outcome counts and checkpoint. No additional write/trigger for each imported row. |
| `Service.publishExport` | Compressed encoded data bytes after the winning job/attempt conditional publication update, before the same commit. |
| `Service.publishPipelineS3` | Manifest data-file bytes beside the winning receipt-window cursor publication. A retry cannot advance that cursor twice. |
| `delivery.WebhookQueue.Complete` | One unique successful delivery only when its `sending` → `delivered` acknowledgment actually changes a row, before that acknowledgment commits. |

Stable-ID ingestion retries, chunk retries and publication retries add no new accepted or published work. A live event later repeated in an import belongs to whichever source class first retained its canonical row. Controls and person snapshots count only when they create a new canonical row; an ignored/unresolved label alone does not determine whether a row was retained. Edits/deletions never refund counters.

This preserves the current quota's exact retained-UUID semantics, including identity controls and SDK no-ops omitted from the analytical `events` view. A malformed atomic batch contributes nothing. Analytics-view `COUNT(*)` is not a reconciliation source for accepted usage.

Live accounting uses the existing batch acceptance timestamp. Import chunks freeze acceptance with their committed outcome, rather than using the original event date. Output accounting dates the first successful durable operational transition; an S3 manifest may already exist after a crash, so this is not a claim about its first remote visibility time. Presigned downloads bypass Porcupine and are not inferred from output publication. Receiver-side effects are not inferred from webhook acknowledgments either: a receiver may process a request whose response is lost.

The source journal keeps one `usage_rollups` row per source/project/day, not an everlasting row per event or batch. A positive change advances the bucket revision. Duplicate-only acceptance does not create an extra metering write. Arithmetic guards reject overflow instead of wrapping a counter.

The metering hook belongs to `durableAcceptance.Accept`, not to the `liveAcceptor` caller or response serializer. `acceptance_basis: durable_commit` identifies the accounting boundary. Hosted HTTP acknowledgment only confirms memory admission. Hosted usage requires native commit proof.

## Durable query admission and actual return

`sql`, `catalog` and `profile` are interactive backend attempts. After normal authorization and capacity admission, a query start commits its immutable execution ID, tenant/project, start day and operation with count +1 and unresolved +1 **before backend dispatch**. This is an indexed transaction per query, not per event. It stores no actor, SQL, parameters or result payload.

A process can crash between this commit and dispatch. The contract therefore counts durably admitted attempts, including a possible attempt with zero backend work. It does not claim successful executions or exact CPU seconds.

After durable admission, a lightweight `Begin` starts monotonic time immediately before `LakeStore.Query`, `Catalog` or `Profile` is invoked. It finishes immediately at that method's actual return, using Phase 1's existing native-return boundary. Service context setup before dispatch, HTTP status enrichment, profile summarization and final JSON serialization are excluded. Timeout, HTTP cancellation, expired query permits and a stopped client do not prove native execution has returned.

`Finish` captures the duration in memory without waiting for SQLite observation I/O, then releases the execution permit through the existing lifecycle. A common ten-second maintenance loop persists cumulative observations and final completion; there is no ticker per query. Only the delta above the prior persisted elapsed value is added. Completing twice or replaying an observation adds nothing twice. All duration belongs to the query's start day, so a late return can increase an earlier bucket.

An unreturned or crash-interrupted execution retains its last measured lower bound and unresolved marker. In-memory final completion can be lost on a crash, including all duration of a short execution. The pre-dispatch admitted count survives. Standalone restart marks unfinished records as lost. Hosted workers sweep open executions that have gone five minutes without an observation. A worker that resumes observing an execution clears the lost flag. Recovery preserves the admitted count, measured duration and unresolved marker. Numerical query records follow the same retention horizon as their buckets.

Integer microseconds are the stored unit. The wire uses decimal seconds with six fractional digits. `Statistics.Elapsed` and HTTP latency are not substitutes. Billing treatment of unknown tails belongs to the external controller.

## Cumulative publication boundary

`internal/usage` owns typed `SourceTotals`, `Recorder`, `Store` and snapshot types. A report contains source incarnation, tenant/project/day, revision, cumulative counter values, unresolved executions and last query observation time. These types are the replacement seam; SQL table layouts are not an external integration contract.

The source publisher sends at most 1,000 dirty buckets per pass. The control-store transaction accepts a newer revision only if all cumulative counters are nondecreasing. An equal revision must contain identical numerical state; conflicting contents are an explicit error. An older replay is a no-op. The source advances its published-revision checkpoint only after the control transaction commits. Losing either acknowledgment causes a replay, not double counting. Unresolved count is a gauge within the bucket and can decrease when a real finish is observed.

This is two durable stores with reconciliation, **not** an atomic cross-database transaction. Only durably published totals appear in the report. `publication_status: unknown` deliberately supplies no global sealed-through-time guarantee across journals, paused sources or workers. A consumer upserts cumulative values and revisits recent periods; it must not add consecutive poll responses. The first page attempts a bounded local publication pass, without pretending it synchronizes every source.

Hosted live and import commits persist usage evidence with the native DuckLake write. PostgreSQL publication applies each effect once, including its monthly ingestion-budget charge. Budget enforcement and `get-tenant-usage` read the same monthly counter. Hosted admission refreshes its policy cache periodically, so it can briefly lag a budget change or a newly published count.

## Current Parquet sampling

The standalone sampler opens only an already registered ready project and uses DuckLake's current-file catalogue API inside one analytical read transaction. The hosted sampler reads current file metadata directly from PostgreSQL in a single statement, without opening an analytical engine. It deduplicates referenced data/delete paths and sums their recorded sizes. It does not list the S3 bucket or count obsolete files, orphan objects, metadata databases, backups, exports or destination objects. A sampling failure does not replace the last successful value with zero. Sampling is maintenance, not an interactive query attempt.

Sampling runs approximately hourly. Hosted workers page through projects with missing or old samples and share sample freshness through PostgreSQL. The control store retains the latest sample per project/UTC day; later samples replace that day's earlier sample. V1 provides no hourly sample history or byte-time integral. A row uses the latest available sample at or before the earlier of its bucket end and report creation. Age beyond two hours marks that measurement stale. Tenant sums report measured/expected projects and oldest/newest observation times; any missing project makes the total unknown rather than inventing zero coverage.

Current catalogue removal is the zero-footprint boundary. An ingestion fence or a tenant state change alone is not proof of zero files. Provider object versions and backup retention remain outside this current-reference gauge and its deletion claims.

Zero publication and project removal share the control transaction. A sampler rechecks and, in PostgreSQL, key-share locks that project row before publishing its value. A bare `INSERT … SELECT EXISTS(project)` is insufficient: its old statement snapshot can survive a wait behind deletion's zero and overwrite it after the project disappears. The real PostgreSQL regression reproduces that overwrite without the lock and verifies zero survives with it.

## Replayable bounded snapshots

`GET /api/v1/usage` is registered once with Huma as `list-usage`, requiring `instance:admin`; CLI generation and MCP use that operation and live authorization on every call. A tenant filter does not grant authority. The existing tenant-admin ingestion-budget endpoint remains separate.

The first page materializes numerical rows from a consistent control-store read snapshot. Subsequent pages read stored encoded pages, not live aggregates. `updated_at <= watermark` on mutable totals can lose rows when they change between pages; a sequence allocated before commit does not establish commit order. Neither is used as a false snapshot boundary.

Before any large read/allocation, a short control transaction locks the usage epoch and admits one global construction reservation against retained snapshot count and byte capacity. Each process also admits one local constructor. Construction then uses an independent read transaction, with no writer transaction held across encoding. The final short transaction verifies the epoch and reservation token before publishing all pages. Errors release the reservation; expiration recovers a crashed builder. An expired reservation under a partition is not a global process-memory guarantee.

The reservation also bounds the project × bucket working set **before** maps, aggregate grids or wire strings grow. A conservative 2 KiB working allowance per row may reject a broad request before its final encoded bytes would reach 32 MiB. Actual encoded pages are measured too. The absolute ceilings are 366 days, 100,000 rows and 32 MiB; they do not guarantee every request below all three fits the available construction capacity. Narrow the period or tenant when it does not.

Snapshots expire after 15 minutes. Defaults are 100 rows/page, maximum 1,000; retained capacity is 16 snapshots and 128 MiB of encoded row data, configurable. Cursor identity includes snapshot, epoch, filter hash and page. Stored canonical filters/page size must match any repeated parameters. An exact cursor replay returns the same rows while it exists. Missing/expired/replaced epochs return `410`; malformed/mismatched cursors return `400`. There is no destructive read or reset acknowledgment.

## Retention, deletion and restore

Numerical history defaults to 365 days, configurable. No usage table has a cascading tenant/project foreign key. It retains opaque IDs, dates and numbers, not names, emails, SQL, parameters, credentials or event bodies. Opaque IDs remain linkable and are not called anonymous. The older detached tenant-deletion audit is a combined historical allowance total; it cannot reconstruct a past project/live/import split and is not counted again as if it could.

`available_since` identifies the instrumentation boundary. Entirely older buckets omit `counters`; overlapping buckets are partial. Neither old analytical rows nor old combined allowance counters are used to invent detailed history. Retention also removes snapshots that refer to expired periods.

Offline restore rotates both the control usage epoch and the journal source incarnation. Existing numerical values belong to the restored history, not a refund or new consumption to add to a previously settled history. Old cursors are invalid. Before shared PostgreSQL PITR, operators must stop/fence writers, reconcile pending evidence and rotate the epoch before reopening access. The complete distributed recovery tool and automatic rewind detection are not implemented before the hosted recovery phase. The external billing consumer must reconcile its own baseline; Porcupine cannot infer a financial correction.

## Documentation fact ledger

| Source | Destination | Facts preserved or clarified |
| --- | --- | --- |
| Operator-usage section in `control-plane-contracts.md` | This record and `reference/usage.md` | Units, live/import separation, current versus obsolete bytes, actual-return duration, unique webhook acknowledgment, cumulative polling and no pricing. |
| Initial usage draft, proposed API | `reference/usage.md` | Chosen `start`/`end`, day default, grouping, cursor replay, expiry/resource limits and explicit unavailable coverage. |
| Initial usage draft, source-to-owner table | Causal accounting and publication sections above | Existing acceptance/checkpoint/ACK transactions; no per-event triggers, no post-commit callback pretending to be atomic. |
| Initial usage draft, query/storage/restore open decisions | Lifecycle, sampling and recovery sections above | Ten-second observations, hourly daily-latest gauge, two-hour staleness, 365-day numerical retention, no byte-time integral and explicit restore epochs. |
| Journal schema inventory | `hosted-architecture.md` | Disposition of both new journal tables and future hosted durable-evidence requirements. |

Existing published guides are retained. See the hosted architecture for the deployment profile and its recovery limits.

## Acceptance cost evidence

Measured on Apple M1 Pro, Darwin/arm64, with the same prebuilt test binary, SQLite configuration, synthetic payloads and native `/ingest/v1/events` handler through in-process `httptest` requests. Socket/network transport is excluded. Each case sends 100 requests, repeated three times. Only the recorder pointer is disabled in the private test control; receipts, identity, allowance checks and journal commits remain enabled. There is no product flag to turn accounting off.

| Fixture | Meter off median | Meter on median | Observation |
| --- | ---: | ---: | --- |
| One event/request, original dynamic SQL | 0.504 ms | 0.572 ms | +13.6%; about 55 extra allocations/request |
| 100 events/request, original dynamic SQL | 8.569 ms | 8.408 ms | Within 8.31–8.77 ms spread; no speedup claim |
| One event/request, constant SQL | 0.536 ms | 0.609 ms | +13.6%; about 29 extra allocations/request |
| 100 events/request, constant SQL | 8.918 ms | 8.910 ms | Within 8.74–10.69 ms spread; no speedup claim |

Hoisting the SQL statements reduced the one-event allocation delta from roughly 6.2 KiB to 0.8 KiB/request, but did not demonstrate a latency improvement. The extra durable update still costs about 0.07 ms in the single-event fixture. Batching amortizes this update; the test does not establish sustained service throughput or a production capacity guarantee. Setup, payload generation, shutdown, source publication and analytical projection are excluded from the measured loop; it includes HTTP parsing/authentication and durable journal acceptance, not only an isolated UPSERT.

Both the [original unfavorable results](usage-acceptance-original.txt) and the [constant-SQL rerun](usage-acceptance-constant-sql.txt) are retained. Reproduce with `go test ./internal/porcupine -run '^$' -bench '^BenchmarkUsageCaptureAcceptance$' -benchtime=100x -count=3` in a quiet local window. Benchmarks are manual and do not lengthen ordinary CI tests.

Measurement provenance: the original dynamic-SQL run used the in-progress usage worktree based on `7dc52b9`, before its first clean commit. The final posting and benchmark implementation were captured in `9db83e8` before rebase, on base `47e5cd7`; the final binary also contained in-progress non-acceptance timing/snapshot changes. Both runs preceded PR #60's acceptor-interface/acknowledgment-response change. Their claims are restricted to the within-binary on/off durable HTTP acceptance comparison above, not exact timings of the later rebased service or hosted buffering. Renaming that transaction owner to `durableAcceptance` does not relocate its posting outside the SQLite commit.
