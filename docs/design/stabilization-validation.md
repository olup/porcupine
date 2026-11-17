# Stabilization validation, 17 November 2026

These checks cover the development branch after the v0.14.0 review. They do not describe a new release or a production capacity guarantee.

## Functional checks

| Check | Local result |
| --- | --- |
| `make check` | Passed in 23.13 seconds, including contracts, vet, CLI, and short tests |
| `make check-full` | Passed in 91.15 seconds, including the full Go race suite |
| `make shared-integration` | Passed in 52.35 seconds with native PostgreSQL and isolated S3 |
| Published-form server and standalone CLI smoke | Passed with compiled binaries and two isolated projects |
| S3 HTTP and private-CA HTTPS fixtures | Passed, including storage failure and recovery |
| `make ducklake-restore-drill` | Passed with the rebuilt nonroot image, read-only root filesystem, rotated credentials, and restored stable event IDs |
| `scripts/check_config_compose.sh` | Passed against the same image: private init/install, masked config, restart, custom port and storage, and environment-only startup |
| Browser OTP/OIDC tests | Passed with signed local provider fixtures; no claim about a configured live Google account |
| `make compat` | All 13 supported PostHog scenarios passed |
| Segment SDK contracts | Passed, including retry duplicates |
| `npm --prefix compat run test:identify` | Passed with supported browser and backend SDKs |
| Python tooling tests | All 18 dependency-free tooling checks and 5 documentation checks passed in their respective environments |
| Strict HTML/Markdown documentation build | Passed with links and anchor checks |

An independent GPT-6 Astra review checked the storage, authorization, configuration, and test changes. It found and prompted fixes for unfamiliar catalog adoption, reordered columns, dormant-catalog recovery, global admission blocking, person snapshot retries, and container defaults. Existing project/tenant authorization and full CI suites remain in place.

The first combined restore attempt exposed a real container default-path bug. The distroless image inherited `/home/nonroot`; relative defaults therefore wrote outside `/data`. The image now starts at `/`, and the same restore drill passes without adding path overrides to hide the problem. The test prints the recovered server's logs on failure.

A short-suite failure also exposed a test fixture that expired receipts by one nanosecond despite millisecond storage. Explicit expired dates replaced that timing assumption. The affected tests passed 20 race-enabled repetitions without changing their deadlines.

## Cost of historical ID lookup

Run this opt-in Go benchmark:

```sh
go test ./internal/porcupine -run '^$' -bench '^BenchmarkReceiptFreshIDLookup$' -benchtime=10x -benchmem -count=3 -timeout=300s
```

This measures only the receipt marker check and batched DuckLake UUID lookup, not full event acceptance, analytical projection, network requests, or ingestion throughput. The fixture uses actual SQLite and DuckLake on local SSD, a warm resident engine with 128 MiB and one DuckDB thread, no background workers, and fresh IDs. The host is an Apple M1 Pro on macOS arm64. Each cell below is the median of three ten-iteration runs. [Raw output](benchmarks/receipt-lookup.txt)

| Historical IDs | New IDs in a batch | Before first prune, ms | After first prune, ms |
| --- | --- | --- | --- |
| 20,000 | 1 | 0.009 | 6.571 |
| 20,000 | 100 | 0.013 | 13.998 |
| 20,000 | 1,000 | 0.043 | 84.388 |
| 100,000 | 1 | 0.009 | 15.044 |
| 100,000 | 100 | 0.013 | 24.347 |
| 100,000 | 1,000 | 0.044 | 104.719 |

The cost is material, especially for single-event batches. Pruning bounds the local receipt ledger but introduces an analytical lookup before accepting new producer IDs. Lookup failure rejects the batch for retry. This tradeoff needs sustained-load and S3 measurements before hosted capacity or pricing claims. It is not evidence of a particular events-per-second capacity.

Exact source timestamps and flexible JSON properties remain. Typed property promotion and reduced time representations are separate optimizations to measure. No unrepeatable storage-savings percentage is claimed here.
