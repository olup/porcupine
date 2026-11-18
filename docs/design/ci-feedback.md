# Test feedback measurements

This record measures the test execution changes in [PR #54](https://github.com/olup/porcupine/pull/54) against baseline `a34027a`. Production behavior, test fixtures, assertions, dataset sizes, operation deadlines, race settings, and pre-merge scenarios were held constant.

## What changed

- Generators, tests and server builds use `-trimpath`, so the final reproducible server build can reuse compilation performed by earlier non-race commands. Race and non-race builds still have separate cache entries.
- Go cache identities include the Makefile and the relevant existing entrypoint. The old immutable `go.sum`-only cache could never retain compilation for the new flags, and the fastest documentation job could populate the shared cache without native/race objects. These component identities keep that work separate without adding a rolling cache service.
- Two backend race jobs discover the executable suite with `go test -race -list .`. Stable hashing assigns every Test, Example and Fuzz entry to exactly one job. New tests are included automatically.
- Each shard records terminal results, including subtests and fuzz seeds. The final `go` check rejects missing shards, mismatched inventories, duplicate assignments and missing or unsuccessful top-level results.
- Other packages run with the race detector alongside the backend jobs. Local `make check` and `make check-full` retain their existing short/full meanings.
- The existing nonroot Docker restoration runs independently of the native HTTP/TLS S3 journeys. The final `go` check waits for every existing validation job, including restoration and SDKs. Existing security and tag-only CLI conditions remain explicit.

No checks have been moved to nightly runs or hidden behind changed-file filters. At the time of measurement, `main` had no branch protection. The workflow provided a single aggregate check that branch protection could require.

## Local before and after

Host: Apple M1 Pro, 10 logical CPUs, 16 GiB RAM, macOS arm64, Go 1.26.11. Runs were sequential, with no other broad local suites running. Go dependencies and some compilation were cached; test execution was forced with `-count=1`. The first shard also paid for compilation under the new `-trimpath` flags.

| Measurement | Seconds |
| --- | ---: |
| Before, complete `go test -race -json ./... -count=1` wall time | 139.35 |
| Before, backend package execution | 121.243 |
| After, backend shard 0 package execution | 40.223 |
| After, backend shard 1 package execution | 47.188 |
| After, shard 0 wall including discovery and compilation | 78.69 |
| After, shard 1 wall including discovery and compilation | 54.90 |

The two shard executions total 87.411 seconds. They were measured sequentially on this machine; their maximum is not a measured parallel completion time. Cloud CI measurements below determine the actual feedback improvement and added runner usage.

Both runs produced the same 511 top-level backend outcomes: 450 passed and 61 skipped. Including nested cases, both produced 857 terminal outcomes, with 14 fuzz-seed cases and 72 skips. The comparison normalizes only the two generated project UUID names below `TestMCPConcurrentCredentials`, retaining both outcomes. Skipped external-fixture/helper cases are not counted as exercised; the dedicated SDK, S3, PostgreSQL and restoration jobs remain mandatory in the aggregate check.

Local validation also passed the remaining package race suite, contracts, vet, pure-Go helper tests, server/CLI builds, and the actual standalone CLI lifecycle/isolation/retry/revocation journey. No performance claim is based on cached test results.

Reproduce the backend measurements from the repository root:

```sh
CGO_ENABLED=1 go test -race -json ./... -count=1 -timeout=600s > before.jsonl
CGO_ENABLED=0 GOFLAGS=-trimpath go run ./internal/cmd/test-shard -index 0 -count 2 -report shard-0.json -log shard-0.jsonl
CGO_ENABLED=0 GOFLAGS=-trimpath go run ./internal/cmd/test-shard -index 1 -count 2 -report shard-1.json -log shard-1.jsonl
CGO_ENABLED=0 go run ./internal/cmd/test-shard verify shard-0.json shard-1.json
```

Run the baseline command at the baseline revision. Run the shard commands at this revision. The JSON streams include individual execution timings and all subtest results.

## Cloud CI

The baseline [PR execution](https://github.com/olup/porcupine/actions/runs/44094995473) completed in 7 minutes 12 seconds. Its successful jobs totalled 1,060 runner seconds. The Go job took 429 seconds, including 335 seconds for contracts, vet and the full race suite, followed by a 55-second build. Native integration took 272 seconds; SDKs took 160 seconds; shared PostgreSQL took 92 seconds.

A second [baseline execution on the merge commit](https://github.com/olup/porcupine/actions/runs/44096418057) had a 304-second Go job and a 280-second native integration job. That variation is material: runner scheduling, cache state and host contention prevent attributing every timing difference to the change. Both baselines restored an existing Go dependency cache. A cold compilation benchmark was not run, and this change does not promise cold-build performance.

The [first reorganized execution](https://github.com/olup/porcupine/actions/runs/44102355669) passed in 274 seconds, with 1,510 summed runner seconds. Relative to the preceding PR, feedback arrived 37% sooner but runner time increased 42%. This first run restored the old compiler cache and paid for the new `-trimpath` builds. It is evidence of lower latency, not lower compute usage. Both shard reports contain the same 511 top-level outcomes and 857 terminal cases as the local inventory, including all 14 fuzz seeds.

That measurement prompted the component-specific cache identities described above. Their first run seeds the new immutable cache entries. The subsequent warm-cache run URLs, complete per-job measurements, summed runner time and any external runner queue delays are recorded in [PR #54](https://github.com/olup/porcupine/pull/54). A warm cache does not eliminate native linking, execution, Docker restoration or artifact verification. All shard test executions still use `-count=1`; no result-cache hit is counted as a test performance improvement.
