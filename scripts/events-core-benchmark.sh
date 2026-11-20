#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
baseline=${1:-ff63be0}
benchmark_dir=$(mktemp -d)
trap 'git worktree remove --force "$benchmark_dir/base" && rm -rf "$benchmark_dir"' EXIT
git worktree add --detach "$benchmark_dir/base" "$baseline"

for file in event_catalog_benchmark_test.go events_core_benchmark_test.go native_proof_history_benchmark_test.go; do
  cp "internal/lake/$file" "$benchmark_dir/base/internal/lake/$file"
done
(
  cd "$benchmark_dir/base"
  CGO_ENABLED=1 go test -c ./internal/lake -o "$benchmark_dir/baseline.test"
)
export PORCUPINE_TEST_EVENTS_CORE_BENCH_BASELINE="$benchmark_dir/baseline.test"
CGO_ENABLED=0 go run ./internal/cmd/shared-postgres-integration -- \
  go run ./internal/cmd/ducklake-s3-integration -run '^TestDuckLakeS3SharedPostgresEventsCorePerformance$' \
  -race=false -timeout=25m
