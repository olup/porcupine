#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
docker build -f scripts/analytics-benchmark.Dockerfile -t porcupine-analytics-benchmark .
# No ports, host credentials or data are mounted. Native workloads run
# with an explicit CPU/RAM budget and temporary Docker volume filesystem.
docker run --rm --cpus=2 --memory=2g --memory-swap=2g \
    --env PORCUPINE_BENCH_BATCHING="${PORCUPINE_BENCH_BATCHING:-}" \
    --mount type=volume,destination=/benchmark-data porcupine-analytics-benchmark
