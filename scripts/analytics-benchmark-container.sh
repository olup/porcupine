#!/bin/sh
set -eu
mkdir -p "$TMPDIR"
printf 'Linux architecture: '; uname -m
cat /go-version.txt
if [ -n "${PORCUPINE_ARCH_PHASE:-}" ]; then
    /analytics-bench -test.run='^TestAnalyticsArchitectureExperiment$' -test.v -test.timeout=15m
    exit
fi
if [ -n "${PORCUPINE_BENCH_BATCHING:-}" ]; then
    pattern='^BenchmarkDuckLakeBatchWindows$'
    windows=5
    repetitions=3
    if [ "$PORCUPINE_BENCH_BATCHING" = large ]; then pattern='^BenchmarkDuckLakeBatchWindows/max3000bytes32MiB$'; fi
    if [ "$PORCUPINE_BENCH_BATCHING" = long ]; then pattern='^BenchmarkDuckLakeBatchWindows/max3000bytes32MiB$'; windows=30; repetitions=1; fi
    /analytics-bench -test.run='^$' -test.bench="$pattern" -test.benchtime="${windows}x" -test.count="$repetitions" -test.timeout=15m
    exit
fi
/analytics-bench -test.run='^$' -test.bench='^BenchmarkDuckLake' -test.benchtime=30x -test.count=3 -test.timeout=15m
