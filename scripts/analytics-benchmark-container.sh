#!/bin/sh
set -eu
mkdir -p "$TMPDIR" /benchmark-data/clickhouse /var/log/clickhouse-server
ch_pid=
cleanup() {
    if [ -n "$ch_pid" ]; then kill "$ch_pid" 2>/dev/null || true; fi
}
trap cleanup EXIT INT TERM
printf 'Linux architecture: '; uname -m
cat /go-version.txt
clickhouse --version
printf '\nDuckLake, ClickHouse stopped, 30 iterations, three repetitions\n'
PORCUPINE_BENCH_ENGINE=ducklake /analytics-bench -test.run='^$' -test.bench='^BenchmarkAnalyticsComparison$' -test.benchtime=30x -test.count=3 -test.timeout=15m
mkdir -p /etc/clickhouse-server/config.d
cat > /etc/clickhouse-server/config.d/zz-benchmark.xml <<'XML'
<clickhouse>
    <listen_host replace="replace">127.0.0.1</listen_host>
    <path>/benchmark-data/clickhouse/</path>
    <tmp_path>/benchmark-data/clickhouse/tmp/</tmp_path>
    <user_files_path>/benchmark-data/clickhouse/user_files/</user_files_path>
    <format_schema_path>/benchmark-data/clickhouse/format_schemas/</format_schema_path>
    <background_pool_size>4</background_pool_size>
    <background_schedule_pool_size>4</background_schedule_pool_size>
    <merge_tree>
        <number_of_free_entries_in_pool_to_execute_mutation>4</number_of_free_entries_in_pool_to_execute_mutation>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>4</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    </merge_tree>
    <logger><level>warning</level><console>0</console></logger>
</clickhouse>
XML
CLICKHOUSE_WATCHDOG_ENABLE=0 clickhouse server --config-file=/etc/clickhouse-server/config.xml > /benchmark-data/clickhouse-startup.log 2>&1 &
ch_pid=$!
ready=false
for attempt in $(seq 1 60); do
    if clickhouse client --query 'SELECT 1' >/dev/null 2>&1; then ready=true; break; fi
    if ! kill -0 "$ch_pid" 2>/dev/null; then break; fi
    sleep 1
done
if [ "$ready" != true ]; then cat /benchmark-data/clickhouse-startup.log; tail -40 /var/log/clickhouse-server/clickhouse-server.err.log; exit 1; fi
printf '\nClickHouse, same container budget, 30 iterations, three repetitions\n'
PORCUPINE_BENCH_ENGINE=clickhouse /analytics-bench -test.run='^$' -test.bench='^BenchmarkAnalyticsComparison$' -test.benchtime=30x -test.count=3 -test.timeout=15m
