#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
run_id=${PORCUPINE_ARCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
case "$run_id" in ''|*[!a-zA-Z0-9_-]*) echo 'invalid run id'; exit 1;; esac
run_dir="benchmark-results/architecture/$run_id"
mkdir -p benchmark-results/architecture
if ! mkdir "$run_dir"; then
    printf 'Cannot create run directory; choose a new PORCUPINE_ARCH_RUN_ID.\n' >&2
    exit 1
fi
printf 'Run directory: %s\n' "$run_dir"
if [ "${PORCUPINE_ARCH_SKIP_BUILD:-}" != 1 ]; then
    docker build -f scripts/analytics-benchmark.Dockerfile -t porcupine-analytics-benchmark .
fi
failed=0
for engine in ${PORCUPINE_ARCH_ENGINES:-ducklake clickhouse}; do
    case "$engine" in ducklake|clickhouse) ;; *) echo 'invalid engine'; exit 1;; esac
    for phase in ${PORCUPINE_ARCH_PHASES:-history mixed projects payloads guard crash}; do
        case "$phase" in history|mixed|projects|payloads|guard|crash) ;; *) echo 'invalid phase'; exit 1;; esac
        if [ "$engine" = clickhouse ] && [ "$phase" = crash ]; then continue; fi
        log="$run_dir/$engine-$phase.log"
        printf '\nRunning %s / %s\n' "$engine" "$phase"
        if docker run --rm --cpus=2 --memory=2g --memory-swap=2g \
            --env PORCUPINE_ARCH_ENGINE="$engine" --env PORCUPINE_ARCH_PHASE="$phase" \
            --mount type=volume,destination=/benchmark-data porcupine-analytics-benchmark > "$log" 2>&1; then
            printf 'Exit: 0\n' >> "$log"
        else
            code=$?
            printf 'Exit: %s\n' "$code" >> "$log"
            failed=1
        fi
        cat "$log"
        sed -n 's/^ARCH_RESULT //p' "$log" > "$run_dir/$engine-$phase.jsonl"
        if grep -q '"verdict":"NOT VERIFIED"' "$run_dir/$engine-$phase.jsonl"; then
            printf 'Performance or setup predicate: NOT VERIFIED\n' >> "$log"
            failed=1
        fi
    done
done
exit "$failed"
