#!/bin/sh
# Exercise the nonroot, file-based Docker setup without operator credentials.
set -eu
cd "$(dirname "$0")/.."
project="porcupine-config-$$"
work=$(mktemp -d)
export PORCUPINE_IMAGE="${PORCUPINE_CONFIG_TEST_IMAGE:-porcupine-config-review:test}"
compose() {
  docker compose -p "$project" -f compose.yaml -f "$work/ports.yaml" "$@"
}
cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM
cat > "$work/ports.yaml" <<'YAML'
services:
  porcupine:
    ports: !override
      - "127.0.0.1::8080"
YAML
if [ "${PORCUPINE_CONFIG_TEST_SKIP_BUILD:-}" != 1 ]; then
  docker build -t "$PORCUPINE_IMAGE" .
fi
compose --profile setup run --rm --no-deps init
compose --profile tools run --rm --no-deps config-check > "$work/check.json"
if ! grep -q '\[redacted\]' "$work/check.json"; then
  printf 'Configuration output did not mask credentials.\n' >&2
  exit 1
fi
if compose --profile setup run --rm --no-deps init > "$work/repeat.log" 2>&1; then
  printf 'Initialization overwrote an existing configuration.\n' >&2
  exit 1
fi
grep -q 'refusing to overwrite' "$work/repeat.log"
compose up -d --no-build porcupine
address=$(compose port porcupine 8080)
attempt=0
until curl --fail --silent "http://$address/readyz" > /dev/null; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 100 ]; then
    compose logs porcupine >&2
    exit 1
  fi
  sleep 0.2
done
container=$(compose ps -q porcupine)
user=$(docker inspect --format '{{.Config.User}}' "$container")
case "$user" in ''|0|0:*) printf 'Server ran as root.\n' >&2; exit 1;; esac
compose exec -T porcupine /porcupine config check --config /data/porcupine.json > "$work/running.json"
grep -q '\[redacted\]' "$work/running.json"
compose restart porcupine >/dev/null
attempt=0
until curl --fail --silent "http://$address/readyz" > /dev/null; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 100 ]; then exit 1; fi
  sleep 0.2
done
printf 'Private initialization, masked config check, nonroot startup and restart passed.\n'
