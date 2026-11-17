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
  docker rm -fv "$project-env-only" >/dev/null 2>&1 || true
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
address=$(compose port porcupine 8080)
attempt=0
until curl --fail --silent "http://$address/readyz" > /dev/null; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 100 ]; then compose logs porcupine >&2; exit 1; fi
  sleep 0.2
done
compose stop porcupine >/dev/null
compose cp porcupine:/data/porcupine.json "$work/edited.json"
sed -e 's/0.0.0.0:8080/0.0.0.0:8081/' -e 's|/data/porcupine.db|/data/custom/ingestion.db|g' -e 's|/data/ducklake|/data/custom/lake|g' "$work/edited.json" > "$work/custom.json"
compose run --rm --no-deps -T porcupine config install --config /data/porcupine.json < "$work/custom.json"
compose --profile tools run --rm --no-deps config-check > "$work/reinstalled.json"
grep -q '\[redacted\]' "$work/reinstalled.json"
cat > "$work/ports.yaml" <<'YAML'
services:
  porcupine:
    ports: !override
      - "127.0.0.1::8081"
YAML
compose up -d --no-build porcupine
address=$(compose port porcupine 8081)
attempt=0
until curl --fail --silent "http://$address/readyz" > /dev/null; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 100 ]; then compose logs porcupine >&2; exit 1; fi
  sleep 0.2
done
compose cp porcupine:/data/custom/ingestion.db "$work/custom-journal.db"
compose cp porcupine:/data/custom/ingestion.db.control "$work/custom-control.db"
compose cp porcupine:/data/custom/lake/registry.sqlite "$work/custom-registry.sqlite"
printf 'Private initialization, masked config check, nonroot startup, restart, atomic config install and custom storage paths passed.\n'

docker run -d --name "$project-env-only" -p "127.0.0.1::8080" \
  -e PORCUPINE_BOOTSTRAP_TOKEN=config-smoke-bootstrap-not-a-production-secret \
  "$PORCUPINE_IMAGE" >/dev/null
address=$(docker port "$project-env-only" 8080)
attempt=0
until curl --fail --silent "http://$address/readyz" > /dev/null; do
  attempt=$((attempt+1))
  if [ "$attempt" -ge 100 ]; then docker logs "$project-env-only" >&2; exit 1; fi
  sleep 0.2
done
printf 'Environment-only container startup also passed without an explicit listen address.\n'
