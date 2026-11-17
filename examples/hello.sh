#!/bin/sh
# Create an isolated local demo project and query one event with the released CLI.
set -eu
export PORCUPINE_URL=http://localhost:8080
command -v porcupine >/dev/null
command -v jq >/dev/null

umask 077
config_dir=$(mktemp -d)
trap 'rm -f "$config_dir/porcupine.json"; rmdir "$config_dir"' EXIT
docker compose -f compose.yaml -f compose.release.yaml cp porcupine:/data/porcupine.json "$config_dir/porcupine.json"
PORCUPINE_API_KEY=$(jq -er '.bootstrap_token' "$config_dir/porcupine.json")
export PORCUPINE_API_KEY
project=$(porcupine create-project --body '{"name":"Quickstart"}')
project_id=$(printf '%s' "$project" | jq -er '.project.id')
ingest_key=$(printf '%s' "$project" | jq -er '.ingest_token')
event_id=$(openssl rand -hex 16)
payload=$(jq -nc --arg key "$ingest_key" --arg id "$event_id"     '{api_key:$key,uuid:$id,event:"hello",distinct_id:"$posthog_cookieless",properties:{source:"quickstart"}}')
printf '%s' "$payload" |
    curl --fail --silent --show-error "$PORCUPINE_URL/capture/"         -H 'Content-Type: application/json' --data-binary @- >/dev/null
query='{"sql":"SELECT event, count(*) AS total FROM events WHERE event=$name GROUP BY event","parameters":{"name":"hello"}}'
attempt=0
while [ "$attempt" -lt 90 ]; do
    if result=$(porcupine query-events --id "$project_id" --body "$query") &&
        printf '%s' "$result" | jq -e '.result.data | length > 0' >/dev/null; then
        printf 'Project: %s\n' "$project_id"
        printf '%s' "$result" | jq '.result.data'
        exit 0
    fi
    attempt=$((attempt + 1))
    sleep 1
done
echo "Timed out waiting for analytical delivery" >&2
exit 1
