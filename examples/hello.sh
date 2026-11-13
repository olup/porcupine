#!/bin/sh
# Create an isolated local demo project and query one event with the released CLI.
set -eu
. ./.env
if [ "$PORCUPINE_OAUTH_ISSUER" != http://localhost:8080 ]; then
    echo "Use the local quickstart issuer http://localhost:8080" >&2
    exit 1
fi
export PORCUPINE_URL="$PORCUPINE_OAUTH_ISSUER"
command -v porcupine >/dev/null
command -v jq >/dev/null

# Pass the bootstrap credential through stdin, keeping it out of process arguments.
PORCUPINE_API_KEY=$(
    printf 'user = "bootstrap:%s"\n' "$PORCUPINE_BOOTSTRAP_TOKEN" |
        curl --fail --silent --show-error --config - "$PORCUPINE_URL/oauth/token"             --data-urlencode grant_type=client_credentials             --data-urlencode scope=instance:admin             --data-urlencode "resource=$PORCUPINE_URL/api/v1" |
        jq -er '.access_token'
)
export PORCUPINE_API_KEY
project=$(porcupine create-project --body '{"name":"Quickstart"}')
project_id=$(printf '%s' "$project" | jq -er '.project.id')
ingest_key=$(printf '%s' "$project" | jq -er '.ingest_token')
event_id=$(openssl rand -hex 16)
payload=$(jq -nc --arg key "$ingest_key" --arg id "$event_id"     '{api_key:$key,uuid:$id,event:"hello",distinct_id:"$posthog_cookieless",properties:{source:"quickstart"}}')
printf '%s' "$payload" |
    curl --fail --silent --show-error "$PORCUPINE_URL/capture/"         -H 'Content-Type: application/json' --data-binary @- >/dev/null
dialect=$(porcupine get-event-schema --id "$project_id" | jq -er '.sql.dialect')
case "$dialect" in
    duckdb) query='{"sql":"SELECT event, count(*) AS total FROM events WHERE event=$name GROUP BY event","parameters":{"name":"hello"},"use_cache":false}' ;;
    clickhouse) query='{"sql":"SELECT event, count() AS total FROM events WHERE event={name:String} GROUP BY event","parameters":{"name":"hello"},"use_cache":false}' ;;
    *) echo "Unsupported SQL dialect: $dialect" >&2; exit 1 ;;
esac
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
