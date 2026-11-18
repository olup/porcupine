# Query or export events

Use an interactive query to explore one project's delivered events or return a small result. Use a one-off export when you need a larger file and can wait for it. The operator must configure private S3-compatible export storage. For recurring delivery, use a [pipeline](pipelines.md).

[Connect your agent or CLI](connect-tools.md) first. Both workflows require `events:read` for the project. Listing projects also requires `management:read`.

## Run an interactive query

Ask your MCP agent to inspect the project's schema and observed event names with `porcupine_schema`, then run SQL with `porcupine_query`. Specify `project_id` if the credential can access several projects. For the CLI, look up the project ID, then inspect its schema and catalog.

```sh
porcupine list-projects
export PROJECT_ID=YOUR_PROJECT_ID
porcupine get-event-schema --id "$PROJECT_ID"
porcupine get-event-catalog --id "$PROJECT_ID"
```

Pick an event from the catalog. Use `person_id` to count resolved people. For retention queries, see [identity and sessions](data-and-identity.md).

This example counts one event by UTC day. Save it as `query.json` and replace the event name.

```json
{
	"sql": "SELECT CAST(event_time AS DATE) AS day, count(*) AS total FROM events WHERE event = $event GROUP BY day ORDER BY day",
	"parameters": {"event": "signup_completed"}
}
```

```sh
porcupine query-events --id "$PROJECT_ID" --body @query.json
```

For HTTP or MCP examples, see the [analytics API](reference/analytics.md#sql-request-and-result). Rows appear in `result.data`. If you just sent an event, verify it in SQL after delivery. See [query consistency and errors](reference/analytics.md#delivery-consistency-and-errors), [event discovery](reference/event-discovery.md), and [SQL recipes](reference/duckdb-recipes.md).

## Export a result

Copy the query body to `export.json` and add an `idempotency_key`. Reuse that key for a retry of the same export. Set `format` to `csv` if you want CSV.

```sh
porcupine create-event-export --id "$PROJECT_ID" --body @export.json
export EXPORT_ID=RETURNED_EXPORT_ID
porcupine get-event-export --id "$PROJECT_ID" --export-id "$EXPORT_ID"
```

Poll the status with a delay. When it reports `succeeded`, request file links.

```sh
porcupine list-event-export-files --id "$PROJECT_ID" --export-id "$EXPORT_ID"
```

Keep the links private and download the files before the links expire. See the [export API](reference/export-api.md) for formats, budgets, retention, and snapshot behavior on each profile.
