# Query your events

Use SQL to ask questions about one project's delivered events. The HTTP API, CLI, and MCP share the same project isolation and query limits. You write the SQL, or ask your agent to write it.

Before starting, [connect your tools](connect-tools.md). You need `events:read` on the project. Listing projects also requires `management:read`.

## Ask an agent

With MCP connected, ask:

> Find the project I want to analyze. Inspect its SQL schema and observed event names. Count each event by UTC day, and explain which data and identity fields you used.

The agent discovers operation contracts with `porcupine_docs`, then calls these operations through `porcupine_call`:

1. `list-projects` to resolve a name to a project ID.
2. `get-event-schema` to read columns, supported functions, and limits.
3. `get-event-catalog` to inspect observed event names and property keys.
4. `query-events` to execute SQL for that project.

The catalog samples delivered events. It helps discover what is present, but does not describe every historical property or explain your application's business meaning. The [discovery reference](analytics.md#observed-event-and-property-discovery) describes its bounds.

## Discover data from the CLI

Set `PORCUPINE_URL` and sign in, then use the returned project ID:

```sh
porcupine list-projects
export PROJECT_ID=YOUR_PROJECT_ID
porcupine get-event-schema --id "$PROJECT_ID"
porcupine get-event-catalog --id "$PROJECT_ID"
```

Inspect an event name from the catalog to find its properties:

```sh
porcupine get-event-catalog --id "$PROJECT_ID" --event article_published
```

Replace `article_published` with a name from your project. The schema and catalog contain the field names an agent needs to build a query.

## Run a query

This query counts each event by UTC day:

```sh
porcupine query-events --id "$PROJECT_ID" --body '{"sql":"SELECT CAST(event_time AS DATE) AS day, event, count() AS total FROM events GROUP BY day, event ORDER BY day, event LIMIT 100"}'
```

Rows appear under `result.data`. Porcupine returns 64-bit integers, including counts, as strings to preserve precision. An empty array means the query found no delivered rows.

For a filtered query, save the following as `query.json`:

```json
{
	"sql": "SELECT json_extract_string(properties, '$.category') AS category, count() AS total FROM events WHERE event=$name GROUP BY category ORDER BY total DESC LIMIT 100",
	"parameters": {"name": "article_published"}
}
```

Run it with:

```sh
porcupine query-events --id "$PROJECT_ID" --body @query.json
```

Replace the event name and property key with values observed in your project. Bound parameters keep values separate from SQL. The [SQL reference](analytics.md) lists supported syntax, bindings, and fixed execution limits.

For the same request over HTTP, send `query.json` to `POST /api/v1/projects/PROJECT_ID/query` with an API-audience bearer credential. Through MCP, call `query-events` with `path: {"id": "PROJECT_ID"}` and the JSON as `body`. See the [MCP request examples](mcp.md#execution-with-porcupine_call).

## Check a recently sent event

DuckLake queries execute against the current delivered state. This backend does not implement a result cache; `sql.result_cache` is `false` in schema discovery. `"use_cache": false` is accepted and explicitly requests a fresh query. Optional ClickHouse deployments can use a 60-second cache. Neither mode waits for queued events to reach analytical storage.

The response's `delivery.pending` and `delivery.pending_mutations` report work awaiting projection. After sending an event, allow that work to drain before checking its count. See [query consistency](analytics.md#delivery-consistency-and-errors) and [cache behavior](analytics.md#cached-and-fresh-queries).

## Count people or export more data

Use `person_id` for the currently resolved identity and `distinct_id` for the actor recorded at capture. Daily cookieless identifiers cannot measure a person's return across days. Read [identity and sessions](data-and-identity.md) before writing a retention or unique-user query.

For a large result, create a [one-off export](exports.md). To deliver results on a schedule, configure an [S3 pipeline](pipelines.md#scheduled-s3-query-windows).
