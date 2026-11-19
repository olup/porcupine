# Analytics API reference

For a first query with your agent or CLI, follow [Query or export events](../query-events.md). This reference describes the DuckDB SQL dialect. Always inspect the running instance's schema before generating SQL.

Every request targets one project and requires `events:read`. HTTP and the CLI accept an API-audience service key or OAuth token. MCP requires an MCP-audience credential. There are no prebuilt metric endpoints, saved query catalogs, or HogQL compatibility.

## Operations

| Operation ID | HTTP endpoint | Response |
| --- | --- | --- |
| `get-event-schema` | `GET /api/v1/projects/{id}/schema` | SQL columns, functions, features, limits, and delivery status |
| `get-event-catalog` | `GET /api/v1/projects/{id}/catalog` | Observed event names, sampled property types, UUID pointers, and delivery status |
| `get-event-profile` | `GET /api/v1/projects/{id}/event-profile` | Activity, recent examples, and bounded property statistics for one event name |
| `query-events` | `POST /api/v1/projects/{id}/query` | Typed result and delivery status |

Schema discovery returns `enabled: false` when no analytical backend is configured. Catalog and SQL requests then return `503`. The [recent-events endpoint](management-api.md#operations) is separate from SQL history.

## Event schema

The queryable table is `events`. Schema discovery returns exact `tables`, `sql`, `limits`, and `delivery` fields. `sql.functions` is the function allowlist. `sql.features` and `sql.unsupported` describe the running backend.

| Column | DuckDB type | Meaning |
| --- | --- | --- |
| `id`, `event_id` | `BIGINT` | Durable event ID, stable across retries with a stable producer message ID |
| `uuid` | `VARCHAR` | Stable event identifier within the project |
| `project_id` | `VARCHAR` | Owning project |
| `source_id`, `message_id` | `VARCHAR` | Authenticated producer namespace and original producer message ID |
| `event`, `event_type` | `VARCHAR` | Event name and normalized category |
| `distinct_id` | `VARCHAR` | Actor recorded at capture |
| `person_id` | `VARCHAR` | Current resolved project-local person |
| `person_properties` | `VARCHAR` | Current traits encoded as JSON |
| `properties`, `context` | `VARCHAR` | Custom properties and normalized context encoded as JSON |
| `timestamp`, `received_at` | `VARCHAR` | Original occurrence and server receipt times in UTC RFC3339 |
| `event_time` | `TIMESTAMP WITH TIME ZONE` | Occurrence time for temporal SQL |
| `timestamp_ms`, `received_at_ms` | `BIGINT` | Occurrence and receipt times in Unix milliseconds |
| `revision` | `UBIGINT` | Current revision for compare-and-swap mutations |
| `is_imported` | `UTINYINT` | `1` for historical imports, `0` for live ingestion |

Use `json_extract_string(properties, '$.category')` for a custom string property. Cast a numeric extraction when needed. Occurrence time and receipt time answer different questions; client timestamps can be older or newer than receipt.

## Observed event and property discovery

Catalog parameters are:

| Parameter | Default | Accepted values |
| --- | --- | --- |
| `event` | Omitted | Exact event name, at most 500 bytes |
| `limit` | `20` | `1` through `100` event names |
| `offset` | `0` | `0` through `10000` |
| `property_prefix` | Omitted | Literal, case-sensitive key prefix, at most 200 bytes; requires `event` |
| `property_kind` | `all` | `custom` excludes `$` keys; `reserved` includes only `$` keys; a filtered kind requires `event` |
| `property_limit` | `20` | `1` through `100` properties per page |
| `property_cursor` | Omitted | Returned `next_property_cursor`, reused with the same project, event, prefix and kind |

`events` lists names by latest durable event ID. Each item contains `event`, decimal-string `count`, `first_received_at`, `last_received_at`, `latest_event_uuid`, and `latest_event_timestamp`. Counts cover delivered logical events. First and last receipt times come from the lowest and highest durable IDs. `next_offset` appears when another supported page exists.

Supplying `event` also returns sampled `properties`. Discovery examines at most the latest 1,000 matching events, 200 eligible top-level keys per event per request, and keys of at most 200 bytes. Filters and the cursor apply before the per-event key bound, so reserved SDK keys do not consume a custom-key page. Each page contains up to `property_limit` keys in lexical order, independently of event-name `limit` and `offset`.

Follow `next_property_cursor` until it is absent. Keep the project, event, prefix and kind unchanged. The cursor is an opaque continuation, not an authorization token or pinned snapshot. Each request samples current delivered data; concurrent ingestion can change observed keys and counts between pages. Application-specific meaning comes from the descriptions project editors provide. A missing historical key is not proof that no event ever contained it.

For a CLI property page, run `porcupine get-event-catalog --id "$PROJECT_ID" --event page_viewed --property-kind custom --property-prefix thread --property-limit 10`. Use `--property-cursor` for the next property page or `--limit` and `--offset` for event names. If you need to inspect a pointer while discovery is incomplete, query `SELECT json_keys(properties) AS keys FROM events WHERE uuid=$uuid LIMIT 1`. One example still does not define a complete historical schema.

Each property contains `key`, observed JSON `types`, decimal-string `sample_occurrences`, `latest_event_uuid`, and `last_received_at`. Raw property values are excluded. The UUID points to the latest sampled event containing that key and can be used in a project-scoped query.

`sampling` reports the bounds, `properties_requested`, `properties_truncated`, `pagination_truncated`, and `historical_properties_complete`. Historical completeness is always false. Missing keys may be outside the sample or exceed its bounds.

## Event profiles

See [Inspect an event](event-discovery.md) for HTTP, CLI, and MCP examples of `get-event-profile`.

Profiles distinguish occurrence time from receipt time. Exact activity covers delivered logical events. Property statistics describe only a bounded recent sample. A failed storage read returns an error rather than zero activity. Edits and deletions change a profile after analytical projection, under the same delivery rules as SQL.

## SQL request and result

A body contains `sql`. It can include a `parameters` string map and a `time_range` object.

```json
{
	"sql": "SELECT event, count(*) AS total FROM events WHERE event=$event GROUP BY event",
	"parameters": {"event": "article_published"},
	"time_range": {"from": "2026-11-01T00:00:00Z", "to": "2026-11-08T00:00:00Z"}
}
```

`time_range` requires two RFC3339 timestamps, with `from` earlier than `to`. It includes events that occurred at `from` and excludes events at `to`. The range applies to every reference to `events`, including joins, CTEs, and subqueries. Omit `time_range` to query all delivered history.

Rows appear under `result.data`, with column metadata under `result.meta`. The result preserves 64-bit integers as decimal strings to avoid client precision loss. `result.statistics.elapsed` reports query duration. An empty `result.data` means no delivered rows matched the query. Physical scan counts and bytes are omitted.

For example:

```json
{
	"result": {
		"meta": [{"name": "event", "type": "VARCHAR"}, {"name": "total", "type": "BIGINT"}],
		"data": [{"event": "article_published", "total": "12"}],
		"rows": 1,
		"statistics": {"elapsed": 0.002}
	},
	"delivery": {"enabled": true, "pending": 0, "pending_mutations": 0}
}
```

### Fresh queries

Every query executes against delivered data without a result cache. Servers from v0.15.0 onward reject the former `use_cache` field. See [delivery consistency](#delivery-consistency-and-errors) before checking a recent capture.

### Group a custom property by day

```json
{
	"sql": "SELECT CAST(event_time AS DATE) AS day, json_extract_string(properties, '$.category') AS category, count(*) AS total FROM events WHERE event=$name GROUP BY day, category ORDER BY day, category LIMIT 100",
	"parameters": {"name": "article_published"}
}
```

For Paris calendar days, use `CAST(timezone('Europe/Paris', event_time) AS DATE)`. The running schema lists allowed date, JSON, aggregate, and window functions. Funnels and retention can be expressed with joins, CTEs, and window functions.

The [DuckDB recipes](duckdb-recipes.md) cover trends, sessions, custom dimensions, percentiles, ordered funnels, retention and moving averages. Their SQL is exercised through the real MCP client tests.

### Inspect a latest-event UUID pointer

```json
{
	"sql": "SELECT uuid, event, timestamp, received_at, properties FROM events WHERE uuid=$uuid LIMIT 1",
	"parameters": {"uuid": "UUID_FROM_CATALOG"}
}
```

Send this body to `POST /api/v1/projects/PROJECT_ID/query` with an API-audience bearer credential. For MCP, pass it to `porcupine_call` with `operation_id: "query-events"` and `path: {"id": "PROJECT_ID"}`. See [MCP execution](mcp.md#execution-with-porcupine_call).

## Supported SQL and parameters

One statement is allowed, with at most 16 KiB of SQL. Native validation supports `SELECT`, grouping, sequential CTEs, subqueries, joins, `UNION`, window functions, and scalar casts. Sources are `events` and local CTEs. Exact supported functions come from schema discovery.

Writes, qualified tables, private columns, external sources, table functions, recursive CTEs, time travel, configuration changes, and unknown functions are rejected. The server checks the native DuckDB AST before execution. An allowed outer function does not make an unsafe nested expression acceptable.

DuckDB placeholders use `$name`. Parameters have at most 32 names, each at most 64 ASCII letters, digits, or underscores and starting with a letter or underscore. Each supplied value is a string of at most 4,096 bytes. Names must exactly match the query, with no missing or extra values. Use an explicit cast when SQL requires a numeric or temporal type. Values are bound separately from SQL.

## Large result exports

Use a [one-off export](../query-events.md#export-a-result) for large files. The [export API](export-api.md) defines admission, output limits, and profile-specific consistency.

## Query limits

Default interactive limits are listed below.

| Limit | DuckLake value |
| --- | --- |
| Execution time | 5 seconds |
| Result rows | 1,000 |
| Result bytes | 1 MiB |
| Concurrent analytical slots | 2 per server process |
| Engine memory | Configured per project and shared by its work |
| Physical scanned-row or read-byte quota | Not implemented |

SQL, catalog reads, event profiles, exports, and scheduled S3 queries share worker-local capacity. One background job can start when the worker is idle and occupies one slot. Row or byte overflow returns an error. An explicit SQL `LIMIT` can bound a valid result. DuckDB's memory setting does not cap total process RSS; container limits remain necessary for containment.

[Tenant budgets](tenants.md#interactive-query-limits) and [deployment ceilings](configuration.md#interactive-query-ceilings) can lower the effective limits. For each project, effective concurrency, timeout, row count, and result bytes are the lower of its tenant budget and the deployment ceiling. `get-event-schema` returns configured `tenant_budgets` and `deployment_limits` alongside effective `limits`; `limits.max_execution_ms` expresses the precise deadline. This reports configured allowances, including the monthly ingestion budget. Aggregate consumption is available through [tenant usage](tenants.md#monthly-ingestion-allowance). SQL, catalog reads, and event profiles also share tenant admission. When all of a tenant's interactive slots are in use, the server returns `429` with `tenant_query_concurrency_limit`. Background jobs have separate execution and output budgets.

## Delivery consistency and errors

Successful schema, catalog, and query responses include project-scoped delivery state. `pending` counts queued originals; `pending_mutations` counts corrections and deletions. `last_success` is optional. `oldest_pending_received_at` identifies the earliest pending original by acceptance order. A generic `error` appears only when the project has pending work.

Queries do not wait for queued originals or corrections. Read [hosted delivery status](shared-deployment.md#live-ingestion-and-delivery) before treating a zero pending count as a drained deployment. A committed analytical write can be visible before SQLite records its acknowledgment. The [current-event API](../events.md) reflects accepted pending corrections earlier than analytical SQL. Query results and delivery state have no shared cross-database snapshot, and catalog requests can also observe separate points in time.

Invalid SQL, bindings or reversed or empty time ranges return `400`. Missing or malformed bounds return `422`. Invalid credentials return `401`; missing authority returns `403`. Concurrency and resource limits return `429`. Unavailable storage or invalid storage responses return `503`. Storage errors exclude server messages, SQL text, and credentials.

Porcupine returns `429` with `code: "active_project_limit"` when every resident project engine is in use and another project cannot open within the configured count limit. Retry after active work finishes. This applies to SQL, catalog reads, and export validation. The operator can adjust `PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS` within the deployment's memory budget.

DuckDB query errors can also include `code`, `hint` and a safe `field`. HTTP, MCP and CLI receive the same envelope. For example:

```json
{"error":"Missing bound query parameter","code":"missing_parameter","hint":"Supply this key in parameters; use $name placeholders for DuckDB.","field":"event"}
```

| Code | Recovery |
| --- | --- |
| `invalid_time_range` | Supply an earlier `from` and later `to`, or omit `time_range` for all time |
| `missing_parameter`, `unused_parameter`, `invalid_parameter` | Match the supplied string map to the `$name` bindings |
| `unsupported_function`, `unsupported_sql`, `invalid_sql` | Read the discovered dialect and allowlist; correct the SQL |
| `unknown_column`, `unknown_reference` | Check schema columns, JSON paths, CTEs and aliases |
| `type_mismatch`, `invalid_value`, `invalid_query`, `unresolved_parameter_type` | Check argument types and property values; use explicit casts or `TRY_CAST` |
| `division_by_zero` | Use `nullif(denominator,0)` |
| `result_row_limit`, `result_byte_limit` | Reduce the result or call `create-event-export` |

Native engine errors are classified without returning their raw text, candidate lists, stored values, filesystem paths or internal SQL. Other errors may return only `error`; clients must keep that fallback.

### Analytical S3 request deadlines

On native S3 deployments, `limits.object_storage_request_timeout_seconds` reports the per-request object-storage timeout. Automatic native HTTP retries are disabled. Cancellation does not immediately abort every native HTTP request, so an in-flight object read can extend the SQL deadline by this interval. This field is omitted for local analytical storage. [S3 configuration](ducklake-s3.md)
