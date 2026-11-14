# Analytics API reference

For a first query with your agent or CLI, follow [Query your events](query-events.md). This reference describes the default DuckDB dialect. Optional ClickHouse deployments report `sql.dialect: "clickhouse"` and use ClickHouse syntax instead. Always inspect the running instance's schema before generating SQL.

Every request targets one project and requires `events:read`. HTTP and the CLI accept an API-audience service key or OAuth token. MCP requires an MCP-audience credential. There are no prebuilt metric endpoints, saved query catalogs, or HogQL compatibility.

## Operations

| Operation ID | HTTP endpoint | Response |
| --- | --- | --- |
| `get-event-schema` | `GET /api/v1/projects/{id}/schema` | SQL columns, functions, features, limits, and delivery status |
| `get-event-catalog` | `GET /api/v1/projects/{id}/catalog` | Observed event names, sampled property types, UUID pointers, and delivery status |
| `query-events` | `POST /api/v1/projects/{id}/query` | Typed result and delivery status |

Schema discovery returns `enabled: false` when no analytical backend is configured. Catalog and SQL requests then return `503`. The separate pending-event endpoint inspects SQLite, not delivered history.

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

Property filters, cursor pagination and structured DuckDB errors require v0.12.0 or later.

`events` lists names by latest durable event ID. Each item contains `event`, decimal-string `count`, `first_received_at`, `last_received_at`, `latest_event_uuid`, and `latest_event_timestamp`. Counts cover delivered logical events. First and last receipt times come from the lowest and highest durable IDs. `next_offset` appears when another supported page exists.

Supplying `event` also returns sampled `properties`. Discovery examines at most the latest 1,000 matching events, 200 eligible top-level keys per event per request, and keys of at most 200 bytes. Filters and the cursor apply before the per-event key bound, so reserved SDK keys do not consume a custom-key page. Each page contains up to `property_limit` keys in lexical order, independently of event-name `limit` and `offset`.

Follow `next_property_cursor` until it is absent. Keep the project, event, prefix and kind unchanged. The cursor is an opaque continuation, not an authorization token or pinned snapshot. Each request samples current delivered data; concurrent ingestion can change observed keys and counts between pages. A missing historical key is not proof that no event ever contained it.

For example, request `event=page_viewed&property_kind=custom&property_prefix=thread&property_limit=10` to find matching business keys. If you need to inspect a pointer while discovery is incomplete, query `SELECT json_keys(properties) AS keys FROM events WHERE uuid=$uuid LIMIT 1`. One example still does not define a complete historical schema.

Each property contains `key`, observed JSON `types`, decimal-string `sample_occurrences`, `latest_event_uuid`, and `last_received_at`. Raw property values are excluded. The UUID points to the latest sampled event containing that key and can be used in a project-scoped query.

`sampling` reports the bounds, `properties_requested`, `properties_truncated`, `pagination_truncated`, and `historical_properties_complete`. Historical completeness is always false. Missing keys may be outside the sample or exceed its bounds.

## Event profiles, unreleased

The development operation `get-event-profile` adds activity, recent examples, and property statistics for one exact event name. It is not available in v0.12.0. Discover the running contract before calling it. See [Inspect an event](event-discovery.md) for HTTP, CLI, and MCP examples.

Profiles distinguish occurrence time from receipt time. Exact activity covers delivered logical events. Property statistics describe only a bounded recent sample. A failed storage read returns an error rather than zero activity. Edits and deletions change a profile after analytical projection, under the same delivery rules as SQL.

## SQL request and result

A body contains `sql`, an optional `parameters` string map, and optional `use_cache`:

```json
{
	"sql": "SELECT event, count(*) AS total FROM events WHERE event=$event GROUP BY event",
	"parameters": {"event": "article_published"}
}
```

Rows appear under `result.data`, with column metadata under `result.meta`. The result preserves 64-bit integers as decimal strings to avoid client precision loss. `result.statistics.elapsed` reports query duration. Physical scan counts and bytes are not returned.

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

### Cached and fresh queries

The default DuckLake backend reports `sql.result_cache: false`. Every query executes against delivered data. `use_cache` remains accepted for a common API contract; setting it to `false` explicitly requests a fresh execution.

The optional ClickHouse backend can reuse a result for up to 60 seconds when `use_cache` is omitted or true. Its cache separates projects, parameter values, and mutation generations. Pending mutations bypass cache use. A false value bypasses cache reads and writes. This cache is best effort and is separate from the engine's internal page and file buffers.

Neither backend waits for queued originals or corrections. Inspect delivery status before checking a recent capture.

### Group a custom property by day

```json
{
	"sql": "SELECT CAST(event_time AS DATE) AS day, json_extract_string(properties, '$.category') AS category, count(*) AS total FROM events WHERE event=$name GROUP BY day, category ORDER BY day, category LIMIT 100",
	"parameters": {"name": "article_published"}
}
```

For Paris calendar days, use `CAST(timezone('Europe/Paris', event_time) AS DATE)`. The running schema lists allowed date, JSON, aggregate, and window functions. Funnels and retention can be expressed with joins, CTEs, and window functions. ClickHouse-specific `windowFunnel` and `retention` calls require different SQL on DuckDB.

The [DuckDB recipes](duckdb-recipes.md) cover trends, sessions, custom dimensions, percentiles, ordered funnels, retention and moving averages. Their SQL is exercised through the real MCP client tests.

### Inspect a latest-event UUID pointer

```json
{
	"sql": "SELECT uuid, event, timestamp, received_at, properties FROM events WHERE uuid=$uuid LIMIT 1",
	"parameters": {"uuid": "UUID_FROM_CATALOG"},
	"use_cache": false
}
```

Through MCP, pass this body to `porcupine_call` with `operation_id: "query-events"` and `path: {"id": "PROJECT_ID"}`.

## Supported SQL and parameters

One statement is allowed, with at most 16 KiB of SQL. Native validation supports `SELECT`, grouping, sequential CTEs, subqueries, joins, `UNION`, window functions, and scalar casts. Sources are `events` and local CTEs. Exact supported functions come from schema discovery.

Writes, qualified tables, private columns, external sources, table functions, recursive CTEs, time travel, configuration changes, and unknown functions are rejected. The native DuckDB AST is checked before execution. An allowed outer function does not make an unsafe nested expression acceptable.

DuckDB placeholders use `$name`. Parameters have at most 32 names, each at most 64 ASCII letters, digits, or underscores and starting with a letter or underscore. Each supplied value is a string of at most 4,096 bytes. Names must exactly match the query, with no missing or extra values. Use an explicit cast when SQL requires a numeric or temporal type. Values are bound separately from SQL.

ClickHouse uses typed placeholders such as `{name:String}` instead. Do not mix dialects in one query.

## Large result exports

[SQL result exports](exports.md) use the project's validated dialect with background admission, operator budgets, durable job state, cancellation, and private gzip CSV or JSONL downloads. They do not raise interactive output limits or rewrite SQL into pages. All export operations require project-scoped `events:read`; cancellation also requires the creator credential or instance administration.

A delivery barrier waits for accepted originals and mutations through the job's watermarks. It does not exclude later writes. URLs appear only after the whole attempt succeeds.

## Fixed query limits

| Limit | DuckLake value |
| --- | --- |
| Execution time | 5 seconds |
| Result rows | 1,000 |
| Result bytes | 1 MiB |
| Concurrent analytical requests | 2, instance-wide |
| Engine memory | Configured per project and shared by its work |
| Physical scanned-row or read-byte quota | Not implemented |

Catalog requests share the analytical concurrency pool. Overflow fails rather than silently truncating. An explicit SQL `LIMIT` can bound a valid result. DuckDB's memory setting does not cap total process RSS; container limits remain necessary for containment.

The optional ClickHouse reader additionally applies a 256 MiB per-query memory setting, five-million-row scan limit, and 1 GiB read-byte limit. Read the running schema rather than assuming those apply to DuckLake.

## Delivery consistency and errors

Successful schema, catalog, and query responses include project-scoped delivery state. `pending` counts queued originals; `pending_mutations` counts corrections and deletions. `last_success` is optional. `oldest_pending_received_at` identifies the earliest pending original by acceptance order. A generic `error` appears only when the project has pending work.

Results are eventually consistent. A committed analytical write can be visible before SQLite records its acknowledgment. The [current-event API](events.md) reflects accepted pending corrections earlier than analytical SQL. Query results and delivery state have no shared cross-database snapshot, and catalog requests can also observe separate points in time.

Invalid SQL or bindings return `400`. Invalid credentials return `401`; missing authority returns `403`. Concurrency and resource limits return `429`. Unavailable storage or invalid storage responses return `503`. Storage errors exclude server messages, SQL text, and credentials.

Development builds return `429` with `code: "active_project_limit"` when every resident project engine is in use and another project cannot open within the configured count limit. Retry after active work finishes. This applies to SQL, catalog reads, and export validation. The operator can adjust `PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS` within the deployment's memory budget. Version v0.12.1 reports this condition as `503`.

DuckDB query errors can also include `code`, `hint` and a safe `field`. HTTP, MCP and CLI receive the same envelope. For example:

```json
{"error":"Missing bound query parameter","code":"missing_parameter","hint":"Supply this key in parameters; use $name placeholders for DuckDB.","field":"event"}
```

| Code | Recovery |
| --- | --- |
| `missing_parameter`, `unused_parameter`, `invalid_parameter` | Match the supplied string map to the `$name` bindings |
| `unsupported_function`, `unsupported_sql`, `invalid_sql` | Read the discovered dialect and allowlist; correct the SQL |
| `unknown_column`, `unknown_reference` | Check schema columns, JSON paths, CTEs and aliases |
| `type_mismatch`, `invalid_value`, `invalid_query`, `unresolved_parameter_type` | Check argument types and property values; use explicit casts or `TRY_CAST` |
| `division_by_zero` | Use `nullif(denominator,0)` |
| `result_row_limit`, `result_byte_limit` | Reduce the result or call `create-event-export` |

Native engine errors are classified without returning their raw text, candidate lists, stored values, filesystem paths or internal SQL. Other errors and backends may return only `error`; clients must keep that fallback.
