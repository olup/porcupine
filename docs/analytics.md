# Analytics API reference

Porcupine exposes delivered events through project-scoped ClickHouse SQL. Clients and agents write SQL for their question. There are no named prebuilt metric endpoints, saved query catalogs, or HogQL compatibility.

Every operation below requires `Authorization: Bearer ACCESS_TOKEN` with `events:read` for the path's project. In OAuth mode, obtain an API-resource token through [client credentials](oauth.md). Static API keys are legacy authentication only. HTTP and [MCP](mcp.md) use the same policies. A request targets one project even when its token grants several projects.

## Operations

| Operation ID | HTTP endpoint | Response |
| --- | --- | --- |
| `get-event-schema` | `GET /api/v1/projects/{id}/schema` | SQL columns, functions, features, limits, and delivery status |
| `get-event-catalog` | `GET /api/v1/projects/{id}/catalog` | Observed event names, optional sampled property types, UUID pointers, and delivery status |
| `query-events` | `POST /api/v1/projects/{id}/query` | Typed ClickHouse JSON result and delivery status |

Schema discovery returns `enabled: false` when ClickHouse is not configured. Catalog and SQL queries then return `503`. The separate `/api/v1/projects/{id}/events` endpoint inspects the pending SQLite queue. Delivered payloads leave that queue and remain available in ClickHouse.

## Event schema

Schema discovery returns `tables`, `sql`, `limits`, and `delivery`. `sql.functions` is the exact function allowlist; `sql.features` and `sql.unsupported` describe the accepted grammar. The queryable table is named `events`.

| Column | ClickHouse type | Meaning |
| --- | --- | --- |
| `id` | `UInt64` | Durable SQLite event ID |
| `revision` | `UInt64` | Latest projected revision, returned as a decimal string |
| `project_id` | `String` | Project enforced by the reader and row policy |
| `uuid` | `String` | Event UUID, unique within the project |
| `event` | `String` | Event name |
| `distinct_id` | `String` | Accepted event identity; cookieless IDs rotate daily |
| `properties` | `String` | JSON object encoded as a string |
| `timestamp` | `String` | Client event timestamp, normalized to UTC RFC3339 |
| `timestamp_ms` | `Int64` | Client event timestamp in Unix milliseconds |
| `event_time` | `DateTime64(3, 'UTC')` | Client event timestamp for SQL time operations |
| `received_at` | `String` | Server request arrival time in UTC RFC3339 |
| `received_at_ms` | `Int64` | Server request arrival time in Unix milliseconds |

`JSONExtractString`, `JSONExtractInt`, and related allowed functions read custom properties. `event_time` and receipt time answer different questions. Client timestamps can be older or newer than their arrival time.

## Observed event and property discovery

Catalog parameters are:

| Parameter | Default | Accepted values |
| --- | --- | --- |
| `event` | Omitted | Exact event name, at most 500 bytes |
| `limit` | `20` | `1` through `100` event names |
| `offset` | `0` | `0` through `10000` |

`events` lists names by latest durable event ID. Each item contains `event`, a decimal-string logical `count`, `first_received_at`, `last_received_at`, `latest_event_uuid`, and `latest_event_timestamp`. Counts cover delivered logical events with that name. First and last receipt times come from the lowest and highest durable event IDs, rather than minimum and maximum client timestamps. `next_offset` is present when another supported page exists.

Supplying `event` also returns `properties`. Discovery examines at most the latest 1,000 delivered matching events by durable ID. For each event it considers up to 200 top-level keys whose names contain at most 200 bytes. It returns at most 100 properties in key order.

Each property has `key`, observed JSON `types`, decimal-string `sample_occurrences`, `latest_event_uuid`, and `last_received_at`. It returns no raw property values. The UUID points to the latest sampled event containing that key. A query can inspect the corresponding event within the same project.

`sampling` reports the bounds above, `properties_requested`, `properties_truncated`, `pagination_truncated`, and `historical_properties_complete`. Historical completeness is always false. Missing keys may be older than the sample, exceed key limits, or have names too long for discovery.

Example discovery requests:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/schema" \
	-H "Authorization: Bearer $ACCESS_TOKEN"

curl --fail-with-body --get "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/catalog" \
	-H "Authorization: Bearer $ACCESS_TOKEN" \
	--data-urlencode 'event=conversation_started' --data-urlencode 'limit=20'
```

The examples use `PORCUPINE_URL=http://127.0.0.1:8080`, an existing `PROJECT_ID`, and an authorized `ACCESS_TOKEN`.

## SQL request and result

A query body contains `sql`, an optional `parameters` string map, and an optional `use_cache` Boolean:

```json
{
	"sql": "SELECT event, count() AS total FROM events WHERE event={event:String} GROUP BY event",
	"parameters": {"event": "conversation_started"}
}
```

The response has this shape:

```json
{
	"result": {
		"meta": [{"name": "event", "type": "String"}, {"name": "total", "type": "UInt64"}],
		"data": [{"event": "conversation_started", "total": "12"}],
		"rows": 1,
		"statistics": {"elapsed": 0.002}
	},
	"delivery": {"enabled": true, "pending": 0, "pending_mutations": 0}
}
```

Values in `data` retain ClickHouse JSON types. In particular, 64-bit integers are strings to preserve precision. `result.rows_before_limit_at_least` may also appear. Public statistics expose elapsed time only; physical scan counts and bytes are omitted.

### Cached and fresh queries

Omitting `use_cache` or setting it to `true` allows ClickHouse's native result cache with a 60-second TTL. The cache separates projects, typed parameter values, and mutation generations. Newly projected original events can take up to 60 seconds to appear in an existing cached result. Caching is best effort: server capacity, entry size, and nondeterministic functions such as `now()` can prevent a result from being cached.

Set `use_cache` to `false` to bypass both cache reads and cache writes:

```json
{
	"sql": "SELECT uuid, event, revision FROM events WHERE uuid={uuid:String}",
	"parameters": {"uuid": "EVENT_UUID"},
	"use_cache": false
}
```

A fresh query reads currently projected ClickHouse data. It does not wait for queued originals or mutations. Delivery status is read separately and remains current even when the SQL result comes from cache.

Accepted edits and deletions immediately change the project's durable cache generation. While any mutation is pending, all interactive queries for that project bypass caching. After projection is acknowledged, caching resumes under the new generation, so pre-edit entries cannot reappear. A cached read that races mutation acceptance is retried through the fresh reader. Process restarts use a new cache namespace, including after restore. Authorization is checked for every HTTP or MCP request before any cached result is read.

Each project's cached reader has a maximum of 64 entries and 8 MiB of cache storage, subject to the server's global limits. Cached entries are never shared between ClickHouse users. Reader permissions, execution limits, and output limits remain fixed. Schema and catalog discovery, current-event reads, exports, and scheduled pipeline queries do not use this result cache.

### Conversations by custom client and day

This example assumes `conversation_started` events contain custom `client` and `conversation_id` properties. It counts distinct conversation IDs per UTC event day for one client. Those properties are application-defined, not built-in Porcupine fields.

Save this JSON as `query.json`:

```json
{
	"sql": "SELECT toDate(event_time) AS day, JSONExtractString(properties,'client') AS client, uniqExact(JSONExtractString(properties,'conversation_id')) AS conversations FROM events WHERE event={event:String} AND event_time >= {since:DateTime} AND event_time < {until:DateTime} AND JSONExtractString(properties,'client')={client:String} AND JSONHas(properties,'conversation_id') GROUP BY day, client ORDER BY day, client LIMIT 1000",
	"parameters": {
		"event": "conversation_started",
		"client": "acme",
		"since": "2026-11-01 00:00:00",
		"until": "2026-11-08 00:00:00"
	}
}
```

Submit the request:

```sh
curl --fail-with-body "$PORCUPINE_URL/api/v1/projects/$PROJECT_ID/query" \
	-H "Authorization: Bearer $ACCESS_TOKEN" \
	-H 'Content-Type: application/json' --data-binary @query.json
```

Changing the grouping or filters changes the analytical definition. Counting daily cookieless IDs across several days does not produce exact distinct-person counts. See [the identity contract](data-and-identity.md#cookieless-by-default).

### Inspect a latest-event UUID pointer

Use `latest_event_uuid` from an event or property catalog item as a bound value:

```json
{
	"sql": "SELECT uuid, event, timestamp, received_at, properties FROM events WHERE uuid={uuid:String} LIMIT 1",
	"parameters": {"uuid": "UUID_FROM_CATALOG"}
}
```

The same query through MCP uses:

```json
{
	"operation_id": "query-events",
	"path": {"id": "PROJECT_ID"},
	"body": {
		"sql": "SELECT uuid, event, properties FROM events WHERE uuid={uuid:String} LIMIT 1",
		"parameters": {"uuid": "UUID_FROM_CATALOG"},
		"use_cache": false
	}
}
```

## Supported SQL and parameters

One statement is allowed, with at most 16 KiB of SQL. Supported forms include `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, sequential CTEs, subqueries, joins, and `UNION`. Sources are `events` and local CTEs. Supported aggregate functions include percentiles, `windowFunnel`, and `retention`; the schema response lists exact names.

Writes, qualified source tables, table functions, external sources, system metadata, `FORMAT`, `SETTINGS`, windows, lambdas, `CAST`, and recursive CTEs are excluded. Unknown syntax and functions fail validation. Named scalar conversion functions such as `toUInt64` are available when included in the schema allowlist.

Typed placeholders use ClickHouse syntax such as `{client:String}`. Parameters support scalar string, Boolean, UUID, date, date-time, integer, and floating-point types. There are at most 32 names. Names contain at most 64 ASCII letters, digits, or underscores, starting with a letter or underscore. Each value is a string of at most 4,096 bytes. Supplied names must exactly match the placeholders, with no missing or extra values. One name cannot declare conflicting types. Values stay separate from SQL.

## Large result exports

[SQL result exports](exports.md) run the same validated project query grammar
with a separate restricted reader and operator budgets. They provide durable
job creation, polling, cancellation, and private gzip CSV or JSONL downloads.
They do not raise interactive `query-events` limits or rewrite SQL into pages.
All five export operations require project-scoped `events:read`; cancellation
also requires the creator credential or instance administration.

An accepted-work delivery barrier waits for original events and mutations
through the job's recorded watermarks. It does not create a snapshot or exclude
subsequent writes. Download URLs appear only after full stream validation and
success. Read the export reference for formats, retries, expiring links,
revocation limits, and required bucket lifecycle settings.

## Fixed query limits

| Limit | Value |
| --- | --- |
| ClickHouse execution time | 5 seconds |
| API request timeout | 10 seconds |
| ClickHouse memory | 256 MiB |
| Scanned rows | 5,000,000 |
| Read bytes | 1 GiB |
| Result rows | 1,000 |
| Result bytes | 1 MiB |
| Concurrent analytical requests | 2, instance-wide |

Catalog requests share the same concurrency pool and analytical limits. Readers have fixed read-only settings. Result overflow fails rather than silently truncating. An explicit SQL `LIMIT` can bound a valid result.

## Delivery consistency and errors

Every successful schema, catalog, or query response includes project-scoped delivery status. `pending` counts queued original events and `pending_mutations` counts queued corrections and deletions; `last_success` is optional. `oldest_pending_received_at` gives the server receipt time of the earliest pending original in acceptance order, including quarantined originals, and is absent when that project has none. A generic `error` appears only while that project has either kind of pending work. It does not disclose other projects' queue state.

Results are eventually consistent. Fresh queries show the latest projected event revision and exclude projected deletion tombstones. Cached queries can also lag new original events by up to 60 seconds, as described above. A pending event or correction can already be visible after a lost ClickHouse acknowledgment. The [current-event API](events.md) reflects accepted pending corrections earlier than SQL. Private original snapshots and tombstone internals are excluded from SQL schema and reader grants. Native [S3 tiering](storage.md) keeps the same table and queries across hot and cold parts. SQL results and delivery status have no cross-database snapshot. Catalog event and property queries also run separately. An empty catalog therefore does not prove that no events were accepted.

Invalid or unsupported SQL and invalid parameter bindings return `400`. Missing or invalid credentials return `401`; missing authority returns `403`. Concurrency and database resource limits return `429`. Unconfigured or unavailable storage, reader provisioning failures, and invalid storage responses return `503`. Request schema validation uses the generated OpenAPI error contract. ClickHouse server messages, SQL text, and credentials are not exposed in storage errors.

If a reader account or project database disappears, its failed query can return
`503` while invalidating the cached reader setup. A subsequent request attempts
to provision it again.
