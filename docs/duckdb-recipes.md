# DuckDB SQL recipes

Discover `get-event-schema` before using these recipes. They target `sql.dialect: duckdb`, the default DuckLake backend. Optional ClickHouse deployments need different SQL. Replace example event names, property keys and dates with values from your project.

Each block contains a recipe name, a `parameters` string map and SQL. Send the SQL and parameters as the body of `query-events`. HTTP, CLI and MCP use the same body. The commented metadata is also used by the recipe tests.

```json
{"operation_id":"query-events","path":{"id":"PROJECT_ID"},"body":{"sql":"SELECT count(*) AS total FROM events WHERE event=$event","parameters":{"event":"page_viewed"}}}
```

Read `body.result.data` in an MCP result. Counts and other 64-bit integers are decimal strings. Inspect `delivery.pending` and `pending_mutations` before checking recently sent data. Query larger results through [exports](exports.md).

## Daily volume and people

Use occurrence time, not receipt time, for historical product activity. The interval includes `start` and excludes `end`. `person_id` follows the current identity resolution; an empty ID is not a person. Rotating cookieless identities cannot measure long-term retention.

```sql
-- recipe: daily-volume
-- parameters: {"event":"page_viewed","start":"2026-11-01T00:00:00Z","end":"2026-11-04T00:00:00Z"}
SELECT CAST(event_time AS DATE) AS day,
       count(*) AS total,
       count(DISTINCT nullif(person_id,'')) AS people
FROM events
WHERE event=$event
  AND event_time>=CAST($start AS TIMESTAMPTZ)
  AND event_time<CAST($end AS TIMESTAMPTZ)
GROUP BY day ORDER BY day
```

## Custom dimension breakdown

This query preserves raw property values. Normalize casing with `upper` only if your metric treats differently cased values as equivalent.

```sql
-- recipe: property-breakdown
-- parameters: {"event":"page_viewed"}
SELECT json_extract_string(properties,'$.language') AS language,
       count(*) AS total
FROM events WHERE event=$event
GROUP BY language ORDER BY language
```

## Normalized context and sessions

The context may contain `page.url`, `page.referrer`, `browser.name`, `browser.version`, `device.type`, `os.name`, `locale` and `session_id`. Fields depend on what the producer supplied. Read a field with `json_extract_string(context,'$.browser.name')`.

This query counts sessions that contain the chosen event. It does not measure visitors reading silently, or time spent with the page visible.

```sql
-- recipe: sessions
-- parameters: {"event":"page_viewed"}
SELECT count(DISTINCT nullif(json_extract_string(context,'$.session_id'),'')) AS sessions
FROM events WHERE event=$event
```

## Numeric values and percentiles

Confirm the unit with the producer. This example defines `duration_ms` in milliseconds. `TRY_CAST` turns missing or invalid numeric values into NULL. Aggregations exclude those NULLs; report their count rather than treating them as zero. `quantile_disc` uses a discrete observed value; `quantile_cont` interpolates.

```sql
-- recipe: latency
-- parameters: {"event":"purchase"}
WITH durations AS (
  SELECT TRY_CAST(json_extract_string(properties,'$.duration_ms') AS DOUBLE) AS ms
  FROM events WHERE event=$event
)
SELECT count(ms) AS samples, count_if(ms IS NULL) AS missing_or_invalid,
       avg(ms) AS mean_ms, quantile_disc(ms,0.95) AS p95_ms
FROM durations
```

## Ordered conversion funnel

This definition starts at each person's first observed page view. It counts their earliest later signup within ten minutes, then a purchase after that signup and within ten minutes of the first view. A later visit does not restart this funnel. Choose different rules when your question needs different semantics.

```sql
-- recipe: ordered-funnel
-- parameters: {"start_event":"page_viewed","middle_event":"signup","end_event":"purchase"}
WITH starts AS (
  SELECT person_id,min(event_time) AS t FROM events
  WHERE event=$start_event AND nullif(person_id,'') IS NOT NULL GROUP BY person_id
), signups AS (
  SELECT e.person_id,min(e.event_time) AS t,s.t AS start_time
  FROM events e JOIN starts s ON e.person_id=s.person_id
  WHERE e.event=$middle_event AND e.event_time>=s.t
    AND e.event_time<=s.t+INTERVAL '10 minutes'
  GROUP BY e.person_id,s.t
), conversions AS (
  SELECT DISTINCT e.person_id FROM events e JOIN signups s ON e.person_id=s.person_id
  WHERE e.event=$end_event AND e.event_time>=s.t
    AND e.event_time<=s.start_time+INTERVAL '10 minutes'
)
SELECT (SELECT count(*) FROM starts) AS entered,
       (SELECT count(*) FROM signups) AS signed_up,
       (SELECT count(*) FROM conversions) AS converted
```

## Daily retention cohorts

The first observed matching event defines each person's cohort. This is not necessarily their first-ever use if earlier history was not imported. The query returns observed cohort/day cells, including day zero. An absent future cell is not proof of zero retention before that observation period has elapsed.

```sql
-- recipe: retention
-- parameters: {"event":"page_viewed"}
WITH activity AS (
  SELECT DISTINCT person_id,CAST(event_time AS DATE) AS day FROM events
  WHERE event=$event AND nullif(person_id,'') IS NOT NULL
), cohorts AS (
  SELECT person_id,min(day) AS first_day FROM activity GROUP BY person_id
)
SELECT c.first_day AS cohort_day,date_diff('day',c.first_day,a.day) AS day_number,
       count(DISTINCT a.person_id) AS people
FROM activity a JOIN cohorts c ON a.person_id=c.person_id
GROUP BY c.first_day,day_number ORDER BY cohort_day,day_number
```

## Local calendar days

Convert the timezone before extracting a date. Keep UTC timestamps for storage and absolute time filters.

```sql
-- recipe: local-days
-- parameters: {"event":"page_viewed","timezone":"Europe/Paris"}
SELECT CAST(timezone($timezone,event_time) AS DATE) AS day,count(*) AS total
FROM events WHERE event=$event GROUP BY day ORDER BY day
```

## Moving averages

This averages up to three observed daily groups. Missing days are absent, so it is not automatically a continuous three-calendar-day average. Fill a bounded calendar with zeros in your agent before plotting when that is the desired definition. Arbitrary table functions such as `generate_series` are outside the permitted query subset.

```sql
-- recipe: moving-average
-- parameters: {"event":"page_viewed"}
WITH daily AS (
  SELECT CAST(event_time AS DATE) AS day,count(*) AS total
  FROM events WHERE event=$event GROUP BY day
)
SELECT day,total,avg(total) OVER (ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_average
FROM daily ORDER BY day
```

## Property coverage

Use a bound JSON path to inspect a property without interpolating a key into SQL. This query counts missing or JSON-null values together. Cardinality refers to the chosen event and matching delivered history.

```sql
-- recipe: property-coverage
-- parameters: {"event":"page_viewed","path":"$.language"}
WITH extracted AS (
  SELECT json_extract_string(properties,CAST($path AS VARCHAR)) AS value FROM events WHERE event=$event
)
SELECT count(*) AS total,count_if(value IS NULL) AS missing,
       count(DISTINCT value) AS distinct_values FROM extracted
```

For discovery, filtering and safe error recovery, see the [analytics reference](analytics.md). These recipes describe metric definitions explicitly; they do not infer your application's business meaning.
