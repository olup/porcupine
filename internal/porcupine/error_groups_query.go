package porcupine

// DuckLake's event_time carries microseconds. The original canonical UTC timestamp
// carries nanoseconds, and right-padding its fractional seconds preserves their order.
const errorGroupRowsSQL = `WITH exception_events AS (
 SELECT id, event_time, timestamp,
  concat(substr(timestamp, 1, 19), substr(concat(rtrim(substr(timestamp, 21), 'Z'), '000000000'), 1, 9)) AS occurrence_key,
  TRY_CAST(properties AS JSON) AS props
 FROM events WHERE event = '$exception'
), exception_fields AS (
 SELECT id, event_time, timestamp, occurrence_key,
  concat(occurrence_key, right(concat('0000000000000000000', CAST(id AS VARCHAR)), 19)) AS event_key,
  CASE WHEN json_type(props, '$."$exception_fingerprint"') = 'VARCHAR'
    THEN json_extract_string(props, '$."$exception_fingerprint"') END AS fingerprint,
  CASE WHEN json_type(props, '$."$exception_list"[0].type') = 'VARCHAR'
      AND length(json_extract_string(props, '$."$exception_list"[0].type')) <= 128
    THEN json_extract_string(props, '$."$exception_list"[0].type') END AS error_type,
  CASE WHEN json_type(props, '$."$exception_list"[0].value') = 'VARCHAR'
      AND length(json_extract_string(props, '$."$exception_list"[0].value')) <= 512
    THEN json_extract_string(props, '$."$exception_list"[0].value') END AS message,
  CASE WHEN json_type(props, '$."$exception_build_id"') = 'VARCHAR'
    AND regexp_full_match(json_extract_string(props, '$."$exception_build_id"'), '[a-zA-Z0-9._-]{1,128}')
    THEN json_extract_string(props, '$."$exception_build_id"') END AS build_id,
  CASE WHEN json_type(props, '$."$exception_release"') = 'VARCHAR'
    AND regexp_full_match(json_extract_string(props, '$."$exception_release"'), '[a-zA-Z0-9._-]{1,128}')
    THEN json_extract_string(props, '$."$exception_release"') END AS release,
  CASE WHEN json_type(props, '$."$exception_degraded"') = 'BOOLEAN'
    THEN CAST(json_extract_string(props, '$."$exception_degraded"') AS BOOLEAN) END AS degraded
 FROM exception_events
 WHERE json_type(props, '$."$exception_list"') = 'ARRAY'
 AND json_array_length(props, '$."$exception_list"') <= 4
 AND (
   json_type(props, '$."$exception_list"[0]') = 'OBJECT'
   OR (json_array_length(props, '$."$exception_list"') = 0
     AND json_type(props, '$."$exception_degraded"') = 'BOOLEAN'
     AND json_extract_string(props, '$."$exception_degraded"') = 'true')
 )
)`

const errorGroupAggregateSQL = `fingerprint, count(*) AS matches,
 arg_min(timestamp, event_key) AS first_seen,
 arg_max(timestamp, event_key) AS last_seen,
 arg_max(id, event_key) AS latest_event_id,
 arg_max(coalesce(error_type, ''), event_key) AS error_type,
 arg_max(coalesce(message, ''), event_key) AS message,
 max(occurrence_key) AS last_key`

const errorGroupPageSQL = ` LIMIT CAST($limit AS BIGINT) OFFSET CAST($offset AS BIGINT)`

const errorGroupsListSQL = errorGroupRowsSQL + `
, summaries AS (
 SELECT ` + errorGroupAggregateSQL + `
 FROM exception_fields
 WHERE regexp_full_match(fingerprint, '` + errorGroupFingerprintPattern + `')
 GROUP BY fingerprint
 HAVING $q = '' OR bool_or(contains(lower(error_type), lower($q)) OR contains(lower(message), lower($q)))
)
SELECT fingerprint, error_type AS type, message,
 CAST(matches AS VARCHAR) AS count, first_seen, last_seen, CAST(latest_event_id AS VARCHAR) AS latest_event_id
 FROM summaries ORDER BY `

const errorGroupDetailSQL = errorGroupRowsSQL + `
, matched AS MATERIALIZED (
 SELECT id, event_time, timestamp, occurrence_key, event_key, fingerprint, error_type, message, build_id, release, degraded
 FROM exception_fields WHERE fingerprint = $fingerprint
), summary AS (
 SELECT ` + errorGroupAggregateSQL + `
 FROM matched GROUP BY fingerprint
), buckets AS (
 SELECT date_trunc($bucket, timezone('UTC', event_time)) AS bucket_start, count(*) AS matches
 FROM matched GROUP BY bucket_start
), page AS (
 SELECT id, timestamp, occurrence_key, build_id, release, degraded FROM matched
 ORDER BY occurrence_key DESC, id DESC` + errorGroupPageSQL + `
)
SELECT 'summary' AS kind, fingerprint, error_type AS type, message,
 CAST(matches AS VARCHAR) AS count, first_seen, last_seen, CAST(latest_event_id AS VARCHAR) AS latest_event_id,
 NULL AS start, NULL AS event_id, NULL AS occurred_at, NULL AS build_id, NULL AS release, NULL AS degraded,
 '' AS occurrence_key, 0 AS sort_id
 FROM summary
UNION ALL
SELECT 'bucket', NULL, NULL, NULL, CAST(matches AS VARCHAR), NULL, NULL, NULL,
 strftime(bucket_start, '%Y-%m-%dT%H:%M:%SZ'), NULL, NULL, NULL, NULL, NULL, '', 0
 FROM buckets
UNION ALL
SELECT 'occurrence', NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, CAST(id AS VARCHAR), timestamp, build_id, release, degraded, occurrence_key, id
 FROM page
ORDER BY occurrence_key DESC, sort_id DESC`
