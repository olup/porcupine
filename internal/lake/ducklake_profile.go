package lake

import (
	"context"
	"time"
)

const duckLakeProfileActivitySQL = `SELECT CAST(count(*) AS VARCHAR),
 CAST(min(timestamp_ms) AS VARCHAR), CAST(max(timestamp_ms) AS VARCHAR),
 CAST(min(received_at_ms) AS VARCHAR), CAST(max(received_at_ms) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=? AND timestamp_ms<?) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=? AND timestamp_ms<?) AS VARCHAR),
 CAST(count(*) FILTER (WHERE timestamp_ms>=?) AS VARCHAR)
 FROM lake.current_events WHERE identity_snapshot='' AND event=?`

const duckLakeProfileSampleBoundsSQL = `SELECT coalesce(min(id),0),coalesce(max(id),0) FROM (
 SELECT id FROM lake.current_events WHERE identity_snapshot='' AND event=? ORDER BY timestamp_ms DESC,id DESC LIMIT ?
)`

const duckLakeProfileSampleSQL = `WITH selected AS (
 SELECT sort_id,CAST(sort_id AS VARCHAR) AS id,uuid,timestamp,timestamp_ms,received_at,properties,context,
 row_number() OVER (ORDER BY timestamp_ms DESC,sort_id DESC) AS rn FROM (
  SELECT id AS sort_id,uuid,timestamp,timestamp_ms,received_at,properties,context
  FROM lake.current_events WHERE identity_snapshot='' AND event=? AND id BETWEEN ? AND ?
  ORDER BY timestamp_ms DESC,id DESC LIMIT ?
 )
), bounded AS (
 SELECT *,octet_length(encode(properties)) AS property_bytes,octet_length(encode(context)) AS context_bytes,
 CASE WHEN octet_length(encode(properties))<=? THEN properties ELSE '' END AS safe_properties,
 CASE WHEN rn<=? AND octet_length(encode(context))<=? THEN context ELSE '' END AS safe_context
 FROM selected
), costs AS (
 SELECT *,octet_length(encode(CAST(to_json(safe_properties) AS VARCHAR)))
 +octet_length(encode(CAST(to_json(safe_context) AS VARCHAR))) AS payload_cost,
 octet_length(encode(CAST(to_json(struct_pack(id:=id,uuid:=uuid,timestamp:=timestamp,
 timestamp_ms:=CAST(timestamp_ms AS VARCHAR),received_at:=received_at,
 property_bytes:=property_bytes,context_bytes:=context_bytes)) AS VARCHAR)))+100 AS metadata_cost
 FROM bounded
), budgeted AS (
 SELECT *,sum(payload_cost) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS payload_used,
 sum(metadata_cost) OVER (ORDER BY rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS metadata_used
 FROM costs
)
SELECT id,uuid,timestamp,timestamp_ms,received_at,
 CASE WHEN payload_used<=? THEN safe_properties ELSE '' END,
 CASE WHEN payload_used<=? THEN safe_context ELSE '' END,
 property_bytes,context_bytes FROM budgeted WHERE metadata_used<=?
 ORDER BY timestamp_ms DESC,sort_id DESC`

func (s *DuckLakeStore) Profile(ctx context.Context, project string, options EventProfileOptions, asOf time.Time) (EventProfileRead, error) {
	return profileProject(ctx, s.LeaseProject, project, options, asOf)
}

func profileProject(ctx context.Context, lease analyticalProjectLease, project string, options EventProfileOptions, asOf time.Time) (EventProfileRead, error) {
	p, release, err := lease(ctx, project)
	if err != nil {
		return EventProfileRead{}, duckLakeProjectError(ctx, err)
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	defer tx.Rollback()
	result := EventProfileRead{Rows: []EventProfileRow{}}
	activity := &result.activity
	err = tx.QueryRowContext(ctx, duckLakeProfileActivitySQL,
		asOf.Add(-24*time.Hour).UnixMilli(), asOf.UnixMilli(),
		asOf.Add(-7*24*time.Hour).UnixMilli(), asOf.UnixMilli(), asOf.UnixMilli(), options.event,
	).Scan(&activity.Total, &activity.FirstOccurredAt, &activity.LastOccurredAt,
		&activity.FirstReceivedAt, &activity.LastReceivedAt, &activity.Last24Hours, &activity.Last7Days, &activity.FutureEvents)
	if err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	for _, field := range []**string{&activity.FirstOccurredAt, &activity.LastOccurredAt, &activity.FirstReceivedAt, &activity.LastReceivedAt} {
		*field, err = profileTime(*field)
		if err != nil {
			return EventProfileRead{}, err
		}
	}
	// The range contains the whole sample. Reapplying the same order and
	// limit in this snapshot excludes older events within the range.
	var minID, maxID int64
	if err := tx.QueryRowContext(ctx, duckLakeProfileSampleBoundsSQL, options.event, options.sampleLimit).Scan(&minID, &maxID); err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	rows, err := tx.QueryContext(ctx, duckLakeProfileSampleSQL, options.event, minID, maxID, options.sampleLimit,
		EventProfileFieldBytes, options.exampleLimit, EventProfileFieldBytes,
		EventProfileSampleBytes, EventProfileSampleBytes, eventProfileMetadataBytes)
	if err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	for rows.Next() {
		var row EventProfileRow
		if err = rows.Scan(&row.ID, &row.UUID, &row.Timestamp, &row.TimestampMS, &row.ReceivedAt,
			&row.Properties, &row.Context, &row.PropertyBytes, &row.ContextBytes); err != nil {
			return EventProfileRead{}, DuckLakeQueryError(ctx, err)
		}
		result.Rows = append(result.Rows, row)
	}
	if err = rows.Err(); err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	if err = rows.Close(); err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	if err = tx.Commit(); err != nil {
		return EventProfileRead{}, DuckLakeQueryError(ctx, err)
	}
	return result, nil
}
