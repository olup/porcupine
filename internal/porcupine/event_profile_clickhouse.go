package porcupine

import (
	"context"
	"strconv"
	"time"
)

type eventProfileActivityRow struct {
	Total           string  `json:"total"`
	FirstOccurredMS *string `json:"first_occurred_ms"`
	LastOccurredMS  *string `json:"last_occurred_ms"`
	FirstReceivedMS *string `json:"first_received_ms"`
	LastReceivedMS  *string `json:"last_received_ms"`
	Last24Hours     string  `json:"last_24_hours"`
	Last7Days       string  `json:"last_7_days"`
	FutureEvents    string  `json:"future_events"`
}

func (row eventProfileActivityRow) activity() (EventProfileActivity, error) {
	result := EventProfileActivity{Total: row.Total, Last24Hours: row.Last24Hours, Last7Days: row.Last7Days, FutureEvents: row.FutureEvents}
	for _, count := range []string{row.Total, row.Last24Hours, row.Last7Days, row.FutureEvents} {
		if !catalogCount(count, 0) {
			return result, apiError(503, "invalid event profile activity response")
		}
	}
	var err error
	if result.FirstOccurredAt, err = profileTime(row.FirstOccurredMS); err != nil {
		return result, err
	}
	if result.LastOccurredAt, err = profileTime(row.LastOccurredMS); err != nil {
		return result, err
	}
	if result.FirstReceivedAt, err = profileTime(row.FirstReceivedMS); err != nil {
		return result, err
	}
	if result.LastReceivedAt, err = profileTime(row.LastReceivedMS); err != nil {
		return result, err
	}
	for _, date := range []*string{row.FirstOccurredMS, row.LastOccurredMS, row.FirstReceivedMS, row.LastReceivedMS} {
		if (row.Total == "0") != (date == nil) {
			return result, apiError(503, "invalid event profile activity response")
		}
	}
	total, _ := strconv.ParseUint(row.Total, 10, 64)
	day, _ := strconv.ParseUint(row.Last24Hours, 10, 64)
	week, _ := strconv.ParseUint(row.Last7Days, 10, 64)
	future, _ := strconv.ParseUint(row.FutureEvents, 10, 64)
	if day > week || week > total || future > total || week > total-future {
		return result, apiError(503, "invalid event profile activity response")
	}
	return result, nil
}

const eventProfileClickHouseActivitySQL = `SELECT toString(count()) AS total,
if(count()=0,NULL,toString(min(timestamp_ms))) AS first_occurred_ms,
if(count()=0,NULL,toString(max(timestamp_ms))) AS last_occurred_ms,
if(count()=0,NULL,toString(min(received_at_ms))) AS first_received_ms,
if(count()=0,NULL,toString(max(received_at_ms))) AS last_received_ms,
toString(countIf(timestamp_ms >= {start_24h:Int64} AND timestamp_ms < {as_of:Int64})) AS last_24_hours,
toString(countIf(timestamp_ms >= {start_7d:Int64} AND timestamp_ms < {as_of:Int64})) AS last_7_days,
toString(countIf(timestamp_ms >= {as_of:Int64})) AS future_events
FROM events WHERE event={event:String}`

const eventProfileClickHouseSampleSQL = `WITH picked AS (
 SELECT id,uuid,timestamp,timestamp_ms,received_at,properties,
 if(row_number() OVER (ORDER BY timestamp_ms DESC,id DESC) <= {example_limit:UInt32},context,'') AS sample_context
 FROM events WHERE event={event:String} ORDER BY timestamp_ms DESC,id DESC LIMIT {sample_limit:UInt32}
), sized AS (
 SELECT *,length(properties) AS property_bytes,length(sample_context) AS context_bytes,
 if(length(properties)<={field_bytes:UInt32},length(toJSONString(properties)),0) AS property_cost,
 if(length(sample_context)>0 AND length(sample_context)<={field_bytes:UInt32},length(toJSONString(sample_context)),0) AS context_cost,
 256+length(toJSONString(toString(id)))+length(toJSONString(uuid))+length(toJSONString(timestamp))+length(toJSONString(received_at)) AS metadata_cost
 FROM picked
), budgeted AS (
 SELECT *,sum(property_cost+context_cost) OVER (ORDER BY timestamp_ms DESC,id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS payload_prefix,
 sum(metadata_cost) OVER (ORDER BY timestamp_ms DESC,id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS metadata_prefix
 FROM sized
)
SELECT toString(budgeted.id) AS id,budgeted.uuid AS uuid,budgeted.timestamp AS timestamp,toString(budgeted.timestamp_ms) AS timestamp_ms,budgeted.received_at AS received_at,
if(budgeted.property_bytes<={field_bytes:UInt32} AND budgeted.payload_prefix<={sample_bytes:UInt32},budgeted.properties,'') AS properties,
if(budgeted.context_bytes<={field_bytes:UInt32} AND budgeted.payload_prefix<={sample_bytes:UInt32},budgeted.sample_context,'') AS context,
toUInt32(budgeted.property_bytes) AS property_bytes,toUInt32(budgeted.context_bytes) AS context_bytes
FROM budgeted WHERE metadata_prefix<={metadata_bytes:UInt32} ORDER BY budgeted.timestamp_ms DESC,budgeted.id DESC`

func (s *Service) clickHouseProfile(ctx context.Context, project string, options eventProfileOptions, asOf time.Time) (eventProfileRead, error) {
	namespace, reader, password, err := s.analyticsReader(ctx, project)
	if err != nil {
		return eventProfileRead{}, s.analyticsStorageError(ctx, project, err)
	}
	parameters := map[string]string{
		"event":     options.event,
		"as_of":     strconv.FormatInt(asOf.UnixMilli(), 10),
		"start_24h": strconv.FormatInt(asOf.Add(-24*time.Hour).UnixMilli(), 10),
		"start_7d":  strconv.FormatInt(asOf.Add(-7*24*time.Hour).UnixMilli(), 10),
	}
	data, err := s.ch.query(ctx, reader, password, namespace, eventProfileClickHouseActivitySQL, parameters)
	if err != nil {
		return eventProfileRead{}, s.analyticsStorageError(ctx, project, err)
	}
	activityRows, err := decodeCatalogRows[eventProfileActivityRow](data, 1)
	if err != nil || len(activityRows) != 1 {
		return eventProfileRead{}, apiError(503, "invalid event profile activity response")
	}
	activity, err := activityRows[0].activity()
	if err != nil {
		return eventProfileRead{}, err
	}
	parameters = map[string]string{
		"event": options.event, "example_limit": strconv.Itoa(options.exampleLimit), "sample_limit": strconv.Itoa(options.sampleLimit),
		"field_bytes": strconv.Itoa(eventProfileFieldBytes), "sample_bytes": strconv.Itoa(eventProfileSampleBytes), "metadata_bytes": strconv.Itoa(eventProfileMetadataBytes),
	}
	data, err = s.ch.query(ctx, reader, password, namespace, eventProfileClickHouseSampleSQL, parameters)
	if err != nil {
		return eventProfileRead{}, s.analyticsStorageError(ctx, project, err)
	}
	rows, err := decodeCatalogRows[eventProfileRow](data, options.sampleLimit)
	if err != nil {
		return eventProfileRead{}, err
	}
	return eventProfileRead{activity: activity, rows: rows}, nil
}
