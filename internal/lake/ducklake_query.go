package lake

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/nativequery"
)

func (s *DuckLakeStore) Query(ctx context.Context, project, rawSQL string, parameters map[string]string, limits QueryExecutionLimits) (AnalyticsResult, error) {
	return queryProject(ctx, s.LeaseProject, project, rawSQL, parameters, limits)
}

func queryProject(ctx context.Context, lease analyticalProjectLease, project, rawSQL string, parameters map[string]string, limits QueryExecutionLimits) (AnalyticsResult, error) {
	if limits.Timeout <= 0 || limits.MaxRows < 1 || limits.MaxRows > 100000 || limits.MaxResultBytes < 256 || limits.MaxResultBytes > 64<<20 {
		return AnalyticsResult{}, apierr.New(500, "invalid interactive execution limits")
	}
	ctx, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	p, release, err := lease(ctx, project)
	if err != nil {
		return AnalyticsResult{}, duckLakeProjectError(ctx, err)
	}
	defer release()
	canonical, args, err := nativequery.Prepare(ctx, p.DB, rawSQL, parameters)
	if err != nil {
		if ctx.Err() != nil {
			return AnalyticsResult{}, apierr.New(429, "analytics execution time limit reached")
		}
		return AnalyticsResult{}, duckLakeValidationError(ctx, err)
	}
	prefix := ""
	if limits.TimeRange != nil {
		prefix = eventTimeRangeCTE(*limits.TimeRange)
	}
	start := time.Now()
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return AnalyticsResult{}, DuckLakeQueryError(ctx, err)
	}
	defer conn.Close()
	columns, types, err := duckLakeResultColumns(ctx, conn, prefix, canonical, args)
	if err != nil {
		return AnalyticsResult{}, DuckLakeQueryError(ctx, err)
	}
	alias := "_pc_result"
	for suffix := 0; ; suffix++ {
		conflict := false
		for _, column := range columns {
			if strings.EqualFold(alias, column.Name) {
				conflict = true
				break
			}
		}
		if !conflict {
			break
		}
		alias = "_pc_result_" + strconv.Itoa(suffix+1)
	}
	result := AnalyticsResult{Meta: columns, Data: []map[string]json.RawMessage{}}
	metadata, _ := json.Marshal(result.Meta)
	bytes := len(metadata) + 256
	if bytes > limits.MaxResultBytes {
		return AnalyticsResult{}, apierr.Detailed(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
	}
	rows, err := conn.QueryContext(ctx, prefix+"SELECT CAST(to_json("+alias+") AS VARCHAR) FROM ("+canonical+") AS "+alias+" LIMIT "+strconv.Itoa(limits.MaxRows+1), args...)
	if err != nil {
		return AnalyticsResult{}, DuckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	for rows.Next() {
		if len(result.Data) >= limits.MaxRows {
			return AnalyticsResult{}, apierr.Detailed(429, "result_row_limit", "analytics result row limit reached", "Reduce the result or use create-event-export for a larger CSV or JSONL export.", "")
		}
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return AnalyticsResult{}, DuckLakeQueryError(ctx, err)
		}
		if len(raw) > limits.MaxResultBytes-bytes {
			return AnalyticsResult{}, apierr.Detailed(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
		}
		fields, err := exportJSONObject([]byte(raw))
		if err != nil {
			return AnalyticsResult{}, apierr.New(400, "unsupported analytics result value")
		}
		if len(fields) != len(columns) {
			return AnalyticsResult{}, apierr.New(503, "invalid DuckLake result")
		}
		row := make(map[string]json.RawMessage, len(columns))
		for i, column := range columns {
			value, ok := fields[column.Name]
			if !ok {
				return AnalyticsResult{}, apierr.New(503, "invalid DuckLake result")
			}
			row[column.Name], err = duckLakeResultValue(value, types[i], 0)
			if err != nil {
				return AnalyticsResult{}, apierr.New(400, "unsupported analytics result value")
			}
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return AnalyticsResult{}, apierr.New(503, "invalid DuckLake result")
		}
		bytes += len(encoded) + 1
		if bytes > limits.MaxResultBytes {
			return AnalyticsResult{}, apierr.Detailed(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
		}
		result.Data = append(result.Data, row)
	}
	if err := rows.Err(); err != nil {
		return AnalyticsResult{}, DuckLakeQueryError(ctx, err)
	}
	result.Rows = uint64(len(result.Data))
	result.Statistics.Elapsed = time.Since(start).Seconds()
	return result, nil
}

// The SQL policy reserves the unqualified events name. This statement-local
// CTE supplies that relation to every user query node without changing the
// project view or the connection. Keep it inline so scans can push filters down.
func eventTimeRangeCTE(window QueryTimeRange) string {
	return "WITH events AS NOT MATERIALIZED (SELECT * FROM main.events WHERE " +
		eventTimeBound(window.From, true) + " AND " + eventTimeBound(window.To, false) +
		") "
}

func eventTimeBound(bound time.Time, inclusive bool) string {
	microseconds := bound.UnixMicro()
	instant := "make_timestamptz(" + strconv.FormatInt(microseconds, 10) + ")"
	if bound.Nanosecond()%1000 == 0 {
		if inclusive {
			return "event_time >= " + instant
		}
		return "event_time < " + instant
	}

	// TIMESTAMPTZ stores microseconds. For rows in the endpoint microsecond,
	// compare the original UTC timestamp's fractional seconds at full precision.
	fraction := "rpad(rtrim(substr(timestamp, 21), 'Z'), 9, '0')"
	nanoseconds := LakeLiteral(bound.Format(".000000000")[1:])
	if inclusive {
		return "event_time >= " + instant + " AND (event_time > " + instant + " OR " + fraction + " >= " + nanoseconds + ")"
	}
	next := "make_timestamptz(" + strconv.FormatInt(microseconds+1, 10) + ")"
	return "event_time < " + next + " AND (event_time < " + instant + " OR " + fraction + " < " + nanoseconds + ")"
}

func duckLakeValidationError(ctx context.Context, err error) error {
	var policy *nativequery.Error
	if errors.As(err, &policy) {
		return apierr.Detailed(400, policy.Code, policy.Message, policy.Hint, policy.Field)
	}
	var managed *apierr.Error
	if errors.As(err, &managed) {
		return managed
	}
	if ctx.Err() != nil {
		return DuckLakeQueryError(ctx, err)
	}
	return apierr.Detailed(400, "unsupported_sql", "Unsupported DuckDB SQL", "Use one SELECT against events. Read get-event-schema for supported sources, expressions and functions.", "")
}

func duckLakeProjectError(ctx context.Context, err error) error {
	var managed *apierr.Error
	if errors.As(err, &managed) {
		return managed
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return DuckLakeQueryError(ctx, err)
	}
	return apierr.New(503, "DuckLake storage unavailable")
}

var duckLakeUnknownColumn = regexp.MustCompile(`Referenced column "([A-Za-z_][A-Za-z0-9_]{0,63})" not found`)

func DuckLakeQueryError(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apierr.New(429, "analytics execution time limit reached")
	}
	var managed *apierr.Error
	if errors.As(err, &managed) {
		return managed
	}
	var failure *duckdb.Error
	if errors.As(err, &failure) {
		switch failure.Type {
		case duckdb.ErrorTypeOutOfMemory, duckdb.ErrorTypeInterrupt:
			return apierr.New(429, "analytics resource limit reached")
		case duckdb.ErrorTypeBinder:
			firstLine := strings.SplitN(failure.Msg, "\n", 2)[0]
			if strings.Contains(firstLine, "Referenced column") && strings.Contains(firstLine, "not found") {
				field := ""
				if match := duckLakeUnknownColumn.FindStringSubmatch(firstLine); match != nil {
					field = match[1]
				}
				return apierr.Detailed(400, "unknown_column", "Unknown column in analytics query", "Read get-event-schema for columns. Custom properties use json_extract_string(properties,'$.key'). Check CTE and alias names as well.", field)
			}
			return apierr.Detailed(400, "type_mismatch", "Invalid argument types or query binding", "Check function argument types, casts, aliases and GROUP BY expressions. Read get-event-schema for column types.", "")
		case duckdb.ErrorTypeConversion, duckdb.ErrorTypeOutOfRange:
			return apierr.Detailed(400, "invalid_value", "Value cannot be converted to the requested type", "Check property values and casts. TRY_CAST returns NULL for values that cannot be converted.", "")
		case duckdb.ErrorTypeDivideByZero:
			return apierr.Detailed(400, "division_by_zero", "Division by zero in analytics query", "Use nullif(denominator,0) before division.", "")
		case duckdb.ErrorTypeParser:
			return apierr.Detailed(400, "invalid_sql", "Invalid DuckDB SQL syntax", "Use one SELECT and DuckDB $name placeholders; inspect the query against get-event-schema.", "")
		case duckdb.ErrorTypeCatalog:
			return apierr.Detailed(400, "unknown_reference", "Unknown analytics reference", "Read get-event-schema for the events table and allowed functions. Check CTE and alias names.", "")
		case duckdb.ErrorTypeInvalidInput, duckdb.ErrorTypeNotImplemented, duckdb.ErrorTypeInvalidType, duckdb.ErrorTypeMismatchType,
			duckdb.ErrorTypeParameterNotAllowed, duckdb.ErrorTypeParameterNotResolved:
			return apierr.Detailed(400, "invalid_query", "Invalid DuckDB analytics query", "Check function arguments, parameter values and casts against get-event-schema.", "")
		}
	}
	return apierr.New(503, "DuckLake query unavailable")
}

func (s *DuckLakeStore) Catalog(ctx context.Context, input EventCatalogInput) (EventCatalog, error) {
	return catalogProject(ctx, s.LeaseProject, input)
}

func catalogProject(ctx context.Context, lease analyticalProjectLease, input EventCatalogInput) (EventCatalog, error) {
	input, err := NormalizeCatalogInput(input)
	if err != nil {
		return EventCatalog{}, err
	}
	project, event, limit, offset := input.ID, input.Event, input.Limit, input.Offset
	p, release, err := lease(ctx, project)
	if err != nil {
		return EventCatalog{}, duckLakeProjectError(ctx, err)
	}
	defer release()
	query := `SELECT event, CAST(count(*) AS VARCHAR), arg_min(received_at,id), arg_max(received_at,id), arg_max(uuid,id), arg_max(timestamp,id) FROM events`
	args := []any{}
	if event != "" {
		query += " WHERE event=?"
		args = append(args, event)
	}
	query += " GROUP BY event ORDER BY max(id) DESC LIMIT ? OFFSET ?"
	args = append(args, limit+1, offset)
	rows, err := p.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return EventCatalog{}, DuckLakeQueryError(ctx, err)
	}
	result := EventCatalog{Events: []EventCatalogEvent{}, Properties: []EventCatalogProperty{}, Sampling: EventCatalogSampling{
		PropertiesRequested: event != "", SampleEventLimit: 1000, KeysPerEventLimit: 200, MaxPropertyNameBytes: 200, PropertyLimit: input.PropertyLimit,
	}}
	for rows.Next() {
		var item EventCatalogEvent
		if err := rows.Scan(&item.Event, &item.Count, &item.FirstReceivedAt, &item.LastReceivedAt, &item.LatestEventUUID, &item.LatestEventTimestamp); err != nil {
			rows.Close()
			return EventCatalog{}, DuckLakeQueryError(ctx, err)
		}
		result.Events = append(result.Events, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return EventCatalog{}, DuckLakeQueryError(ctx, err)
	}
	if len(result.Events) > limit {
		result.Events = result.Events[:limit]
		next := offset + limit
		if next <= 10000 {
			result.NextOffset = &next
		} else {
			result.Sampling.PaginationTruncated = true
		}
	}
	if event == "" {
		return result, nil
	}
	rows, err = p.DB.QueryContext(ctx, "SELECT uuid,received_at,properties FROM events WHERE event=? ORDER BY id DESC LIMIT 1000", event)
	if err != nil {
		return EventCatalog{}, DuckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	type propertySample struct {
		count int
		types map[string]bool
		uuid  string
		at    string
	}
	properties := map[string]*propertySample{}
	for rows.Next() {
		var uuid, at, raw string
		if err := rows.Scan(&uuid, &at, &raw); err != nil {
			return EventCatalog{}, DuckLakeQueryError(ctx, err)
		}
		var object map[string]json.RawMessage
		if json.Unmarshal([]byte(raw), &object) != nil {
			return EventCatalog{}, apierr.New(503, "invalid DuckLake property sample")
		}
		keys := make([]string, 0, len(object))
		for key := range object {
			if catalogPropertyEligible(input, key) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		if len(keys) > 200 {
			keys = keys[:200]
		}
		for _, key := range keys {
			sample := properties[key]
			if sample == nil {
				if len(properties) >= input.PropertyLimit+1 {
					maximum := ""
					for candidate := range properties {
						if candidate > maximum {
							maximum = candidate
						}
					}
					if key >= maximum {
						continue
					}
					delete(properties, maximum)
				}
				sample = &propertySample{types: map[string]bool{}, uuid: uuid, at: at}
				properties[key] = sample
			}
			sample.count++
			sample.types[duckLakeJSONType(object[key])] = true
		}
	}
	if err := rows.Err(); err != nil {
		return EventCatalog{}, DuckLakeQueryError(ctx, err)
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		sample := properties[key]
		types := make([]string, 0, len(sample.types))
		for kind := range sample.types {
			types = append(types, kind)
		}
		sort.Strings(types)
		result.Properties = append(result.Properties, EventCatalogProperty{Key: key, Types: types, SampleOccurrences: strconv.Itoa(sample.count), LatestEventUUID: sample.uuid, LastReceivedAt: sample.at})
	}
	finishCatalogProperties(&result, input)
	return result, nil
}

func duckLakeJSONType(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return "Null"
	}
	switch s[0] {
	case '"':
		return "String"
	case '{':
		return "Object"
	case '[':
		return "Array"
	case 't', 'f':
		return "Bool"
	default:
		if strings.ContainsAny(s, ".eE") {
			return "Float64"
		}
		return "Int64"
	}
}
