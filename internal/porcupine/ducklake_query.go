//go:build ducklake

package porcupine

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
)

func (s *duckLakeStore) Query(ctx context.Context, project, rawSQL string, parameters map[string]string) (AnalyticsResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p, release, err := s.leaseProject(ctx, project)
	if err != nil {
		return AnalyticsResult{}, apiError(503, "DuckLake storage unavailable")
	}
	defer release()
	canonical, args, err := validateDuckLakeSQL(ctx, p.db, rawSQL, parameters)
	if err != nil {
		if ctx.Err() != nil {
			return AnalyticsResult{}, apiError(429, "analytics execution time limit reached")
		}
		return AnalyticsResult{}, duckLakeValidationError(ctx, err)
	}
	start := time.Now()
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return AnalyticsResult{}, duckLakeQueryError(ctx, err)
	}
	defer conn.Close()
	columns, types, err := duckLakeResultColumns(ctx, conn, canonical, args)
	if err != nil {
		return AnalyticsResult{}, duckLakeQueryError(ctx, err)
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
	rows, err := conn.QueryContext(ctx, "SELECT CAST(to_json("+alias+") AS VARCHAR) FROM ("+canonical+") AS "+alias+" LIMIT 1001", args...)
	if err != nil {
		return AnalyticsResult{}, duckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	result := AnalyticsResult{Meta: columns, Data: []map[string]json.RawMessage{}}
	metadata, _ := json.Marshal(result.Meta)
	bytes := len(metadata) + 256
	for rows.Next() {
		if len(result.Data) >= analyticsMaxRows {
			return AnalyticsResult{}, analyticsError(429, "result_row_limit", "analytics result row limit reached", "Reduce the result or use create-event-export for a larger CSV or JSONL export.", "")
		}
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return AnalyticsResult{}, duckLakeQueryError(ctx, err)
		}
		if len(raw) > analyticsMaxBytes-bytes {
			return AnalyticsResult{}, analyticsError(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
		}
		fields, err := exportJSONObject([]byte(raw))
		if err != nil {
			return AnalyticsResult{}, apiError(400, "unsupported analytics result value")
		}
		if len(fields) != len(columns) {
			return AnalyticsResult{}, apiError(503, "invalid DuckLake result")
		}
		row := make(map[string]json.RawMessage, len(columns))
		for i, column := range columns {
			value, ok := fields[column.Name]
			if !ok {
				return AnalyticsResult{}, apiError(503, "invalid DuckLake result")
			}
			row[column.Name], err = duckLakeResultValue(value, types[i], 0)
			if err != nil {
				return AnalyticsResult{}, apiError(400, "unsupported analytics result value")
			}
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return AnalyticsResult{}, apiError(503, "invalid DuckLake result")
		}
		bytes += len(encoded) + 1
		if bytes > analyticsMaxBytes {
			return AnalyticsResult{}, analyticsError(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
		}
		result.Data = append(result.Data, row)
	}
	if err := rows.Err(); err != nil {
		return AnalyticsResult{}, duckLakeQueryError(ctx, err)
	}
	result.Rows = uint64(len(result.Data))
	result.Statistics.Elapsed = time.Since(start).Seconds()
	return result, nil
}

func duckLakeValidationError(ctx context.Context, err error) error {
	var managed *managementError
	if errors.As(err, &managed) {
		return managed
	}
	if ctx.Err() != nil {
		return duckLakeQueryError(ctx, err)
	}
	return analyticsError(400, "unsupported_sql", "Unsupported DuckDB SQL", "Use one SELECT against events. Read get-event-schema for supported sources, expressions and functions.", "")
}

var duckLakeUnknownColumn = regexp.MustCompile(`Referenced column "([A-Za-z_][A-Za-z0-9_]{0,63})" not found`)

func duckLakeQueryError(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apiError(429, "analytics execution time limit reached")
	}
	var managed *managementError
	if errors.As(err, &managed) {
		return managed
	}
	var failure *duckdb.Error
	if errors.As(err, &failure) {
		switch failure.Type {
		case duckdb.ErrorTypeOutOfMemory, duckdb.ErrorTypeInterrupt:
			return apiError(429, "analytics resource limit reached")
		case duckdb.ErrorTypeBinder:
			firstLine := strings.SplitN(failure.Msg, "\n", 2)[0]
			if strings.Contains(firstLine, "Referenced column") && strings.Contains(firstLine, "not found") {
				field := ""
				if match := duckLakeUnknownColumn.FindStringSubmatch(firstLine); match != nil {
					field = match[1]
				}
				return analyticsError(400, "unknown_column", "Unknown column in analytics query", "Read get-event-schema for columns. Custom properties use json_extract_string(properties,'$.key'). Check CTE and alias names as well.", field)
			}
			return analyticsError(400, "type_mismatch", "Invalid argument types or query binding", "Check function argument types, casts, aliases and GROUP BY expressions. Read get-event-schema for column types.", "")
		case duckdb.ErrorTypeConversion, duckdb.ErrorTypeOutOfRange:
			return analyticsError(400, "invalid_value", "Value cannot be converted to the requested type", "Check property values and casts. TRY_CAST returns NULL for values that cannot be converted.", "")
		case duckdb.ErrorTypeDivideByZero:
			return analyticsError(400, "division_by_zero", "Division by zero in analytics query", "Use nullif(denominator,0) before division.", "")
		case duckdb.ErrorTypeParser:
			return analyticsError(400, "invalid_sql", "Invalid DuckDB SQL syntax", "Use one SELECT and DuckDB $name placeholders; inspect the query against get-event-schema.", "")
		case duckdb.ErrorTypeCatalog:
			return analyticsError(400, "unknown_reference", "Unknown analytics reference", "Read get-event-schema for the events table and allowed functions. Check CTE and alias names.", "")
		case duckdb.ErrorTypeInvalidInput, duckdb.ErrorTypeNotImplemented, duckdb.ErrorTypeInvalidType, duckdb.ErrorTypeMismatchType,
			duckdb.ErrorTypeParameterNotAllowed, duckdb.ErrorTypeParameterNotResolved:
			return analyticsError(400, "invalid_query", "Invalid DuckDB analytics query", "Check function arguments, parameter values and casts against get-event-schema.", "")
		}
	}
	return apiError(503, "DuckLake query unavailable")
}

func (s *duckLakeStore) Catalog(ctx context.Context, input EventCatalogInput) (EventCatalog, error) {
	input, err := normalizeCatalogInput(input)
	if err != nil {
		return EventCatalog{}, err
	}
	project, event, limit, offset := input.ID, input.Event, input.Limit, input.Offset
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p, release, err := s.leaseProject(ctx, project)
	if err != nil {
		return EventCatalog{}, apiError(503, "DuckLake storage unavailable")
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
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		return EventCatalog{}, duckLakeQueryError(ctx, err)
	}
	result := EventCatalog{Events: []EventCatalogEvent{}, Properties: []EventCatalogProperty{}, Sampling: EventCatalogSampling{
		PropertiesRequested: event != "", SampleEventLimit: 1000, KeysPerEventLimit: 200, MaxPropertyNameBytes: 200, PropertyLimit: input.PropertyLimit,
	}}
	for rows.Next() {
		var item EventCatalogEvent
		if err := rows.Scan(&item.Event, &item.Count, &item.FirstReceivedAt, &item.LastReceivedAt, &item.LatestEventUUID, &item.LatestEventTimestamp); err != nil {
			rows.Close()
			return EventCatalog{}, duckLakeQueryError(ctx, err)
		}
		result.Events = append(result.Events, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return EventCatalog{}, duckLakeQueryError(ctx, err)
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
	rows, err = p.db.QueryContext(ctx, "SELECT uuid,received_at,properties FROM events WHERE event=? ORDER BY id DESC LIMIT 1000", event)
	if err != nil {
		return EventCatalog{}, duckLakeQueryError(ctx, err)
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
			return EventCatalog{}, duckLakeQueryError(ctx, err)
		}
		var object map[string]json.RawMessage
		if json.Unmarshal([]byte(raw), &object) != nil {
			return EventCatalog{}, apiError(503, "invalid DuckLake property sample")
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
		return EventCatalog{}, duckLakeQueryError(ctx, err)
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
