//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
		return AnalyticsResult{}, apiError(400, "unsupported DuckDB SQL or parameters")
	}
	start := time.Now()
	rows, err := p.db.QueryContext(ctx, "SELECT * FROM ("+canonical+") AS porcupine_query LIMIT 1001", args...)
	if err != nil {
		return AnalyticsResult{}, duckLakeQueryError(ctx, err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return AnalyticsResult{}, apiError(503, "invalid DuckLake result")
	}
	result := AnalyticsResult{Meta: make([]AnalyticsResultColumn, len(types)), Data: []map[string]json.RawMessage{}}
	seen := map[string]bool{}
	for i, column := range types {
		if seen[column.Name()] {
			return AnalyticsResult{}, apiError(400, "analytics result columns must have unique names")
		}
		seen[column.Name()] = true
		result.Meta[i] = AnalyticsResultColumn{Name: column.Name(), Type: column.DatabaseTypeName()}
	}
	metadata, _ := json.Marshal(result.Meta)
	bytes := len(metadata) + 256
	values, pointers := make([]any, len(types)), make([]any, len(types))
	for i := range values {
		pointers[i] = &values[i]
	}
	for rows.Next() {
		if len(result.Data) >= analyticsMaxRows {
			return AnalyticsResult{}, apiError(429, "analytics result row limit reached")
		}
		if err := rows.Scan(pointers...); err != nil {
			return AnalyticsResult{}, duckLakeQueryError(ctx, err)
		}
		row := make(map[string]json.RawMessage, len(values))
		rowBytes := 2
		for i, value := range values {
			if date, ok := value.(time.Time); ok && types[i].DatabaseTypeName() == "DATE" {
				value = date.Format(time.DateOnly)
			}
			encoded, err := duckLakeJSONValue(value, analyticsMaxBytes-bytes-rowBytes)
			if err != nil {
				if errors.Is(err, errDuckLakeResultLimit) {
					return AnalyticsResult{}, apiError(429, "analytics result byte limit reached")
				}
				return AnalyticsResult{}, apiError(400, "unsupported analytics result value")
			}
			row[types[i].Name()] = encoded
			rowBytes += len(encoded) + duckLakeJSONStringSize(types[i].Name()) + 2
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return AnalyticsResult{}, apiError(503, "invalid DuckLake result")
		}
		bytes += len(encoded) + 1
		if bytes > analyticsMaxBytes {
			return AnalyticsResult{}, apiError(429, "analytics result byte limit reached")
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

var errDuckLakeResultLimit = errors.New("DuckLake result exceeds byte limit")

func duckLakeJSONValue(value any, budget int) (json.RawMessage, error) {
	converted, _, err := duckLakeJSONCompatible(value, budget, 0)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(converted)
	if len(encoded) > budget {
		return nil, errDuckLakeResultLimit
	}
	return encoded, err
}

func duckLakeJSONCompatible(value any, budget, depth int) (any, int, error) {
	if budget < 1 || depth > 64 {
		return nil, 0, errDuckLakeResultLimit
	}
	switch v := value.(type) {
	case int64:
		return duckLakeJSONCompatible(strconv.FormatInt(v, 10), budget, depth+1)
	case uint64:
		return duckLakeJSONCompatible(strconv.FormatUint(v, 10), budget, depth+1)
	case *big.Int:
		if v != nil {
			return duckLakeJSONCompatible(v.String(), budget, depth+1)
		}
	case duckdb.Decimal:
		return duckLakeJSONCompatible(v.String(), budget, depth+1)
	case time.Time:
		return duckLakeJSONCompatible(v.UTC().Format(time.RFC3339Nano), budget, depth+1)
	case string:
		// Check before json.Marshal duplicates the native result in Go memory.
		if len(v) > budget {
			return nil, 0, errDuckLakeResultLimit
		}
		size := duckLakeJSONStringSize(v)
		if size > budget {
			return nil, 0, errDuckLakeResultLimit
		}
		return v, size, nil
	case []any:
		if len(v) > budget/2 {
			return nil, 0, errDuckLakeResultLimit
		}
		converted := make([]any, 0, len(v))
		used := 2
		for _, child := range v {
			result, size, err := duckLakeJSONCompatible(child, budget-used, depth+1)
			if err != nil {
				return nil, 0, err
			}
			used += size + 1
			converted = append(converted, result)
		}
		return converted, used, nil
	case map[string]any:
		if len(v) > budget/4 {
			return nil, 0, errDuckLakeResultLimit
		}
		converted := make(map[string]any, len(v))
		used := 2
		for key, child := range v {
			used += duckLakeJSONStringSize(key) + 2
			result, size, err := duckLakeJSONCompatible(child, budget-used, depth+1)
			if err != nil {
				return nil, 0, err
			}
			used += size
			converted[key] = result
		}
		return converted, used, nil
	case nil, bool, int8, int16, int32, uint8, uint16, uint32, float32, float64:
	default:
		return nil, 0, errors.New("unsupported DuckLake result type")
	}
	encoded, err := json.Marshal(value)
	if len(encoded) > budget {
		return nil, 0, errDuckLakeResultLimit
	}
	return value, len(encoded), err
}

func duckLakeJSONStringSize(value string) int {
	size := 2
	for len(value) > 0 {
		r, n := utf8.DecodeRuneInString(value)
		value = value[n:]
		switch r {
		case '"', '\\', '\n', '\r', '\t', '\b', '\f':
			size += 2
		case '<', '>', '&', '\u2028', '\u2029':
			size += 6
		default:
			if r < 0x20 {
				size += 6
			} else if r == utf8.RuneError && n == 1 {
				size += 6
			} else {
				size += n
			}
		}
	}
	return size
}

func duckLakeQueryError(ctx context.Context, err error) error {
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return apiError(429, "analytics execution time limit reached")
	}
	var failure *duckdb.Error
	if errors.As(err, &failure) {
		switch failure.Type {
		case duckdb.ErrorTypeOutOfMemory, duckdb.ErrorTypeInterrupt:
			return apiError(429, "analytics resource limit reached")
		case duckdb.ErrorTypeParser, duckdb.ErrorTypeBinder, duckdb.ErrorTypeCatalog,
			duckdb.ErrorTypeConversion, duckdb.ErrorTypeOutOfRange, duckdb.ErrorTypeInvalidInput,
			duckdb.ErrorTypeNotImplemented, duckdb.ErrorTypeInvalidType, duckdb.ErrorTypeMismatchType,
			duckdb.ErrorTypeParameterNotAllowed, duckdb.ErrorTypeParameterNotResolved,
			duckdb.ErrorTypeDivideByZero:
			return apiError(400, "invalid DuckDB analytics query")
		}
	}
	return apiError(503, "DuckLake query unavailable")
}

func (s *duckLakeStore) Catalog(ctx context.Context, project, event string, limit, offset int) (EventCatalog, error) {
	if limit < 1 || limit > 100 || offset < 0 || offset > 10000 || len(event) > 500 {
		return EventCatalog{}, apiError(400, "invalid event catalog parameters")
	}
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
		PropertiesRequested: event != "", SampleEventLimit: 1000, KeysPerEventLimit: 200, MaxPropertyNameBytes: 200, PropertyLimit: 100,
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
			if len(key) <= 200 {
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
				if len(properties) >= 100 {
					result.Sampling.PropertiesTruncated = true
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
