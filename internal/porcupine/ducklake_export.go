//go:build ducklake

package porcupine

import (
	"bufio"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func duckLakeExportBindings(ctx context.Context, p *projectLake, raw string, parameters map[string]string, window *analyticalWindow) (map[string]string, error) {
	bindings := make(map[string]string, len(parameters)+2)
	for name, value := range parameters {
		bindings[name] = value
	}
	if window == nil {
		return bindings, nil
	}
	_, declared, hasEvents, err := parseDuckLakeSQL(ctx, p.db, raw)
	if err != nil || !hasEvents {
		return nil, errors.New("pipeline requires a validated events source")
	}
	for name, value := range map[string]int64{"window_start": window.StartMS, "window_end": window.EndMS} {
		if _, exists := parameters[name]; exists {
			return nil, errors.New("reserved pipeline parameter")
		}
		if declared[name] {
			bindings[name] = strconv.FormatInt(value, 10)
		}
	}
	return bindings, nil
}

func (s *duckLakeStore) ValidateExport(ctx context.Context, project, raw string, parameters map[string]string, pipeline bool) (string, error) {
	p, release, err := s.leaseProject(ctx, project)
	if err != nil {
		return "", apiError(503, "DuckLake storage unavailable")
	}
	defer release()
	var window *analyticalWindow
	if pipeline {
		window = &analyticalWindow{}
	}
	bindings, err := duckLakeExportBindings(ctx, p, raw, parameters, window)
	if err != nil {
		return "", apiError(400, "unsupported DuckDB SQL or parameters")
	}
	canonical, _, err := validateDuckLakeSQL(ctx, p.db, raw, bindings)
	if err != nil {
		return "", apiError(400, "unsupported DuckDB SQL or parameters")
	}
	return canonical, nil
}

// Export keeps a project lease and one native connection for the entire stream.
// It uses the same guarded dialect as interactive queries without their row cap.
func (s *duckLakeStore) Export(ctx context.Context, project, raw string, parameters map[string]string, window *analyticalWindow, maxRowBytes int64, callbacks exportStreamCallbacks) (exportStreamResult, error) {
	var result exportStreamResult
	p, release, err := s.leaseProject(ctx, project)
	if err != nil {
		return result, err
	}
	defer release()
	bindings, err := duckLakeExportBindings(ctx, p, raw, parameters, window)
	if err != nil {
		return result, err
	}
	canonical, args, err := validateDuckLakeSQL(ctx, p.db, raw, bindings)
	if err != nil {
		return result, errExportStreamInvalid
	}
	conn, err := p.db.Conn(ctx)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	if window != nil {
		if window.StartMS < 0 || window.EndMS <= window.StartMS {
			return result, errExportStreamInvalid
		}
		var catalog string
		if err = conn.QueryRowContext(ctx, "SELECT current_database()").Scan(&catalog); err != nil {
			return result, err
		}
		source := "\"" + strings.ReplaceAll(catalog, "\"", "\"\"") + "\".main.events"
		statement := fmt.Sprintf("CREATE TEMP VIEW events AS SELECT * FROM %s WHERE received_at_ms >= %d AND received_at_ms < %d AND is_imported=0", source, window.StartMS, window.EndMS)
		if _, err = conn.ExecContext(ctx, statement); err != nil {
			return result, err
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := conn.ExecContext(cleanup, "DROP VIEW temp.main.events"); err != nil {
				// A window must never leak to a later query on this pooled connection.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}()
	}
	columns, types, err := duckLakeResultColumns(ctx, conn, canonical, args)
	if err != nil {
		return result, err
	}
	result.Columns = columns
	metadata, _ := json.Marshal(result.Columns)
	if len(metadata) > exportMetadataTotalBytes {
		return result, errExportMetadataTooLarge
	}
	if err = callbacks.Columns(result.Columns); err != nil {
		return result, err
	}
	intermediateBytes := maxRowBytes
	for _, column := range columns {
		name, _ := json.Marshal(column.Name)
		intermediateBytes += int64(len(name) + 1)
	}
	err = copyDuckLakeExport(ctx, conn, p.directory, canonical, args, func(reader io.Reader) error {
		input := exportJSONInput{ctx: ctx, reader: bufio.NewReaderSize(reader, 64<<10), limit: intermediateBytes}
		for {
			if _, err := input.peek(); errors.Is(err, io.EOF) {
				return nil
			} else if err != nil {
				return err
			}
			raw, err := input.value(intermediateBytes)
			if err != nil {
				return err
			}
			fields, err := exportJSONObject(raw)
			if err != nil || len(fields) != len(columns) {
				return errExportStreamInvalid
			}
			row := make([]json.RawMessage, len(columns))
			used := 2
			for i, column := range columns {
				value, ok := fields[column.Name]
				if !ok {
					return errExportStreamInvalid
				}
				row[i], err = duckLakeResultValue(value, types[i], 0)
				if err != nil {
					return err
				}
				used += len(row[i])
				if i > 0 {
					used++
				}
			}
			if int64(used) > maxRowBytes {
				return errExportValueTooLarge
			}
			if err = callbacks.Row(row); err != nil {
				return err
			}
			result.Rows++
		}
	})
	return result, err
}

func duckLakeResultColumns(ctx context.Context, conn *sql.Conn, canonical string, args []any) ([]AnalyticsResultColumn, []duckdb.TypeInfo, error) {
	var columns []AnalyticsResultColumn
	var types []duckdb.TypeInfo
	err := conn.Raw(func(raw any) error {
		native, ok := raw.(*duckdb.Conn)
		if !ok {
			return errExportStreamInvalid
		}
		prepared, err := native.PrepareContext(ctx, "SELECT * FROM ("+canonical+") AS _pc_export_schema LIMIT 0")
		if err != nil {
			return err
		}
		defer prepared.Close()
		statement := prepared.(*duckdb.Stmt)
		named := make([]driver.NamedValue, len(args))
		for i, arg := range args {
			value := arg.(sql.NamedArg)
			named[i] = driver.NamedValue{Name: value.Name, Ordinal: i + 1, Value: value.Value}
		}
		rows, err := statement.QueryContext(ctx, named)
		if err != nil {
			return err
		}
		defer rows.Close()
		seen := map[string]bool{}
		for i, name := range rows.Columns() {
			if seen[name] {
				return errExportStreamInvalid
			}
			seen[name] = true
			typeName := rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(i)
			info, err := statement.ColumnTypeInfo(i)
			// A bare bound parameter has a VARCHAR result but the prepared
			// statement can retain INVALID as its pre-binding logical type.
			if err != nil && typeName == "VARCHAR" {
				info, err = duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
			}
			if err != nil {
				return err
			}
			types = append(types, info)
			columns = append(columns, AnalyticsResultColumn{Name: name, Type: typeName})
		}
		return nil
	})
	return columns, types, err
}

func duckLakeResultValue(raw json.RawMessage, info duckdb.TypeInfo, depth int) (json.RawMessage, error) {
	if depth > 64 {
		return nil, errExportValueTooLarge
	}
	if string(raw) == "null" {
		return raw, nil
	}
	if info.Alias() == "JSON" {
		return raw, nil
	}
	switch info.InternalType() {
	case duckdb.TYPE_BIGINT, duckdb.TYPE_UBIGINT, duckdb.TYPE_HUGEINT, duckdb.TYPE_UHUGEINT, duckdb.TYPE_DECIMAL, duckdb.TYPE_BIGNUM:
		var number json.Number
		if json.Unmarshal(raw, &number) != nil {
			return nil, errExportStreamInvalid
		}
		text := string(number)
		if decimal, ok := info.Details().(*duckdb.DecimalDetails); ok {
			rational, ok := new(big.Rat).SetString(text)
			if !ok {
				return nil, errExportStreamInvalid
			}
			scale := int(decimal.Scale)
			rational.Mul(rational, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)))
			if !rational.IsInt() {
				return nil, errExportStreamInvalid
			}
			text = (duckdb.Decimal{Width: decimal.Width, Scale: decimal.Scale, Value: rational.Num()}).String()
		}
		return json.Marshal(text)
	case duckdb.TYPE_TIMESTAMP, duckdb.TYPE_TIMESTAMP_S, duckdb.TYPE_TIMESTAMP_MS, duckdb.TYPE_TIMESTAMP_NS, duckdb.TYPE_TIMESTAMP_TZ, duckdb.TYPE_TIME, duckdb.TYPE_TIME_TZ:
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return nil, errExportStreamInvalid
		}
		text = strings.Replace(text, " ", "T", 1)
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999Z07", "2006-01-02T15:04:05.999999999", "15:04:05.999999999Z07:00", "15:04:05.999999999Z07", "15:04:05.999999999"} {
			if value, err := time.Parse(layout, text); err == nil {
				if info.InternalType() == duckdb.TYPE_TIME || info.InternalType() == duckdb.TYPE_TIME_TZ {
					value = time.Date(1, 1, 1, value.Hour(), value.Minute(), value.Second(), value.Nanosecond(), value.Location())
				}
				return json.Marshal(value.UTC().Format(time.RFC3339Nano))
			}
		}
		return nil, errExportStreamInvalid
	case duckdb.TYPE_LIST, duckdb.TYPE_ARRAY:
		var child duckdb.TypeInfo
		switch details := info.Details().(type) {
		case *duckdb.ListDetails:
			child = details.Child
		case *duckdb.ArrayDetails:
			child = details.Child
		}
		var values []json.RawMessage
		if child == nil || json.Unmarshal(raw, &values) != nil {
			return nil, errExportStreamInvalid
		}
		for i, value := range values {
			converted, err := duckLakeResultValue(value, child, depth+1)
			if err != nil {
				return nil, err
			}
			values[i] = converted
		}
		return json.Marshal(values)
	case duckdb.TYPE_STRUCT:
		details, ok := info.Details().(*duckdb.StructDetails)
		var values map[string]json.RawMessage
		if !ok || json.Unmarshal(raw, &values) != nil || len(values) != len(details.Entries) {
			return nil, errExportStreamInvalid
		}
		for _, entry := range details.Entries {
			converted, err := duckLakeResultValue(values[entry.Name()], entry.Info(), depth+1)
			if err != nil {
				return nil, err
			}
			values[entry.Name()] = converted
		}
		return json.Marshal(values)
	case duckdb.TYPE_VARCHAR, duckdb.TYPE_ENUM, duckdb.TYPE_DATE:
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return nil, errExportStreamInvalid
		}
		return json.Marshal(text)
	case duckdb.TYPE_SQLNULL, duckdb.TYPE_BOOLEAN, duckdb.TYPE_TINYINT, duckdb.TYPE_SMALLINT, duckdb.TYPE_INTEGER, duckdb.TYPE_UTINYINT, duckdb.TYPE_USMALLINT, duckdb.TYPE_UINTEGER, duckdb.TYPE_FLOAT, duckdb.TYPE_DOUBLE:
		return raw, nil
	default:
		return nil, errExportStreamInvalid
	}
}
