package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeExportStreamWindowsAndConnectionIsolation(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	start := time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC)
	projections := make([]EventProjection, 0, 1105)
	for i := range 1105 {
		p := lakeStorageProjection(t, "a", fmt.Sprintf("a-%04d", i), int64(i+1))
		if i == 1103 {
			p.Imported = true
		}
		if i == 1104 {
			p.ReceivedAt = start.Add(61 * time.Second).Format(time.RFC3339Nano)
		}
		projections = append(projections, p)
	}
	for i := 0; i < len(projections); i += 100 {
		if err := store.Apply(context.Background(), projections[i:min(i+100, len(projections))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Apply(context.Background(), []EventProjection{lakeStorageProjection(t, "b", "private", 2000)}); err != nil {
		t.Fatal(err)
	}
	raw := "WITH source AS (SELECT uuid,received_at_ms FROM events) SELECT uuid FROM source WHERE received_at_ms >= CAST($window_start AS BIGINT) ORDER BY uuid"
	if _, err := store.ValidateExport(context.Background(), "a", raw, nil, true); err != nil {
		t.Fatal(err)
	}
	columns := 0
	rows := 0
	result, err := store.Export(context.Background(), "a", raw, nil, &AnalyticalWindow{StartMS: start.UnixMilli(), EndMS: start.Add(time.Minute).UnixMilli()}, 1024, ExportStreamCallbacks{
		Columns: func(c []AnalyticsResultColumn) error { columns = len(c); return nil },
		Row: func(row []json.RawMessage) error {
			if string(row[0]) != fmt.Sprintf(`"a-%04d"`, rows) {
				t.Fatalf("wrong window row: %s", row[0])
			}
			if rows == 0 {
				if count := lakeStorageCount(t, store, "a", "events"); count != 1105 {
					t.Fatalf("window leaked to another connection: %d", count)
				}
			}
			rows++
			return nil
		},
	})
	if err != nil || result.Rows != 1103 || rows != 1103 || columns != 1 {
		t.Fatalf("stream count/window: result=%+v rows=%d err=%v", result, rows, err)
	}
	if count := lakeStorageCount(t, store, "a", "events"); count != 1105 {
		t.Fatalf("window survived stream cleanup: %d", count)
	}
	for _, unsafe := range []string{"SELECT * FROM lake.current_events", "SELECT * FROM read_parquet('secret.parquet')", "SELECT * FROM events AT (VERSION => 0)"} {
		if _, err := store.ValidateExport(context.Background(), "a", unsafe, nil, false); err == nil {
			t.Fatalf("export accepted %s", unsafe)
		}
	}
	count := 0
	_, err = store.Export(context.Background(), "b", "SELECT uuid FROM events", nil, nil, 1024, ExportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func(row []json.RawMessage) error {
		count++
		if string(row[0]) != `"private"` {
			t.Fatal("cross-project export")
		}
		return nil
	}})
	if err != nil || count != 1 {
		t.Fatalf("other project export: %d %v", count, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, err = store.Export(ctx, "a", raw, nil, &AnalyticalWindow{StartMS: start.UnixMilli(), EndMS: start.Add(time.Minute).UnixMilli()}, 1024, ExportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func([]json.RawMessage) error { cancel(); return nil }})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("stream cancellation: %v", err)
	}
	if count := lakeStorageCount(t, store, "a", "events"); count != 1105 {
		t.Fatalf("canceled window survived cleanup: %d", count)
	}
	_, err = store.Export(context.Background(), "a", "SELECT '"+strings.Repeat("x", 200)+"' AS wide", nil, nil, 64, ExportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func([]json.RawMessage) error { return nil }})
	if !errors.Is(err, ErrExportValueTooLarge) {
		t.Fatalf("row limit: %v", err)
	}
}

func TestDuckLakeExportNativeValueParity(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	if err := store.Apply(context.Background(), []EventProjection{lakeStorageProjection(t, "typed", "typed-row", 9007199254740993)}); err != nil {
		t.Fatal(err)
	}
	queries := []string{
		"SELECT CAST(9007199254740993 AS BIGINT) AS big, CAST(18446744073709551615 AS UBIGINT) AS unsigned, CAST('12.30' AS DECIMAL(10,2)) AS amount, CAST('2026-11-13 10:01:02.123456' AS TIMESTAMP) AS stamp, CAST('2026-11-13 10:01:02.123456+00' AS TIMESTAMPTZ) AS zoned, CAST('2026-11-13' AS DATE) AS day, CAST('12:32:09' AS TIME) AS clock",
		"SELECT list(id) AS items, first(events) AS object, CAST('{\"nested\":42}' AS JSON) AS json_value, CAST(NULL AS VARCHAR) AS empty FROM events",
		"SELECT $value AS parameter",
	}
	for _, query := range queries {
		var params map[string]string
		if strings.Contains(query, "$value") {
			params = map[string]string{"value": "東京\n<value>"}
		}
		expected, err := store.Query(context.Background(), "typed", query, params, DefaultQueryExecutionLimits())
		if err != nil {
			t.Fatal(query, err)
		}
		var columns []AnalyticsResultColumn
		var actual []json.RawMessage
		result, err := store.Export(context.Background(), "typed", query, params, nil, 16384, ExportStreamCallbacks{Columns: func(c []AnalyticsResultColumn) error { columns = c; return nil }, Row: func(row []json.RawMessage) error { actual = append([]json.RawMessage(nil), row...); return nil }})
		if err != nil || result.Rows != 1 || len(actual) != len(columns) {
			t.Fatalf("typed export: %s %+v %v", query, result, err)
		}
		for i, column := range columns {
			if string(actual[i]) != string(expected.Data[0][column.Name]) {
				t.Fatalf("native type parity %s: got %s want %s", column.Name, actual[i], expected.Data[0][column.Name])
			}
		}
	}
}

func TestDuckLakeExportEmptyAndRowBudgetIgnoresMetadataNames(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	result, err := store.Export(context.Background(), "empty", "SELECT uuid FROM events WHERE 1=0", nil, nil, 1024, ExportStreamCallbacks{Columns: func(c []AnalyticsResultColumn) error {
		if len(c) != 1 || c[0].Name != "uuid" {
			t.Fatal("empty metadata")
		}
		return nil
	}, Row: func([]json.RawMessage) error { t.Fatal("empty result produced row"); return nil }})
	if err != nil || result.Rows != 0 {
		t.Fatalf("empty streaming export: %+v %v", result, err)
	}
	result, err = store.Export(context.Background(), "empty", `SELECT CAST(1 AS INTEGER) AS "`+strings.Repeat("a", 500)+`"`, nil, nil, 3, ExportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error { return nil }, Row: func(row []json.RawMessage) error {
		if len(row) != 1 || string(row[0]) != "1" {
			t.Fatal("row budget result")
		}
		return nil
	}})
	if err != nil || result.Rows != 1 {
		t.Fatalf("metadata name consumed row budget: %+v %v", result, err)
	}
}

func TestDuckLakeExportCancellationAroundWriterStartup(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	if err := store.Apply(context.Background(), []EventProjection{lakeStorageProjection(t, "startup", "one", 1)}); err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		ctx, cancel := context.WithCancel(context.Background())
		result, err := store.Export(ctx, "startup", "SELECT uuid FROM events", nil, nil, 1024, ExportStreamCallbacks{Columns: func([]AnalyticsResultColumn) error {
			go func() { time.Sleep(time.Duration(i%3) * 250 * time.Microsecond); cancel() }()
			return nil
		}, Row: func([]json.RawMessage) error { return nil }})
		cancel()
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("startup cancel %d: %+v %v", i, result, err)
		}
	}
	if count := lakeStorageCount(t, store, "startup", "events"); count != 1 {
		t.Fatal("startup cancellation broke engine")
	}
}
