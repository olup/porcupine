package nativequery

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
)

func TestPrepareRejectsInvalidInputBeforeParsing(t *testing.T) {
	for _, tc := range []struct {
		name       string
		sql        string
		parameters map[string]string
		code       string
	}{
		{"empty", "", nil, "invalid_sql"},
		{"oversized", strings.Repeat("x", MaxSQLBytes+1), nil, "invalid_sql"},
		{"invalid key", "SELECT $event", map[string]string{"unsafe-key": "secret"}, "invalid_parameter"},
		{"oversized value", "SELECT $event", map[string]string{"event": strings.Repeat("secret", 1000)}, "invalid_parameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := Prepare(context.Background(), nil, tc.sql, tc.parameters)
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != tc.code || failure.Hint == "" || failure.Field != "" {
				t.Fatalf("unexpected policy error: %#v", err)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "unsafe-key") {
				t.Fatal("policy error exposed caller input")
			}
		})
	}
}

func TestGuardFailsClosed(t *testing.T) {
	for _, node := range []map[string]any{
		{"type": "FUTURE_SQL_NODE"},
		{"type": "BASE_TABLE", "table_name": "private"},
		{"type": "BASE_TABLE", "table_name": "events", "catalog_name": "other_project"},
		{"class": "COLUMN_REF", "column_names": []any{"e", "payload"}},
		{"class": "FUNCTION", "function_name": "count", "schema": "main"},
		{"class": "FUNCTION", "function_name": "count", "child": map[string]any{"class": "FUNCTION", "function_name": "read_blob"}},
		{"class": "PARAMETER", "identifier": "1"},
		{"class": "TYPE", "type_name": "INTEGER", "catalog": "private"},
	} {
		guard := sqlGuard{parameters: map[string]bool{}}
		err := guard.walk(node, map[string]bool{"events": true}, 0)
		var failure *Error
		if !errors.As(err, &failure) {
			t.Fatalf("node accepted or untyped rejection: %#v: %v", node, err)
		}
	}
}

func TestGuardFunctionErrorContract(t *testing.T) {
	guard := sqlGuard{parameters: map[string]bool{}}
	err := guard.walk(map[string]any{"class": "FUNCTION", "function_name": "read_blob"}, map[string]bool{"events": true}, 0)
	expected := &Error{
		Code:    "unsupported_function",
		Message: "Function is not allowed in analytics queries",
		Hint:    "Read get-event-schema for allowed DuckDB functions.",
		Field:   "read_blob",
	}
	if !reflect.DeepEqual(err, expected) {
		t.Fatalf("policy error: %#v", err)
	}
}

func TestFunctionsInventoryIsDefensiveAndMatchesGuard(t *testing.T) {
	names := Functions()
	if !sort.StringsAreSorted(names) || len(names) < 100 {
		t.Fatalf("incomplete inventory: %d", len(names))
	}
	for _, name := range names {
		guard := sqlGuard{parameters: map[string]bool{}}
		if err := guard.walk(map[string]any{"class": "FUNCTION", "function_name": name}, nil, 0); err != nil {
			t.Fatalf("advertised function %q rejected: %v", name, err)
		}
	}
	names[0] = "read_blob"
	if Functions()[0] == "read_blob" || functionSet["read_blob"] {
		t.Fatal("caller modified policy inventory")
	}
}

func TestInspectionZeroValueCannotClaimEventsOrParameters(t *testing.T) {
	var statement Statement
	if statement.UsesEvents() || statement.HasParameter("window_start") {
		t.Fatal("zero statement claims checked input")
	}
}

func TestInvalidSQLTextRejectedBeforeEngineAccess(t *testing.T) {
	for _, raw := range []string{"SELECT \x00", "SELECT '\x00'", "SELECT \xff", "SELECT '\xff'", "-- \x00\nSELECT 1", "SELECT '雪' -- \xff"} {
		t.Run(raw, func(t *testing.T) {
			_, _, prepared := Prepare(context.Background(), nil, raw, nil)
			_, inspected := Inspect(context.Background(), nil, raw)
			for _, err := range []error{prepared, inspected} {
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != "invalid_sql" || failure.Message != "Invalid analytics SQL text" || failure.Hint != "Send UTF-8 SQL without NUL bytes." || failure.Field != "" {
					t.Fatalf("invalid text reached engine or returned wrong error: %#v", err)
				}
			}
		})
	}
	for _, raw := range []string{"SELECT '雪 ; DELETE FROM events'", "SELECT 'escaped '' quote'", "SELECT E'\\n'", "SELECT '\ufffd'"} {
		if err := validateSQLText(raw); err != nil {
			t.Fatalf("valid SQL text rejected: %v", err)
		}
	}
}
