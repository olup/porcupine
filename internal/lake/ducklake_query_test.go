package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/apierr"
)

func TestDuckLakeQueryBindingsResultsAndCatalog(t *testing.T) {
	t.Parallel()
	s := openLakeStorageTest(t, t.TempDir(), 2)
	a := lakeStorageProjection(t, "query-a", "one", 1)
	b := lakeStorageProjection(t, "query-a", "two", 2)
	b.Event.Event = "purchase"
	b.Properties = json.RawMessage(`{"language":"fr","amount":25,"nullable":null}`)
	c := lakeStorageProjection(t, "query-b", "foreign", 3)
	c.Event.Event = "private-event"
	if err := s.Apply(context.Background(), []EventProjection{a, b, c}); err != nil {
		t.Fatal(err)
	}
	query := `SELECT event,count(*) AS n FROM events WHERE event=$event GROUP BY event`
	result, err := s.Query(context.Background(), "query-a", query, map[string]string{"event": "purchase"}, DefaultQueryExecutionLimits())
	if err != nil || result.Rows != 1 || string(result.Data[0]["n"]) != `"1"` || string(result.Data[0]["event"]) != `"purchase"` {
		t.Fatalf("parameter query: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", query, map[string]string{"event": "purchase' OR true --"}, DefaultQueryExecutionLimits())
	if err != nil || result.Rows != 0 {
		t.Fatalf("parameter changed syntax: %+v %v", result, err)
	}
	for _, parameters := range []map[string]string{nil, {"wrong": "purchase"}, {"event": "purchase", "extra": "x"}} {
		if _, err := s.Query(context.Background(), "query-a", query, parameters, DefaultQueryExecutionLimits()); err == nil {
			t.Fatal("incorrect parameter names accepted")
		}
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT CAST('18446744073709551615' AS UBIGINT) AS precise, CAST('99999999999999999999999999999999999999' AS HUGEINT) AS huge, CAST('123.45' AS DECIMAL(10,2)) AS amount FROM events LIMIT 1`, nil, DefaultQueryExecutionLimits())
	if err != nil || string(result.Data[0]["precise"]) != `"18446744073709551615"` || string(result.Data[0]["huge"]) != `"99999999999999999999999999999999999999"` || string(result.Data[0]["amount"]) != `"123.45"` {
		t.Fatalf("numeric precision: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT list(id ORDER BY id) AS ids FROM events`, nil, DefaultQueryExecutionLimits())
	if err != nil || string(result.Data[0]["ids"]) != `["1","2"]` {
		t.Fatalf("nested integer precision: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT CAST(timezone('Europe/Paris',TIMESTAMPTZ '2026-11-13 23:30:00+00') AS DATE) AS day FROM events LIMIT 1`, nil, DefaultQueryExecutionLimits())
	if err != nil || string(result.Data[0]["day"]) != `"2026-11-14"` {
		t.Fatalf("local timezone day boundary: %+v %v", result, err)
	}
	catalog, err := s.Catalog(context.Background(), EventCatalogInput{ID: "query-a", Event: "purchase", Limit: 20})
	if err != nil || len(catalog.Events) != 1 || catalog.Events[0].Count != "1" || catalog.Events[0].LatestEventUUID != "two" || len(catalog.Properties) != 3 {
		t.Fatalf("catalog: %+v %v", catalog, err)
	}
	encoded, _ := json.Marshal(catalog)
	for _, forbidden := range []string{"private-event", "foreign", `"fr"`, `"25"`} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("catalog leaked property values or another project: %s", forbidden)
		}
	}
	_, err = s.Query(context.Background(), "query-a", "SELECT CAST('sensitive-test-value' AS BIGINT) AS broken FROM events", nil, DefaultQueryExecutionLimits())
	if err == nil || strings.Contains(err.Error(), "sensitive-test-value") {
		t.Fatalf("raw engine error escaped: %v", err)
	}
}

func TestDuckLakeQueryNativeCancellationAndLimits(t *testing.T) {
	t.Parallel()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	batch := make([]EventProjection, 100)
	for i := range batch {
		batch[i] = lakeStorageProjection(t, "query-a", fmt.Sprintf("event-%d", i), int64(i+1))
	}
	if err := s.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	start := time.Now()
	_, err := s.Query(ctx, "query-a", `SELECT sum(a.timestamp_ms % (b.timestamp_ms+1) + c.timestamp_ms % (d.timestamp_ms+1)) AS heavy FROM events a CROSS JOIN events b CROSS JOIN events c CROSS JOIN events d`, nil, DefaultQueryExecutionLimits())
	cancel()
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("native query did not stop promptly: %v, %s", err, time.Since(start))
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := s.Query(ctx, "query-a", "SELECT count(*) AS n FROM events", nil, DefaultQueryExecutionLimits())
	if err != nil || string(result.Data[0]["n"]) != `"100"` {
		t.Fatalf("native connection did not recover after interrupt: %+v %v", result, err)
	}
	if _, err := s.Query(context.Background(), "query-a", "SELECT a.id,b.id AS other FROM events a CROSS JOIN events b", nil, DefaultQueryExecutionLimits()); err == nil {
		t.Fatal("oversized row result accepted")
	}
	largeSQL := "SELECT concat(" + strings.TrimSuffix(strings.Repeat("$value,", 300), ",") + ") AS too_large FROM events LIMIT 1"
	if _, err := s.Query(context.Background(), "query-a", largeSQL, map[string]string{"value": strings.Repeat("x", 4096)}, DefaultQueryExecutionLimits()); err == nil || queryStatus(err) != 429 {
		t.Fatal("oversized byte result accepted")
	}
}

func TestDuckLakeJSONResultPreflightAndNestedIntegers(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	item := lakeStorageProjection(t, "json", "precise", 9007199254740993)
	item.Properties = json.RawMessage(`{"ids":[9007199254740993,18446744073709551615]}`)
	if err := store.Apply(context.Background(), []EventProjection{item}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(context.Background(), "json", "SELECT json_extract(properties,'$') AS document, first(events) OVER() AS row FROM events", nil, DefaultQueryExecutionLimits())
	if err != nil || len(result.Data) != 1 {
		t.Fatalf("raw JSON query: %+v %v", result, err)
	}
	if string(result.Data[0]["document"]) != `{"ids":[9007199254740993,18446744073709551615]}` {
		t.Fatalf("JSON precision lost: %s", result.Data[0]["document"])
	}
	var row map[string]json.RawMessage
	if json.Unmarshal(result.Data[0]["row"], &row) != nil || string(row["id"]) != `"9007199254740993"` {
		t.Fatalf("typed nested integer precision: %s", result.Data[0]["row"])
	}
	for _, value := range []string{"plain", "<>&", "a\x00b", "\n\t\"\\", "日本語", "\u2028\u2029"} {
		result, err := store.Query(context.Background(), "json", "SELECT $value AS value", map[string]string{"value": value}, DefaultQueryExecutionLimits())
		encoded, _ := json.Marshal(value)
		if err != nil || string(result.Data[0]["value"]) != string(encoded) {
			t.Fatalf("string encoding for %q: %+v %v", value, result, err)
		}
	}
}

func TestDuckLakeQuerySerializationAliasesAndNonfiniteValues(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	result, err := store.Query(context.Background(), "aliases", "SELECT 1 AS _pc_result, 2 AS _PC_RESULT_1, 3 AS porcupine_query", nil, DefaultQueryExecutionLimits())
	if err != nil || len(result.Data) != 1 || len(result.Data[0]) != 3 || string(result.Data[0]["porcupine_query"]) != "3" {
		t.Fatalf("row alias collided with output column: %+v %v", result, err)
	}
	for _, value := range []string{"NaN", "Infinity", "-Infinity"} {
		_, err := store.Query(context.Background(), "aliases", "SELECT CAST('"+value+"' AS DOUBLE) AS unsupported", nil, DefaultQueryExecutionLimits())
		if err == nil || queryStatus(err) != 400 {
			t.Fatalf("non-finite number %s: %v", value, err)
		}
	}
}

func TestDuckLakeQueryAndExportSharePolicyErrors(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	if err := store.Apply(context.Background(), []EventProjection{lakeStorageProjection(t, "policy", "one", 1)}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		sql         string
		params      map[string]string
		code, field string
	}{
		{"SELECT read_blob('/private/secret') FROM events", nil, "unsupported_function", "read_blob"},
		{"SELECT event FROM events WHERE event=$event", nil, "missing_parameter", "event"},
		{"SELECT event FROM events", map[string]string{"extra": "private parameter value"}, "unused_parameter", "extra"},
		{"SELECT * FROM other_project.events", nil, "unsupported_sql", ""},
		{"SELECT FROM", nil, "invalid_sql", ""},
		{"SELECT \x00", nil, "invalid_sql", ""},
		{"SELECT \xff", nil, "invalid_sql", ""},
	} {
		t.Run(tc.code, func(t *testing.T) {
			_, queryErr := store.Query(context.Background(), "policy", tc.sql, tc.params, DefaultQueryExecutionLimits())
			_, exportErr := store.ValidateExport(context.Background(), "policy", tc.sql, tc.params, false)
			var queryFailure, exportFailure *apierr.Error
			if !errors.As(queryErr, &queryFailure) || !errors.As(exportErr, &exportFailure) {
				t.Fatalf("missing transport policy error: query=%v export=%v", queryErr, exportErr)
			}
			if queryFailure.GetStatus() != 400 || exportFailure.GetStatus() != 400 || queryFailure.Code != tc.code || queryFailure.Field != tc.field || queryFailure.Hint == "" || *queryFailure != *exportFailure {
				t.Fatalf("query/export policy drift: query=%+v export=%+v", queryFailure, exportFailure)
			}
			encoded, _ := json.Marshal(exportFailure)
			for _, secret := range []string{"/private/secret", "private parameter value", "other_project"} {
				if strings.Contains(string(encoded), secret) {
					t.Fatal("policy error exposed source or value")
				}
			}
		})
	}
}
