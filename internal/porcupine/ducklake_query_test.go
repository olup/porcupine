//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeSQLGuardNativeGrammar(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		`SELECT * FROM events`,
		`SELECT event,count(*) AS n FROM events GROUP BY event HAVING n>0 ORDER BY n DESC LIMIT 10 OFFSET 1`,
		`SELECT COUNT(DISTINCT distinct_id) FILTER (WHERE event='page_viewed') AS people FROM events`,
		`SELECT CASE WHEN event='a' THEN 1 ELSE 0 END FROM events`,
		`SELECT properties->>'language', CAST(event_time AS DATE), TIMESTAMPTZ '2026-11-01 00:00:00+00' FROM events`,
		`SELECT event_time + INTERVAL '1 day' FROM events`,
		`SELECT a.uuid FROM events a LEFT JOIN (SELECT uuid FROM events) b ON a.uuid=b.uuid`,
		`SELECT event FROM events UNION ALL SELECT event FROM events`,
		`SELECT event FROM events INTERSECT SELECT event FROM events`,
		`SELECT * FROM events WHERE uuid IN (SELECT uuid FROM events WHERE event='x')`,
		`WITH a AS (SELECT * FROM events), b AS (SELECT * FROM a) SELECT lag(id) OVER(PARTITION BY event ORDER BY id) FROM b`,
		`SELECT row_number() OVER(ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM events`,
		`SELECT event FROM events WHERE id BETWEEN 1 AND 3 AND event LIKE 'p%' AND id IS NOT NULL`,
		`SELECT DISTINCT ON(event) event,id FROM events ORDER BY event,id`,
		`SELECT quantile_cont(CAST(json_extract_string(properties,'$.duration_ms') AS DOUBLE),0.95) FROM events`,
		`/* before */ SELECT '; DELETE FROM events' AS harmless FROM events; -- after` + "\n",
	} {
		t.Run(query, func(t *testing.T) {
			canonical, _, err := validateDuckLakeSQL(context.Background(), db, query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = validateDuckLakeSQL(context.Background(), db, canonical, nil); err != nil {
				t.Fatalf("canonical query rejected: %s: %v", canonical, err)
			}
		})
	}
}

func TestDuckLakeSQLGuardRejectsEscapeRoutes(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, query := range []string{
		``, `SELECT`, `SELECT 1; SELECT 2`, `DELETE FROM events`, `INSERT INTO events VALUES(1)`,
		`SET enable_external_access=true`, `PRAGMA database_list`, `ATTACH 'other.db'`,
		`COPY events TO '/tmp/stolen'`, `INSTALL httpfs`, `LOAD httpfs`,
		`SELECT * FROM lake.current_events`, `SELECT * FROM lake.heads`, `SELECT * FROM sqlite_master`,
		`SELECT * FROM information_schema.tables`, `SELECT * FROM other_project.events`,
		`SELECT * FROM read_parquet('/tmp/private.parquet')`, `SELECT * FROM read_csv('/etc/passwd')`,
		`SELECT * FROM glob('/tmp/*')`, `SELECT * FROM query('SELECT * FROM lake.current_events')`,
		`SELECT * FROM query_table('lake.current_events')`, `SELECT * FROM duckdb_settings()`,
		`SELECT read_blob('/etc/passwd') FROM events`, `SELECT current_setting('allowed_directories') FROM events`,
		`SELECT repeat('x',83886080) FROM events`,
		`SELECT main.count(*) FROM events`,
		`SELECT CAST(id AS main.INTEGER) FROM events`,
		`SELECT * REPLACE(read_blob('/etc/passwd') AS properties) FROM events`,
		`SELECT row_number() OVER w FROM events WINDOW w AS (ORDER BY current_user)`,
		`SELECT current_user FROM events`, `SELECT current_database() FROM events`,
		`SELECT original_snapshot FROM events`, `SELECT e.payload FROM events e`,
		`SELECT sum(current_setting('threads')) FROM events`,
		`SELECT CASE WHEN true THEN 1 ELSE current_setting('threads') END FROM events`,
		`SELECT count(*) FROM events WHERE id IN (SELECT 1 FROM read_csv('/etc/passwd'))`,
		`SELECT * FROM events a JOIN other.events b ON true`,
		`SELECT * FROM events UNION ALL SELECT * FROM lake.current_events`,
		`WITH a AS (SELECT * FROM other.events) SELECT * FROM a`,
		`WITH a AS (SELECT * FROM b), b AS (SELECT * FROM events) SELECT * FROM a`,
		`WITH events AS (SELECT 1) SELECT * FROM events`,
		`WITH RECURSIVE a AS (SELECT * FROM events UNION ALL SELECT * FROM a) SELECT * FROM a`,
		`SELECT * FROM events AT (VERSION => 1)`,
		`SELECT COLUMNS('.*') FROM events`,
		`SELECT count(*) OVER (ORDER BY current_setting('threads')) FROM events`,
		strings.Repeat(" ", maxSQLBytes+1),
	} {
		t.Run(query, func(t *testing.T) {
			if canonical, _, err := validateDuckLakeSQL(context.Background(), db, query, nil); err == nil {
				t.Fatalf("unsafe SQL accepted: %s", canonical)
			}
		})
	}
	guard := duckLakeSQLGuard{parameters: map[string]bool{}}
	if err := guard.walk(map[string]any{"type": "FUTURE_SQL_NODE"}, map[string]bool{"events": true}, 0); err == nil {
		t.Fatal("unknown parser node must fail closed")
	}
}

func TestDuckLakeQueryBindingsResultsAndCatalog(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 2)
	a := lakeStorageProjection(t, "query-a", "one", 1)
	b := lakeStorageProjection(t, "query-a", "two", 2)
	b.Event.Event = "purchase"
	b.Properties = json.RawMessage(`{"language":"fr","amount":25,"nullable":null}`)
	c := lakeStorageProjection(t, "query-b", "foreign", 3)
	c.Event.Event = "private-event"
	if err := s.Apply(context.Background(), []eventProjection{a, b, c}); err != nil {
		t.Fatal(err)
	}
	query := `SELECT event,count(*) AS n FROM events WHERE event=$event GROUP BY event`
	result, err := s.Query(context.Background(), "query-a", query, map[string]string{"event": "purchase"})
	if err != nil || result.Rows != 1 || string(result.Data[0]["n"]) != `"1"` || string(result.Data[0]["event"]) != `"purchase"` {
		t.Fatalf("parameter query: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", query, map[string]string{"event": "purchase' OR true --"})
	if err != nil || result.Rows != 0 {
		t.Fatalf("parameter changed syntax: %+v %v", result, err)
	}
	for _, parameters := range []map[string]string{nil, {"wrong": "purchase"}, {"event": "purchase", "extra": "x"}} {
		if _, err := s.Query(context.Background(), "query-a", query, parameters); err == nil {
			t.Fatal("incorrect parameter names accepted")
		}
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT CAST('18446744073709551615' AS UBIGINT) AS precise, CAST('99999999999999999999999999999999999999' AS HUGEINT) AS huge, CAST('123.45' AS DECIMAL(10,2)) AS amount FROM events LIMIT 1`, nil)
	if err != nil || string(result.Data[0]["precise"]) != `"18446744073709551615"` || string(result.Data[0]["huge"]) != `"99999999999999999999999999999999999999"` || string(result.Data[0]["amount"]) != `"123.45"` {
		t.Fatalf("numeric precision: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT list(id ORDER BY id) AS ids FROM events`, nil)
	if err != nil || string(result.Data[0]["ids"]) != `["1","2"]` {
		t.Fatalf("nested integer precision: %+v %v", result, err)
	}
	result, err = s.Query(context.Background(), "query-a", `SELECT CAST(timezone('Europe/Paris',TIMESTAMPTZ '2026-11-13 23:30:00+00') AS DATE) AS day FROM events LIMIT 1`, nil)
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
	_, err = s.Query(context.Background(), "query-a", "SELECT CAST('sensitive-test-value' AS BIGINT) AS broken FROM events", nil)
	if err == nil || strings.Contains(err.Error(), "sensitive-test-value") {
		t.Fatalf("raw engine error escaped: %v", err)
	}
}

func TestDuckLakeQueryNativeCancellationAndLimits(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 1)
	batch := make([]eventProjection, 100)
	for i := range batch {
		batch[i] = lakeStorageProjection(t, "query-a", fmt.Sprintf("event-%d", i), int64(i+1))
	}
	if err := s.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	start := time.Now()
	_, err := s.Query(ctx, "query-a", `SELECT sum(a.timestamp_ms % (b.timestamp_ms+1) + c.timestamp_ms % (d.timestamp_ms+1)) AS heavy FROM events a CROSS JOIN events b CROSS JOIN events c CROSS JOIN events d`, nil)
	cancel()
	if err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("native query did not stop promptly: %v, %s", err, time.Since(start))
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := s.Query(ctx, "query-a", "SELECT count(*) AS n FROM events", nil)
	if err != nil || string(result.Data[0]["n"]) != `"100"` {
		t.Fatalf("native connection did not recover after interrupt: %+v %v", result, err)
	}
	if _, err := s.Query(context.Background(), "query-a", "SELECT a.id,b.id AS other FROM events a CROSS JOIN events b", nil); err == nil {
		t.Fatal("oversized row result accepted")
	}
	largeSQL := "SELECT concat(" + strings.TrimSuffix(strings.Repeat("$value,", 300), ",") + ") AS too_large FROM events LIMIT 1"
	if _, err := s.Query(context.Background(), "query-a", largeSQL, map[string]string{"value": strings.Repeat("x", 4096)}); err == nil || queryStatus(err) != 429 {
		t.Fatal("oversized byte result accepted")
	}
}

func TestDuckLakeJSONResultPreflightAndNestedIntegers(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 1)
	item := lakeStorageProjection(t, "json", "precise", 9007199254740993)
	item.Properties = json.RawMessage(`{"ids":[9007199254740993,18446744073709551615]}`)
	if err := store.Apply(context.Background(), []eventProjection{item}); err != nil {
		t.Fatal(err)
	}
	result, err := store.Query(context.Background(), "json", "SELECT json_extract(properties,'$') AS document, first(events) OVER() AS row FROM events", nil)
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
		result, err := store.Query(context.Background(), "json", "SELECT $value AS value", map[string]string{"value": value})
		encoded, _ := json.Marshal(value)
		if err != nil || string(result.Data[0]["value"]) != string(encoded) {
			t.Fatalf("string encoding for %q: %+v %v", value, result, err)
		}
	}
}

func TestDuckLakeQuerySerializationAliasesAndNonfiniteValues(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 1)
	result, err := store.Query(context.Background(), "aliases", "SELECT 1 AS _pc_result, 2 AS _PC_RESULT_1, 3 AS porcupine_query", nil)
	if err != nil || len(result.Data) != 1 || len(result.Data[0]) != 3 || string(result.Data[0]["porcupine_query"]) != "3" {
		t.Fatalf("row alias collided with output column: %+v %v", result, err)
	}
	for _, value := range []string{"NaN", "Infinity", "-Infinity"} {
		_, err := store.Query(context.Background(), "aliases", "SELECT CAST('"+value+"' AS DOUBLE) AS unsupported", nil)
		if err == nil || queryStatus(err) != 400 {
			t.Fatalf("non-finite number %s: %v", value, err)
		}
	}
}
