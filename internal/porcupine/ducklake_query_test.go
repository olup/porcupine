package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/nativequery"
)

func TestDuckLakeSQLGuardNativeGrammar(t *testing.T) {
	t.Parallel()
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
			canonical, _, err := nativequery.Prepare(context.Background(), db, query, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, err = nativequery.Prepare(context.Background(), db, canonical, nil); err != nil {
				t.Fatalf("canonical query rejected: %s: %v", canonical, err)
			}
		})
	}
}

func TestDuckLakeSQLGuardRejectsEscapeRoutes(t *testing.T) {
	t.Parallel()
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
		`SELECT * FROM query_table('lake.current_events')`, `SELECT * FROM duckdb_settings()`, `SELECT * FROM duckdb_secrets()`,
		`SELECT getvariable('AWS_SECRET_ACCESS_KEY') FROM events`, `SELECT getenv('AWS_SECRET_ACCESS_KEY') FROM events`,
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
			if canonical, _, err := nativequery.Prepare(context.Background(), db, query, nil); err == nil {
				t.Fatalf("unsafe SQL accepted: %s", canonical)
			}
		})
	}

}

func TestDuckLakeSQLTextRoundTripBoundary(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, raw := range []string{"SELECT \x00", "SELECT '\x00'", "SELECT \xff", "SELECT '\xff'", "SELECT '雪' -- \xff"} {
		_, _, err := nativequery.Prepare(context.Background(), db, raw, nil)
		var failure *nativequery.Error
		if !errors.As(err, &failure) || failure.Code != "invalid_sql" {
			t.Fatalf("invalid SQL text accepted: %q: %v", raw, err)
		}
		if _, err := nativequery.Inspect(context.Background(), db, raw); err == nil {
			t.Fatalf("inspection accepted invalid text: %q", raw)
		}
	}
	for _, raw := range []string{"SELECT '雪 ; DELETE FROM events' AS value", "SELECT 'escaped '' quote' AS value", "SELECT E'\\n' AS value", "SELECT '\ufffd' AS value"} {
		canonical, _, err := nativequery.Prepare(context.Background(), db, raw, nil)
		if err != nil {
			t.Fatalf("valid literal rejected: %q: %v", raw, err)
		}
		again, _, err := nativequery.Prepare(context.Background(), db, canonical, nil)
		if err != nil || again != canonical {
			t.Fatalf("literal roundtrip failed: %q: %v", raw, err)
		}
	}
}
