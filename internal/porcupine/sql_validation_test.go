package porcupine

import (
	"strings"
	"testing"

	chsql "github.com/AfterShip/clickhouse-sql-parser/parser"
)

func TestSQLValidationAnalytics(t *testing.T) {
	for _, raw := range []string{
		`SELECT * FROM events`,
		`SELECT id, project_id, event_time FROM events`,
		`SELECT windowFunnel(1800)(event_time, event='started', event='finished') FROM events`,
		`SELECT retention(event='started', event='finished') FROM events`,
		`SELECT event FROM events WHERE event = {event_name:String} AND timestamp_ms >= {start:Int64}`,
		`SELECT count() FROM events WHERE event_time >= {since:DateTime64(3, 'UTC')}`,
		`SELECT count() FROM events WHERE event IN ({first:String}, {second:String})`,
		`SELECT toStartOfDay(timestamp) AS day, JSONExtractString(properties, 'plan') AS plan, count() AS events, uniqExact(distinct_id) AS users FROM events WHERE event = 'purchased' GROUP BY day, plan ORDER BY day DESC LIMIT 100`,
		`SELECT countIf(event = 'purchased') / nullIf(countIf(event = 'visited'), 0) AS conversion FROM events`,
		`SELECT quantileExactIf(0.95)(JSONExtractFloat(properties, 'duration'), event = 'finished') FROM events`,
		`SELECT a.event, count() FROM events AS a LEFT JOIN events AS b ON a.distinct_id = b.distinct_id GROUP BY a.event`,
		`WITH daily AS (SELECT toDate(timestamp) AS day, count() AS n FROM events GROUP BY day) SELECT day, n FROM daily ORDER BY day`,
		`WITH first AS (SELECT distinct_id FROM events WHERE event = 'a'), second AS (SELECT distinct_id FROM first) SELECT count() FROM second`,
		`WITH toDate('2025-01-01') AS start SELECT count() FROM events WHERE timestamp >= start`,
		`SELECT event FROM events UNION ALL SELECT event FROM events`,
		`SELECT event FROM events UNION DISTINCT SELECT event FROM events`,
		`SELECT count() FROM (SELECT distinct_id FROM events GROUP BY distinct_id) AS people`,
		`SELECT count() FROM events WHERE distinct_id IN (SELECT distinct_id FROM events WHERE event = 'a')`,
		`SELECT event FROM events WHERE event IN ('a', 'b')`,
		`SELECT count() FROM events WHERE timestamp >= now() - INTERVAL 7 DAY`,
		`SELECT CASE WHEN event = 'a' THEN 1 ELSE 0 END FROM events`,
		`SELECT count() FROM events WHERE timestamp BETWEEN toDate('2025-01-01') AND toDate('2025-02-01')`,
		"/* before */ SELECT count() FROM events; -- after\n",
		`SELECT 'FORMAT JSON; DROP TABLE events' AS harmless FROM events`,
	} {
		t.Run(raw, func(t *testing.T) {
			canonical, err := validateSQL(raw)
			if err != nil {
				t.Fatal(err)
			}
			if canonical == "" {
				t.Fatal("empty canonical SQL")
			}
			again, err := validateSQL(canonical)
			if err != nil || again != canonical {
				t.Fatalf("canonical SQL not stable: %q, %v", canonical, err)
			}
		})
	}
}

func TestSQLValidationRejectsUnsafeQueries(t *testing.T) {
	for _, raw := range []string{
		``, `;`, `SELECT `, `SELECT 1; SELECT 2`, `SELECT * FROM events; DROP TABLE events`,
		`INSERT INTO events SELECT * FROM events`, `DELETE FROM events WHERE 1`,
		`CREATE TABLE stolen AS SELECT * FROM events`, `ALTER TABLE events DELETE WHERE 1`,
		`EXPLAIN SELECT * FROM events`, `SHOW TABLES`, `DESCRIBE events`, `SET readonly = 0`,
		`SELECT * FROM other`, `SELECT * FROM analytics.events`, `SELECT * FROM other_project.events`,
		"SELECT * FROM `analytics.events`", `SELECT * FROM system.tables`, `SELECT * FROM information_schema.tables`,
		`SELECT * FROM events WHERE distinct_id IN system.users`,
		`SELECT * FROM events WHERE distinct_id IN events`,
		`SELECT * FROM events WHERE distinct_id IN (events)`,
		`SELECT * FROM events WHERE distinct_id IN (system.users)`,
		`SELECT * FROM url('https://evil.invalid', 'JSONEachRow')`,
		`SELECT * FROM remote('evil.invalid', 'db', 'events')`, `SELECT * FROM file('/etc/passwd')`,
		`SELECT * FROM numbers(10)`, `SELECT * FROM merge('db', '.*')`, `SELECT * FROM events()`,
		`SELECT dictGet('secret', 'value', 1) FROM events`, `SELECT addressToLine(1) FROM events`,
		`SELECT hostName() FROM events`, `SELECT currentUser() FROM events`,
		`SELECT currentUser FROM events`, `SELECT CURRENT_USER FROM events`, `SELECT USER FROM events`,
		`SELECT SESSION_USER FROM events`, `SELECT CURRENT_DATABASE FROM events`, `SELECT DATABASE FROM events`,
		`SELECT current_user AS current_user FROM events`, `SELECT hostName AS hostName FROM events`,
		`SELECT version() FROM events`, `SELECT sleep(1) FROM events`, `SELECT myUDF(event) FROM events`,
		`SELECT * FROM {table:Identifier}`,
		`SELECT {table:Identifier} FROM events`,
		`SELECT {value:Array(String)} FROM events`,
		`SELECT {value:AggregateFunction(sum, UInt64)} FROM events`,
		`SELECT {value:DateTime64(99)} FROM events`,
		`SELECT {value:DateTime64(hostName())} FROM events`,
		`SELECT count() FROM events FORMAT JSON`, `SELECT count() FROM events FORMAT JSON FORMAT CSV`,
		`SELECT count() FROM events SETTINGS readonly = 0`, `SELECT count() FROM events INTO OUTFILE '/tmp/leak'`,
		`SELECT count() FROM events UNION ALL SELECT count() FROM events SETTINGS max_memory_usage = 0`,
		`SELECT count() FROM (SELECT * FROM events FORMAT JSON)`,
		`SELECT (SELECT count() FROM system.tables) FROM events`,
		`SELECT count() FROM events WHERE distinct_id IN (SELECT name FROM system.users)`,
		`WITH stolen AS (SELECT * FROM other_project.events) SELECT * FROM stolen`,
		`WITH stolen AS (SELECT hostName()) SELECT * FROM stolen`,
		`WITH recursive AS (SELECT * FROM recursive) SELECT * FROM recursive`,
		`WITH events AS (SELECT 1) SELECT * FROM events`,
		`SELECT * FROM events e JOIN system.tables s ON 1`,
		`SELECT * FROM events e JOIN url('https://evil.invalid', 'JSONEachRow') s ON 1`,
		`SELECT count() FROM events UNION ALL SELECT count() FROM system.tables`,
		`SELECT count() FROM events WHERE 1 /* comment */; /* disguise */ DROP TABLE events`,
		`SELECT count() FROM events /* hide */ FORMAT /* hide */ JSON`,
		`SELECT count() FROM events WHERE 1 GLOBAL IN (1, 2)`,
		`SELECT groupArray(hostName()) FROM events`,
		`SELECT quantileExactIf(hostName())(1, 1) FROM events`,
		`SELECT arraySort(x -> hostName(), [1]) FROM events`,
		`SELECT row_number() OVER (ORDER BY timestamp) FROM events`,
		strings.Repeat(" ", maxSQLBytes+1),
	} {
		t.Run(raw, func(t *testing.T) {
			if canonical, err := validateSQL(raw); err == nil {
				t.Fatalf("unsafe SQL accepted as %q", canonical)
			}
		})
	}
}

func TestSQLValidationVisitsEverySupportedClause(t *testing.T) {
	for _, raw := range []string{
		`SELECT count() FROM events WHERE hostName() = ''`,
		`SELECT count() FROM events PREWHERE hostName() = ''`,
		`SELECT count() FROM events GROUP BY hostName()`,
		`SELECT count() FROM events HAVING hostName() = ''`,
		`SELECT count() FROM events ORDER BY hostName()`,
		`SELECT count() FROM events LIMIT hostName()`,
		`SELECT count() FROM events LIMIT 1 OFFSET hostName()`,
		`SELECT count() FROM events LIMIT 1 BY hostName()`,
		`SELECT count() FROM events a JOIN events b ON hostName() = ''`,
		`WITH hostName() AS hostname SELECT count() FROM events`,
		`SELECT CASE WHEN 1 THEN 1 ELSE hostName() END FROM events`,
		`SELECT [hostName()][1] FROM events`,
		`SELECT count() FROM events EXCEPT SELECT hostName() FROM events`,
		`SELECT count() FROM events INTERSECT SELECT hostName() FROM events`,
	} {
		t.Run(raw, func(t *testing.T) {
			// Ensure these checks exercise traversal rather than only parser rejection.
			if _, err := chsql.NewParser(raw).ParseStmts(); err != nil {
				t.Fatalf("invalid traversal test: %v", err)
			}
			if canonical, err := validateSQL(raw); err == nil {
				t.Fatalf("nested function accepted as %q", canonical)
			}
		})
	}
}

func TestSQLValidationCanonicalizesInput(t *testing.T) {
	raw := "select /* comment not sent to ClickHouse */ count() from events;"
	canonical, err := validateSQL(raw)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(canonical, "comment") || strings.HasSuffix(canonical, ";") {
		t.Fatalf("raw SQL escaped canonicalization: %q", canonical)
	}
	if canonical != "SELECT count() FROM events" {
		t.Fatalf("unexpected canonical SQL: %q", canonical)
	}
}

func TestSQLValidationParameterBindings(t *testing.T) {
	query := "SELECT event FROM events WHERE event = {name:String} AND timestamp_ms >= {start:Int64} AND event != {name:String}"
	if err := validateSQLParameters(query, map[string]string{"name": "' OR 1=1; DROP TABLE events", "start": "0"}); err != nil {
		t.Fatal(err)
	}
	for _, parameters := range []map[string]string{nil, {"name": "a"}, {"name": "a", "start": "0", "readonly": "0"}} {
		if err := validateSQLParameters(query, parameters); err == nil {
			t.Fatal("invalid parameter names accepted")
		}
	}
	if err := validateSQLParameters("SELECT {value:String}, {value:UInt64}", map[string]string{"value": "0"}); err == nil {
		t.Fatal("conflicting parameter types accepted")
	}
	if err := validateSQLParameters("SELECT {value:Identifier}", map[string]string{"value": "system.users"}); err == nil {
		t.Fatal("dynamic identifier accepted")
	}
	if err := validateSQLParameters("SELECT count() FROM events", nil); err != nil {
		t.Fatal(err)
	}
}

func FuzzValidateSQL(f *testing.F) {
	for _, seed := range []string{
		"SELECT count() FROM events", "WITH d AS (SELECT event FROM events) SELECT * FROM d",
		"SELECT count() FROM events WHERE event IN ('a', 'b')", "SELECT {name:String} FROM events",
		"SELECT count() FROM events FORMAT JSON", "SELECT current_user AS current_user FROM events",
		"SELECT * FROM system.users", "SELECT * FROM remote('example.invalid','events')",
		"SELECT 1; SELECT 2", "SELECT count() FROM events INTO OUTFILE 'dump'",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		canonical, err := validateSQL(raw)
		if err != nil {
			return
		}
		again, err := validateSQL(canonical)
		if err != nil || again != canonical {
			t.Fatalf("canonical SQL failed validation: %q: %v", canonical, err)
		}
	})
}
