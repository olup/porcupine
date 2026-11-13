//go:build ducklake

package porcupine

import (
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"testing"
)

// This runs inside the existing disposable ClickHouse integration environment:
// GOFLAGS=-tags=ducklake CGO_ENABLED=1 make integration
func TestClickHouseIntegrationDuckLakeEquivalence(t *testing.T) {
	config := integrationClickHouseConfig(t)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	clickHouseService := service(t, 100)
	configureAnalyticsTest(clickHouseService, ch)
	duckLakeService := duckLakeJourneyService(t)
	type backend struct {
		name, project, ingest string
		service               *Service
	}
	backends := []backend{{name: "clickhouse", service: clickHouseService}, {name: "ducklake", service: duckLakeService}}
	fixture := []struct {
		uuid, actor, event, timestamp, session, language string
		amount                                           float64
		duration                                         int
	}{
		{"u1-visit", "u1", "visit", "2026-11-01T23:30:00Z", "a", "fr", 0, 0},
		{"u1-signup", "u1", "signup", "2026-11-01T23:31:00Z", "a", "fr", 0, 0},
		{"u1-purchase", "u1", "purchase", "2026-11-01T23:32:00Z", "a", "fr", 2.5, 10},
		{"u1-repeat", "u1", "purchase", "2026-11-01T23:33:00Z", "a", "fr", 4.5, 50},
		{"u1-return", "u1", "visit", "2026-11-02T23:40:00Z", "a2", "fr", 0, 0},
		{"u2-visit", "u2", "visit", "2026-11-01T10:00:00Z", "b", "en", 0, 0},
		{"u2-signup", "u2", "signup", "2026-11-01T10:01:00Z", "b", "en", 0, 0},
		{"u3-visit", "u3", "visit", "2026-11-01T12:00:00Z", "c", "fr", 0, 0},
		{"u3-purchase", "u3", "purchase", "2026-11-01T12:02:00Z", "c", "fr", 7.25, 30},
		{"u3-repeat", "u3", "purchase", "2026-11-01T12:04:00Z", "c", "fr", 1.75, 90},
	}
	for i := range backends {
		engine := &backends[i]
		engine.project, engine.ingest = project(t, engine.service)
		batch := make([]map[string]any, 0, len(fixture))
		for _, row := range fixture {
			batch = append(batch, map[string]any{"uuid": row.uuid, "distinct_id": row.actor, "event": row.event, "timestamp": row.timestamp,
				"properties": map[string]any{"$session_id": row.session, "language": row.language, "amount": row.amount, "duration_ms": row.duration}})
		}
		identityCapture(t, engine.service, engine.ingest, batch, 200)
		duckLakeJourneyDrain(t, engine.service)
	}
	compare := func(t *testing.T, name, clickHouseSQL, duckDBSQL string) []map[string]string {
		t.Helper()
		var results [2][]map[string]string
		for i, engine := range backends {
			sql := clickHouseSQL
			if i == 1 {
				sql = duckDBSQL
			}
			rows := duckLakeJourneyQuery(t, engine.service, engine.project, adminToken, sql, nil)
			results[i] = normalizeEquivalentRows(t, rows)
		}
		if !reflect.DeepEqual(results[0], results[1]) {
			t.Fatalf("%s differs: ClickHouse=%v DuckLake=%v", name, results[0], results[1])
		}
		return results[0]
	}
	checks := []struct{ name, clickHouse, duckDB string }{
		{"counts_and_actors", "SELECT count() AS total,uniqExact(distinct_id) AS users FROM events", "SELECT count(*) AS total,count(DISTINCT distinct_id) AS users FROM events"},
		{"event_volumes", "SELECT event,count() AS total FROM events GROUP BY event ORDER BY event", "SELECT event,count(*) AS total FROM events GROUP BY event ORDER BY event"},
		{"hourly_trend", "SELECT formatDateTime(event_time,'%Y-%m-%dT%H:00:00','UTC') AS hour,count() AS total FROM events GROUP BY hour ORDER BY hour", "SELECT strftime(event_time,'%Y-%m-%dT%H:00:00') AS hour,count(*) AS total FROM events GROUP BY hour ORDER BY hour"},
		{"custom_dimensions", "SELECT JSONExtractString(properties,'language') AS language,event,count() AS total FROM events GROUP BY language,event ORDER BY language,event", "SELECT json_extract_string(properties,'$.language') AS language,event,count(*) AS total FROM events GROUP BY language,event ORDER BY language,event"},
		// Both use nearest-rank p95, rank=ceil(0.95*N), not two libraries'
		// potentially different interpolation or midpoint conventions.
		{"amount_and_nearest_rank_p95", "SELECT sum(JSONExtractFloat(properties,'amount')) AS amount,arrayElement(arraySort(groupArray(JSONExtractInt(properties,'duration_ms'))),toUInt64(ceil(count()*0.95))) AS p95 FROM events WHERE event='purchase'", "SELECT sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount,quantile_disc(CAST(json_extract_string(properties,'$.duration_ms') AS BIGINT),0.95) AS p95 FROM events WHERE event='purchase'"},
		{"sessions", "SELECT uniqExact(JSONExtractString(context,'session_id')) AS total FROM events", "SELECT count(DISTINCT json_extract_string(context,'$.session_id')) AS total FROM events"},
		{"ordered_funnel", `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='visit' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup' AND e.event_time>=v.t AND e.event_time<=addMinutes(v.t,10) GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=addMinutes(s.t,10) GROUP BY e.distinct_id) SELECT count() AS total FROM purchases`, `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='visit' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup' AND e.event_time>=v.t AND e.event_time<=v.t+INTERVAL '10 minutes' GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=s.t+INTERVAL '10 minutes' GROUP BY e.distinct_id) SELECT count(*) AS total FROM purchases`},
		{"next_day_retention", `WITH cohort AS (SELECT distinct_id,min(toDate(event_time)) AS first_day FROM events WHERE event='visit' GROUP BY distinct_id) SELECT uniqExact(e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='visit' AND toDate(e.event_time)=addDays(c.first_day,1)`, `WITH cohort AS (SELECT distinct_id,min(CAST(event_time AS DATE)) AS first_day FROM events WHERE event='visit' GROUP BY distinct_id) SELECT count(DISTINCT e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='visit' AND CAST(e.event_time AS DATE)=c.first_day+INTERVAL '1 day'`},
		{"paris_day_boundary", "SELECT formatDateTime(toTimeZone(event_time,'Europe/Paris'),'%Y-%m-%d') AS day,count() AS total FROM events GROUP BY day ORDER BY day", "SELECT strftime(timezone('Europe/Paris',event_time),'%Y-%m-%d') AS day,count(*) AS total FROM events GROUP BY day ORDER BY day"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) { compare(t, check.name, check.clickHouse, check.duckDB) })
	}
	initial := compare(t, "initial known count", checks[0].clickHouse, checks[0].duckDB)
	if len(initial) != 1 || initial[0]["total"] != "10" || initial[0]["users"] != "3" {
		t.Fatal("both engines failed the synthetic fixture's known counts")
	}
	for _, engine := range backends {
		edit := map[string]any{"expected_revision": "0", "idempotency_key": "equivalence-edit", "snapshot": map[string]any{"event": "purchase_corrected", "distinct_id": "u1", "timestamp": "2026-11-01T23:32:00Z", "properties": map[string]any{"language": "fr", "amount": 100.25, "duration_ms": 20}}}
		body, _ := json.Marshal(edit)
		request(t, engine.service, "PATCH", "/api/v1/projects/"+engine.project+"/events/u1-purchase", adminToken, string(body), 202)
		duckLakeJourneyDrain(t, engine.service)
	}
	corrected := compare(t, "corrected row", "SELECT uuid,event,JSONExtractFloat(properties,'amount') AS amount FROM events WHERE uuid='u1-purchase'", "SELECT uuid,event,CAST(json_extract_string(properties,'$.amount') AS DOUBLE) AS amount FROM events WHERE uuid='u1-purchase'")
	if len(corrected) != 1 || corrected[0]["event"] != "purchase_corrected" || corrected[0]["amount"] != "401/4" {
		t.Fatal("both engines failed to expose the correction")
	}
	for _, engine := range backends {
		request(t, engine.service, "DELETE", "/api/v1/projects/"+engine.project+"/events/u3-repeat", adminToken, `{"expected_revision":"0","idempotency_key":"equivalence-delete"}`, 202)
		duckLakeJourneyDrain(t, engine.service)
	}
	final := compare(t, "post-mutation totals", "SELECT count() AS total,sum(JSONExtractFloat(properties,'amount')) AS amount FROM events", "SELECT count(*) AS total,sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount FROM events")
	if len(final) != 1 || final[0]["total"] != "9" || final[0]["amount"] != "112" {
		t.Fatal("both engines failed the known post-mutation totals")
	}
	compare(t, "deleted row absent", "SELECT count() AS total FROM events WHERE uuid='u3-repeat'", "SELECT count(*) AS total FROM events WHERE uuid='u3-repeat'")
	t.Log("Compared nine analytical scenarios plus correction and deletion effects through real HTTP query handlers against ClickHouse and DuckLake")
}

func normalizeEquivalentRows(t *testing.T, rows []any) []map[string]string {
	t.Helper()
	result := make([]map[string]string, len(rows))
	for i, value := range rows {
		row := value.(map[string]any)
		result[i] = make(map[string]string, len(row))
		for name, value := range row {
			text := fmt.Sprint(value)
			switch name {
			case "total", "users", "amount", "p95":
				number, ok := new(big.Rat).SetString(text)
				if !ok {
					t.Fatalf("non-numeric analytical result in %s", name)
				}
				text = number.RatString()
			}
			result[i][name] = text
		}
	}
	return result
}
