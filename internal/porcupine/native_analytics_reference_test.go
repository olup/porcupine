package porcupine

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

func TestDuckLakeNativeAnalyticalReference(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	projectID, ingest := project(t, s)
	otherProject, otherIngest := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{projectID})
	_, editor := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{projectID})
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
	batch := make([]map[string]any, 0, len(fixture))
	for _, row := range fixture {
		batch = append(batch, map[string]any{
			"uuid": row.uuid, "distinct_id": row.actor, "event": row.event, "timestamp": row.timestamp,
			"properties": map[string]any{"$session_id": row.session, "language": row.language, "amount": row.amount, "duration_ms": row.duration},
		})
	}
	identityCapture(t, s, ingest, batch, 200)
	identityCapture(t, s, otherIngest, []map[string]any{{
		"uuid": "u1-purchase", "distinct_id": "foreign-user", "event": "purchase", "timestamp": "2026-11-01T23:32:00Z",
		"properties": map[string]any{"$session_id": "foreign-session", "language": "de", "amount": 1000000, "duration_ms": 1000000},
	}}, 200)
	duckLakeJourneyDrain(t, s)

	assertRows := func(t *testing.T, sql string, want []map[string]string) {
		t.Helper()
		rows := duckLakeJourneyQuery(t, s, projectID, reader, sql, nil)
		got := make([]map[string]string, len(rows))
		for i, value := range rows {
			row := value.(map[string]any)
			got[i] = make(map[string]string, len(row))
			for name, value := range row {
				got[i][name] = fmt.Sprint(value)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unexpected analytical result:\n got %v\nwant %v", got, want)
		}
	}
	checks := []struct {
		name, sql string
		want      []map[string]string
	}{
		{"counts_and_actors", "SELECT count(*) AS total,count(DISTINCT distinct_id) AS users FROM events", []map[string]string{{"total": "10", "users": "3"}}},
		{"event_volumes", "SELECT event,count(*) AS total FROM events GROUP BY event ORDER BY event", []map[string]string{
			{"event": "purchase", "total": "4"}, {"event": "signup", "total": "2"}, {"event": "visit", "total": "4"},
		}},
		{"hourly_trend", "SELECT strftime(event_time,'%Y-%m-%dT%H:00:00') AS hour,count(*) AS total FROM events GROUP BY hour ORDER BY hour", []map[string]string{
			{"hour": "2026-11-01T10:00:00", "total": "2"}, {"hour": "2026-11-01T12:00:00", "total": "3"},
			{"hour": "2026-11-01T23:00:00", "total": "4"}, {"hour": "2026-11-02T23:00:00", "total": "1"},
		}},
		{"custom_dimensions", "SELECT json_extract_string(properties,'$.language') AS language,event,count(*) AS total FROM events GROUP BY language,event ORDER BY language,event", []map[string]string{
			{"language": "en", "event": "signup", "total": "1"}, {"language": "en", "event": "visit", "total": "1"},
			{"language": "fr", "event": "purchase", "total": "4"}, {"language": "fr", "event": "signup", "total": "1"}, {"language": "fr", "event": "visit", "total": "3"},
		}},
		// Four purchase durations are 10, 30, 50, 90. Nearest-rank p95
		// selects ceil(0.95*4)=4, independently of interpolation conventions.
		{"amount_and_nearest_rank_p95", "SELECT sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount,quantile_disc(CAST(json_extract_string(properties,'$.duration_ms') AS BIGINT),0.95) AS p95 FROM events WHERE event='purchase'", []map[string]string{{"amount": "16", "p95": "90"}}},
		{"sessions", "SELECT count(DISTINCT json_extract_string(context,'$.session_id')) AS total FROM events", []map[string]string{{"total": "4"}}},
		{"ordered_funnel", `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='visit' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup' AND e.event_time>=v.t AND e.event_time<=v.t+INTERVAL '10 minutes' GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=s.t+INTERVAL '10 minutes' GROUP BY e.distinct_id) SELECT count(*) AS total FROM purchases`, []map[string]string{{"total": "1"}}},
		{"next_day_retention", `WITH cohort AS (SELECT distinct_id,min(CAST(event_time AS DATE)) AS first_day FROM events WHERE event='visit' GROUP BY distinct_id) SELECT count(DISTINCT e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='visit' AND CAST(e.event_time AS DATE)=c.first_day+INTERVAL '1 day'`, []map[string]string{{"total": "1"}}},
		{"paris_day_boundary", "SELECT strftime(timezone('Europe/Paris',event_time),'%Y-%m-%d') AS day,count(*) AS total FROM events GROUP BY day ORDER BY day", []map[string]string{
			{"day": "2026-11-01", "total": "5"}, {"day": "2026-11-02", "total": "4"}, {"day": "2026-11-03", "total": "1"},
		}},
		{"source_identity", "SELECT source_id,message_id,uuid FROM events WHERE uuid='u1-purchase'", []map[string]string{{"source_id": "posthog", "message_id": "u1-purchase", "uuid": "u1-purchase"}}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) { assertRows(t, check.sql, check.want) })
	}
	request(t, s, "POST", "/api/v1/projects/"+otherProject+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 403)
	original := duckLakeJourneyQuery(t, s, projectID, reader, "SELECT event_id FROM events WHERE uuid='u1-purchase'", nil)[0].(map[string]any)["event_id"]

	edit := map[string]any{"expected_revision": "0", "idempotency_key": "native-reference-edit", "snapshot": map[string]any{
		"event": "purchase_corrected", "distinct_id": "u1", "timestamp": "2026-11-01T23:32:00Z", "properties": map[string]any{"language": "fr", "amount": 100.25, "duration_ms": 20},
	}}
	body, _ := json.Marshal(edit)
	request(t, s, "PATCH", "/api/v1/projects/"+projectID+"/events/u1-purchase", editor, string(body), 202)
	duckLakeJourneyDrain(t, s)
	assertRows(t, "SELECT uuid,event,CAST(json_extract_string(properties,'$.amount') AS DOUBLE) AS amount FROM events WHERE uuid='u1-purchase'", []map[string]string{{"uuid": "u1-purchase", "event": "purchase_corrected", "amount": "100.25"}})
	corrected := duckLakeJourneyQuery(t, s, projectID, reader, "SELECT event_id,source_id,message_id FROM events WHERE uuid='u1-purchase'", nil)[0].(map[string]any)
	if corrected["event_id"] != original || corrected["source_id"] != "posthog" || corrected["message_id"] != "u1-purchase" {
		t.Fatal("correction changed the immutable event or source identity", corrected)
	}

	request(t, s, "DELETE", "/api/v1/projects/"+projectID+"/events/u3-repeat", editor, `{"expected_revision":"0","idempotency_key":"native-reference-delete"}`, 202)
	duckLakeJourneyDrain(t, s)
	assertRows(t, "SELECT count(*) AS total,sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount FROM events", []map[string]string{{"total": "9", "amount": "112"}})
	assertRows(t, "SELECT count(*) AS total FROM events WHERE uuid='u3-repeat'", []map[string]string{{"total": "0"}})
	assertRows(t, "SELECT sum(CAST(json_extract_string(properties,'$.amount') AS DOUBLE)) AS amount,quantile_disc(CAST(json_extract_string(properties,'$.duration_ms') AS BIGINT),0.95) AS p95 FROM events WHERE event='purchase'", []map[string]string{{"amount": "11.75", "p95": "50"}})
}
