//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func duckLakeJourneyService(t *testing.T) *Service {
	t.Helper()
	s := service(t, 100)
	lake, err := newDuckLake(DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake")})
	if err != nil {
		t.Fatal(err)
	}
	s.lake = lake
	return s
}

func duckLakeJourneyDrain(t *testing.T, s *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		var pending int
		if err := s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox)`).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			return
		}
		if err := s.flushDelivery(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func duckLakeJourneyQuery(t *testing.T, s *Service, project, token, sql string, parameters map[string]string) []any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"sql": sql, "parameters": parameters, "use_cache": false})
	if err != nil {
		t.Fatal(err)
	}
	result := request(t, s, "POST", "/api/v1/projects/"+project+"/query", token, string(body), 200)
	return result["result"].(map[string]any)["data"].([]any)
}

func duckLakeJourneyProjections(t *testing.T, events []Event) []eventProjection {
	t.Helper()
	projections := make([]eventProjection, 0, len(events))
	for _, event := range events {
		snapshot, err := originalSnapshot(event)
		if err != nil {
			t.Fatal(err)
		}
		projections = append(projections, eventProjection{Event: event, OriginalSnapshot: snapshot})
	}
	return projections
}

func TestDuckLakeJourneyAdaptersIdentityMutationsAndIsolation(t *testing.T) {
	s := duckLakeJourneyService(t)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	readerID, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{a})
	page, _ := json.Marshal(map[string]any{"api_key": ingestA, "uuid": "page", "event": "$pageview", "distinct_id": "browser", "timestamp": "2026-11-01T10:00:00Z", "properties": map[string]any{"$session_id": "session-a"}})
	request(t, s, "POST", "/capture/", "", string(page), 200)
	native := `{"type":"track","message_id":"purchase","actor_id":"person","name":"Purchase","timestamp":"2026-11-01T10:01:00Z","properties":{"amount":42,"large_id":9007199254740993},"context":{"session_id":"session-a"}}`
	first := nativeRequest(t, s, ingestA, native, 200)
	firstReceipt := receiptFrom(t, first, 0)
	before := nativeRequest(t, s, ingestA, native, 200)
	if before["duplicates"] != float64(1) || receiptFrom(t, before, 0)["event_id"] != firstReceipt["event_id"] {
		t.Fatal("native retry before projection changed its durable event ID")
	}
	segment, _ := json.Marshal(map[string]any{"writeKey": ingestA, "messageId": "signup", "anonymousId": "browser", "event": "Signup", "timestamp": "2026-11-01T10:02:00Z", "properties": map[string]any{"channel": "web"}})
	transportRequest(t, s, "POST", "/v1/track", segment, segmentHeaders(""), 200)
	identify, _ := json.Marshal(map[string]any{"writeKey": ingestA, "messageId": "identify", "anonymousId": "browser", "userId": "person", "traits": map[string]any{"plan": "pro"}})
	transportRequest(t, s, "POST", "/v1/identify", identify, segmentHeaders(""), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "page"), 200)
	originals, err := s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 5 {
		t.Fatalf("missing mixed-adapter originals: %d, %v", len(originals), err)
	}
	duckLakeJourneyDrain(t, s)
	if s.ch != nil {
		t.Fatal("DuckLake journey unexpectedly configured ClickHouse")
	}
	after := nativeRequest(t, s, ingestA, native, 200)
	if after["duplicates"] != float64(1) || receiptFrom(t, after, 0)["event_id"] != firstReceipt["event_id"] {
		t.Fatal("native retry after projection changed its durable event ID")
	}
	// Replaying exactly the saved original batch must leave logical rows alone.
	for range 2 {
		if err := s.lake.Apply(context.Background(), duckLakeJourneyProjections(t, originals)); err != nil {
			t.Fatal(err)
		}
	}
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT event_id,uuid,event,distinct_id,person_id,person_properties,properties FROM events ORDER BY id", nil)
	if len(rows) != 3 {
		t.Fatalf("identity control or retry changed ordinary event count: %d", len(rows))
	}
	for _, value := range rows {
		row := value.(map[string]any)
		if row["person_id"] != "person" || !strings.Contains(row["person_properties"].(string), `"plan":"pro"`) {
			t.Fatalf("cross-adapter anonymous activity did not resolve its person: %v", row)
		}
		if row["uuid"] == firstReceipt["uuid"] && (row["event_id"] != firstReceipt["event_id"] || !strings.Contains(row["properties"].(string), "9007199254740993")) {
			t.Fatal("native provenance or custom integer precision changed")
		}
	}
	request(t, s, "POST", "/api/v1/projects/"+b+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 403)
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", reader, `{"sql":"SELECT original_snapshot FROM events"}`, 400)
	selected := duckLakeJourneyQuery(t, s, a, reader, "SELECT count(*) AS total FROM events WHERE event=$name", map[string]string{"name": "Purchase' OR 1=1 --"})
	if selected[0].(map[string]any)["total"] != "0" {
		t.Fatal("bound native SQL parameter changed query syntax")
	}
	updatedIdentify, _ := json.Marshal(map[string]any{"writeKey": ingestA, "messageId": "profile-new", "userId": "person", "traits": map[string]any{"plan": "team"}})
	transportRequest(t, s, "POST", "/v1/identify", updatedIdentify, segmentHeaders(""), 200)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Apply(context.Background(), duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	traits := duckLakeJourneyQuery(t, s, a, reader, "SELECT person_properties FROM events WHERE uuid='page'", nil)
	if len(traits) != 1 || !strings.Contains(traits[0].(map[string]any)["person_properties"].(string), `"plan":"team"`) {
		t.Fatal("older identity replay replaced current person traits")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	mcpCtx, session := mcpTestSession(t, host.URL, reader, "")
	mcpCount := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": a}, Body: map[string]any{"sql": "SELECT count(*) AS total FROM events", "use_cache": false}}, 200)
	if mcpCount["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "3" {
		t.Fatal("MCP did not use native DuckLake analytics")
	}
	mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": b}, Body: map[string]any{"sql": "SELECT count(*) FROM events"}}, 403)
	uuid := firstReceipt["uuid"].(string)
	path := "/api/v1/projects/" + a + "/events/" + url.PathEscape(uuid)
	editBody := `{"expected_revision":"0","idempotency_key":"correct-purchase","snapshot":{"event":"Corrected purchase","distinct_id":"person","properties":{"amount":99},"timestamp":"2026-11-01T10:01:00Z"}}`
	request(t, s, "PATCH", path, reader, editBody, 403)
	edit := request(t, s, "PATCH", path, manager, editBody, 202)
	replayedEdit := request(t, s, "PATCH", path, manager, editBody, 202)
	if edit["mutation_id"] != replayedEdit["mutation_id"] {
		t.Fatal("edit retry allocated another mutation")
	}
	request(t, s, "PATCH", path, manager, strings.Replace(editBody, "correct-purchase", "stale-revision", 1), 409)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Apply(context.Background(), duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	corrected := duckLakeJourneyQuery(t, s, a, reader, "SELECT event,revision FROM events WHERE uuid=$uuid", map[string]string{"uuid": uuid})
	if len(corrected) != 1 || corrected[0].(map[string]any)["event"] != "Corrected purchase" || corrected[0].(map[string]any)["revision"] != edit["revision"] {
		t.Fatal("late original replaced an applied correction")
	}
	deletedBody, _ := json.Marshal(map[string]any{"expected_revision": edit["revision"], "idempotency_key": "delete-purchase"})
	request(t, s, "DELETE", path, manager, string(deletedBody), 202)
	request(t, s, "GET", path, manager, "", 410)
	duckLakeJourneyDrain(t, s)
	if err := s.lake.Apply(context.Background(), duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	if got := duckLakeJourneyQuery(t, s, a, reader, "SELECT count(*) AS total FROM events WHERE uuid=$uuid", map[string]string{"uuid": uuid}); got[0].(map[string]any)["total"] != "0" {
		t.Fatal("late original resurrected a deleted event")
	}
	deletedRetry := nativeRequest(t, s, ingestA, native, 200)
	if deletedRetry["duplicates"] != float64(1) || receiptFrom(t, deletedRetry, 0)["event_id"] != firstReceipt["event_id"] {
		t.Fatal("deleted event retry lost its original durable ID")
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+readerID, adminToken, "", 204)
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 401)
	adminCtx, adminSession := mcpTestSession(t, host.URL, adminToken, "")
	mcpTestCall(t, adminCtx, adminSession, mcpCallInput{OperationID: "rename-project", Path: map[string]string{"id": a}, Body: map[string]any{"name": "DuckLake renamed"}}, 200)
	mcpTestCall(t, adminCtx, adminSession, mcpCallInput{OperationID: "delete-project", Path: map[string]string{"id": a}}, 204)
	request(t, s, "POST", "/capture/", "", string(page), 401)
	foreign := duckLakeJourneyQuery(t, s, b, adminToken, "SELECT count(*) AS total FROM events", nil)
	if foreign[0].(map[string]any)["total"] != "1" {
		t.Fatal("project deletion changed the other project's analytics")
	}
}

func TestDuckLakeJourneyConfiguredRestart(t *testing.T) {
	directory := t.TempDir()
	config := Config{DBPath: filepath.Join(directory, "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(directory, "lake")}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	projectID, ingest := project(t, s)
	native := `{"type":"track","message_id":"restart-stable","actor_id":"user","name":"Restart"}`
	first := receiptFrom(t, nativeRequest(t, s, ingest, native, 200), 0)
	duckLakeJourneyDrain(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	retry := nativeRequest(t, s, ingest, native, 200)
	if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != first["event_id"] {
		t.Fatal("configured service restart changed native retry identity")
	}
	rows := duckLakeJourneyQuery(t, s, projectID, adminToken, "SELECT event_id,event FROM events", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != first["event_id"] || rows[0].(map[string]any)["event"] != "Restart" {
		t.Fatal("configured service restart lost its projected event")
	}
}

func TestDuckLakeJourneyLostAcknowledgment(t *testing.T) {
	directory := t.TempDir()
	config := Config{DBPath: filepath.Join(directory, "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(directory, "lake")}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	projectID, ingest := project(t, s)
	native := `{"type":"track","message_id":"lost-ack","actor_id":"user","name":"Durable acceptance"}`
	if _, err := s.db.Exec(`CREATE TRIGGER ducklake_block_ack BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'synthetic acknowledgment failure'); END`); err != nil {
		t.Fatal(err)
	}
	first := receiptFrom(t, nativeRequest(t, s, ingest, native, 200), 0)
	if err := s.flushDelivery(context.Background()); err == nil {
		t.Fatal("projection acknowledgment unexpectedly committed")
	}
	var pending, receipts int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM event_receipts)`).Scan(&pending, &receipts); err != nil || pending != 1 || receipts != 0 {
		t.Fatalf("failed acknowledgment removed its payload or committed a receipt: pending=%d receipts=%d err=%v", pending, receipts, err)
	}
	rows := duckLakeJourneyQuery(t, s, projectID, adminToken, "SELECT event_id FROM events", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != first["event_id"] {
		t.Fatal("analytical commit did not precede the failed acknowledgment")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	restartedRetry := nativeRequest(t, s, ingest, native, 200)
	if restartedRetry["duplicates"] != float64(1) || receiptFrom(t, restartedRetry, 0)["event_id"] != first["event_id"] {
		t.Fatal("restart after an uncertain analytical commit changed the stable receipt")
	}
	if _, err := s.db.Exec(`DROP TRIGGER ducklake_block_ack`); err != nil {
		t.Fatal(err)
	}
	duckLakeJourneyDrain(t, s)
	rows = duckLakeJourneyQuery(t, s, projectID, adminToken, "SELECT event_id FROM events", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != first["event_id"] {
		t.Fatal("retry of the uncertain analytical commit changed its logical event")
	}
	retry := nativeRequest(t, s, ingest, native, 200)
	if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != first["event_id"] {
		t.Fatal("retry after acknowledgment recovery changed the stable receipt")
	}
}

func TestDuckLakeJourneyAnalyticalReferenceResults(t *testing.T) {
	s := duckLakeJourneyService(t)
	projectID, ingest := project(t, s)
	fixture := []struct {
		user, event, timestamp, session string
		duration                        int
	}{
		{"u1", "visit", "2026-11-01T00:00:00Z", "a", 0},
		{"u1", "signup", "2026-11-01T00:01:00Z", "a", 0},
		{"u1", "purchase", "2026-11-01T00:02:00Z", "a", 10},
		{"u1", "visit", "2026-11-02T00:00:00Z", "a2", 0},
		{"u2", "visit", "2026-11-01T00:00:00Z", "b", 0},
		{"u2", "signup", "2026-11-01T00:01:00Z", "b", 0},
		{"u3", "visit", "2026-11-01T00:00:00Z", "c", 0},
		{"u3", "purchase", "2026-11-01T00:02:00Z", "c", 30},
	}
	batch := make([]map[string]any, 0, len(fixture))
	for i, row := range fixture {
		batch = append(batch, map[string]any{"type": "track", "message_id": fmt.Sprintf("reference-%d", i), "actor_id": row.user, "name": row.event, "timestamp": row.timestamp, "context": map[string]any{"session_id": row.session}, "properties": map[string]any{"duration_ms": row.duration}})
	}
	encoded, _ := json.Marshal(batch)
	nativeRequest(t, s, ingest, string(encoded), 200)
	duckLakeJourneyDrain(t, s)
	checks := []struct{ name, sql, want string }{
		{"events", "SELECT count(*) AS total FROM events", "8"},
		{"actors", "SELECT count(DISTINCT distinct_id) AS total FROM events", "3"},
		{"daily", "SELECT count(*) AS total FROM events WHERE CAST(event_time AS DATE)=DATE '2026-11-02'", "1"},
		{"sessions", "SELECT count(DISTINCT json_extract_string(context,'$.session_id')) AS total FROM events", "4"},
		{"percentile", "SELECT quantile_cont(CAST(json_extract_string(properties,'$.duration_ms') AS DOUBLE),0.5) AS total FROM events WHERE event='purchase'", "20"},
		{"ordered_funnel", `WITH visits AS (SELECT distinct_id,min(event_time) AS t FROM events WHERE event='visit' GROUP BY distinct_id), signups AS (SELECT e.distinct_id,min(e.event_time) AS t FROM events e JOIN visits v ON e.distinct_id=v.distinct_id WHERE e.event='signup' AND e.event_time>=v.t AND e.event_time<=v.t+INTERVAL '10 minutes' GROUP BY e.distinct_id), purchases AS (SELECT e.distinct_id FROM events e JOIN signups s ON e.distinct_id=s.distinct_id WHERE e.event='purchase' AND e.event_time>=s.t AND e.event_time<=s.t+INTERVAL '10 minutes' GROUP BY e.distinct_id) SELECT count(*) AS total FROM purchases`, "1"},
		{"next_day_retention", `WITH cohort AS (SELECT distinct_id,min(CAST(event_time AS DATE)) AS first_day FROM events WHERE event='visit' GROUP BY distinct_id) SELECT count(DISTINCT e.distinct_id) AS total FROM events e JOIN cohort c ON e.distinct_id=c.distinct_id WHERE e.event='visit' AND CAST(e.event_time AS DATE)=c.first_day+INTERVAL '1 day'`, "1"},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			rows := duckLakeJourneyQuery(t, s, projectID, adminToken, check.sql, nil)
			if len(rows) != 1 {
				t.Fatalf("analytical reference returned %d rows", len(rows))
			}
			got := rows[0].(map[string]any)["total"]
			if fmt.Sprint(got) != check.want {
				t.Fatalf("analytical result %v, want %s", got, check.want)
			}
		})
	}
	grouped := duckLakeJourneyQuery(t, s, projectID, adminToken, "SELECT event,count(*) AS total FROM events GROUP BY event ORDER BY event", nil)
	if len(grouped) != 3 {
		t.Fatal("event grouping omitted a reference event")
	}
	for i, want := range []struct{ event, count string }{{"purchase", "2"}, {"signup", "2"}, {"visit", "4"}} {
		row := grouped[i].(map[string]any)
		if row["event"] != want.event || row["total"] != want.count {
			t.Fatalf("wrong event aggregation: %v", row)
		}
	}
	if value, err := strconv.ParseUint(grouped[0].(map[string]any)["total"].(string), 10, 64); err != nil || value != 2 {
		t.Fatal("count result did not retain integer precision")
	}
}
