package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationIdentityProjection(t *testing.T) {
	config := integrationClickHouseConfig(t)
	path := filepath.Join(t.TempDir(), "identity-query.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	a, _ := project(t, s)
	b, _ := project(t, s)
	_, readerA := key(t, s, []string{"events:read"}, []string{a})
	_, readerB := key(t, s, []string{"events:read"}, []string{b})
	if err = ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	receipt := time.Now().UTC().Format(time.RFC3339Nano)
	event := func(id int64, project, uuid, name, distinct, snapshot string) Event {
		return Event{ID: id, ProjectID: project, UUID: uuid, Event: name, DistinctID: distinct, Properties: json.RawMessage(`{"original":true}`), Timestamp: "2026-01-01T00:00:00.123Z", ReceivedAt: receipt, IdentitySnapshot: snapshot}
	}
	snapshot := func(version, id, traits, links string) string {
		return fmt.Sprintf(`{"version":%q,"identified_id":%q,"traits":%s,"links":%s}`, version, id, traits, links)
	}
	identify := snapshot("10", "person-a", `{"name":"initial","once":"first"}`, `[{"anonymous_id":"anon","identified_id":"person-a","version":"10"}]`)
	newest := snapshot("100", "person-a", `{"name":"café東京","once":"first","nested":{"x":true}}`, `[]`)
	older := snapshot("20", "person-a", `{"name":"stale"}`, `[]`)
	ignored := snapshot("200", "", `{}`, `[]`)
	rows := []Event{
		event(1, a, "before", "visit", "anon", ""), event(2, a, "after", "purchase", "person-a", ""), event(3, a, "unlinked", "visit", "unlinked", ""),
		event(4, a, "legacy-name", "$identify", "legacy", ""),
		event(10, a, "identify", "$identify", "person-a", identify), event(100, a, "profile-new", "$set", "person-a", newest), event(20, a, "profile-old", "$set", "person-a", older), event(200, a, "ignored", "$identify", "person-a", ignored),
		event(1, b, "foreign", "visit", "anon", ""), event(11, b, "foreign-identify", "$identify", "foreign-person", snapshot("11", "foreign-person", `{"name":"private-other-project"}`, `[{"anonymous_id":"anon","identified_id":"foreign-person","version":"11"}]`)),
	}
	if err = ch.insert(ctx, rows); err != nil {
		t.Fatal(err)
	}
	// Lost-delivery acknowledgement replay must not multiply control state.
	if err = ch.insert(ctx, []Event{rows[4], rows[5]}); err != nil {
		t.Fatal(err)
	}
	query := "SELECT uuid,distinct_id,person_id,person_properties FROM events ORDER BY uuid"
	assertRows := func(service *Service) {
		t.Helper()
		result, e := service.analyticsQuery(ctx, queryInput(a, query))
		if e != nil {
			t.Fatalf("resolved query failed: %v", e)
		}
		if result.Body.Result.Rows != 4 {
			t.Fatalf("controlrows changed public count: %d", result.Body.Result.Rows)
		}
		for _, row := range result.Body.Result.Data {
			var uuid, distinct, person, properties string
			json.Unmarshal(row["uuid"], &uuid)
			json.Unmarshal(row["distinct_id"], &distinct)
			json.Unmarshal(row["person_id"], &person)
			json.Unmarshal(row["person_properties"], &properties)
			if uuid == "before" || uuid == "after" {
				if person != "person-a" {
					t.Fatal("historical/directidentity mismatch")
				}
				if uuid == "before" && distinct != "anon" {
					t.Fatal("distinct_id was rewritten")
				}
				var traits map[string]any
				if json.Unmarshal([]byte(properties), &traits) != nil || traits["name"] != "café東京" || traits["once"] != "first" {
					t.Fatal("latest numeric profile snapshot not resolved")
				}
			}
			if uuid == "unlinked" && (person != "unlinked" || properties != "{}") {
				t.Fatal("unlinked identity must fall back")
			}
			if strings.Contains(properties, "private-other-project") {
				t.Fatal("foreign profile leaked")
			}
		}
	}
	assertRows(s)
	collision, collisionErr := s.analyticsQuery(ctx, queryInput(a, "WITH events_with_persons AS (SELECT event FROM events) SELECT person_id FROM events WHERE uuid='before'"))
	if collisionErr != nil || collision.Body.Result.Rows != 1 || string(collision.Body.Result.Data[0]["person_id"]) != `"person-a"` {
		t.Fatal("user CTE shadows internal identity view")
	}
	unique, uniqueErr := s.analyticsQuery(ctx, queryInput(a, "SELECT uniqExact(distinct_id) AS original,uniqExact(person_id) AS resolved FROM events"))
	if uniqueErr != nil || string(unique.Body.Result.Data[0]["original"]) != `"4"` || string(unique.Body.Result.Data[0]["resolved"]) != `"3"` {
		t.Fatal("resolved identity aggregation cardinality incorrect")
	}
	normal, err := s.analyticsQuery(ctx, queryInput(a, "SELECT count(*) AS n FROM events"))
	if err != nil || string(normal.Body.Result.Data[0]["n"]) != `"4"` {
		t.Fatal("normalcounts include accepted identitycontrols")
	}
	all, err := s.analyticsQuery(ctx, queryInput(a, "SELECT * FROM events ORDER BY uuid"))
	if err != nil || all.Body.Result.Rows != 4 {
		t.Fatal("SELECTstar identity contract failed")
	}
	for _, row := range all.Body.Result.Data {
		if _, ok := row["person_id"]; !ok {
			t.Fatal("star missing person_id")
		}
		if _, ok := row["identity_snapshot"]; ok {
			t.Fatal("private snapshot exposed")
		}
	}
	catalog, err := s.eventCatalog(ctx, &EventCatalogInput{ID: a, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range catalog.Body.Events {
		if e.Event == "$set" {
			t.Fatal("control rows exposed in discovery")
		}
		if e.Event == "$identify" && e.Count != "1" {
			t.Fatal("legacy same-nameevent incorrectly hidden or controls exposed")
		}
	}
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", readerB, `{"sql":"SELECT person_id FROM events"}`, 403)
	response := request(t, s, "POST", "/api/v1/projects/"+a+"/query", readerA, `{"sql":"SELECT uuid,person_id FROM events ORDER BY uuid"}`, 200)
	if response["result"].(map[string]any)["rows"] != float64(4) {
		t.Fatal("HTTP SQL projection differs")
	}
	for _, sql := range []string{"SELECT identity_snapshot FROM events", "SELECT e.original_snapshot FROM events e"} {
		body, _ := json.Marshal(map[string]string{"sql": sql})
		request(t, s, "POST", "/api/v1/projects/"+a+"/query", readerA, string(body), 400)
	}
	ns, restricted, password, err := s.analyticsReader(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	fastPlan, planErr := ch.admin(ctx, "EXPLAIN PLAN SELECT count(*) FROM "+ns+".events")
	if planErr != nil || strings.Contains(string(fastPlan), "Join") {
		t.Fatal("ordinary event count executes identity joins")
	}
	if _, err = ch.query(ctx, restricted, password, ns, "SELECT original_snapshot FROM "+config.Database+"."+eventTable, nil); err == nil {
		t.Fatal("restricted reader can access private original snapshots")
	}
	foreign, err := ch.query(ctx, restricted, password, ns, "SELECT project_id,identity_snapshot FROM "+config.Database+"."+eventTable+" WHERE project_id='"+b+"'", nil)
	if err != nil {
		t.Fatal("direct row-policy probe failed")
	}
	var isolated AnalyticsResult
	if json.Unmarshal(foreign, &isolated) != nil || isolated.Rows != 0 {
		t.Fatal("physical rowpolicy allows foreign identity snapshots")
	}
	// Export and scheduled-query profiles use the same identity-aware projection.
	budget, _ := normalizeExportConfig(ExportConfig{MaxExecutionSeconds: 30})
	s.exports = &exportRuntime{config: budget}
	_, exportReader, exportPassword, err := s.exportReader(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := identitySQL(query, ns)
	if err != nil {
		t.Fatal(err)
	}
	for _, scheduled := range []bool{false, true} {
		sql := canonical
		var params map[string]string
		if scheduled {
			sql, err = pipelineWindowSQL(query, ns)
			if err != nil {
				t.Fatal(err)
			}
			params = map[string]string{"window_start": "0", "window_end": "9223372036854775807"}
		}
		body, e := ch.openExport(ctx, ns, exportReader, exportPassword, "identity_"+strings.ReplaceAll(newID(), "-", ""), sql, params)
		if e != nil {
			t.Fatal(e)
		}
		columns := []AnalyticsResultColumn{}
		var captured [][]json.RawMessage
		stream, e := streamExportJSON(ctx, body, 1<<20, exportStreamCallbacks{Columns: func(c []AnalyticsResultColumn) error { columns = c; return nil }, Row: func(r []json.RawMessage) error {
			captured = append(captured, append([]json.RawMessage{}, r...))
			return nil
		}})
		body.Close()
		if e != nil || stream.Rows != 4 || len(captured) != 4 || len(columns) != 4 {
			t.Fatalf("export/scheduled identity projection differs: %v", e)
		}
	}
	// Re-provisioning after an application restart derives identical identities
	// exclusively from persisted ClickHouse controls, not process memory.
	s.exports = nil
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	ch, err = newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	assertRows(s)
}

func TestClickHouseIntegrationIdentityCapture(t *testing.T) {
	config := integrationClickHouseConfig(t)
	dbPath := filepath.Join(t.TempDir(), "identity-capture.db")
	open := func() *Service {
		t.Helper()
		s, err := Open(Config{DBPath: dbPath, BootstrapToken: adminToken, ClickHouse: &config})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	a, token := project(t, s)
	b, otherToken := project(t, s)
	_, read := key(t, s, []string{"events:read"}, []string{a})
	original := identifyEvent("identify-real", "person-real", "browser-real", map[string]any{"name": "first"}, map[string]any{"first": "only-once"})
	identityCapture(t, s, token, []map[string]any{
		{"uuid": "before-real", "event": "visit", "distinct_id": "browser-real"},
		setIdentityEvent("anonymous-traits", "browser-real", map[string]any{"anonymous_trait": "preserved"}, map[string]any{}),
		original,
		{"uuid": "after-real", "event": "purchase", "distinct_id": "person-real"},
		setIdentityEvent("final-traits", "person-real", map[string]any{"name": "café東京"}, map[string]any{"first": "must-not-replace"}),
	}, 200)
	identityCapture(t, s, otherToken, []map[string]any{
		{"uuid": "before-real", "event": "visit", "distinct_id": "browser-real"},
		identifyEvent("identify-real", "other-person", "browser-real", map[string]any{"private_other_project": "secret"}, map[string]any{}),
	}, 200)
	retry := identifyEvent("identify-real", "person-real", "browser-real", map[string]any{"name": "retry-must-not-apply"}, map[string]any{})
	if got := identityCapture(t, s, token, []map[string]any{retry}, 200); got["duplicates"] != float64(1) {
		t.Fatal("accepted control replay reapplied")
	}
	drain := func() {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		for {
			var queued int
			if s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&queued) != nil {
				t.Fatal("pending identity queue")
			}
			if queued == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("identity control delivery did not drain")
			}
			if err := s.flushDelivery(context.Background()); err != nil {
				t.Fatal("identity control delivery failed")
			}
		}
	}
	assert := func() {
		t.Helper()
		output, err := s.analyticsQuery(context.Background(), queryInput(a, "SELECT uuid,distinct_id,person_id,person_properties FROM events ORDER BY uuid"))
		if err != nil || output.Body.Result.Rows != 2 {
			t.Fatalf("accepted controls visible as ordinary rows: %v", err)
		}
		for _, row := range output.Body.Result.Data {
			var id, person, props string
			json.Unmarshal(row["uuid"], &id)
			json.Unmarshal(row["person_id"], &person)
			json.Unmarshal(row["person_properties"], &props)
			var traits map[string]any
			json.Unmarshal([]byte(props), &traits)
			if person != "person-real" || traits["name"] != "café東京" || traits["first"] != "only-once" || traits["anonymous_trait"] != "preserved" {
				t.Fatalf("accepted canonical identity/profile differs in ClickHouse: uuid=%s person=%q traits=%s", id, person, props)
			}
			if id == "before-real" && string(row["distinct_id"]) != `"browser-real"` {
				t.Fatal("original anonymous distinct_id lost")
			}
			if _, leaked := traits["private_other_project"]; leaked {
				t.Fatal("cross-project profile leak")
			}
		}
		request(t, s, "GET", "/api/v1/projects/"+a+"/events/identify-real", read, "", 404)
		other, err := s.analyticsQuery(context.Background(), queryInput(b, "SELECT person_id FROM events"))
		if err != nil || other.Body.Result.Rows != 1 || string(other.Body.Result.Data[0]["person_id"]) != `"other-person"` {
			t.Fatal("separate project identity mapping changed")
		}
	}
	drain()
	assert()
	projected, err := s.ch.loadProjection(context.Background(), a, "identify-real")
	if err != nil || projected.IdentitySnapshot == "" {
		t.Fatal("delivered control lost canonical snapshot")
	}
	if err = s.ch.insertProjections(context.Background(), []eventProjection{projected}); err != nil {
		t.Fatal("identity projection replay failed")
	}
	assert()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	s = open()
	if got := identityCapture(t, s, token, []map[string]any{retry}, 200); got["duplicates"] != float64(1) {
		t.Fatal("restart forgot identity receipt")
	}
	drain()
	assert()
}
