package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationMutations(t *testing.T) {
	config := integrationClickHouseConfig(t)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	s := service(t, 100)
	configureAnalyticsTest(s, ch)
	oldReceipt := time.Now().UTC().Add(-45 * 24 * time.Hour)
	s.clock = func() time.Time { return oldReceipt }
	ctx := context.Background()
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{a})
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, other := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{b})
	uuid := "../event ?#%"
	body, _ := json.Marshal(map[string]any{"api_key": ingest, "uuid": uuid, "event": "original", "distinct_id": "visitor", "properties": map[string]any{"amount": 1}})
	request(t, s, "POST", "/capture/", "", string(body), 200)
	originals, err := s.deliveryBatch(ctx)
	if err != nil || len(originals) != 1 {
		t.Fatal("missing original queued event")
	}
	path := "/api/v1/projects/" + a + "/events/" + strings.ReplaceAll(url.PathEscape(uuid), ".", "%2E")
	initial := request(t, s, "GET", path, manager, "", 200)
	if initial["revision"] != "0" {
		t.Fatal("original revision must be zero")
	}
	snapshot := map[string]any{"event": "corrected", "distinct_id": "visitor", "properties": map[string]any{"amount": 42, "token": "must-not-persist"}, "timestamp": oldReceipt.Format(time.RFC3339Nano)}
	editBody := func(revision, key string) string {
		encoded, _ := json.Marshal(map[string]any{"expected_revision": revision, "idempotency_key": key, "snapshot": snapshot})
		return string(encoded)
	}
	request(t, s, "PATCH", path, reader, editBody("0", "forbidden"), 403)
	request(t, s, "PATCH", path, other, editBody("0", "wrong-project"), 403)
	request(t, s, "PATCH", path, manager, `{"expected_revision":"0","idempotency_key":"missing-snapshot"}`, 422)
	edit := request(t, s, "PATCH", path, manager, editBody("0", "edit-once"), 202)
	repeat := request(t, s, "PATCH", path, manager, editBody("0", "edit-once"), 202)
	if edit["mutation_id"] != repeat["mutation_id"] {
		t.Fatal("mutation retry was not idempotent")
	}
	request(t, s, "PATCH", path, manager, editBody("0", "concurrent"), 409)
	pending := request(t, s, "GET", path, manager, "", 200)
	if pending["pending"] != true || pending["event"].(map[string]any)["event"] != "corrected" {
		t.Fatal("accepted correction was not readable")
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	statusPath := "/api/v1/projects/" + a + "/event-mutations/" + edit["mutation_id"].(string)
	if status := request(t, s, "GET", statusPath, manager, "", 200); status["state"] != "applied" {
		t.Fatal("mutation not acknowledged")
	}
	var payloads int
	if err := s.db.QueryRow(`SELECT count(*) FROM mutation_outbox`).Scan(&payloads); err != nil || payloads != 0 {
		t.Fatal("applied payload retained in mutation outbox")
	}
	query := func(sql string, status int) map[string]any {
		encoded, _ := json.Marshal(map[string]string{"sql": sql})
		return request(t, s, "POST", "/api/v1/projects/"+a+"/query", manager, string(encoded), status)
	}
	result := query("SELECT event,revision,properties FROM events", 200)
	data := result["result"].(map[string]any)["data"].([]any)
	if len(data) != 1 || data[0].(map[string]any)["event"] != "corrected" || strings.Contains(data[0].(map[string]any)["properties"].(string), "must-not-persist") {
		t.Fatal("correction was not projected safely")
	}
	if query("SELECT count() AS total FROM events WHERE event='original'", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
		t.Fatal("old event predicate resurrected a replaced revision")
	}
	private, err := ch.loadProjection(ctx, a, uuid)
	if err != nil || !strings.Contains(private.OriginalSnapshot, `"event":"original"`) {
		t.Fatal("original evidence was lost")
	}
	if private.ID != originals[0].ID || private.ReceivedAt != originals[0].ReceivedAt {
		t.Fatal("correction changed immutable identity or receipt")
	}
	if err := ch.insert(ctx, originals); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	if query("SELECT count() AS total FROM events WHERE event='corrected'", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "1" {
		t.Fatal("late original overwrote correction")
	}
	if config.StoragePolicy != "" {
		disks, err := ch.admin(ctx, "SELECT disk_name,count() AS parts FROM system.parts WHERE database='"+config.Database+"' AND table='"+eventTable+"' AND active GROUP BY disk_name FORMAT JSONEachRow")
		if err != nil || !strings.Contains(string(disks), `"disk_name":"s3"`) {
			t.Fatalf("expired corrected parts did not reside on S3: %v", err)
		}
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	mcpCtx, session := mcpTestSession(t, host.URL, manager, "")
	current := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "get-event", Path: map[string]string{"id": a, "uuid": uuid}}, 200)
	if current["revision"] != edit["revision"] {
		t.Fatal("MCP event lookup or UUID escaping failed")
	}
	snapshot["event"] = "second-correction"
	second := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "edit-event", Path: map[string]string{"id": a, "uuid": uuid}, Body: map[string]any{"expected_revision": edit["revision"], "idempotency_key": "second-edit", "snapshot": snapshot}}, 202)
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	deleteRequest := map[string]any{"expected_revision": second["revision"], "idempotency_key": "delete-once"}
	encodedDelete, _ := json.Marshal(deleteRequest)
	request(t, s, "DELETE", path, reader, string(encodedDelete), 403)
	deleted := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "delete-event", Path: map[string]string{"id": a, "uuid": uuid}, Body: deleteRequest}, 202)
	request(t, s, "GET", path, manager, "", 410)
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	applied := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "get-event-mutation", Path: map[string]string{"id": a, "mutation_id": deleted["mutation_id"].(string)}}, 200)
	if applied["state"] != "applied" {
		t.Fatal("MCP deletion status not applied")
	}
	if query("SELECT count() AS total FROM events", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
		t.Fatal("deletion did not hide event")
	}
	namespace, account, password, err := s.analyticsReader(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ch.query(ctx, account, password, namespace, "SELECT original_snapshot FROM "+config.Database+"."+eventTable, nil); err == nil {
		t.Fatal("reader could access private original evidence")
	}
	if _, err := ch.query(ctx, account, password, namespace, "SELECT event FROM events SETTINGS final=0", nil); err == nil {
		t.Fatal("reader disabled mandatory FINAL")
	}
	for range 2 {
		if err := ch.insert(ctx, originals); err != nil {
			t.Fatal(err)
		}
		raw, err := ch.query(ctx, account, password, namespace, "SELECT count() AS total FROM "+config.Database+"."+eventTable, nil)
		if err != nil || !strings.Contains(string(raw), `"total": "0"`) {
			t.Fatalf("raw reader resurrected deleted history: %v", err)
		}
		if _, err := ch.admin(ctx, "OPTIMIZE TABLE "+config.Database+"."+eventTable+" FINAL"); err != nil {
			t.Fatal(err)
		}
	}
	tombstone, err := ch.loadProjection(ctx, a, uuid)
	if err != nil || !tombstone.IsDeleted || tombstone.Event.Event != "" || tombstone.DistinctID != "" || string(tombstone.Properties) != "{}" || tombstone.OriginalSnapshot != "" {
		t.Fatal("tombstone retained payload or disappeared")
	}
	if tombstone.ID != originals[0].ID || tombstone.ReceivedAt != originals[0].ReceivedAt {
		t.Fatal("deletion changed receipt partition")
	}
	if config.StoragePolicy != "" {
		s.clock = time.Now
		request(t, s, "POST", "/capture/", "", captureBody(ingest, "warm-event"), 200)
		if err := s.flushDelivery(ctx); err != nil {
			t.Fatal(err)
		}
		parts, err := ch.admin(ctx, "SELECT name FROM system.parts WHERE database='"+config.Database+"' AND table='"+eventTable+"' AND active AND disk_name='default' LIMIT 1 FORMAT JSONEachRow")
		var part struct {
			Name string `json:"name"`
		}
		if err != nil || json.Unmarshal(parts, &part) != nil || !mcpPathValue.MatchString(part.Name) {
			t.Fatal("missing warm part before native MOVE")
		}
		if _, err := ch.admin(ctx, "ALTER TABLE "+config.Database+"."+eventTable+" MOVE PART '"+part.Name+"' TO VOLUME 'cold'"); err != nil {
			t.Fatal(err)
		}
		mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": a}, Body: map[string]any{"sql": "SELECT uuid,event FROM events WHERE uuid='warm-event'"}}, 200)
	}
}

func TestClickHouseIntegrationLegacyMigration(t *testing.T) {
	config := integrationClickHouseConfig(t)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, sql := range []string{"CREATE DATABASE " + config.Database, `CREATE TABLE ` + config.Database + `.events (event_id UInt64,project_id String,uuid String,event String,distinct_id String,properties String,timestamp String,timestamp_ms Int64,received_at String,received_at_ms Int64,version UInt64) ENGINE=ReplacingMergeTree(version) PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(received_at_ms,'UTC')) ORDER BY(project_id,uuid)`} {
		if _, err := ch.admin(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	stamp := time.Now().UTC().Add(-45 * 24 * time.Hour)
	projectID := newID()
	for id := 1; id <= 2; id++ {
		name := "original"
		if id == 2 {
			name = "ignored-retry"
		}
		sql := "INSERT INTO " + config.Database + ".events SELECT toUInt64(" + strconv.Itoa(id) + "),'" + projectID + "','legacy','" + name + "','visitor','{\"amount\":1}','" + stamp.Format(time.RFC3339Nano) + "',toInt64(" + strconv.FormatInt(stamp.UnixMilli(), 10) + "),'" + stamp.Format(time.RFC3339Nano) + "',toInt64(" + strconv.FormatInt(stamp.UnixMilli(), 10) + "),toUInt64(18446744073709551615)-toUInt64(" + strconv.Itoa(id) + ")"
		if _, err := ch.admin(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	// An upgrade can encounter project views created by the previous binary.
	if _, err := ch.admin(ctx, "CREATE VIEW "+config.Database+".legacy_project_view AS SELECT event_id,event FROM "+config.Database+".events FINAL"); err != nil {
		t.Fatal(err)
	}
	oldOperator := "old_operator_" + strings.ReplaceAll(newID(), "-", "")
	if _, err := ch.admin(ctx, "CREATE USER "+oldOperator+" HOST NONE"); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.admin(ctx, "CREATE ROW POLICY old_operator_access ON "+config.Database+".events USING 1 TO "+oldOperator); err != nil {
		t.Fatal(err)
	}
	if err := ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	projection, err := ch.loadProjection(ctx, projectID, "legacy")
	if err != nil || projection.Event.Event != "original" || projection.ID != 1 || projection.Revision != 0 || !strings.Contains(projection.OriginalSnapshot, `"amount":1`) {
		t.Fatal("migration lost original data or first-UUID semantics")
	}
	legacy, err := ch.admin(ctx, "EXISTS TABLE "+config.Database+".events")
	if err != nil || strings.TrimSpace(string(legacy)) != "0" {
		t.Fatal("verified legacy table was not retired")
	}
	projection.Revision = 1
	projection.IsDeleted = true
	if err := ch.insertProjections(ctx, []eventProjection{projection}); err != nil {
		t.Fatal(err)
	}
	if err := ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	projection, err = ch.loadProjection(ctx, projectID, "legacy")
	if err != nil || !projection.IsDeleted || projection.OriginalSnapshot != "" {
		t.Fatal("initialization resurrected migrated deleted payload")
	}
	if _, err := ch.admin(ctx, "CREATE TABLE "+config.Database+".events AS "+config.Database+"."+eventTable); err != nil {
		t.Fatal(err)
	}
	if err := ch.initialize(ctx); err == nil {
		t.Fatal("migration accepted unrelated recreated legacy table")
	}
	untouched, err := ch.admin(ctx, "EXISTS TABLE "+config.Database+".events")
	if err != nil || strings.TrimSpace(string(untouched)) != "1" {
		t.Fatal("migration dropped unverified legacy table")
	}
}
