package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestNativeMutationsHTTPAndMCPJourney(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	attachCoreLake(t, s)
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
	if query("SELECT count(*) AS total FROM events WHERE event='original'", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
		t.Fatal("old event predicate resurrected a replaced revision")
	}
	private, err := s.lake.Load(ctx, a, uuid)
	if err != nil || !strings.Contains(private.OriginalSnapshot, `"event":"original"`) {
		t.Fatal("original evidence was lost")
	}
	if private.ID != originals[0].ID || private.ReceivedAt != originals[0].ReceivedAt {
		t.Fatal("correction changed immutable identity or receipt")
	}
	if err := s.lake.Apply(ctx, duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	if query("SELECT count(*) AS total FROM events WHERE event='corrected'", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "1" {
		t.Fatal("late original overwrote correction")
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
	if query("SELECT count(*) AS total FROM events", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
		t.Fatal("deletion did not hide event")
	}
	query("SELECT original_snapshot FROM events", 400)
	for range 2 {
		if err := s.lake.Apply(ctx, duckLakeJourneyProjections(t, originals)); err != nil {
			t.Fatal(err)
		}
		if query("SELECT count(*) AS total FROM events", 200)["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
			t.Fatal("late original resurrected deleted history")
		}
	}

	tombstone, err := s.lake.Load(ctx, a, uuid)
	if err != nil || !tombstone.IsDeleted || tombstone.Event.Event != "" || tombstone.DistinctID != "" || len(tombstone.Properties) != 0 || tombstone.OriginalSnapshot != "" {
		t.Fatal("tombstone retained payload or disappeared")
	}
	if tombstone.ID != originals[0].ID || tombstone.ReceivedAt != originals[0].ReceivedAt {
		t.Fatal("deletion changed receipt partition")
	}
}
