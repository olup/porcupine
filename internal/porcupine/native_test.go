package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func nativeRequest(t *testing.T, s *Service, key, raw string, status int) map[string]any {
	t.Helper()
	req := httptest.NewRequest("POST", "/ingest/v1/events", strings.NewReader(raw))
	if key != "" {
		req.Header.Set("X-Porcupine-Key", key)
	}
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, req)
	if response.Code != status {
		t.Fatalf("native capture returned %d, expected %d: %s", response.Code, status, response.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestNativeJourneyReceiptsAndBusinessProperties(t *testing.T) {
	s := service(t, 10)
	projectID, key := project(t, s)
	const messages = `[
{"type":"pageview","message_id":"msg-001","actor_id":"browser-7","context":{"page":{"url":"https://example.test/pricing"},"locale":"fr-FR"}},
{"type":"identify","message_id":"msg-002","user_id":"user-42","anonymous_id":"browser-7","set":{"plan":"pro"},"set_once":{"origin":"docs"}},
{"type":"track","message_id":"msg-003","name":"order_completed","actor_id":"user-42","properties":{"amount_minor":2900,"large_id":9007199254740993,"token":"business-token"}}
]`
	first := nativeRequest(t, s, key, messages, 200)
	retry := nativeRequest(t, s, key, messages, 200)
	if first["accepted"] != float64(3) || retry["duplicates"] != float64(3) {
		t.Fatalf("unexpected capture counts: %v %v", first, retry)
	}
	firstReceipts := first["receipts"].([]any)
	replayed := retry["receipts"].([]any)
	if len(firstReceipts) != 3 || len(replayed) != 3 {
		t.Fatal("native capture must return a receipt per input")
	}
	seen := map[string]bool{}
	for i, item := range firstReceipts {
		r := item.(map[string]any)
		dup := replayed[i].(map[string]any)
		id, ok := r["event_id"].(string)
		if !ok || id == "" || seen[id] || id != dup["event_id"] || r["message_id"] != dup["message_id"] || dup["duplicate"] != true {
			t.Fatalf("unstable native event ID: %v, retry %v", r, dup)
		}
		seen[id] = true
	}
	var props, target string
	if err := s.db.QueryRow("SELECT properties FROM events WHERE project_id=? AND event='order_completed'", projectID).Scan(&props); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(props, `"large_id":9007199254740993`) || !strings.Contains(props, `"token":"business-token"`) {
		t.Fatal("changed a producer property:", props)
	}
	if err := s.db.QueryRow("SELECT identified_id FROM identity_links WHERE project_id=? AND anonymous_id='browser-7'", projectID).Scan(&target); err != nil || target != "user-42" {
		t.Fatalf("historical anonymous link missing: %q, %v", target, err)
	}
}

func TestNativeRejectsForgedOrAmbiguousMessagesAtomically(t *testing.T) {
	s := service(t, 10)
	p, k := project(t, s)
	good := `{"type":"track","name":"ping","message_id":"one"}`
	for _, malformed := range []string{
		`{"type":"track","name":"$identify"}`,
		`{"type":"identify","actor_id":"a","user_id":"u"}`,
		`{"type":"pageview","source_id":"spoof"}`,
		`{"type":"track","name":"ping","event_id":"42"}`,
		`{"type":"track","name":"ping","project_id":"other"}`,
		`{"type":"set","actor_id":"u"}`,
		`{"type":"track","name":"ping","set":{"x":1}}`,
		`null`,
	} {
		nativeRequest(t, s, k, "["+good+","+malformed+"]", 400)
	}
	for _, raw := range []string{good + " {}", "[]", "", "[" + good, strings.Repeat(" ", 10) + "[" + strings.Repeat(good+",", 1000) + good + "]"} {
		nativeRequest(t, s, k, raw, 400)
	}
	var count int
	if err := s.db.QueryRow("SELECT count(*) FROM events WHERE project_id=?", p).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial native batch: rows=%d, %v", count, err)
	}
	nativeRequest(t, s, "", good, 401)
	first := nativeRequest(t, s, k, good, 200)
	if first["accepted"] != float64(1) {
		t.Fatal("actorless backend track rejected")
	}
	var id string
	if err := s.db.QueryRow("SELECT distinct_id FROM events WHERE project_id=?", p).Scan(&id); err != nil || id != "" {
		t.Fatalf("invented actor for actorless event: %q %v", id, err)
	}
}

func TestNativeNoMessageIDCreatesDistinctReceipts(t *testing.T) {
	s := service(t, 10)
	_, k := project(t, s)
	const raw = `{"type":"track","name":"inventory_restocked"}`
	first := nativeRequest(t, s, k, raw, 200)["receipts"].([]any)[0].(map[string]any)
	second := nativeRequest(t, s, k, raw, 200)["receipts"].([]any)[0].(map[string]any)
	if first["event_id"] == second["event_id"] || first["message_id"] == second["message_id"] {
		t.Fatal("independent events got the same receipt")
	}
}

func TestNativeReceiptIsAddressableOverMCP(t *testing.T) {
	s := service(t, 5)
	p, k := project(t, s)
	response := nativeRequest(t, s, k, `{"type":"track","name":"inventory_restocked","message_id":"inventory-1"}`, 200)
	receipt := response["receipts"].([]any)[0].(map[string]any)
	physicalID := receipt["uuid"].(string)
	if !validStoredEventUUID(physicalID) || len(physicalID) <= 128 {
		t.Fatalf("new receipt UUID overlaps the legacy namespace: %q", physicalID)
	}
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, session := mcpTestSession(t, host.URL, adminToken, "2025-11-25")
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event", Path: map[string]string{"id": p, "uuid": physicalID}}, 200)
	event := result["event"].(map[string]any)
	if event["uuid"] != physicalID || event["message_id"] != "inventory-1" {
		t.Fatalf("MCP did not resolve receipt: %v", result)
	}
	for _, invalid := range []string{strings.Repeat("x", 129), sourceEventUUIDPrefix + strings.Repeat("!", 128)} {
		mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event", Path: map[string]string{"id": p, "uuid": invalid}}, 400)
	}
}
