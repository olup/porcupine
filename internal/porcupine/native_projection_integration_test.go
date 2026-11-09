package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationNativeSegmentPostHogJourney(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "mixed-journey.db"), BootstrapToken: adminToken, ClickHouse: &config})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	call := func(method, path string, body any, headers map[string]string, status int) map[string]any {
		t.Helper()
		var raw []byte
		if body != nil {
			raw, _ = json.Marshal(body)
		}
		req, e := http.NewRequest(method, host.URL+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		res, e := client.Do(req)
		if e != nil {
			t.Fatal("mixed journey HTTP failed")
		}
		defer res.Body.Close()
		if res.StatusCode != status {
			t.Fatalf("%s %s status %d want %d", method, path, res.StatusCode, status)
		}
		out := map[string]any{}
		data, e := io.ReadAll(io.LimitReader(res.Body, 4<<20))
		if e != nil {
			t.Fatal(e)
		}
		if len(data) > 0 && json.Unmarshal(data, &out) != nil {
			t.Fatal("invalid API JSON")
		}
		return out
	}
	admin := map[string]string{"Authorization": "Bearer " + adminToken}
	created := call("POST", "/api/v1/projects", map[string]any{"name": "synthetic mixed adapter journey"}, admin, 201)
	projectID := created["project"].(map[string]any)["id"].(string)
	postHogKey := created["ingest_token"].(string)
	foreign := call("POST", "/api/v1/projects", map[string]any{"name": "synthetic separate adapter project"}, admin, 201)
	foreignID := foreign["project"].(map[string]any)["id"].(string)
	sourceKey := func(project, source string) string {
		t.Helper()
		v := call("POST", "/api/v1/access-keys", map[string]any{"name": "synthetic " + source, "source_id": source, "permissions": []string{"events:write"}, "projects": []string{project}}, admin, 201)
		return v["token"].(string)
	}
	nativeKey, segmentKey, sharedKey := sourceKey(projectID, "native-web"), sourceKey(projectID, "segment-web"), sourceKey(projectID, "shared-wire")
	foreignKey := sourceKey(foreignID, "native-web")
	contextFields := map[string]any{"page": map[string]any{"url": "https://example.test/p?q=東京&n=1", "referrer": "https://referrer.test/", "title": "café 東京", "path": "/p", "search": "?q=東京&n=1"}, "browser": map[string]any{"name": "Chrome", "version": "132"}, "os": map[string]any{"name": "Mac OS X", "version": "15.1"}, "device": map[string]any{"type": "Desktop"}, "locale": "fr-FR", "session_id": "session-a"}
	properties := map[string]any{"$current_url": "https://example.test/p?q=東京&n=1", "$referrer": "https://referrer.test/", "$title": "café 東京", "$pathname": "/p", "$search": "?q=東京&n=1", "$browser": "Chrome", "$browser_version": 132, "$os": "Mac OS X", "$os_version": "15.1", "$device_type": "Desktop", "$browser_language": "fr-FR", "$session_id": "session-a"}
	postHogPage := map[string]any{"api_key": postHogKey, "uuid": "page-posthog", "event": "$pageview", "distinct_id": "journey-anon", "timestamp": "2026-01-01T00:00:00.123Z", "properties": properties}
	call("POST", "/capture/", postHogPage, nil, 200)
	nativePage := map[string]any{"type": "pageview", "message_id": "page-native", "actor_id": "journey-anon", "context": contextFields, "timestamp": "2026-01-01T00:00:00.123Z"}
	nativeHeaders := map[string]string{"X-Porcupine-Key": nativeKey}
	nativeReceipt := call("POST", "/ingest/v1/events", nativePage, nativeHeaders, 200)
	retryReceipt := call("POST", "/ingest/v1/events", nativePage, nativeHeaders, 200)
	if retryReceipt["duplicates"] != float64(1) || nativeReceipt["receipts"].([]any)[0].(map[string]any)["event_id"] != retryReceipt["receipts"].([]any)[0].(map[string]any)["event_id"] {
		t.Fatal("native durable receipt changed across retry")
	}
	call("POST", "/v1/page", map[string]any{"writeKey": segmentKey, "messageId": "page-segment", "anonymousId": "journey-anon", "context": contextFields, "timestamp": "2026-01-01T00:00:00.123Z"}, nil, 200)
	call("POST", "/v1/identify", map[string]any{"writeKey": segmentKey, "messageId": "identify-segment", "anonymousId": "journey-anon", "userId": "journey-user", "traits": map[string]any{"name": "café 東京", "plan": "pro"}}, nil, 200)
	nativeTrack := map[string]any{"type": "track", "message_id": "track-native", "actor_id": "journey-user", "name": "Purchase", "properties": map[string]any{"large_id": json.Number("9007199254740993"), "amount": 42}, "context": map[string]any{"page": contextFields["page"], "additional": map[string]any{"preserved": true}, "session_id": "session-a"}}
	call("POST", "/ingest/v1/events", nativeTrack, nativeHeaders, 200)
	call("POST", "/capture/", map[string]any{"api_key": postHogKey, "uuid": "track-posthog", "event": "Purchase", "distinct_id": "journey-user", "properties": map[string]any{"amount": 42}}, nil, 200)
	call("POST", "/v1/batch", map[string]any{"writeKey": segmentKey, "batch": []any{map[string]any{"type": "track", "messageId": "track-segment", "userId": "journey-user", "event": "Purchase", "properties": map[string]any{"amount": 42}, "context": map[string]any{"additional": map[string]any{"preserved": true}}}}}, nil, 200)
	call("POST", "/capture/", map[string]any{"api_key": sharedKey, "uuid": "cross-wire", "event": "Shared", "distinct_id": "journey-user", "properties": map[string]any{"$current_url": "https://example.test/shared"}}, nil, 200)
	sharedRetry := call("POST", "/ingest/v1/events", map[string]any{"type": "track", "message_id": "cross-wire", "actor_id": "journey-user", "name": "Shared"}, map[string]string{"X-Porcupine-Key": sharedKey}, 200)
	if sharedRetry["duplicates"] != float64(1) {
		t.Fatal("authenticated source/message must deduplicate across adapters")
	}
	call("POST", "/ingest/v1/events", nativePage, map[string]string{"X-Porcupine-Key": foreignKey}, 200)
	call("POST", "/ingest/v1/events", map[string]any{"type": "identify", "message_id": "foreign-identify", "anonymous_id": "journey-anon", "user_id": "private-other-project", "set": map[string]any{"private": true}}, map[string]string{"X-Porcupine-Key": foreignKey}, 200)
	call("POST", "/ingest/v1/events", map[string]any{"type": "track", "name": "cookieless_probe", "message_id": "privacy-native", "actor_id": cookielessSentinel, "context": map[string]any{"session_id": "must-remove", "ip": "192.0.2.1", "nested": map[string]any{"user_id": "must-remove"}, "page": map[string]any{"url": "https://example.test/"}}}, nativeHeaders, 200)
	drain := func() {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for {
			var pending int
			if s.db.QueryRow(`SELECT (SELECT count(*) FROM events)+(SELECT count(*) FROM mutation_outbox)`).Scan(&pending) != nil {
				t.Fatal("queue unavailable")
			}
			if pending == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("mixed journey projection did not drain")
			}
			if err := s.flushDelivery(context.Background()); err != nil {
				t.Fatal("mixed journey projection failed")
			}
		}
	}
	drain()
	// An older physical PostHog row has no new metadata bytes at all. Defaults
	// must enrich it without a backfill or access to private original_snapshot.
	legacy := Event{ID: 100000, ProjectID: projectID, UUID: "legacy-posthog", Event: "$pageview", DistinctID: "journey-anon", Properties: metadataProperties(t, properties), Timestamp: "2026-01-01T00:00:00.123Z", ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	snapshot, _ := originalSnapshot(legacy)
	timestamp, _ := time.Parse(time.RFC3339Nano, legacy.Timestamp)
	received, _ := time.Parse(time.RFC3339Nano, legacy.ReceivedAt)
	raw, _ := json.Marshal(map[string]any{"event_id": legacy.ID, "project_id": projectID, "uuid": legacy.UUID, "event": legacy.Event, "distinct_id": legacy.DistinctID, "properties": string(legacy.Properties), "timestamp": legacy.Timestamp, "timestamp_ms": timestamp.UnixMilli(), "received_at": legacy.ReceivedAt, "received_at_ms": received.UnixMilli(), "revision": 0, "is_deleted": 0, "original_snapshot": snapshot})
	if _, err = s.ch.admin(context.Background(), "INSERT INTO "+config.Database+"."+eventTable+" FORMAT JSONEachRow\n"+string(raw)); err != nil {
		t.Fatal("insert legacy projection fixture")
	}
	query := func(sql string) []map[string]any {
		t.Helper()
		result := call("POST", "/api/v1/projects/"+projectID+"/query", map[string]any{"sql": sql}, admin, 200)
		data := result["result"].(map[string]any)["data"].([]any)
		rows := make([]map[string]any, len(data))
		for i, r := range data {
			rows[i] = r.(map[string]any)
		}
		return rows
	}
	rows := query("SELECT id,event_id,uuid,source_id,message_id,event_type,context,distinct_id,person_id,person_properties,properties FROM events WHERE event!='cookieless_probe' ORDER BY id")
	if len(rows) != 8 {
		t.Fatalf("mixed journey count %d, wanted8 ordinaryevents", len(rows))
	}
	byMessage := map[string]map[string]any{}
	for _, row := range rows {
		if row["id"] != row["event_id"] {
			t.Fatal("event_id alias changed durableID")
		}
		if _, err := strconv.ParseUint(row["event_id"].(string), 10, 64); err != nil {
			t.Fatal("event ID precision lost")
		}
		if row["person_id"] != "journey-user" {
			t.Fatal("cross-adapter historical journey not identified")
		}
		if strings.Contains(row["person_properties"].(string), "private-other-project") {
			t.Fatal("foreign identity leaked")
		}
		message := row["message_id"].(string)
		byMessage[message] = row
		if strings.HasPrefix(message, "page-") || message == "legacy-posthog" {
			if row["event_type"] != "pageview" || row["distinct_id"] != "journey-anon" {
				t.Fatal("page category or original anonymous identity changed")
			}
			var got any
			json.Unmarshal([]byte(row["context"].(string)), &got)
			if !reflect.DeepEqual(got, contextFields) {
				t.Fatalf("context parity failed for %s: %s", message, row["context"])
			}
		}
	}
	if byMessage["page-posthog"]["source_id"] != "posthog" || byMessage["page-native"]["source_id"] != "native-web" || byMessage["page-segment"]["source_id"] != "segment-web" || byMessage["cross-wire"]["source_id"] != "shared-wire" {
		t.Fatal("credential-bound source metadata was lost or spoofed")
	}
	if !strings.Contains(byMessage["track-native"]["properties"].(string), `"large_id":9007199254740993`) {
		t.Fatal("large business property lost precision")
	}
	private := query("SELECT distinct_id,context FROM events WHERE event='cookieless_probe'")
	if len(private) != 1 || !strings.HasPrefix(private[0]["distinct_id"].(string), "cookieless_") || strings.Contains(private[0]["context"].(string), "must-remove") || strings.Contains(private[0]["context"].(string), "192.0.2.1") {
		t.Fatal("known identity/IP reappeared in canonical cookieless context")
	}
	catalog := call("GET", "/api/v1/projects/"+projectID+"/catalog?event=Purchase", nil, admin, 200)
	events := catalog["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["latest_event_uuid"] != byMessage["track-segment"]["uuid"] {
		t.Fatal("catalog latest event pointer lost source-scoped UUID")
	}
	pointer := events[0].(map[string]any)["latest_event_uuid"].(string)
	if len(pointer) <= 128 {
		t.Fatal("catalog fixture did not exercise long native UUID")
	}
	call("GET", "/api/v1/projects/"+projectID+"/events/"+pointer, nil, admin, 200)
	foundAmount := false
	for _, property := range catalog["properties"].([]any) {
		prop := property.(map[string]any)
		if prop["key"] == "amount" {
			foundAmount = true
			uuid := prop["latest_event_uuid"].(string)
			if uuid != pointer {
				t.Fatal("catalog property pointer differs from newest source message")
			}
			call("GET", "/api/v1/projects/"+projectID+"/events/"+uuid, nil, admin, 200)
		}
	}
	if !foundAmount {
		t.Fatal("catalog property discovery omitted source-aware event")
	}
	// Corrections preserve accepted provenance even when editable event fields
	// and properties are replaced. A legacy-row edit freezes derived context too.
	for _, message := range []string{"track-native", "legacy-posthog"} {
		before := byMessage[message]
		uuid := before["uuid"].(string)
		path := "/api/v1/projects/" + projectID + "/events/" + uuid
		current := call("GET", path, nil, admin, 200)
		name := "Purchase corrected"
		if message == "legacy-posthog" {
			name = "$pageview"
		}
		call("PATCH", path, map[string]any{"idempotency_key": "metadata-correct-" + message, "expected_revision": current["revision"], "snapshot": map[string]any{"event": name, "distinct_id": before["distinct_id"], "properties": map[string]any{"$current_url": "https://changed.invalid/"}, "timestamp": "2026-02-01T00:00:00Z"}}, admin, 202)
		drain()
		after := query("SELECT source_id,message_id,event_type,context FROM events WHERE uuid='" + uuid + "'")
		if len(after) != 1 {
			t.Fatal("corrected row missing")
		}
		for _, field := range []string{"source_id", "message_id", "event_type", "context"} {
			var old, new any
			if field == "context" {
				json.Unmarshal([]byte(before[field].(string)), &old)
				json.Unmarshal([]byte(after[0][field].(string)), &new)
			} else {
				old, new = before[field], after[0][field]
			}
			if !reflect.DeepEqual(old, new) {
				t.Fatalf("correction changed immutable %s", field)
			}
		}
	}
	// Both restricted export profiles see the same extended public columns.
	limits, _ := normalizeExportConfig(ExportConfig{MaxExecutionSeconds: 30})
	s.exports = &exportRuntime{config: limits}
	defer func() { s.exports = nil }()
	ns, reader, password, err := s.exportReader(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	for _, scheduled := range []bool{false, true} {
		sql := "SELECT event_id,source_id,message_id,event_type,context FROM events WHERE event!='cookieless_probe' ORDER BY id"
		var params map[string]string
		if scheduled {
			sql, err = pipelineWindowSQL(sql, ns)
			if err != nil {
				t.Fatal(err)
			}
			params = map[string]string{"window_start": "0", "window_end": "9223372036854775807"}
		}
		response, e := s.ch.openExport(context.Background(), ns, reader, password, "native_metadata_"+strings.ReplaceAll(newID(), "-", ""), sql, params)
		if e != nil {
			t.Fatal(e)
		}
		result, e := streamExportJSON(context.Background(), response, 1<<20, exportStreamCallbacks{})
		response.Close()
		if e != nil || result.Rows != 8 {
			t.Fatalf("export/pipeline metadata differs: %v", e)
		}
	}
	// Tenant boundaries apply to the extended metadata and original event IDs.
	outsider := call("POST", "/api/v1/access-keys", map[string]any{"name": "foreign reader", "permissions": []string{"events:read"}, "projects": []string{foreignID}}, admin, 201)["token"].(string)
	call("POST", "/api/v1/projects/"+projectID+"/query", map[string]any{"sql": "SELECT source_id,message_id,context FROM events"}, map[string]string{"Authorization": "Bearer " + outsider}, 403)
	t.Log(fmt.Sprintf("mixed native/Segment/PostHog journey: %d ordinary rows, source/context parity, correction and isolation verified", len(rows)))
}

func TestClickHouseIntegrationNativeColdReceiptNumericID(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "native-cold-receipts.db"), BootstrapToken: adminToken, ClickHouse: &config, HotReceiptLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	projectID, key := project(t, s)
	batch := `[{"type":"track","message_id":"first-cold","actor_id":"synthetic","name":"Receipt"},{"type":"track","message_id":"second-hot","actor_id":"synthetic","name":"Receipt"}]`
	first := nativeRequest(t, s, key, batch, 200)
	firstID := first["receipts"].([]any)[0].(map[string]any)["event_id"].(string)
	deadline := time.Now().Add(20 * time.Second)
	for {
		var queued, hot int
		if s.db.QueryRow(`SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM event_receipts)`).Scan(&queued, &hot) != nil {
			t.Fatal("receipt cache query")
		}
		if queued == 0 && hot <= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("finite native receipts did not settle")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal("finite native delivery failed")
		}
	}
	result, err := s.analyticsQuery(context.Background(), queryInput(projectID, "SELECT uuid,event_id,message_id FROM events ORDER BY id"))
	if err != nil || result.Body.Result.Rows != 2 {
		t.Fatal("native receipt projection unavailable")
	}
	var uuid, message string
	for _, row := range result.Body.Result.Data {
		var candidate string
		json.Unmarshal(row["uuid"], &candidate)
		var hot bool
		if s.db.QueryRow(`SELECT EXISTS(SELECT 1 FROM event_receipts WHERE project_id=? AND uuid=?)`, projectID, candidate).Scan(&hot) != nil {
			t.Fatal("cache lookup")
		}
		if !hot {
			uuid = candidate
			json.Unmarshal(row["message_id"], &message)
			json.Unmarshal(row["event_id"], &firstID)
			break
		}
	}
	if uuid == "" {
		t.Fatal("fixture did not evict any native receipt")
	}
	originalID := ""
	for _, item := range first["receipts"].([]any) {
		r := item.(map[string]any)
		if r["message_id"] == message {
			originalID = r["event_id"].(string)
		}
	}
	if originalID == "" || originalID != firstID {
		t.Fatal("physical numeric ID differs from acknowledged receipt")
	}

	retry := nativeRequest(t, s, key, fmt.Sprintf(`{"type":"track","message_id":%q,"actor_id":"synthetic","name":"Receipt"}`, message), 200)
	if retry["duplicates"] != float64(1) || retry["receipts"].([]any)[0].(map[string]any)["event_id"] != firstID {
		t.Fatal("cold native replay lost original numeric eventID")
	}
	// Native tombstones clear producer metadata while physical history still
	// proves the immutable original numeric receipt ID.
	current, _, err := s.currentEvent(context.Background(), projectID, uuid)
	if err != nil {
		t.Fatal(err)
	}
	ctx := exportContext(t, s, adminToken)
	deleted, err := s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: uuid, Body: EventDeleteBody{IdempotencyKey: "native-delete", ExpectedRevision: strconv.FormatUint(current.Revision, 10)}})
	if err != nil || deleted.Body.State != "pending" {
		t.Fatal("native deletion unavailable")
	}
	for {
		var queued int
		s.db.QueryRow(`SELECT count(*) FROM mutation_outbox`).Scan(&queued)
		if queued == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("native deletion did not project")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	_, ids, err := s.ch.receiptFactsWithIDs(context.Background(), []receiptKey{{ProjectID: projectID, UUID: uuid}})
	expected, _ := strconv.ParseInt(firstID, 10, 64)
	if err != nil || ids[receiptKey{ProjectID: projectID, UUID: uuid}] != expected {
		t.Fatal("physical tombstone receipt numeric identity lost")
	}
	projection, err := s.ch.loadProjection(context.Background(), projectID, uuid)
	if err != nil || !projection.IsDeleted || projection.SourceID != "" || projection.MessageID != "" || projection.EventType != "" || len(projection.Context) != 0 {
		t.Fatal("native tombstone retained producer metadata")
	}
}
