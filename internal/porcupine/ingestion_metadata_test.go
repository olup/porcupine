package porcupine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func normalizedCapture(t *testing.T, s *Service, token string, batch eventBatch, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest("POST", "/ingest/v1/events", nil)
	w := httptest.NewRecorder()
	s.captureNormalized(w, r, eventInput{APIKey: token}, batch)
	if w.Code != status {
		t.Fatalf("normalized capture got %d want %d: %s", w.Code, status, w.Body.String())
	}
	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func normalizedTrack(protocol, message string) eventInput {
	return eventInput{Event: "purchase", DistinctID: "actor", Properties: map[string]any{"token": "business", "distinct_id": "business-distinct", "cookieless": "business-marker"}, ingestion: &ingestionMetadata{Protocol: protocol, MessageID: message, EventType: "track", Context: json.RawMessage(`{"large":9007199254740993,"page":{"url":"https://example.test/checkout"}}`)}}
}

func receiptFrom(t *testing.T, result map[string]any, index int) map[string]any {
	t.Helper()
	return result["receipts"].([]any)[index].(map[string]any)
}

func bindIngestionSource(t *testing.T, s *Service, token, source string) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE access_keys SET source_id=? WHERE token_hash=?`, source, hash(token)); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizedCaptureSourceNamespacesAndLegacyUUIDs(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	bindIngestionSource(t, s, token, "checkout")
	first := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "shared-message")}, 200)
	receipt := receiptFrom(t, first, 0)
	uuid, id := receipt["uuid"].(string), receipt["event_id"].(string)
	if len(uuid) <= 128 || !validStoredEventUUID(uuid) || id == "" || receipt["duplicate"] != false {
		t.Fatal("native event lacks stable disjoint physical UUID and numeric ID", receipt)
	}
	second := normalizedCapture(t, s, token, eventBatch{normalizedTrack("segment", "shared-message")}, 200)
	if duplicate := receiptFrom(t, second, 0); duplicate["event_id"] != id || duplicate["uuid"] != uuid || duplicate["duplicate"] != true {
		t.Fatal("same configured source did not deduplicate across adapters", duplicate)
	}
	posthog := request(t, s, "POST", "/capture/", "", captureBody(token, "shared-message"), 200)
	if posthog["duplicates"] != float64(1) || eventCount(t, s) != 1 {
		t.Fatal("source-bound PostHog did not share producer identity")
	}
	_, rotated := key(t, s, []string{"events:write"}, []string{projectID})
	bindIngestionSource(t, s, rotated, "checkout")
	rotatedResult := normalizedCapture(t, s, rotated, eventBatch{normalizedTrack("native", "shared-message")}, 200)
	if receiptFrom(t, rotatedResult, 0)["event_id"] != id {
		t.Fatal("credential rotation changed stable source identity")
	}
	_, other := key(t, s, []string{"events:write"}, []string{projectID})
	bindIngestionSource(t, s, other, "other-source")
	independent := normalizedCapture(t, s, other, eventBatch{normalizedTrack("native", "shared-message")}, 200)
	if receiptFrom(t, independent, 0)["event_id"] == id {
		t.Fatal("different logical source collided")
	}
	_, legacy := key(t, s, []string{"events:write"}, []string{projectID})
	legacyUUID := sourceEventUUIDPrefix + strings.Repeat("a", 128-len(sourceEventUUIDPrefix))
	request(t, s, "POST", "/capture/", "", captureBody(legacy, legacyUUID), 200)
	var stored string
	if err := s.db.QueryRow(`SELECT uuid FROM events WHERE uuid=?`, legacyUUID).Scan(&stored); err != nil || stored != legacyUUID {
		t.Fatal("legacy PostHog UUID changed", err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(legacy, uuid), 400)
	forged := `{"api_key":"` + legacy + `","uuid":"public-json","event":"purchase","distinct_id":"actor","ingestion":{"Protocol":"native","MessageID":"shared-message"},"source_id":"checkout","message_id":"shared-message"}`
	request(t, s, "POST", "/capture/", "", forged, 200)
	var metadata string
	if err := s.db.QueryRow(`SELECT ingestion_metadata FROM events WHERE uuid='public-json'`).Scan(&metadata); err != nil || metadata != "" {
		t.Fatal("public JSON forged trusted provenance", err)
	}
}

func TestNormalizedMetadataPersistsWithoutChangingBusinessProperties(t *testing.T) {
	s := service(t, 100)
	projectID, token := project(t, s)
	first := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "metadata")}, 200)
	uuid := receiptFrom(t, first, 0)["uuid"].(string)
	projection, _, err := s.currentEvent(context.Background(), projectID, uuid)
	if err != nil || projection.SourceID != "native" || projection.MessageID != "metadata" || projection.EventType != "track" || !strings.Contains(string(projection.Context), "9007199254740993") {
		t.Fatalf("native metadata was lost or rounded: %+v %v", projection, err)
	}
	var props map[string]any
	json.Unmarshal(projection.Properties, &props)
	if props["token"] != "business" || props["distinct_id"] != "business-distinct" || props["cookieless"] != "business-marker" {
		t.Fatal("PostHog-specific property handling changed native business properties")
	}
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 1 || batch[0].MessageID != "metadata" || string(batch[0].Context) != string(projection.Context) {
		t.Fatal("queued delivery lost immutable provenance", err)
	}
	var actual int64
	s.db.QueryRow(`SELECT sum(` + queueByteExpression("") + `) FROM events`).Scan(&actual)
	if count, bytes := queueUsage(t, s); count != 1 || bytes != actual {
		t.Fatal("private provenance is not included in queue capacity")
	}
	privacy := normalizedTrack("native", "privacy")
	privacy.DistinctID = cookielessSentinel
	privacy.ingestion.Context = json.RawMessage(`{"large":9007199254740993,"ip":"192.0.2.3","session_id":"secret","nested":{"anonymousId":"secret","page":{"url":"https://example.test"}},"list":[{"user_id":"secret"}]}`)
	result := normalizedCapture(t, s, token, eventBatch{privacy}, 200)
	projection, _, err = s.currentEvent(context.Background(), projectID, receiptFrom(t, result, 0)["uuid"].(string))
	if err != nil || strings.Contains(string(projection.Context), "secret") || strings.Contains(string(projection.Context), "192.0.2.3") || !strings.Contains(string(projection.Context), "9007199254740993") {
		t.Fatal("cookieless context retained known identity/IP fields or rounded numeric values", err)
	}
}

func TestNormalizedNumericReceiptsSurviveHotColdRestartAndDeletion(t *testing.T) {
	for _, limit := range []int64{0, 1} {
		t.Run(strconv.FormatInt(limit, 10), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "native.db")
			s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, QueueMaxEvents: 100})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { s.Close() }()
			projectID, token := project(t, s)
			first := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "stable"), normalizedTrack("native", "stable")}, 200)
			id, uuid := receiptFrom(t, first, 0)["event_id"], receiptFrom(t, first, 0)["uuid"].(string)
			if receiptFrom(t, first, 1)["event_id"] != id || receiptFrom(t, first, 1)["duplicate"] != true {
				t.Fatal("same-batch duplicate changed numeric ID")
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(Config{DBPath: path})
			if err != nil {
				t.Fatal(err)
			}
			retry := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "stable")}, 200)
			if receiptFrom(t, retry, 0)["event_id"] != id {
				t.Fatal("restart changed queued event ID")
			}
			f := receiptHistory(t)
			s.ch, s.hotReceiptLimit = f.c, limit
			if err := s.flushDelivery(context.Background()); err != nil {
				t.Fatal(err)
			}
			deliverReceipt(t, s, token, "evict-first")
			retry = normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "stable")}, 200)
			if receiptFrom(t, retry, 0)["event_id"] != id || receiptFrom(t, retry, 0)["duplicate"] != true {
				t.Fatal("hot/cold numeric receipt changed accepted event ID")
			}
			// A pending native tombstone keeps its durable numeric ID in the permanent head.
			fresh := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "delete-pending")}, 200)
			freshReceipt := receiptFrom(t, fresh, 0)
			var actorID string
			s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&actorID)
			_, err = s.deleteEvent(mutationContext(actorID), &EventDeleteInput{ID: projectID, UUID: freshReceipt["uuid"].(string), Body: EventDeleteBody{IdempotencyKey: "delete", ExpectedRevision: "0"}})
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			f.unavailable = true
			f.mu.Unlock()
			deletedRetry := normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "delete-pending")}, 200)
			if receiptFrom(t, deletedRetry, 0)["event_id"] != freshReceipt["event_id"] {
				t.Fatal("pending deletion lost numeric event ID")
			}
			_ = uuid
		})
	}
}

func TestNormalizedAtomicAdmissionAndActorlessMutation(t *testing.T) {
	s := service(t, 2)
	projectID, token := project(t, s)
	invalid := normalizedTrack("native", "bad")
	invalid.ingestion.Context = json.RawMessage(`[1]`)
	normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "first"), invalid}, 400)
	if eventCount(t, s) != 0 {
		t.Fatal("invalid translated batch partially committed")
	}
	first := normalizedTrack("native", "actorless")
	first.DistinctID = ""
	result := normalizedCapture(t, s, token, eventBatch{first}, 200)
	uuid := receiptFrom(t, result, 0)["uuid"].(string)
	normalizedCapture(t, s, token, eventBatch{normalizedTrack("native", "second"), normalizedTrack("native", "third")}, 503)
	if eventCount(t, s) != 1 {
		t.Fatal("bounded queue partially accepted translated batch")
	}
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	var actorID string
	s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&actorID)
	edit := mutationEdit(projectID, uuid, "edit-actorless", "0")
	edit.Body.Snapshot.DistinctID = ""
	edit.Body.Snapshot.Properties = map[string]any{"token": "still-business"}
	if _, err := s.editEvent(mutationContext(actorID), edit); err != nil {
		t.Fatal("actorless native event could not preserve empty actor in correction", err)
	}
	current, _, err := s.currentEvent(context.Background(), projectID, uuid)
	if err != nil || current.DistinctID != "" || current.SourceID != "native" || current.MessageID != "actorless" || !strings.Contains(string(current.Properties), "still-business") {
		t.Fatal("correction changed accepted native provenance", err)
	}
}

func TestNormalizedControlContextNeverReachesSQLiteWALOrProjection(t *testing.T) {
	s := service(t, 100)
	_, token := project(t, s)
	accepted := eventInput{Event: "$identify", DistinctID: "account", Properties: map[string]any{"$anon_distinct_id": "anon", "$set": map[string]any{"plan": "approved"}}, ingestion: &ingestionMetadata{Protocol: "native", MessageID: "accepted-control", EventType: "identify", Context: json.RawMessage(`{"ip":"192.0.2.81","private":"accepted-control-context-marker"}`)}}
	ignored := eventInput{Event: "$identify", DistinctID: cookielessSentinel, Properties: map[string]any{"$set": map[string]any{"email": "discarded-control-email-marker"}}, ingestion: &ingestionMetadata{Protocol: "segment", MessageID: "ignored-control", EventType: "identify", Context: json.RawMessage(`{"ip":"192.0.2.82","nested":{"session_id":"ignored-control-context-marker"}}`)}}
	response := normalizedCapture(t, s, token, eventBatch{accepted, ignored}, 200)
	if response["identity_ignored"] != float64(1) {
		t.Fatal("cookieless control was not ignored")
	}
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) != 2 {
		t.Fatal(err)
	}
	for _, event := range batch {
		if string(event.Context) != `{}` || event.DistinctID != "" || string(event.Properties) != `{}` || event.SourceID == "" || event.MessageID == "" || event.EventType != "identify" {
			t.Fatal("control retained unnecessary raw context or lost required provenance")
		}
	}
	markers := []string{"192.0.2.81", "192.0.2.82", "accepted-control-context-marker", "ignored-control-context-marker", "discarded-control-email-marker"}
	files, err := filepath.Glob(s.dbPathForReceiptTest(t) + "*")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, marker := range markers {
			if strings.Contains(string(data), marker) {
				t.Fatalf("discarded control context %q reached SQLite/WAL", marker)
			}
		}
	}
	var projected bool
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Query().Get("query"), "INSERT INTO ") {
			return
		}
		projected = true
		data, _ := io.ReadAll(r.Body)
		for _, marker := range markers {
			if strings.Contains(string(data), marker) {
				t.Errorf("discarded control context %q reached ClickHouse projection", marker)
			}
		}
	})
	if err := c.insert(context.Background(), batch); err != nil || !projected {
		t.Fatal("control projection failed", err)
	}
}
