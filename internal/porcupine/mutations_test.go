package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
)

func mutationContext(id string) context.Context {
	return context.WithValue(context.Background(), actorContextKey{}, actor{ID: id, Permissions: []string{"instance:admin"}, Projects: []string{"*"}})
}

func mutationErrorStatus(t *testing.T, err error, status int) {
	t.Helper()
	var api *managementError
	if !errors.As(err, &api) || api.GetStatus() != status {
		t.Fatalf("got error %v, want status %d", err, status)
	}
}

func mutationEdit(project, uuid, key, revision string) *EventEditInput {
	return &EventEditInput{ID: project, UUID: uuid, Body: EventEditBody{IdempotencyKey: key, ExpectedRevision: revision, Snapshot: EventSnapshot{Event: "corrected", DistinctID: "corrected-person", Properties: map[string]any{"amount": 42}, Timestamp: "2026-11-08T01:00:00+01:00"}}}
}

func mutationCount(t *testing.T, s *Service, table string) int {
	t.Helper()
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestMutationQueuedReadCASIdempotencyAndPending(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor-one")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	current, err := s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "original"})
	if err != nil || current.Body.Revision != "0" || current.Body.Pending || current.Body.Event.DistinctID != "user-1" {
		t.Fatalf("queued original unavailable: %+v %v", current, err)
	}
	_, err = s.editEvent(ctx, mutationEdit(projectID, "original", "disabled", "0"))
	mutationErrorStatus(t, err, 503)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	_, err = s.editEvent(ctx, mutationEdit(projectID, "original", "stale", "7"))
	mutationErrorStatus(t, err, 409)
	input := mutationEdit(projectID, "original", "stable-request", "0")
	response, err := s.editEvent(ctx, input)
	if err != nil || response.Body.State != "pending" || response.Body.Revision != "1" {
		t.Fatalf("edit was not durably queued: %+v %v", response, err)
	}
	if mutationCount(t, s, "mutation_audit") != 1 || mutationCount(t, s, "mutation_outbox") != 1 {
		t.Fatal("mutation did not commit audit and payload together")
	}
	current, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "original"})
	if err != nil || !current.Body.Pending || current.Body.Revision != "1" || current.Body.Event.Event != "corrected" || current.Body.Event.Timestamp != "2026-11-08T00:00:00Z" {
		t.Fatalf("pending corrected head unavailable: %+v %v", current, err)
	}
	if current.Body.Event.ID == 0 || current.Body.Event.ReceivedAt == "" {
		t.Fatal("edit changed immutable event metadata")
	}
	retry, err := s.editEvent(ctx, input)
	if err != nil || retry.Body != response.Body || mutationCount(t, s, "mutation_audit") != 1 {
		t.Fatalf("same request was not idempotent: %+v %v", retry, err)
	}
	_, err = s.editEvent(mutationContext("actor-two"), input)
	mutationErrorStatus(t, err, 409)
	for _, change := range []func(*EventEditInput){
		func(i *EventEditInput) { i.UUID = "other-event" },
		func(i *EventEditInput) { i.Body.ExpectedRevision = "1" },
		func(i *EventEditInput) { i.Body.Snapshot.Properties["amount"] = 43 },
	} {
		changed := mutationEdit(projectID, "original", "stable-request", "0")
		change(changed)
		_, err := s.editEvent(ctx, changed)
		mutationErrorStatus(t, err, 409)
	}
	_, err = s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "original", Body: EventDeleteBody{IdempotencyKey: "stable-request", ExpectedRevision: "0"}})
	mutationErrorStatus(t, err, 409)
	_, err = s.editEvent(ctx, mutationEdit(projectID, "original", "next-request", "1"))
	mutationErrorStatus(t, err, 409)
	status, err := s.getEventMutation(ctx, &EventMutationInput{ID: projectID, MutationID: response.Body.MutationID})
	if err != nil || status.Body != response.Body {
		t.Fatalf("status missing: %+v %v", status, err)
	}
	_, err = s.getEventMutation(ctx, &EventMutationInput{ID: "another-project", MutationID: response.Body.MutationID})
	mutationErrorStatus(t, err, 404)
	_, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "unknown"})
	mutationErrorStatus(t, err, 404)
	encoded, _ := json.Marshal(current)
	if strings.Contains(string(encoded), "OriginalSnapshot") || strings.Contains(string(encoded), "original_snapshot") {
		t.Fatal("private original snapshot leaked from current event")
	}
}

func TestMutationCookielessSanitizationAndIdentifiedMarkers(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	cookielessRequest(t, s, ingest, "private", "192.0.2.73:1234", "ua", nil, 200)
	original := storedCookieless(t, s, "private")
	input := mutationEdit(projectID, "private", "private-edit", "0")
	_, err := s.editEvent(ctx, input)
	mutationErrorStatus(t, err, 400)
	input.Body.Snapshot.DistinctID = original.DistinctID
	for _, field := range []string{"$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for", "$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id"} {
		input.Body.Snapshot.Properties[field] = "192.0.2.73"
	}
	input.Body.Snapshot.Properties["cookieless"] = false
	input.Body.Snapshot.Properties["identity_day"] = "fake-day"
	if _, err := s.editEvent(ctx, input); err != nil {
		t.Fatal(err)
	}
	current, err := s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "private"})
	if err != nil || strings.Contains(string(current.Body.Event.Properties), "192.0.2.73") || strings.Contains(string(current.Body.Event.Properties), "fake-day") {
		t.Fatalf("cookieless edit reintroduced identifiers: %+v %v", current, err)
	}
	var props map[string]any
	json.Unmarshal(current.Body.Event.Properties, &props)
	if props["cookieless"] != true || props["distinct_id"] != original.DistinctID || props["identity_day"] == "" {
		t.Fatal("cookieless metadata was changed")
	}
	if input.Body.Snapshot.Properties["$ip"] != "192.0.2.73" {
		t.Fatal("handler mutated caller-owned request map")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "identified"), 200)
	input = mutationEdit(projectID, "identified", "identified-edit", "0")
	input.Body.Snapshot.Properties["cookieless"] = true
	input.Body.Snapshot.Properties["identity_day"] = "fake"
	if _, err := s.editEvent(ctx, input); err != nil {
		t.Fatal(err)
	}
	current, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "identified"})
	if err != nil || strings.Contains(string(current.Body.Event.Properties), "cookieless") || strings.Contains(string(current.Body.Event.Properties), "identity_day") {
		t.Fatal("identified event accepted spoofed server identity markers")
	}
}

func TestMutationQueuedDeleteTombstoneAndDurableReceipt(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "delete-me"), 200)
	before := storedCookieless(t, s, "delete-me")
	var originalID int64
	s.db.QueryRow(`SELECT id FROM events WHERE uuid='delete-me'`).Scan(&originalID)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	input := &EventDeleteInput{ID: projectID, UUID: "delete-me", Body: EventDeleteBody{IdempotencyKey: "delete-request", ExpectedRevision: "0"}}
	response, err := s.deleteEvent(ctx, input)
	if err != nil || response.Body.State != "pending" || eventCount(t, s) != 0 || receiptCount(t, s) != 1 {
		t.Fatalf("queued delete did not replace payload atomically: %+v %v", response, err)
	}
	var payload string
	s.db.QueryRow(`SELECT payload FROM mutation_outbox`).Scan(&payload)
	var tombstone eventProjection
	if json.Unmarshal([]byte(payload), &tombstone) != nil || !tombstone.IsDeleted || tombstone.Event.Event != "" || tombstone.DistinctID != "" || tombstone.Timestamp != "" || string(tombstone.Properties) != `{}` || tombstone.OriginalSnapshot != "" {
		t.Fatal("tombstone retained deleted content")
	}
	if tombstone.ID != originalID || tombstone.ProjectID != projectID || tombstone.UUID != "delete-me" || tombstone.ReceivedAt != before.ReceivedAt {
		t.Fatal("tombstone changed stable partition/identity metadata")
	}
	_, err = s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "delete-me"})
	mutationErrorStatus(t, err, 410)
	retry, err := s.deleteEvent(ctx, input)
	if err != nil || retry.Body != response.Body {
		t.Fatal("pending delete retry was not idempotent", err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "delete-me"), 200)
	if eventCount(t, s) != 0 {
		t.Fatal("capture retry resurrected deleted queued event")
	}
}

func TestMutationRestartPreservesHeadAuditAndOutbox(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "restart"), 200)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	input := mutationEdit(projectID, "restart", "durable-request", "0")
	response, err := s.editEvent(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Durable idempotent retries and queued reads do not need network access.
	retry, err := s.editEvent(ctx, input)
	if err != nil || retry.Body != response.Body {
		t.Fatal("restart lost idempotency", err)
	}
	current, err := s.getEvent(ctx, &EventGetInput{ID: projectID, UUID: "restart"})
	if err != nil || !current.Body.Pending || current.Body.Event.Event != "corrected" || mutationCount(t, s, "mutation_outbox") != 1 || mutationCount(t, s, "mutation_audit") != 1 {
		t.Fatal("restart lost pending snapshot", err)
	}
}

func TestMutationValidationAndAtomicWriteFailure(t *testing.T) {
	s := service(t, 20)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "original"), 200)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	for _, raw := range []string{"", "-1", "+1", "1.0", " 0", "9223372036854775808"} {
		_, err := s.editEvent(ctx, mutationEdit(projectID, "original", "invalid-revision", raw))
		mutationErrorStatus(t, err, 400)
	}
	for _, change := range []func(*EventEditInput){
		func(i *EventEditInput) { i.Body.Snapshot.Event = "" },
		func(i *EventEditInput) { i.Body.Snapshot.Event = "$identify" },
		func(i *EventEditInput) { i.Body.Snapshot.DistinctID = cookielessSentinel },
		func(i *EventEditInput) { i.Body.Snapshot.Properties = nil },
		func(i *EventEditInput) { i.Body.Snapshot.Timestamp = "yesterday" },
	} {
		input := mutationEdit(projectID, "original", "invalid-snapshot", "0")
		change(input)
		_, err := s.editEvent(ctx, input)
		mutationErrorStatus(t, err, 400)
	}
	oversize := mutationEdit(projectID, "original", "too-big", "0")
	oversize.Body.Snapshot.Properties["large"] = strings.Repeat("x", maxBodyBytes)
	_, err := s.editEvent(ctx, oversize)
	mutationErrorStatus(t, err, 413)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_mutation BEFORE INSERT ON mutation_outbox BEGIN SELECT RAISE(FAIL,'disk failure'); END`); err != nil {
		t.Fatal(err)
	}
	_, err = s.deleteEvent(ctx, &EventDeleteInput{ID: projectID, UUID: "original", Body: EventDeleteBody{IdempotencyKey: "atomic-delete", ExpectedRevision: "0"}})
	mutationErrorStatus(t, err, 503)
	if eventCount(t, s) != 1 || receiptCount(t, s) != 0 || mutationCount(t, s, "mutation_audit") != 0 || mutationCount(t, s, "event_heads") != 0 {
		t.Fatal("failed mutation partially committed")
	}
}

func TestMutationOutboxGlobalCountAndByteBounds(t *testing.T) {
	s := service(t, 1)
	ctx := mutationContext("actor")
	projectID, ingest := project(t, s)
	s.ch = testClickHouse(t, func(http.ResponseWriter, *http.Request) {})
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "first"), 200)
	if _, err := s.editEvent(ctx, mutationEdit(projectID, "first", "first-mutation", "0")); err != nil {
		t.Fatal(err)
	}
	// Remove only the original queue payload, leaving its mutation pending globally.
	if _, err := s.db.Exec(`DELETE FROM events`); err != nil {
		t.Fatal(err)
	}
	otherProject, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "other-project"), 200)
	_, err := s.editEvent(ctx, mutationEdit(otherProject, "other-project", "second-mutation", "0"))
	mutationErrorStatus(t, err, 503)
	if mutationCount(t, s, "mutation_audit") != 1 {
		t.Fatal("queue bound committed rejected audit")
	}
	s.queueMax = 2000
	if _, err := s.db.Exec(`UPDATE mutation_outbox SET payload=CAST(zeroblob(?) AS TEXT)`, mutationMaxBytes); err != nil {
		t.Fatal(err)
	}
	_, err = s.editEvent(ctx, mutationEdit(otherProject, "other-project", "byte-bound", "0"))
	mutationErrorStatus(t, err, 503)
	if mutationCount(t, s, "mutation_audit") != 1 {
		t.Fatal("byte bound committed rejected audit")
	}
	if _, err := s.db.Exec(`DELETE FROM mutation_outbox`); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := range 1000 {
		uuid := "cap-" + strings.Repeat("x", i/100) + string(rune(1000+i))
		result, err := tx.Exec(`INSERT INTO mutation_audit(mutation_id,project_id,uuid,actor_id,operation,idempotency_key_hash,request_hash,expected_revision,state,created_at) VALUES(?,?,?,'actor','edit',?,'hash',0,'pending','now')`, uuid, projectID, uuid, uuid)
		if err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		seq, _ := result.LastInsertId()
		if _, err := tx.Exec(`INSERT INTO mutation_outbox(seq,project_id,uuid,payload) VALUES(?,?,?,'{}')`, seq, projectID, uuid); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = s.editEvent(ctx, mutationEdit(otherProject, "other-project", "hard-count-bound", "0"))
	mutationErrorStatus(t, err, 503)
	if mutationCount(t, s, "mutation_outbox") != 1000 {
		t.Fatal("hard mutation count bound exceeded")
	}
}
