package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type webhookRoundTrip func(*http.Request) (*http.Response, error)

func (f webhookRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func webhookTestService(t *testing.T, mode string) (*Service, string, string, string) {
	t.Helper()
	s := service(t, 10000)
	rt, err := newPipelineRuntime(&PipelineConfig{Key: bytes.Repeat([]byte{7}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s.pipelines = rt
	p, token := project(t, s)
	var creator string
	s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&creator)
	id := newID()
	config, _ := json.Marshal(WebhookPipelineSpec{DeliveryMode: mode, IntervalSeconds: 60, IncludeEvents: []string{"test_event", "purchase"}, ExcludeEvents: []string{"blocked"}})
	secret, _ := json.Marshal(WebhookPipelineSecret{Endpoint: "https://hooks.example.test/events?private=secret", Headers: map[string]string{"Authorization": "Bearer synthetic", "X-Custom-Key": "private"}})
	encrypted, err := s.sealPipelineSecret(p, id, secret)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES(?,?,?,'webhook','test','active',?,?,?)`, id, p, creator, string(config), encrypted, s.clock().UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	return s, p, token, id
}
func TestWebhookBatchHundredAtomicMembers(t *testing.T) {
	s, p, _, id := webhookTestService(t, "batch")
	now := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	s.clock = func() time.Time { return now }
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for i := range 201 {
		event := Event{ProjectID: p, UUID: fmt.Sprintf("event-%d", i), Event: "purchase", DistinctID: "visitor", Properties: json.RawMessage(`{"x":1}`), ReceivedAt: now.Format(time.RFC3339Nano)}
		if err = s.enqueueWebhook(context.Background(), tx, event); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var chunks, count, members int
	s.db.QueryRow(`SELECT count(*),sum(event_count) FROM webhook_deliveries WHERE pipeline_id=?`, id).Scan(&chunks, &count)
	s.db.QueryRow(`SELECT count(*) FROM webhook_members`).Scan(&members)
	if chunks != 3 || count != 201 || members != 201 {
		t.Fatalf("chunks=%d events=%d members=%d", chunks, count, members)
	}
}
func TestWebhookEventRetryAndCapacity(t *testing.T) {
	s, p, token, id := webhookTestService(t, "event")
	_ = p
	s.pipelines.config.WebhookProjectMaxQueuedEvents = 1
	body := captureBody(token, "one")
	request(t, s, "POST", "/capture/", "", body, 200)
	request(t, s, "POST", "/capture/", "", captureBody(token, "two"), 503)
	request(t, s, "POST", "/capture/", "", body, 200)
	if eventCount(t, s) != 1 {
		t.Fatal("webhook overflow partially accepted originals")
	}
	now := s.clock()
	s.clock = func() time.Time { return now }
	var bodies, ids []string
	status := 503
	s.pipelines.webhookClient = &http.Client{Transport: webhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		ids = append(ids, r.Header.Get("X-Porcupine-Delivery-ID"))
		if r.Method != "POST" || r.Header.Get("Authorization") != "Bearer synthetic" || r.Header.Get("Content-Type") != "application/json" {
			t.Error("bad webhook request")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("ok")), Header: http.Header{}}, nil
	})}
	if progress, err := s.stepWebhook(context.Background()); !progress || err != nil {
		t.Fatal(progress, err)
	}
	now = now.Add(2 * time.Second)
	status = 200
	if progress, err := s.stepWebhook(context.Background()); !progress || err != nil {
		t.Fatal(progress, err)
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] || ids[0] != ids[1] {
		t.Fatal("retry changed stable payload or id")
	}
	var payload string
	var count int
	s.db.QueryRow(`SELECT payload FROM webhook_deliveries WHERE pipeline_id=?`, id).Scan(&payload)
	s.db.QueryRow(`SELECT count FROM webhook_queue_usage WHERE scope='instance'`).Scan(&count)
	if payload != "" || count != 0 {
		t.Fatal("ack did not release queued bytes/events")
	}
	request(t, s, "POST", "/capture/", "", captureBody(token, "two"), 200)
}

func TestWebhookBatchUnicodeEscapesAndByteRotation(t *testing.T) {
	s, p, _, _ := webhookTestService(t, "batch")
	now := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	s.clock = func() time.Time { return now }
	value := strings.Repeat("世界é\"\\<", 24000)
	properties, _ := json.Marshal(map[string]string{"value": value})
	tx, _ := s.db.Begin()
	defer tx.Rollback()
	for i := range 5 {
		event := Event{ProjectID: p, UUID: fmt.Sprintf("unicode-%d", i), Event: "purchase", Properties: properties, ReceivedAt: now.Format(time.RFC3339Nano)}
		if err := s.enqueueWebhook(context.Background(), tx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query(`SELECT payload,bytes,length(CAST(payload AS BLOB)),event_count FROM webhook_deliveries`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	chunks, total := 0, 0
	seen := map[string]bool{}
	for rows.Next() {
		var raw string
		var charged, actual, count int
		if err = rows.Scan(&raw, &charged, &actual, &count); err != nil {
			t.Fatal(err)
		}
		if charged != actual {
			t.Fatal("UTF-8 byte charge mismatch")
		}
		var payload webhookPayload
		if json.Unmarshal([]byte(raw), &payload) != nil || payload.Event != nil || len(payload.Events) != count {
			t.Fatal("invalid appended chunk")
		}
		if actual > webhookChunkBytes && count != 1 {
			t.Fatal("soft byte budget split a row incorrectly")
		}
		for _, event := range payload.Events {
			var got map[string]string
			json.Unmarshal(event.Properties, &got)
			if got["value"] != value || seen[event.UUID] {
				t.Fatal("escaped Unicode changed or duplicated")
			}
			seen[event.UUID] = true
			total++
		}
		chunks++
	}
	if rows.Err() != nil || chunks < 2 || total != 5 {
		t.Fatalf("chunks=%d total=%d err=%v", chunks, total, rows.Err())
	}
}

func TestWebhookDeletePreservesOtherBatchMembersAndAttemptIdentity(t *testing.T) {
	s, p, _, pipeline := webhookTestService(t, "batch")
	now := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	s.clock = func() time.Time { return now }
	tx, _ := s.db.Begin()
	for _, uuid := range []string{"deleted-before-send", "deleted-after-attempt", "survivor"} {
		event := Event{ProjectID: p, UUID: uuid, Event: "purchase", Properties: json.RawMessage(`{}`), ReceivedAt: now.Format(time.RFC3339Nano)}
		if err := s.enqueueWebhook(context.Background(), tx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var originalID string
	s.db.QueryRow(`SELECT id FROM webhook_deliveries`).Scan(&originalID)
	tx, _ = s.db.Begin()
	if err := s.cancelWebhookEvent(context.Background(), tx, p, "deleted-before-send"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	run, err := s.getWebhookPipelineRun(context.Background(), p, pipeline, originalID)
	if err != nil || run.EventCount != 2 || run.State != "queued" {
		t.Fatal(run, err)
	}
	var immutable string
	s.db.QueryRow(`SELECT payload FROM webhook_deliveries WHERE id=?`, originalID).Scan(&immutable)
	s.db.Exec(`UPDATE webhook_deliveries SET state='sending',attempts=1 WHERE id=?`, originalID)
	tx, _ = s.db.Begin()
	if err = s.cancelWebhookEvent(context.Background(), tx, p, "deleted-after-attempt"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var id, raw, state string
	var count int
	s.db.QueryRow(`SELECT id,payload,state,event_count FROM webhook_deliveries WHERE payload<>''`).Scan(&id, &raw, &state, &count)
	if id == originalID || state != "queued" || count != 1 {
		t.Fatal("attempted chunk was reused or survivor lost")
	}
	var payload webhookPayload
	json.Unmarshal([]byte(raw), &payload)
	if len(payload.Events) != 1 || payload.Events[0].UUID != "survivor" || payload.DeliveryID != id {
		t.Fatal("wrong surviving body")
	}
	if err = s.finishWebhook(context.Background(), &webhookDelivery{id: originalID, payload: immutable, attempts: 1}, 200, "", false); err != nil {
		t.Fatal(err)
	}
	var originalState string
	s.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, originalID).Scan(&originalState)
	if originalState != "canceled" {
		t.Fatal("late ACK resurrected canceled body")
	}
	s.db.QueryRow(`SELECT count FROM webhook_queue_usage WHERE scope='instance'`).Scan(&count)
	if count != 1 {
		t.Fatal("survivor queue accounting changed")
	}
}

func TestWebhookPauseRevocationTerminalRetryAndRecovery(t *testing.T) {
	s, _, token, id := webhookTestService(t, "event")
	calls := 0
	status := 400
	s.pipelines.webhookClient = &http.Client{Transport: webhookRoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
	})}
	request(t, s, "POST", "/capture/", "", captureBody(token, "queued"), 200)
	s.db.Exec(`UPDATE pipelines SET state='paused' WHERE id=?`, id)
	if progress, err := s.stepWebhook(context.Background()); progress || err != nil {
		t.Fatal(progress, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(token, "paused-original"), 200)
	s.db.Exec(`UPDATE pipelines SET state='active' WHERE id=?`, id)
	s.db.Exec(`UPDATE access_keys SET revoked_at='revoked' WHERE id=(SELECT actor_id FROM pipelines WHERE id=?)`, id)
	if _, err := s.stepWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatal("paused/revoked pipeline sent data")
	}
	var run, project, state string
	s.db.QueryRow(`SELECT id,project_id,state FROM webhook_deliveries`).Scan(&run, &project, &state)
	if state != "failed" {
		t.Fatal("revocation did not retain a terminal failure")
	}
	s.db.Exec(`UPDATE access_keys SET revoked_at=NULL WHERE id=(SELECT actor_id FROM pipelines WHERE id=?)`, id)
	if retried, err := s.retryWebhookDelivery(context.Background(), project, id, run); !retried || err != nil {
		t.Fatal(retried, err)
	}
	if _, err := s.stepWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("manual retry did not run")
	}
	s.db.Exec(`UPDATE webhook_deliveries SET state='sending' WHERE id=?`, run)
	if err := s.initializeWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	status = 200
	if _, err := s.stepWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("restart failed to recover in-flight delivery")
	}
}

func TestWebhookManualRetryCannotAppendToFrozenBody(t *testing.T) {
	s, p, _, _ := webhookTestService(t, "batch")
	received := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	now := received
	s.clock = func() time.Time { return now }
	enqueue := func(uuid string) {
		t.Helper()
		tx, _ := s.db.Begin()
		defer tx.Rollback()
		if err := s.enqueueWebhook(context.Background(), tx, Event{ProjectID: p, UUID: uuid, Event: "purchase", Properties: json.RawMessage(`{}`), ReceivedAt: received.Format(time.RFC3339Nano)}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	enqueue("original")
	var bodies, ids []string
	status := 400
	s.pipelines.webhookClient = &http.Client{Transport: webhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		ids = append(ids, r.Header.Get("Idempotency-Key"))
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
	})}
	now = received.Add(time.Minute)
	if _, err := s.stepWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	var pipeline string
	s.db.QueryRow(`SELECT pipeline_id FROM webhook_deliveries WHERE id=?`, ids[0]).Scan(&pipeline)
	if retried, err := s.retryWebhookDelivery(context.Background(), p, pipeline, ids[0]); !retried || err != nil {
		t.Fatal(retried, err)
	}
	// Model a request whose server receipt preceded window closure but whose
	// acceptance transaction obtained the SQLite lock after manual retry.
	enqueue("late-accepted")
	var chunks int
	s.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&chunks)
	if chunks != 2 {
		t.Fatal("late capture grew a frozen retried chunk")
	}
	status = 200
	if _, err := s.stepWebhook(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != ids[1] || bodies[0] != bodies[1] {
		t.Fatal("manual retry changed its body or id")
	}
}

func TestWebhookDestinationCooldownLetsHealthyReceiverAdvance(t *testing.T) {
	s, p, token, bad := webhookTestService(t, "event")
	now := s.clock()
	s.clock = func() time.Time { return now }
	var actorID string
	s.db.QueryRow(`SELECT actor_id FROM pipelines WHERE id=?`, bad).Scan(&actorID)
	healthy := newID()
	secret, _ := json.Marshal(WebhookPipelineSecret{Endpoint: "https://healthy.example.test/events"})
	encrypted, _ := s.sealPipelineSecret(p, healthy, secret)
	if _, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) SELECT ?,project_id,actor_id,kind,'healthy',state,config,?,created_at_ms FROM pipelines WHERE id=?`, healthy, encrypted, bad); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(token, "first"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(token, "second"), 200)
	// Give the unhealthy destination an older due time so it is selected first.
	s.db.Exec(`UPDATE webhook_deliveries SET next_attempt_ms=next_attempt_ms-1 WHERE pipeline_id=?`, bad)
	var calls []string
	s.pipelines.webhookClient = &http.Client{Transport: webhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls = append(calls, r.URL.Host)
		status := 200
		if r.URL.Host != "healthy.example.test" {
			status = 503
		}
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	for range 3 {
		if progress, err := s.stepWebhook(context.Background()); !progress || err != nil {
			t.Fatal(progress, err)
		}
	}
	if len(calls) != 3 || calls[0] == "healthy.example.test" || calls[1] != "healthy.example.test" || calls[2] != "healthy.example.test" {
		t.Fatal("failed destination starved healthy work", calls)
	}
	var count int
	s.db.QueryRow(`SELECT count FROM webhook_queue_usage WHERE scope='instance'`).Scan(&count)
	if count != 2 {
		t.Fatal("failed destination payloads stopped counting")
	}
	if progress, err := s.stepWebhook(context.Background()); progress || err != nil {
		t.Fatal("cooldown did not defer untouched bad work", progress, err)
	}
	now = now.Add(2 * time.Second)
	if progress, err := s.stepWebhook(context.Background()); !progress || err != nil {
		t.Fatal("cooldown did not recover", progress, err)
	}
}
