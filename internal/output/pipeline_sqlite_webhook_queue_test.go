package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	_ "modernc.org/sqlite"
)

type webhookTestHost struct {
	authorize func() bool
	finish    func()
}

func (h webhookTestHost) AuthorizeWebhook(_ context.Context, actor, project string) (bool, error) {
	if actor != "actor" || project != "project" {
		return false, errors.New("unexpected authority scope")
	}
	return h.authorize(), nil
}
func (webhookTestHost) OpenWebhookSecret(project, pipeline string, blob []byte) ([]byte, error) {
	if project != "project" || pipeline != "pipeline" {
		return nil, errors.New("unexpected secret scope")
	}
	return blob, nil
}
func (h webhookTestHost) BeginWebhookActivity(ctx context.Context, project string) (context.Context, func(), bool) {
	if project != "project" {
		return ctx, nil, false
	}
	return ctx, h.finish, true
}

func webhookQueueFixture(t *testing.T, host webhookTestHost) (*sqliteWebhookQueue, *time.Time) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "journal.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	schema, err := os.ReadFile("../porcupine/journal_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(schema) + PipelineSQLiteRetirementSchemaV1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys=ON;
INSERT INTO metadata VALUES('initialized','1');
INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES('tenant','Tenant','now',1,1000,100,4096);
INSERT INTO projects(id,tenant_id,name,created_at) VALUES('project','tenant','Project','now');
INSERT INTO access_keys VALUES('actor','Creator','synthetic','[]','[]',NULL,'now');
INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES('pipeline','project','actor','webhook','Destination','active','{"delivery_mode":"event"}',CAST('{"endpoint":"https://hooks.example.test/events"}' AS BLOB),1)`); err != nil {
		t.Fatal(err)
	}
	if host.authorize == nil {
		host.authorize = func() bool { return true }
	}
	if host.finish == nil {
		host.finish = func() {}
	}
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	q := newWebhookQueue(db, &webhookLimits{MaxQueuedEvents: 100, MaxQueuedBytes: 1 << 20, ProjectMaxQueuedEvents: 100, ProjectMaxQueuedBytes: 1 << 20, MaxAttempts: 8, TimeoutSeconds: 10}, func() time.Time { return now }, host.AuthorizeWebhook, host.OpenWebhookSecret, host.BeginWebhookActivity)
	t.Cleanup(q.Client.CloseIdleConnections)
	if err := initializeWebhooks(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	return q, &now
}

func enqueueWebhookTestEvent(t *testing.T, q *sqliteWebhookQueue) string {
	t.Helper()
	tx, err := q.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := q.Enqueuer().Enqueue(t.Context(), tx, []eventapi.Event{{ProjectID: "project", UUID: "event", Event: "purchase", Properties: json.RawMessage(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var id string
	if err := q.db.QueryRow(`SELECT id FROM webhook_deliveries`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestWebhookEnqueueAndCancelKeepCallerTransaction(t *testing.T) {
	t.Parallel()
	q, _ := webhookQueueFixture(t, webhookTestHost{})
	tx, err := q.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueuer().Enqueue(t.Context(), tx, []eventapi.Event{{ProjectID: "project", UUID: "rolled-back", Event: "purchase", Properties: json.RawMessage(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal("enqueuer committed caller transaction", err)
	}
	var count int
	if err := q.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&count); err != nil || count != 0 {
		t.Fatal("rollback retained a delivery", count, err)
	}
	id := enqueueWebhookTestEvent(t, q)
	tx, err = q.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := CancelWebhookEvent(t.Context(), tx, "project", "event", q.clock); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal("cancellation committed caller transaction", err)
	}
	var state, payload string
	if err := q.db.QueryRow(`SELECT state,payload FROM webhook_deliveries WHERE id=?`, id).Scan(&state, &payload); err != nil || state != "queued" || payload == "" {
		t.Fatal("rollback changed pending delivery", state, payload, err)
	}
}

func TestWebhookRevocationRetryAndCooldownPersist(t *testing.T) {
	t.Parallel()
	authorizations := 0
	revoked := true
	q, now := webhookQueueFixture(t, webhookTestHost{authorize: func() bool {
		authorizations++
		return !revoked || authorizations == 1
	}})
	id := enqueueWebhookTestEvent(t, q)
	sends, status := 0, http.StatusTooManyRequests
	q.Client = &http.Client{Transport: webhookTransport(func(r *http.Request) (*http.Response, error) {
		sends++
		if r.Header.Get("Idempotency-Key") != id {
			t.Fatal("retry changed delivery identity")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if progress, err := q.Step(t.Context()); !progress || err != nil || sends != 0 {
		t.Fatal("revocation before send was ignored", progress, sends, err)
	}
	var state string
	if err := q.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, id).Scan(&state); err != nil || state != "failed" {
		t.Fatal(state, err)
	}
	if _, err := q.Retry(t.Context(), "other-project", "pipeline", id); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("cross-project retry allowed", err)
	}
	revoked = false
	if retried, err := q.Retry(t.Context(), "project", "pipeline", id); !retried || err != nil {
		t.Fatal(retried, err)
	}
	if progress, err := q.Step(t.Context()); !progress || err != nil || sends != 1 {
		t.Fatal(progress, sends, err)
	}
	if progress, err := q.Step(t.Context()); progress || err != nil || sends != 1 {
		t.Fatal("cooldown was bypassed", progress, sends, err)
	}
	*now = now.Add(time.Second)
	status = http.StatusNoContent
	if progress, err := q.Step(t.Context()); !progress || err != nil || sends != 2 {
		t.Fatal(progress, sends, err)
	}
	var pending int
	if err := q.db.QueryRow(`SELECT count FROM webhook_queue_usage WHERE scope='instance'`).Scan(&pending); err != nil || pending != 0 {
		t.Fatal("success retained queued usage", pending, err)
	}
}

func TestWebhookCanceledSendRequeuesBeforeLeaseFinishes(t *testing.T) {
	t.Parallel()
	finished := make(chan struct{})
	q, _ := webhookQueueFixture(t, webhookTestHost{finish: func() { close(finished) }})
	id := enqueueWebhookTestEvent(t, q)
	started := make(chan struct{})
	q.Client = &http.Client{Transport: webhookTransport(func(r *http.Request) (*http.Response, error) {
		close(started)
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := q.Step(ctx); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("send did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled send did not stop")
	}
	select {
	case <-finished:
	default:
		t.Fatal("project activity lease leaked")
	}
	var state string
	if err := q.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, id).Scan(&state); err != nil || state != "queued" {
		t.Fatal("cancellation stranded the attempt", state, err)
	}
	if _, err := q.db.Exec(`UPDATE webhook_deliveries SET state='sending' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := initializeWebhooks(t.Context(), q.db); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.db.QueryRow(`SELECT state FROM webhook_deliveries WHERE id=?`, id).Scan(&state); err != nil || state != "queued" {
		t.Fatal("restart did not recover interrupted attempt", state, err)
	}
}

func TestWebhookAcknowledgmentRollbackKeepsAttemptReplayable(t *testing.T) {
	t.Parallel()
	q, now := webhookQueueFixture(t, webhookTestHost{})
	id := enqueueWebhookTestEvent(t, q)
	status, sends := http.StatusTooManyRequests, 0
	q.Client = &http.Client{Transport: webhookTransport(func(r *http.Request) (*http.Response, error) {
		sends++
		if r.Header.Get("Idempotency-Key") != id {
			t.Fatal("failed acknowledgment changed retry identity")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(""))}, nil
	})}
	if _, err := q.db.Exec(`CREATE TRIGGER reject_cooldown BEFORE INSERT ON webhook_cooldowns BEGIN SELECT RAISE(ABORT,'synthetic acknowledgment failure'); END`); err != nil {
		t.Fatal(err)
	}
	if progress, err := q.Step(t.Context()); !progress || err == nil {
		t.Fatal("failed acknowledgment did not surface", progress, err)
	}
	var state, payload string
	var lastStatus int
	if err := q.db.QueryRow(`SELECT state,payload,last_status FROM webhook_deliveries WHERE id=?`, id).Scan(&state, &payload, &lastStatus); err != nil || state != "queued" || payload == "" || lastStatus != 0 {
		t.Fatal("partial acknowledgment survived rollback", state, payload, lastStatus, err)
	}
	if _, err := q.db.Exec(`DROP TRIGGER reject_cooldown`); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	status = http.StatusNoContent
	if progress, err := q.Step(t.Context()); !progress || err != nil || sends != 2 {
		t.Fatal("acknowledgment failure stranded delivery", progress, sends, err)
	}
}

type webhookTransport func(*http.Request) (*http.Response, error)

func (f webhookTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
