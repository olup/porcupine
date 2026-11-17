package output

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

type pipelineTestReader struct{}

func (pipelineTestReader) ValidateExport(_ context.Context, _ string, sql string, _ map[string]string, _ bool) (string, error) {
	return sql, nil
}
func (pipelineTestReader) Export(context.Context, string, string, map[string]string, *lakeapi.AnalyticalWindow, int64, lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error) {
	return lakeapi.ExportStreamResult{}, errors.New("unexpected query")
}

func isolatedPipelines(t *testing.T, client *http.Client, endpoints ...string) *PipelineJobs {
	t.Helper()
	q, at := webhookQueueFixture(t, webhookTestHost{})
	if _, err := q.db.Exec(`DELETE FROM pipelines;ALTER TABLE project_deletions ADD COLUMN tenant_id TEXT NOT NULL DEFAULT '';CREATE TABLE managed_artifact_ownership(tenant_id TEXT,project_id TEXT,kind TEXT,storage_id TEXT,object_key TEXT,upload_id TEXT,PRIMARY KEY(tenant_id,project_id,kind,storage_id,object_key,upload_id));` + usage.JournalSchema); err != nil {
		t.Fatal(err)
	}
	p, err := NewSQLitePipelines(PipelineDependencies{Journal: q.db, Control: q.db, Clock: func() time.Time { return *at }, Reader: pipelineTestReader{}, Usage: usage.NewRecorder(q.db, "isolated-pipelines", func() time.Time { return *at }), LoadActor: func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{ID: "actor", Permissions: []string{"events:read", "pipelines:write"}, Projects: []string{"project"}}, nil
	}, RequireActive: func(context.Context, string) error { return nil }, RequireOwner: func(context.Context, string) error { return nil }, BeginActivity: func(ctx context.Context, _ string) (context.Context, func(), bool) { return ctx, func() {}, true }, Acquire: func(context.Context) (func(), bool) { return func() {}, true }, WebhookClient: client}, &PipelineConfig{Key: bytes.Repeat([]byte{7}, 32), AllowedS3Endpoints: endpoints})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Initialize(t.Context()); err != nil {
		t.Fatal(err)
	}
	return p
}
func enqueuePipelineEvent(t *testing.T, p *PipelineJobs, id string) {
	t.Helper()
	tx, err := p.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = p.Enqueuer().Enqueue(t.Context(), tx, []eventapi.Event{{ProjectID: "project", UUID: id, Event: "purchase", Properties: json.RawMessage(`{"amount":1}`), ReceivedAt: p.clock().Format(time.RFC3339Nano)}}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func TestSQLitePipelineReplacementRetiresInFlightWebhookAtSameURL(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	client := &http.Client{Transport: webhookTransport(func(r *http.Request) (*http.Response, error) {
		n := calls.Add(1)
		want := "Bearer old-account"
		if n == 1 {
			close(started)
			<-release
		} else {
			want = "Bearer new-account"
		}
		if r.Header.Get("Authorization") != want {
			t.Errorf("attempt %d used %q", n, r.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: http.Header{}}, nil
	})}
	p := isolatedPipelines(t, client)
	created, err := p.Define(t.Context(), "project", PipelineRequest{Name: "webhook", Kind: "webhook", Webhook: &WebhookPipelineSpec{Endpoint: "https://hooks.example.test/events"}, Credentials: PipelineCredentials{BearerToken: "old-account"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	enqueuePipelineEvent(t, p, "old")
	if _, err = p.Replace(t.Context(), "project", created.ID, PipelineChange{Credentials: PipelineCredentials{BearerToken: "old-account"}}); err != nil {
		t.Fatal(err)
	}
	var queued int
	if err = p.db.QueryRow(`SELECT count(*) FROM webhook_deliveries WHERE event_uuid='old' AND state='queued'`).Scan(&queued); err != nil || queued != 1 {
		t.Fatal("unchanged destination retired backlog", queued, err)
	}

	done := make(chan error, 1)
	go func() { _, err := p.RunWebhook(t.Context()); done <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("webhook did not start")
	}
	_, err = p.Replace(t.Context(), "project", created.ID, PipelineChange{Credentials: PipelineCredentials{BearerToken: "new-account"}})
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var state, payload string
	if err = p.db.QueryRow(`SELECT state,payload FROM webhook_deliveries WHERE event_uuid='old'`).Scan(&state, &payload); err != nil || state != "canceled" || payload != "" {
		t.Fatal("old work survived replacement", state, payload, err)
	}
	reports, err := p.usage.Pending(t.Context())
	if err != nil || len(reports) != 0 {
		t.Fatal("retired ACK changed usage", reports, err)
	}
	enqueuePipelineEvent(t, p, "new")
	if worked, err := p.RunWebhook(t.Context()); err != nil || !worked {
		t.Fatal(worked, err)
	}
	reports, err = p.usage.Pending(t.Context())
	if err != nil || len(reports) != 1 || reports[0].Counters.Webhooks != 1 || calls.Load() != 2 {
		t.Fatal("new account delivery missing or duplicate", reports, calls.Load(), err)
	}
}
func TestSQLitePipelineReplacementRetainsRevokedS3CleanupIdentity(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if !strings.Contains(r.Header.Get("Authorization"), "old-access/") {
			t.Errorf("cleanup used replacement identity: %s", r.Header.Get("Authorization"))
		}
		w.WriteHeader(403)
	}))
	defer server.Close()
	p := isolatedPipelines(t, nil, server.URL)
	created, err := p.Define(t.Context(), "project", PipelineRequest{Name: "S3", Kind: "s3", S3: &S3PipelineSpec{SQL: "SELECT event FROM events", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-17T12:00:00Z", Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Prefix: "reports", Endpoint: server.URL}}, Credentials: PipelineCredentials{AccessKeyID: "old-access", SecretAccessKey: "old-secret"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	original, err := p.loadPipeline(t.Context(), "project", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, "2026-11-17T12:00:00Z")
	if _, err = p.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms) VALUES('old-run',?,?,?,'queued',1);INSERT INTO pipeline_s3_files VALUES('old-run','old-attempt/part.jsonl.gz','old-upload')`, created.ID, start.UnixMilli(), start.Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Replace(t.Context(), "project", created.ID, PipelineChange{Credentials: PipelineCredentials{AccessKeyID: "new-access", SecretAccessKey: "new-secret"}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("replacement contacted revoked destination")
	}
	var oldSecret []byte
	var state string
	var cursor int64
	if err = p.db.QueryRow(`SELECT secret FROM pipeline_retired_destinations WHERE id='old-run'`).Scan(&oldSecret); err != nil || !bytes.Equal(oldSecret, original.Secret) {
		t.Fatal("old encrypted credentials lost", err)
	}
	if err = p.db.QueryRow(`SELECT state FROM pipeline_s3_runs WHERE id='old-run'`).Scan(&state); err != nil || state != "canceled" {
		t.Fatal(state, err)
	}
	if err = p.db.QueryRow(`SELECT cursor_ms FROM pipeline_s3_state WHERE pipeline_id=?`, created.ID).Scan(&cursor); err != nil || cursor != start.Add(time.Minute).UnixMilli() {
		t.Fatal("old window could retarget", cursor, err)
	}
	if _, err = p.cleanupRetired(t.Context(), "project", ""); err == nil {
		t.Fatal("revoked old credentials reported clean")
	}
	if err = p.Delete(t.Context(), "project", created.ID); err == nil {
		t.Fatal("revoked retired cleanup reported deletion success")
	}
	if _, err = p.db.Exec(`DELETE FROM pipelines WHERE id=?`, created.ID); err != nil {
		t.Fatal(err)
	}
	holds, err := PipelineRestoreArtifacts(t.Context(), p.db)
	if err != nil || len(holds) != 1 || holds[0].Key != "old-attempt/part.jsonl.gz" || holds[0].Upload != "old-upload" || holds[0].Storage == "" {
		t.Fatal("old ownership did not outlive live pipeline", holds, err)
	}
	if err = p.CleanupProject(t.Context(), "project"); err == nil {
		t.Fatal("project cleanup discarded unavailable old destination")
	}
	before := calls.Load()
	tx, err := p.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = ResetRestoredPipelinesTx(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != before {
		t.Fatal("restore contacted original destination")
	}
}
func TestWebhookDeletePreservesOtherBatchMembersAndAttemptIdentity(t *testing.T) {
	t.Parallel()
	s := isolatedPipelines(t, nil)
	created, err := s.Define(t.Context(), "project", PipelineRequest{Name: "batch", Kind: "webhook", Webhook: &WebhookPipelineSpec{Endpoint: "https://hooks.example.test/events", DeliveryMode: "batch", IntervalSeconds: 60, StartAt: "2026-11-08T12:00:00Z"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	p, pipeline := "project", created.ID

	now := time.Date(2026, 11, 8, 12, 0, 1, 0, time.UTC)
	s.clock = func() time.Time { return now }
	tx, _ := s.db.Begin()
	for _, uuid := range []string{"deleted-before-send", "deleted-after-attempt", "survivor"} {
		event := eventapi.Event{ProjectID: p, UUID: uuid, Event: "purchase", Properties: json.RawMessage(`{}`), ReceivedAt: now.Format(time.RFC3339Nano)}
		if err := s.Enqueuer().Enqueue(context.Background(), tx, []eventapi.Event{event}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var originalID string
	s.db.QueryRow(`SELECT id FROM webhook_deliveries`).Scan(&originalID)
	tx, _ = s.db.Begin()
	if err := CancelWebhookEvent(context.Background(), tx, p, "deleted-before-send", s.clock); err != nil {
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
	if err = CancelWebhookEvent(context.Background(), tx, p, "deleted-after-attempt", s.clock); err != nil {
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
	var payload delivery.WebhookPayload
	json.Unmarshal([]byte(raw), &payload)
	if len(payload.Events) != 1 || payload.Events[0].UUID != "survivor" || payload.DeliveryID != id {
		t.Fatal("wrong surviving body")
	}
	if err = s.webhooks.Complete(context.Background(), &webhookAttempt{ID: originalID, Payload: immutable, Attempts: 1}, 200, "", false); err != nil {
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

type blockingPipelineReader struct {
	pipelineTestReader
	started, canceled, release chan struct{}
}

func (r *blockingPipelineReader) Export(ctx context.Context, _ string, _ string, _ map[string]string, _ *lakeapi.AnalyticalWindow, _ int64, _ lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error) {
	close(r.started)
	<-ctx.Done()
	close(r.canceled)
	<-r.release
	return lakeapi.ExportStreamResult{}, ctx.Err()
}
func TestSQLitePipelineStopWaitsForActualNativeReturn(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(404)
		io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
	}))
	defer server.Close()
	p := isolatedPipelines(t, nil, server.URL)
	reader := &blockingPipelineReader{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	p.lake = reader
	var released, finished atomic.Int32
	p.acquire = func(context.Context) (func(), bool) { return func() { released.Add(1) }, true }
	p.beginActivity = func(ctx context.Context, _ string) (context.Context, func(), bool) {
		return ctx, func() { finished.Add(1) }, true
	}
	if _, err := p.Define(t.Context(), "project", PipelineRequest{Name: "native", Kind: "s3", S3: &S3PipelineSpec{SQL: "SELECT event FROM events", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-17T11:59:00Z", Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Prefix: "reports", Endpoint: server.URL}}, Credentials: PipelineCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, auth.Principal{ID: "actor"}); err != nil {
		t.Fatal(err)
	}
	p.Start()
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("native call did not begin")
	}
	var pipelineID string
	if err := p.db.QueryRow(`SELECT id FROM pipelines WHERE kind='s3'`).Scan(&pipelineID); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Replace(t.Context(), "project", pipelineID, PipelineChange{Credentials: PipelineCredentials{AccessKeyID: "new-access", SecretAccessKey: "new-secret"}}); err == nil {
		t.Fatal("replacement proceeded during actual native call")
	} else {
		var status interface{ GetStatus() int }
		if !errors.As(err, &status) || status.GetStatus() != 409 {
			t.Fatal("native replacement conflict was not retryable", err)
		}
	}
	stopped := make(chan struct{})
	go func() { p.Stop(); close(stopped) }()
	select {
	case <-reader.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not cancel native context")
	}
	select {
	case <-stopped:
		t.Fatal("stop returned while native call was active")
	default:
	}
	if released.Load() != 0 || finished.Load() != 0 {
		t.Fatal("native permit or project activity released before actual return", released.Load(), finished.Load())
	}
	close(reader.release)
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not drain native call")
	}
	if released.Load() != 1 || finished.Load() != 1 {
		t.Fatal("native lifetime did not release exactly once", released.Load(), finished.Load())
	}
}

func TestSQLiteRetiredS3CleanupPreservesPublishedDataAndOtherProjects(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"unpublished", "missing-upload-checkpoint", "published", "conflicting-manifest"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			var operations []string
			manifest := `{"version":1,"run_id":"old-run"}`
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/xml")
				if !strings.Contains(r.Header.Get("Authorization"), "old-access/") {
					t.Error("old namespace used replacement credentials")
				}
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "manifest.json"):
					if mode == "published" {
						io.WriteString(w, manifest)
					} else if mode == "conflicting-manifest" {
						io.WriteString(w, `{"other":true}`)
					} else {
						w.WriteHeader(404)
						io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
					}
				case r.Method == http.MethodGet && r.URL.Query().Has("uploads"):
					operations = append(operations, "reconcile")
					io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated><Upload><Key>reports/old-attempt/part.jsonl.gz</Key><UploadId>orphan-upload</UploadId></Upload></ListMultipartUploadsResult>`)
				case r.Method == http.MethodDelete:
					if r.URL.Query().Has("uploadId") {
						operations = append(operations, "abort")
					} else {
						operations = append(operations, "delete")
					}
					w.WriteHeader(204)
				default:
					t.Errorf("unexpected cleanup request %s %s", r.Method, r.URL)
					w.WriteHeader(400)
				}
			}))
			defer server.Close()
			p := isolatedPipelines(t, nil, server.URL)
			created, err := p.Define(t.Context(), "project", PipelineRequest{Name: "S3", Kind: "s3", S3: &S3PipelineSpec{SQL: "SELECT event FROM events", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-17T12:00:00Z", Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Prefix: "reports", Endpoint: server.URL}}, Credentials: PipelineCredentials{AccessKeyID: "old-access", SecretAccessKey: "old-secret"}}, auth.Principal{ID: "actor"})
			if err != nil {
				t.Fatal(err)
			}
			upload := "old-upload"
			if mode == "missing-upload-checkpoint" {
				upload = ""
			}
			if _, err = p.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,manifest) VALUES('old-run',?,0,60000,'queued',1,?)`, created.ID, manifest); err != nil {
				t.Fatal(err)
			}
			if _, err = p.db.Exec(`INSERT INTO pipeline_s3_files VALUES('old-run','old-attempt/part.jsonl.gz',?)`, upload); err != nil {
				t.Fatal(err)
			}
			if _, err = p.Replace(t.Context(), "project", created.ID, PipelineChange{Credentials: PipelineCredentials{AccessKeyID: "new-access", SecretAccessKey: "new-secret"}}); err != nil {
				t.Fatal(err)
			}
			if _, err = p.db.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES('other','tenant','Other','now');INSERT INTO pipeline_retired_destinations(id,pipeline_id,project_id,config,secret,window_start_ms,manifest,created_at_ms) SELECT 'other-hold','other-pipeline','other',config,secret,0,'',1 FROM pipeline_retired_destinations WHERE id='old-run';INSERT INTO pipeline_retired_files VALUES('other-hold','other-key','other-upload')`); err != nil {
				t.Fatal(err)
			}
			worked, err := p.cleanupRetired(t.Context(), "project", "")
			if mode == "conflicting-manifest" {
				if err == nil || worked || len(operations) != 0 {
					t.Fatal("ambiguous manifest authorized cleanup", worked, err, operations)
				}
			} else if err != nil || !worked {
				t.Fatal(worked, err)
			}
			want := 2
			if mode == "missing-upload-checkpoint" {
				want = 3
			} else if mode == "published" || mode == "conflicting-manifest" {
				want = 0
			}
			expected := "abort,delete"
			if mode == "missing-upload-checkpoint" {
				expected = "reconcile,abort,delete"
			} else if mode == "published" || mode == "conflicting-manifest" {
				expected = ""
			}
			if strings.Join(operations, ",") != expected {
				t.Fatal("cleanup operation sequence changed", operations)
			}
			if len(operations) != want {
				t.Fatal(mode, operations)
			}
			var other, inventory, remaining int
			if err = p.db.QueryRow(`SELECT count(*) FROM pipeline_retired_files WHERE destination_id='other-hold'`).Scan(&other); err != nil || other != 1 {
				t.Fatal("project B ownership changed", other, err)
			}
			if err = p.db.QueryRow(`SELECT count(*) FROM managed_artifact_ownership WHERE project_id='project'`).Scan(&inventory); err != nil {
				t.Fatal(err)
			}
			if mode == "published" || mode == "conflicting-manifest" {
				if inventory != 0 {
					t.Fatal("customer or ambiguous data claimed as private cleanup")
				}
			} else if inventory != 1 {
				t.Fatal("private ownership disappeared", inventory)
			}
			if err = p.db.QueryRow(`SELECT count(*) FROM pipeline_retired_destinations WHERE id='old-run'`).Scan(&remaining); err != nil {
				t.Fatal(err)
			}
			if mode == "conflicting-manifest" && remaining != 1 || mode != "conflicting-manifest" && remaining != 0 {
				t.Fatal("retired evidence cleared before settlement", remaining)
			}
		})
	}
}

func TestSQLiteRotationBoundsCanceledHistoryAndPreservesPrefixOwnership(t *testing.T) {
	t.Parallel()
	p := isolatedPipelines(t, nil)
	created, err := p.Define(t.Context(), "project", PipelineRequest{Name: "S3", Kind: "s3", S3: &S3PipelineSpec{SQL: "SELECT event FROM events", IntervalSeconds: 60, StartAt: "2026-11-17T12:00:00Z", Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Prefix: "reports"}}, Credentials: PipelineCredentials{AccessKeyID: "old-access", SecretAccessKey: "old-secret"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	original, err := p.loadPipeline(t.Context(), "project", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,attempt_id,created_at_ms) VALUES('uncertain-prefix',?,0,60000,'failed','original-attempt',1)`, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = p.db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1005) INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms) SELECT 'fileless-'||i,?,i*60000,(i+1)*60000,'queued',1 FROM n`, created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = p.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,completed_at_ms) VALUES('already-canceled',?,1006*60000,1007*60000,'canceled',1,123)`, created.ID); err != nil {
		t.Fatal(err)
	}
	for _, access := range []string{"replacement-one", "replacement-two"} {
		if _, err = p.Replace(t.Context(), "project", created.ID, PipelineChange{Credentials: PipelineCredentials{AccessKeyID: access, SecretAccessKey: "replacement-secret"}}); err != nil {
			t.Fatal(err)
		}
	}
	var count, pruned int
	var completed int64
	var attempt string
	var secret []byte
	if err = p.db.QueryRow(`SELECT count(*) FROM pipeline_s3_runs WHERE pipeline_id=? AND state='canceled'`, created.ID).Scan(&count); err != nil || count != 1000 {
		t.Fatal("canceled history is unbounded", count, err)
	}
	if err = p.db.QueryRow(`SELECT completed_at_ms FROM pipeline_s3_runs WHERE id='already-canceled'`).Scan(&completed); err != nil || completed != 123 {
		t.Fatal("rotation rewrote terminal completion time", completed, err)
	}
	if err = p.db.QueryRow(`SELECT count(*) FROM pipeline_s3_runs WHERE id='uncertain-prefix'`).Scan(&pruned); err != nil || pruned != 0 {
		t.Fatal("old terminal history was not pruned", pruned, err)
	}
	if err = p.db.QueryRow(`SELECT attempt_id,secret FROM pipeline_retired_destinations WHERE id='uncertain-prefix'`).Scan(&attempt, &secret); err != nil || attempt != "original-attempt" || !bytes.Equal(secret, original.Secret) {
		t.Fatal("prefix ownership or original credentials vanished with history", attempt, err)
	}
	holds, err := PipelineRestoreArtifacts(t.Context(), p.db)
	if err != nil || len(holds) != 1 || !strings.HasSuffix(holds[0].Key, "/0/original-attempt/") {
		t.Fatal("prefix-only restore hold missing", holds, err)
	}
}

func TestSQLitePipelineDeleteCommitsPauseBeforeCancelingActualNativeCall(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Has("uploads") {
			io.WriteString(w, `<ListMultipartUploadsResult><IsTruncated>false</IsTruncated></ListMultipartUploadsResult>`)
			return
		}
		w.WriteHeader(404)
		io.WriteString(w, `<Error><Code>NoSuchKey</Code></Error>`)
	}))
	defer server.Close()
	p := isolatedPipelines(t, nil, server.URL)
	reader := &blockingPipelineReader{started: make(chan struct{}), canceled: make(chan struct{}), release: make(chan struct{})}
	p.lake = reader
	var released, finished atomic.Int32
	p.acquire = func(context.Context) (func(), bool) { return func() { released.Add(1) }, true }
	p.beginActivity = func(ctx context.Context, _ string) (context.Context, func(), bool) {
		return ctx, func() { finished.Add(1) }, true
	}
	created, err := p.Define(t.Context(), "project", PipelineRequest{Name: "native delete", Kind: "s3", S3: &S3PipelineSpec{SQL: "SELECT event FROM events", IntervalSeconds: 60, StartAt: "2026-11-17T11:59:00Z", Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Prefix: "reports", Endpoint: server.URL}}, Credentials: PipelineCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := p.RunS3(t.Context()); done <- err }()
	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("native call did not start")
	}
	if _, err = p.db.Exec(`CREATE TRIGGER reject_pause BEFORE UPDATE OF state ON pipelines BEGIN SELECT RAISE(ABORT,'pause rejected'); END`); err != nil {
		t.Fatal(err)
	}
	if err = p.Delete(t.Context(), "project", created.ID); err == nil {
		t.Fatal("failed pause reported deletion")
	}
	select {
	case <-reader.canceled:
		t.Fatal("rolled-back pause canceled native call")
	default:
	}
	if _, err = p.db.Exec(`DROP TRIGGER reject_pause`); err != nil {
		t.Fatal(err)
	}
	if err = p.Delete(t.Context(), "project", created.ID); err == nil {
		t.Fatal("deletion reported success before actual return")
	} else {
		var status interface{ GetStatus() int }
		if !errors.As(err, &status) || status.GetStatus() != 503 || !strings.Contains(err.Error(), "retry deletion cleanup") {
			t.Fatal("missing explicit pending cleanup", err)
		}
	}
	var state string
	if err = p.db.QueryRow(`SELECT state FROM pipelines WHERE id=?`, created.ID).Scan(&state); err != nil || state != "paused" {
		t.Fatal("deletion did not durably pause", state, err)
	}
	select {
	case <-reader.canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("deletion did not request native cancellation")
	}
	if released.Load() != 0 || finished.Load() != 0 {
		t.Fatal("native capacity released before actual return")
	}
	if worked, err := p.RunS3(t.Context()); worked || err != nil {
		t.Fatal("new run began during pending deletion", worked, err)
	}
	close(reader.release)
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if released.Load() != 1 || finished.Load() != 1 {
		t.Fatal("actual native lifetime not drained exactly once")
	}
	if worked, err := p.RunS3(t.Context()); worked || err != nil {
		t.Fatal("paused backlog restarted after native return", worked, err)
	}
	if err = p.Delete(t.Context(), "project", created.ID); err != nil {
		t.Fatal("deletion retry did not settle cleanup", err)
	}
}
