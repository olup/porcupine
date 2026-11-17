package output

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"

	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/usage"
)

func TestPipelineS3SuccessfulHistoryBound(t *testing.T) {
	t.Parallel()
	var committed []byte
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("history finalization should only read existing commit")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(committed)
	}))
	defer endpoint.Close()
	q, _ := webhookQueueFixture(t, webhookTestHost{})
	if _, err := q.db.Exec(usage.JournalSchema); err != nil {
		t.Fatal(err)
	}
	s, err := NewSQLitePipelines(PipelineDependencies{Journal: q.db, Clock: time.Now, Usage: usage.NewRecorder(q.db, "history-test", time.Now), LoadActor: func(context.Context, string) (auth.Principal, error) {
		return auth.Principal{ID: "actor", Permissions: []string{"events:read", "pipelines:write"}, Projects: []string{"project"}}, nil
	}, RequireActive: func(context.Context, string) error { return nil }, RequireOwner: func(context.Context, string) error { return nil }}, &PipelineConfig{Key: bytes.Repeat([]byte{9}, 32), AllowedS3Endpoints: []string{endpoint.URL}})
	if err != nil {
		t.Fatal(err)
	}
	projectID, id := "project", "s3-history"
	spec := S3PipelineSpec{SQL: "SELECT event FROM events", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-08T12:00:00Z", Destination: S3PipelineDestination{Bucket: "client-bucket", Region: "us-east-1", Prefix: "private", Endpoint: endpoint.URL, UsePathStyle: true}}
	config, _ := json.Marshal(spec)
	secret, _ := json.Marshal(S3PipelineSecret{AccessKeyID: "test-id", SecretAccessKey: "test-secret"})
	encrypted, err := s.SealSecret(projectID, id, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES(?,?,'actor','s3','history','active',?,?,1)`, id, projectID, string(config), encrypted); err != nil {
		t.Fatal(err)
	}
	p, err := s.loadPipeline(t.Context(), projectID, id)
	if err != nil {
		t.Fatal(err)
	}
	start, _ := time.Parse(time.RFC3339, spec.StartAt)
	run := eventapi.NewID()
	_, err = s.db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1001) INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,completed_at_ms) SELECT 'history-'||i,?,?-i*60000,?-(i-1)*60000,'succeeded',1,2 FROM n`, id, start.UnixMilli(), start.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms) VALUES('retained-failure',?,?,?,'failed',1)`, id, start.UnixMilli()-1002*60000, start.UnixMilli()-1001*60000)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO pipeline_s3_state(pipeline_id,cursor_ms) VALUES(?,?)`, id, start.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	w := &s3PipelineWork{pipeline: p, spec: spec, id: run, start: start.UnixMilli(), end: start.Add(time.Minute).UnixMilli()}
	manifest := pipelineS3Manifest{Version: 1, ProjectID: projectID, PipelineID: id, RunID: run, WindowStartMS: w.start, WindowEndMS: w.end, Format: "jsonl", Rows: 3, Files: []pipelineS3File{{Key: "private/part.jsonl.gz", Rows: 3, Bytes: 20}}}
	committed, _ = json.Marshal(manifest)
	_, err = s.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,manifest) VALUES(?,?,?,?,'running',1,?)`, run, id, w.start, w.end, string(committed))
	if err != nil {
		t.Fatal(err)
	}
	store, err := s.pipelineS3Store(context.Background(), p, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err = s.db.Exec(`CREATE TRIGGER reject_pipeline_usage BEFORE INSERT ON usage_rollups BEGIN SELECT RAISE(ABORT,'meter unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	if err = s.publishPipelineS3(t.Context(), w, store, committed); err == nil {
		t.Fatal("publication escaped usage rollback")
	}
	var cursor int64
	var state string
	if err = s.db.QueryRow(`SELECT st.cursor_ms,r.state FROM pipeline_s3_state st JOIN pipeline_s3_runs r ON r.pipeline_id=st.pipeline_id WHERE r.id=?`, run).Scan(&cursor, &state); err != nil || cursor != w.start || state != "running" {
		t.Fatal("failed metering moved state", cursor, state, err)
	}
	if _, err = s.db.Exec(`DROP TRIGGER reject_pipeline_usage`); err != nil {
		t.Fatal(err)
	}
	stale := *w
	stale.claimID = "stale-claim"
	if err = s.publishPipelineS3(t.Context(), &stale, store, committed); err == nil {
		t.Fatal("stale claim published")
	}
	if err = newPipelineS3Sink(t.Context(), s, &stale, store).beginFile(); err == nil {
		t.Fatal("stale claim created a multipart checkpoint")
	}
	if err = s.publishPipelineS3(context.Background(), w, store, committed); err != nil {
		t.Fatal(err)
	}
	if err = s.publishPipelineS3(context.Background(), w, store, committed); err == nil {
		t.Fatal("publication retry bypassed the cursor checkpoint")
	}
	var published int64
	if err = s.db.QueryRow(`SELECT COALESCE(SUM(pipeline_bytes),0) FROM usage_rollups WHERE project_id=?`, projectID).Scan(&published); err != nil || published != 20 {
		t.Fatal("manifest publication bytes changed on retry", published, err)
	}
	var count, failed, manifestBytes int
	if s.db.QueryRow(`SELECT count(*) FROM pipeline_s3_runs WHERE pipeline_id=? AND state='succeeded'`, id).Scan(&count) != nil || count != 1000 {
		t.Fatal("successful history is not bounded")
	}
	if s.db.QueryRow(`SELECT count(*) FROM pipeline_s3_runs WHERE id='retained-failure'`).Scan(&failed) != nil || failed != 1 {
		t.Fatal("failed history removed")
	}
	if s.db.QueryRow(`SELECT length(manifest) FROM pipeline_s3_runs WHERE id=?`, run).Scan(&manifestBytes) != nil || manifestBytes != 0 {
		t.Fatal("committed full manifest retained")
	}
	got, err := s.getS3PipelineRun(context.Background(), projectID, id, run)
	if err != nil || got.Rows != 3 || got.Bytes != 20 || got.WindowStartMS != w.start || got.WindowEndMS != w.end {
		t.Fatal("compact committed summary lost")
	}
}
