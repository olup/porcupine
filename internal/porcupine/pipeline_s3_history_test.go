package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestPipelineS3SuccessfulHistoryBound(t *testing.T) {
	var committed []byte
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("history finalization should only read existing commit")
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(committed)
	}))
	defer endpoint.Close()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "history.db"), BootstrapToken: adminToken, Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{9}, 32), AllowedS3Endpoints: []string{endpoint.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.stopPipelines()
	ch, err := newClickHouse(ClickHouseConfig{URL: "http://127.0.0.1:1", Database: "history_test"})
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	projectID, _ := project(t, s)
	spec := testS3Spec()
	spec.Destination.Endpoint = endpoint.URL
	body, _ := json.Marshal(PipelineCreateBody{Name: "history", Kind: "s3", S3: &spec, Credentials: PipelineCredentials{AccessKeyID: "test-id", SecretAccessKey: "test-secret"}})
	created := request(t, s, "POST", "/api/v1/projects/"+projectID+"/pipelines", adminToken, string(body), 201)
	id := created["id"].(string)
	p, err := s.loadPipeline(context.Background(), projectID, id)
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(p.Config, &spec)
	start, _ := time.Parse(time.RFC3339, spec.StartAt)
	run := newID()
	_, err = s.db.Exec(`WITH RECURSIVE n(i) AS(SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1001) INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,completed_at_ms) SELECT 'history-'||i,?,?-i*60000,?-(i-1)*60000,'succeeded',1,2 FROM n`, id, start.UnixMilli(), start.UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms) VALUES('retained-failure',?,?,?,'failed',1)`, id, start.UnixMilli()-1002*60000, start.UnixMilli()-1001*60000)
	if err != nil {
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
	if err = s.publishPipelineS3(context.Background(), w, store, committed); err != nil {
		t.Fatal(err)
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
