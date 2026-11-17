package porcupine

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

func TestRestoreArtifactHoldsPreventFalseTenantCompletionWithoutSourceCalls(t *testing.T) {
	t.Parallel()
	var sourceCalls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sourceCalls.Add(1)
		w.WriteHeader(500)
	}))
	defer source.Close()
	s := service(t, 100)
	s.pipelines = &pipelineRuntime{}
	a, b := tenantTestCreate(t, s, "A"), tenantTestCreate(t, s, "B")
	pa, ka := tenantJourneyProject(t, s, a)
	pb, kb := tenantJourneyProject(t, s, b)
	request(t, s, "POST", "/capture/", "", captureBody(ka, "A before restore"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "B before restore"), 200)
	var actor string
	if err := s.control.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&actor); err != nil {
		t.Fatal(err)
	}
	storage := exportstore.NamespaceOwnershipID(exportstore.Config{Endpoint: source.URL, Region: "us-east-1", Bucket: "source-bucket", Prefix: "private"})
	for _, project := range []string{pa, pb} {
		config := tenantTestBody(t, S3PipelineSpec{SQL: "SELECT private_payload FROM events", Destination: S3PipelineDestination{Endpoint: source.URL, Region: "us-east-1", Bucket: "source-bucket", Prefix: "private"}})
		for _, statement := range []struct {
			sql  string
			args []any
		}{
			{`INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at,attempt_id) VALUES($1,$1,$2,'SELECT private_payload','{}','jsonl',$1,$1,'running',0,0,'now','attempt')`, []any{project, actor}},
			{`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,upload_id,storage_id) VALUES($1,'attempt',1,$2,'upload',$3)`, []any{project, project + "/export/part.jsonl.gz", storage}},
			{`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES($1,$1,$2,'s3','source label','active',$3,X'0102',1)`, []any{project, actor, config}},
			{`INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms) VALUES($1,$1,0,1,'running',1)`, []any{project}},
			{`INSERT INTO pipeline_s3_files(run_id,object_key,upload_id) VALUES($1,$2,'upload')`, []any{project, project + "/pipeline/part.jsonl.gz"}},
		} {
			if _, err := s.db.Exec(statement.sql, statement.args...); err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := s.db.Exec(`CREATE TRIGGER fail_reset BEFORE DELETE ON export_files BEGIN SELECT RAISE(ABORT,'fixture reset interrupted'); END`); err != nil {
		t.Fatal(err)
	}
	bootstrap := strings.Repeat("fresh-recovery-token", 2)
	if _, err := resetNativeRestoreCredentials(t.Context(), s.db, s.control, bootstrap); err == nil {
		t.Fatal("fixture did not interrupt reset")
	}
	var count int
	if err := s.control.QueryRow(`SELECT count(*) FROM restored_artifact_holds`).Scan(&count); err != nil || count != 4 {
		t.Fatal("holds were not durable before the interrupted reset", count, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_reset`); err != nil {
		t.Fatal(err)
	}
	result, err := resetNativeRestoreCredentials(t.Context(), s.db, s.control, bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"export_files", "pipeline_s3_files", "pipeline_s3_runs"} {
		if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("reset retained executable source checkpoints", table, count, err)
		}
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM restored_artifact_holds`).Scan(&count); err != nil || count != 4 {
		t.Fatal("reset retry duplicated or lost holds", count, err)
	}
	var freshB string
	for _, key := range result.IngestionKeys {
		if key.ProjectID == pb {
			freshB = key.Token
		}
	}
	if freshB == "" {
		t.Fatal("restore did not preserve B's recovery ingestion capability")
	}
	request(t, s, "DELETE", "/api/v1/tenants/"+a, bootstrap, "", 202)
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := s.tenantDeletionStatus(t.Context(), a)
		if err != nil {
			t.Fatal(err)
		}
		if status.State != "deleting" {
			t.Fatal("unreconciled current artifacts produced a completion claim", status)
		}
		if status.ErrorCode == "restored_artifacts_pending" && status.ProjectsRemaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restored artifact hold did not explain pending deletion", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	request(t, s, "POST", "/capture/", "", captureBody(freshB, "B after A deletion"), 200)
	request(t, s, "GET", "/api/v1/tenants/"+b, bootstrap, "", 200)
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=$1`, pb).Scan(&count); err != nil || count != 2 {
		t.Fatal("A deletion changed B's restored data", count, err)
	}
	if sourceCalls.Load() != 0 {
		t.Fatal("restore/reset/deletion contacted or deleted the source destination", sourceCalls.Load())
	}
}
