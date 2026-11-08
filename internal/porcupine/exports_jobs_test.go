package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func exportContext(t *testing.T, s *Service, token string) context.Context {
	t.Helper()
	a, err := s.authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	return context.WithValue(context.Background(), actorContextKey{}, a)
}

func exportInput(project, key string) *ExportCreateInput {
	return &ExportCreateInput{ID: project, Body: ExportCreateBody{IdempotencyKey: key, SQL: "SELECT uuid,event FROM events WHERE event={name:String}", Parameters: map[string]string{"name": "purchase"}}}
}

func enableTestExports(s *Service, maxQueued int) {
	s.exports = &exportRuntime{config: ExportConfig{MaxQueuedJobs: maxQueued, MaxExecutionSeconds: 100, MaxReadBytes: 1 << 30, MaxOutputBytes: 1 << 30, FileBytes: 1 << 20, MaxRowBytes: 1 << 20, MemoryBytes: 256 << 20, BandwidthBytesPerSecond: 1 << 20, RetentionHours: 24}, wake: make(chan struct{}, 1)}
}

func TestExportJobDurabilityIdempotencyWatermarksAndPrivateFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	ctx := exportContext(t, s, adminToken)
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "first"), 200)
	_, err = s.db.Exec(`INSERT INTO mutation_audit(mutation_id,project_id,uuid,actor_id,operation,idempotency_key_hash,request_hash,expected_revision,state,created_at) VALUES('watermark',?,'first',?,'edit','key','request',0,'applied','now')`, projectID, principal(ctx).ID)
	if err != nil {
		t.Fatal(err)
	}
	enableTestExports(s, 100)
	s.clock = func() time.Time { return time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC) }
	input := exportInput(projectID, "stable-job")
	created, err := s.createEventExport(ctx, input)
	if err != nil || created.Body.State != ExportQueued || created.Body.Format != "jsonl" || created.Body.Consistency != exportConsistency || created.Body.CreatedAt != "2026-11-08T12:00:00Z" {
		t.Fatalf("create failed: %+v %v", created, err)
	}
	stored, err := s.loadExport(ctx, projectID, created.Body.ID)
	if err != nil || stored.EventWatermark != 1 || stored.MutationWatermark != 1 || stored.ActorID != principal(ctx).ID || stored.Parameters["name"] != "purchase" {
		t.Fatalf("durable job lost request or delivery watermarks: %+v %v", stored, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "later"), 200)
	retry, err := s.createEventExport(ctx, input)
	if err != nil || retry.Body.ID != created.Body.ID {
		t.Fatal("idempotent retry allocated another job", err)
	}
	stored, _ = s.loadExport(ctx, projectID, created.Body.ID)
	if stored.EventWatermark != 1 {
		t.Fatal("retry moved the accepted-at-start delivery barrier")
	}
	for _, change := range []func(*ExportCreateInput){
		func(i *ExportCreateInput) { i.Body.Parameters["name"] = "other" },
		func(i *ExportCreateInput) { i.Body.SQL = "SELECT event FROM events WHERE event={name:String}" },
		func(i *ExportCreateInput) { i.Body.Format = "csv" },
	} {
		changed := exportInput(projectID, "stable-job")
		change(changed)
		_, err := s.createEventExport(ctx, changed)
		mutationErrorStatus(t, err, 409)
	}
	_, token := key(t, s, []string{"events:read"}, []string{projectID})
	other, err := s.createEventExport(exportContext(t, s, token), input)
	if err != nil || other.Body.ID == created.Body.ID {
		t.Fatal("different creators shared an idempotency namespace", err)
	}
	otherProject, _ := project(t, s)
	other, err = s.createEventExport(ctx, exportInput(otherProject, "stable-job"))
	if err != nil || other.Body.ID == created.Body.ID {
		t.Fatal("different projects shared an idempotency namespace", err)
	}
	encoded, _ := json.Marshal(created.Body)
	for _, private := range []string{"sql", "parameters", "actor_id", "idempotency", "watermark", "purchase", "attempt_id", "query_id", "object_key"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("public job exposed %s", private)
		}
	}
	var keyHash, requestHash string
	if err := s.db.QueryRow(`SELECT idempotency_hash,request_hash FROM export_jobs WHERE id=?`, created.Body.ID).Scan(&keyHash, &requestHash); err != nil || len(keyHash) != 64 || len(requestHash) != 64 || keyHash == input.Body.IdempotencyKey {
		t.Fatal("job did not retain private request hashes", err)
	}
	// Helpers and job history remain available after a restart with exports disabled.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := s.loadExport(ctx, projectID, created.Body.ID)
	if err != nil || loaded.SQL != stored.SQL || loaded.EventWatermark != 1 {
		t.Fatal("restart lost durable export request", err)
	}
	list, err := s.listEventExports(ctx, &ExportListInput{ID: projectID, Limit: 20})
	if err != nil || list.Body.Enabled || len(list.Body.Jobs) != 2 || list.Body.Limits != nil {
		t.Fatal("disabled exports hid durable job history", err)
	}
}

func TestExportValidationGlobalQueueAndActorRevocation(t *testing.T) {
	s := service(t, 20)
	ctx := exportContext(t, s, adminToken)
	projectID, _ := project(t, s)
	_, err := s.createEventExport(ctx, exportInput(projectID, "disabled"))
	mutationErrorStatus(t, err, 503)
	enableTestExports(s, 1)
	for _, change := range []func(*ExportCreateInput){
		func(i *ExportCreateInput) { i.Body.IdempotencyKey = "" },
		func(i *ExportCreateInput) { i.Body.SQL = "DELETE FROM events" },
		func(i *ExportCreateInput) { i.Body.SQL = "SELECT * FROM events FORMAT CSV" },
		func(i *ExportCreateInput) { i.Body.SQL = "SELECT * FROM other.events" },
		func(i *ExportCreateInput) { i.Body.Parameters = map[string]string{} },
		func(i *ExportCreateInput) { i.Body.Parameters["bad-name"] = "x" },
		func(i *ExportCreateInput) { i.Body.Format = "parquet" },
	} {
		input := exportInput(projectID, "invalid")
		change(input)
		_, err := s.createEventExport(ctx, input)
		mutationErrorStatus(t, err, 400)
	}
	created, err := s.createEventExport(ctx, exportInput(projectID, "one"))
	if err != nil {
		t.Fatal(err)
	}
	otherProject, _ := project(t, s)
	_, err = s.createEventExport(ctx, exportInput(otherProject, "over-global-bound"))
	mutationErrorStatus(t, err, 503)
	retry, err := s.createEventExport(ctx, exportInput(projectID, "one"))
	if err != nil || retry.Body.ID != created.Body.ID {
		t.Fatal("full queue broke an exact idempotent retry", err)
	}
	s.exports.config.MaxQueuedJobs = 999
	for i := range 99 {
		if _, err := s.createEventExport(ctx, exportInput(projectID, fmt.Sprintf("cap-%d", i))); err != nil {
			t.Fatal("export queue filled before its hard bound", err)
		}
	}
	_, err = s.createEventExport(ctx, exportInput(otherProject, "hard-global-bound"))
	mutationErrorStatus(t, err, 503)
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM export_jobs`).Scan(&count); err != nil || count != 100 {
		t.Fatal("hard global job bound exceeded", err)
	}
	keyID, token := key(t, s, []string{"events:read"}, []string{projectID})
	creator, err := s.exportActor(ctx, keyID)
	if err != nil || !authorize(creator, "events:read", projectID) {
		t.Fatal("worker cannot resolve current creator grants", err)
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+keyID, adminToken, "", 204)
	if _, err := s.exportActor(ctx, keyID); err == nil {
		t.Fatal("revoked creator remained authorized to execute an export")
	}
	request(t, s, "GET", "/api/v1/projects/"+projectID+"/exports", token, "", 401)
}

func TestExportCancelOwnershipCommitBeforeSignalAndExtras(t *testing.T) {
	s := service(t, 20)
	enableTestExports(s, 100)
	projectID, _ := project(t, s)
	_, creatorToken := key(t, s, []string{"events:read"}, []string{projectID})
	_, peerToken := key(t, s, []string{"events:read"}, []string{projectID})
	ctx := exportContext(t, s, creatorToken)
	created, err := s.createEventExport(ctx, exportInput(projectID, "cancel"))
	if err != nil {
		t.Fatal(err)
	}
	input := &ExportGetInput{ID: projectID, ExportID: created.Body.ID}
	_, err = s.cancelEventExport(exportContext(t, s, peerToken), input)
	mutationErrorStatus(t, err, 403)
	canceled, err := s.cancelEventExport(ctx, input)
	if err != nil || canceled.Body.State != ExportCanceled {
		t.Fatal("creator cancellation failed", err)
	}
	job, err := s.loadExport(ctx, projectID, created.Body.ID)
	if err != nil || !job.CancelRequested || !job.CleanupPending || job.SQL != "" || len(job.Parameters) != 0 || job.CompletedAt == "" {
		t.Fatal("queued cancellation was not durable or retained SQL", err)
	}
	_, err = s.cancelEventExport(ctx, input)
	mutationErrorStatus(t, err, 409)
	retry, err := s.createEventExport(ctx, exportInput(projectID, "cancel"))
	if err != nil || retry.Body.ID != created.Body.ID || retry.Body.State != ExportCanceled {
		t.Fatal("terminal payload cleanup lost durable request idempotency", err)
	}
	created, err = s.createEventExport(ctx, exportInput(projectID, "running"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE export_jobs SET state='running',attempt_id='attempt',columns_json='[{"name":"uuid","type":"String"}]',extras_json='{"totals":["total"],"extremes":{"min":["a"],"max":["z"]},"rows_before_limit_at_least":17}' WHERE id=?`, created.Body.ID); err != nil {
		t.Fatal(err)
	}
	called := false
	s.exports.activeJob = created.Body.ID
	s.exports.activeCancel = func() {
		called = true
		var requested, cleanup bool
		if err := s.db.QueryRow(`SELECT cancel_requested,cleanup_pending FROM export_jobs WHERE id=?`, created.Body.ID).Scan(&requested, &cleanup); err != nil || !requested || !cleanup {
			t.Errorf("active cancellation ran before the SQLite flag committed: %v", err)
		}
	}
	_, err = s.cancelEventExport(exportContext(t, s, adminToken), &ExportGetInput{ID: projectID, ExportID: created.Body.ID})
	if err != nil || !called {
		t.Fatal("administrator did not signal durable running cancellation", err)
	}
	job, err = s.loadExport(ctx, projectID, created.Body.ID)
	if err != nil || string(job.Totals) != `["total"]` || job.RowsBeforeLimitAtLeast == nil || *job.RowsBeforeLimitAtLeast != 17 || len(job.Columns) != 1 || len(job.Extremes) == 0 {
		t.Fatal("SQL result extras were not preserved in the manifest", err)
	}
}

func TestExportConcurrentIdempotencyAndQueueAcceptance(t *testing.T) {
	s := service(t, 20)
	enableTestExports(s, 1)
	ctx := exportContext(t, s, adminToken)
	projectID, _ := project(t, s)
	type result struct {
		job *ExportJobOutput
		err error
	}
	results := make(chan result, 12)
	for range 12 {
		go func() {
			job, err := s.createEventExport(ctx, exportInput(projectID, "one-concurrent-request"))
			results <- result{job, err}
		}()
	}
	jobID := ""
	for range 12 {
		result := <-results
		if result.err != nil {
			t.Fatal("concurrent exact retry was rejected", result.err)
		}
		if jobID == "" {
			jobID = result.job.Body.ID
		} else if result.job.Body.ID != jobID {
			t.Fatal("concurrent retries allocated multiple export jobs")
		}
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM export_jobs`).Scan(&count); err != nil || count != 1 {
		t.Fatal("concurrent export requests exceeded queue capacity", err)
	}
}
