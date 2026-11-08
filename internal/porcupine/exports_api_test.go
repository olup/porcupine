package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

func TestExportHTTPAndMCPShareAuthorizationIdempotencyAndContracts(t *testing.T) {
	s := service(t, 20)
	enableTestExports(s, 100)
	projectID, ingest := project(t, s)
	otherProject, _ := project(t, s)
	creatorID, creator := key(t, s, []string{"events:read"}, []string{projectID})
	_, peer := key(t, s, []string{"events:read"}, []string{projectID})
	_, wrongPermission := key(t, s, []string{"events:write"}, []string{projectID})
	base := "/api/v1/projects/" + projectID + "/exports"
	body := `{"idempotency_key":"via-http","sql":"SELECT uuid FROM events"}`
	request(t, s, "POST", base, "", body, 401)
	request(t, s, "POST", base, wrongPermission, body, 403)
	request(t, s, "POST", "/api/v1/projects/"+otherProject+"/exports", creator, body, 403)
	created := request(t, s, "POST", base, creator, body, 202)
	jobID := created["id"].(string)
	retry := request(t, s, "POST", base, creator, body, 202)
	if retry["id"] != jobID || created["format"] != "jsonl" || created["state"] != "queued" {
		t.Fatal("HTTP create/default format/idempotency contract failed")
	}
	request(t, s, "POST", base, creator, `{"idempotency_key":"via-http","sql":"SELECT event FROM events"}`, 409)
	request(t, s, "GET", "/api/v1/projects/"+otherProject+"/exports/"+jobID, creator, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+otherProject+"/exports/"+jobID, adminToken, "", 404)
	request(t, s, "GET", base+"/"+jobID+"/files", creator, "", 409)
	request(t, s, "DELETE", base+"/"+jobID, peer, "", 403)
	request(t, s, "GET", base+"?limit=101", creator, "", 422)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, creator, "")
	catalog := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 20}, false)
	operations := []string{}
	for _, item := range catalog["operations"].([]any) {
		operations = append(operations, item.(map[string]any)["operation_id"].(string))
	}
	for _, operation := range []string{"create-event-export", "list-event-exports", "get-event-export", "cancel-event-export", "list-event-export-files"} {
		if !slices.Contains(operations, operation) {
			t.Fatalf("MCP discovery omitted %s", operation)
		}
	}
	mcpJob := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-event-export", Path: map[string]string{"id": projectID}, Body: map[string]any{"idempotency_key": "via-mcp", "sql": "SELECT uuid FROM events", "format": "csv"}}, 202)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-export", Path: map[string]string{"id": projectID, "export_id": mcpJob["id"].(string)}}, 200)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-export", Path: map[string]string{"id": otherProject, "export_id": mcpJob["id"].(string)}}, 403)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "cancel-event-export", Path: map[string]string{"id": projectID, "export_id": mcpJob["id"].(string)}}, 202)
	request(t, s, "DELETE", "/api/v1/access-keys/"+creatorID, adminToken, "", 204)
	request(t, s, "GET", base+"/"+jobID, creator, "", 401)
	if _, err := session.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatal("revoked key retained export MCP access", err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "ingestion-still-available"), 200)
}

func TestExportFilesPaginationFreshLinksAttemptIsolationAndIntegerExpiry(t *testing.T) {
	s := service(t, 20)
	enableTestExports(s, 100)
	ctx := exportContext(t, s, adminToken)
	store, err := exportstore.New(context.Background(), exportstore.Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "porcupine-exports", Endpoint: "http://127.0.0.1:9000", AccessKeyID: "test-access", SecretAccessKey: "test-secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	s.exports.store = store
	clock := time.Now().UTC().Truncate(time.Second)
	s.clock = func() time.Time { return clock }
	projectID, _ := project(t, s)
	created, err := s.createEventExport(ctx, exportInput(projectID, "download"))
	if err != nil {
		t.Fatal(err)
	}
	jobID := created.Body.ID
	expires := clock.Add(time.Hour)
	if _, err := s.db.Exec(`UPDATE export_jobs SET state='succeeded',attempt_id='current',file_count=3,expires_at=?,expires_at_ms=? WHERE id=?`, expires.Format(time.RFC3339Nano), expires.UnixMilli(), jobID); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := s.db.Exec(`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,complete,rows,bytes,decoded_bytes,sha256) VALUES(?,'current',?,?,1,10,100,200,?)`, jobID, i, fmt.Sprintf("jobs/%s/current/%d.jsonl.gz", jobID, i), strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
	}
	for _, attempt := range []string{"current", "old-attempt"} {
		if _, err := s.db.Exec(`INSERT INTO export_files(job_id,attempt_id,file_index,object_key,complete) VALUES(?,?,99,'must-not-be-exposed',?)`, jobID, attempt, attempt == "old-attempt"); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.eventExportFiles(ctx, &ExportFilesInput{ID: projectID, ExportID: jobID, Limit: 2})
	if err != nil || len(page.Body.Files) != 2 || page.Body.NextOffset == nil || *page.Body.NextOffset != 2 {
		t.Fatalf("file pagination failed: %+v %v", page, err)
	}
	first := page.Body.Files[0]
	parsed, err := url.Parse(first.URL)
	if err != nil || parsed.Query().Get("X-Amz-Expires") != "300" || parsed.Query().Get("X-Amz-Signature") == "" || first.Name != "part-000000.jsonl.gz" || first.ExpiresAt != clock.Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatalf("download URL was not freshly presigned for five minutes: %+v %v", first, err)
	}
	encoded, _ := json.Marshal(page.Body)
	for _, private := range []string{"object_key", "upload_id", "test-secret", "must-not-be-exposed"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("file page exposed private metadata %s", private)
		}
	}
	last, err := s.eventExportFiles(ctx, &ExportFilesInput{ID: projectID, ExportID: jobID, Limit: 2, Offset: 2})
	if err != nil || len(last.Body.Files) != 1 || last.Body.NextOffset != nil || last.Body.Files[0].Index != 2 {
		t.Fatal("file page leaked incomplete or previous-attempt artifacts", err)
	}
	clock = clock.Add(4 * time.Minute)
	fresh, err := s.eventExportFiles(ctx, &ExportFilesInput{ID: projectID, ExportID: jobID, Limit: 1})
	if err != nil || fresh.Body.Files[0].ExpiresAt != clock.Add(5*time.Minute).Format(time.RFC3339Nano) {
		t.Fatal("file page reused an old expiration", err)
	}
	// The integer expiry is authoritative even if the RFC3339 text sorts later.
	if _, err := s.db.Exec(`UPDATE export_jobs SET expires_at='2099-01-01T00:00:00Z',expires_at_ms=? WHERE id=?`, clock.UnixMilli(), jobID); err != nil {
		t.Fatal(err)
	}
	_, err = s.eventExportFiles(ctx, &ExportFilesInput{ID: projectID, ExportID: jobID, Limit: 1})
	mutationErrorStatus(t, err, 410)
	current, err := s.getEventExport(ctx, &ExportGetInput{ID: projectID, ExportID: jobID})
	if err != nil || current.Body.State != ExportExpired {
		t.Fatal("public status ignored integer expiry", err)
	}
	if _, err := s.db.Exec(`UPDATE export_jobs SET expires_at_ms=? WHERE id=?`, clock.Add(2*time.Minute).UnixMilli(), jobID); err != nil {
		t.Fatal(err)
	}
	nearExpiry, err := s.eventExportFiles(ctx, &ExportFilesInput{ID: projectID, ExportID: jobID, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ = url.Parse(nearExpiry.Body.Files[0].URL)
	if parsed.Query().Get("X-Amz-Expires") != "120" {
		t.Fatal("download link outlived artifact retention")
	}
	artifacts, err := s.exportArtifacts(ctx, jobID, "current")
	if err != nil || len(artifacts) != 4 || artifacts[3].Complete {
		t.Fatal("worker artifact helper omitted incomplete upload cleanup metadata", err)
	}
}

func TestExportListLimitsDoNotExposeOperatorSecretsAndRemainProjectScoped(t *testing.T) {
	s := service(t, 20)
	enableTestExports(s, 100)
	s.exports.config.S3 = exportstore.Config{Bucket: "secret-bucket", Endpoint: "https://private.example", AccessKeyID: "private-access", SecretAccessKey: "private-secret"}
	ctx := exportContext(t, s, adminToken)
	projectID, _ := project(t, s)
	otherProject, _ := project(t, s)
	for _, entry := range []struct{ project, key string }{{projectID, "one"}, {projectID, "two"}, {otherProject, "hidden"}} {
		if _, err := s.createEventExport(ctx, exportInput(entry.project, entry.key)); err != nil {
			t.Fatal(err)
		}
	}
	page, err := s.listEventExports(ctx, &ExportListInput{ID: projectID, Limit: 1})
	if err != nil || !page.Body.Enabled || page.Body.Limits == nil || page.Body.Limits.MaxConcurrentJobs != 1 || page.Body.Limits.MaxQueuedJobs != 100 || len(page.Body.Jobs) != 1 || page.Body.NextOffset == nil || *page.Body.NextOffset != 1 {
		t.Fatalf("export limits/list page missing: %+v %v", page, err)
	}
	encoded, _ := json.Marshal(page.Body)
	for _, private := range []string{otherProject, "secret-bucket", "private.example", "private-access", "private-secret", "parameters", "SELECT"} {
		if strings.Contains(string(encoded), private) {
			t.Fatalf("export list exposed private value %s", private)
		}
	}
}
