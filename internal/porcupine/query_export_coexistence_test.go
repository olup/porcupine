package porcupine

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/query"
)

const coexistenceQuerySQL = "SELECT uuid FROM events"

type queryAdmissionHook struct {
	query.Reader
	before func(context.Context)
}

func (r *queryAdmissionHook) Query(ctx context.Context, project, sql string, params map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	if sql == coexistenceQuerySQL {
		r.before(ctx)
	}
	return r.Reader.Query(ctx, project, sql, params, limits)
}

func TestStandaloneQueryAndCaptureDuringExport(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	fake := &exportTestS3{}
	bucket := httptest.NewServer(fake)
	t.Cleanup(bucket.Close)
	config := output.ExportConfig{S3: exportstore.Config{
		Region: "us-east-1", Bucket: "private-exports", Prefix: "test", Endpoint: bucket.URL,
		AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true,
	}}
	if err := s.configureExports(&config); err != nil {
		t.Fatal(err)
	}
	id, ingest := project(t, s)
	_, token := key(t, s, []string{"events:read"}, []string{id})
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "before-export"), 200)
	duckLakeJourneyDrain(t, s)

	base := "/api/v1/projects/" + id
	created := request(t, s, "POST", base+"/exports", token, `{"idempotency_key":"coexistence","sql":"SELECT uuid FROM events"}`, 202)
	job := base + "/exports/" + created["id"].(string)

	query := &queryAdmissionHook{Reader: s.lake}
	s.standaloneReader = query
	assertQuery := func(before func(context.Context)) {
		t.Helper()
		entered := false
		query.before = func(ctx context.Context) {
			entered = true
			before(ctx)
		}
		rows := duckLakeJourneyQuery(t, s, id, token, coexistenceQuerySQL, nil)
		if !entered {
			t.Fatal("interactive query bypassed the admission hook")
		}
		if len(rows) != 1 || rows[0].(map[string]any)["uuid"] != "before-export" {
			t.Fatalf("unexpected interactive result: %v", rows)
		}
	}
	// Run one worker cycle while the HTTP handler holds its interactive slot.
	assertQuery(func(ctx context.Context) {
		if worked, err := s.exports.RunOne(ctx); err != nil || worked {
			t.Fatalf("worker did not defer to active interactive query: worked=%t, error=%v", worked, err)
		}
		status := request(t, s, "GET", job, token, "", 200)
		if status["state"] != "queued" || status["attempts"] != float64(0) {
			t.Fatalf("deferred export was claimed: %v", status)
		}
	})

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	uploading, resumeUpload := make(chan struct{}), make(chan struct{})
	fake.mu.Lock()
	fake.onComplete = func() {
		close(uploading)
		select {
		case <-resumeUpload:
		case <-ctx.Done():
		}
	}
	fake.mu.Unlock()
	done := make(chan struct{})
	var worked bool
	var runErr error
	go func() {
		defer close(done)
		worked, runErr = s.exports.RunOne(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	select {
	case <-uploading:
	case <-done:
		t.Fatalf("export returned before upload: worked=%t, error=%v", worked, runErr)
	case <-ctx.Done():
		t.Fatal("export did not reach upload")
	}
	status := request(t, s, "GET", job, token, "", 200)
	if status["state"] != "running" {
		t.Fatalf("expected an active export during upload: %v", status)
	}
	request(t, s, "GET", job+"/files", token, "", 409)

	assertQuery(func(context.Context) {
		request(t, s, "POST", base+"/query", token, `{"sql":"SELECT event FROM events"}`, 429)
	})
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "during-export"), 200)
	if count := eventCount(t, s); count != 1 {
		t.Fatalf("capture did not commit while export was running: %d queued events", count)
	}

	close(resumeUpload)
	select {
	case <-done:
		if runErr != nil || !worked {
			t.Fatalf("export worker did not finish: worked=%t, error=%v", worked, runErr)
		}
	case <-ctx.Done():
		t.Fatal("export did not finish after upload resumed")
	}
	status = request(t, s, "GET", job, token, "", 200)
	if status["state"] != "succeeded" || status["attempts"] != float64(1) || status["rows"] != float64(1) {
		t.Fatalf("unexpected published export: %v", status)
	}
	files := request(t, s, "GET", job+"/files", token, "", 200)["files"].([]any)
	if len(files) != 1 {
		t.Fatalf("expected one published file: %v", files)
	}
	if download, _ := files[0].(map[string]any)["url"].(string); download == "" {
		t.Fatalf("published export is missing its download: %v", files)
	}
}
