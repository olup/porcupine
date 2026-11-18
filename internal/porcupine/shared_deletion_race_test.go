package porcupine

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type sharedDeletionHTTPResult struct {
	status int
	body   []byte
	err    error
}

func sharedDeletionHTTPRequest(t *testing.T, origin, method, path string, body []byte) <-chan sharedDeletionHTTPResult {
	t.Helper()
	done := make(chan sharedDeletionHTTPResult, 1)
	request, err := http.NewRequestWithContext(t.Context(), method, origin+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+adminToken)
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	go func() {
		response, err := (&http.Client{Timeout: 10 * time.Second}).Do(request)
		if err != nil {
			done <- sharedDeletionHTTPResult{err: err}
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		done <- sharedDeletionHTTPResult{status: response.StatusCode, body: raw, err: err}
	}()
	return done
}

func awaitSharedDeletionHTTP(t *testing.T, done <-chan sharedDeletionHTTPResult, timeout time.Duration) sharedDeletionHTTPResult {
	t.Helper()
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal("shared deletion journey request failed", result.err)
		}
		return result
	case <-time.After(timeout):
		t.Fatal("shared deletion journey request did not finish")
		return sharedDeletionHTTPResult{}
	}
}

func TestStandaloneProjectDeletionCancelsBlockedMaterialization(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "Deletion race target")
	kept, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "Unrelated project")
	// Occupy the sole local SQLite connection. The import must register activity
	// before it waits to materialize its project and local actor anchor.
	held, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback()
	if _, err := held.ExecContext(t.Context(), `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		t.Fatal(err)
	}
	empty := sha256.Sum256(nil)
	manifest, _ := json.Marshal(map[string]any{"format": "native", "idempotency_key": "racing-manifest", "files": []map[string]any{{"path": "private-after-delete.jsonl", "kind": "events", "size_bytes": 0, "sha256": fmt.Sprintf("%x", empty)}}})
	creating := sharedDeletionHTTPRequest(t, host.URL, "POST", "/api/v1/projects/"+project+"/imports", manifest)
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.activity.mu.Lock()
		activity := s.activity.projects[project]
		registered := activity != nil && len(activity.work) > 0
		s.activity.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("import request did not register project activity before local materialization")
		}
		time.Sleep(time.Millisecond)
	}
	deleting := sharedDeletionHTTPRequest(t, host.URL, "DELETE", "/api/v1/projects/"+project, nil)
	// DELETE must cancel the blocked child without requiring the lock holder to
	// release SQLite first. Otherwise the old check/materialize race reappears.
	created := awaitSharedDeletionHTTP(t, creating, 2*time.Second)
	if created.status == http.StatusCreated {
		t.Fatal("canceled import created a manifest during deletion")
	}
	if err := held.Rollback(); err != nil {
		t.Fatal(err)
	}
	deleted := awaitSharedDeletionHTTP(t, deleting, 5*time.Second)
	if deleted.status != http.StatusNoContent {
		t.Fatalf("delete returned %d: %s", deleted.status, deleted.body)
	}
	for _, table := range []string{"projects", "events", "import_uploads", "pipelines"} {
		column := "project_id"
		if table == "projects" {
			column = "id"
		}
		var rows int
		if err := s.db.QueryRow(`SELECT count(*) FROM `+table+` WHERE `+column+`=$1`, project).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("deleted local %s resurrected: rows=%d error=%v", table, rows, err)
		}
	}
	var active, retained int
	if err := s.control.QueryRow(`SELECT count(*) FROM projects WHERE id=$1`, project).Scan(&active); err != nil || active != 0 {
		t.Fatal("deleted project remained", active, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM projects WHERE id=$1`, kept).Scan(&retained); err != nil || retained != 1 {
		t.Fatal("deletion changed unrelated project", retained, err)
	}
	// Completed deletion retries remain idempotent when concurrent.
	left := sharedDeletionHTTPRequest(t, host.URL, "DELETE", "/api/v1/projects/"+project, nil)
	right := sharedDeletionHTTPRequest(t, host.URL, "DELETE", "/api/v1/projects/"+project, nil)
	for _, done := range []<-chan sharedDeletionHTTPResult{left, right} {
		if result := awaitSharedDeletionHTTP(t, done, 3*time.Second); result.status != http.StatusNoContent {
			t.Fatalf("completed deletion retry returned %d: %s", result.status, result.body)
		}
	}
}
