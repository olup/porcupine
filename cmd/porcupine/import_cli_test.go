package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/porcupine"
)

func TestImportManifestPortableAndBounded(t *testing.T) {
	dir := t.TempDir()
	data := []byte("{\"uuid\":\"event-1\",\"event\":\"page\",\"distinct_id\":\"visitor\"}\n")
	if err := os.Mkdir(filepath.Join(dir, "events"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "events", "one.ndjson"), data, 0600); err != nil {
		t.Fatal(err)
	}
	persons := []byte("{\"distinct_id\":\"browser\",\"person_id\":\"person\",\"properties\":{}}\n")
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, _ = z.Write(persons)
	_ = z.Close()
	if err := os.WriteFile(filepath.Join(dir, "two.jsonl.gz"), compressed.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := importManifest(context.Background(), dir, "auto")
	if err != nil || len(files) != 2 || files[0].Path != "events/one.ndjson" || files[0].Kind != "events" || files[1].Kind != "persons" {
		t.Fatalf("manifest: %+v %v", files, err)
	}
	sum := sha256.Sum256(persons)
	if files[1].SHA256 != hex.EncodeToString(sum[:]) || files[1].Size != int64(len(persons)) {
		t.Fatal("hash and offsets must describe uncompressed input")
	}
	if err := os.Symlink(files[0].local, filepath.Join(dir, "link.jsonl")); err != nil {
		t.Fatal(err)
	}
	if _, err := importManifest(context.Background(), dir, "auto"); err == nil {
		t.Fatal("followed a symlink")
	}
}

// Use an actual server to exercise client, queue, authentication and the on-disk
// cursor together. Drop the response to a committed chunk, interrupt, then run
// the same command against a reopened SQLite database.
func TestImportCLIResumesAfterLostAcknowledgmentAndRestart(t *testing.T) {
	db := filepath.Join(t.TempDir(), "data.sqlite")
	const admin = "test-import-bootstrap-restart"
	open := func() *porcupine.Service {
		s, err := porcupine.Open(porcupine.Config{DBPath: db, BootstrapToken: admin})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var intercepted bool
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" && !intercepted {
			intercepted = true
			response := httptest.NewRecorder()
			s.Handler().ServeHTTP(response, r)
			if response.Code != 200 {
				t.Errorf("first chunk returned %d: %s", response.Code, response.Body)
				w.WriteHeader(response.Code)
				return
			}
			stop()
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		s.Handler().ServeHTTP(w, r)
	}))
	create := func(name string, body string) map[string]any {
		r, _ := http.NewRequest("POST", first.URL+"/api/v1/"+name, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+admin)
		r.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != 201 {
			t.Fatalf("create %s returned %d", name, res.StatusCode)
		}
		var v map[string]any
		if err = json.NewDecoder(res.Body).Decode(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	project := create("projects", `{"name":"test imports"}`)["project"].(map[string]any)["id"].(string)
	writer := create("access-keys", fmt.Sprintf(`{"name":"importer","projects":[%q],"permissions":["imports:write","events:write"]}`, project))["token"].(string)
	t.Setenv("PORCUPINE_ACCESS_TOKEN", writer)
	dir := t.TempDir()
	raw := strings.Builder{}
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&raw, `{"event":"test","uuid":"event-%d","distinct_id":"browser","timestamp":"2024-01-01T00:00:00Z","properties":{"payload":"%s"}}`+"\n", i, strings.Repeat("x", 40000))
	}
	var gz bytes.Buffer
	z := gzip.NewWriter(&gz)
	io.WriteString(z, raw.String())
	z.Close()
	if err := os.WriteFile(filepath.Join(dir, "events.jsonl.gz"), gz.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "extra.jsonl"), []byte(`{"event":"last","uuid":"event-last","distinct_id":"browser","timestamp":"2024-01-01T00:00:00Z"}`), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"posthog", dir, "--project", project}
	firstArgs := append(append([]string{}, args...), "--url", first.URL)
	err := runImport(ctx, firstArgs, io.Discard, io.Discard)
	first.Close()
	s.Close()
	if err == nil || !intercepted {
		t.Fatalf("failed to interrupt upload after first committed chunk: %v", err)
	}
	s = open()
	t.Cleanup(func() { s.Close() })
	var ranges []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			ranges = append(ranges, r.Header.Get("Upload-Offset"))
		}
		s.Handler().ServeHTTP(w, r)
	}))
	defer server.Close()
	var result bytes.Buffer
	if err = runImport(context.Background(), append(args, "--url", server.URL), &result, io.Discard); err != nil {
		t.Fatal(err)
	}
	var job importJob
	if err = json.Unmarshal(result.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if len(ranges) < 2 || ranges[0] != "262144" || job.State != "completed" || job.Counts["accepted"] != 13 || job.Counts["duplicates"] != 0 || len(job.Files) != 2 {
		t.Fatalf("bad resumed import: offsets=%v counts=%v state=%s", ranges, job.Counts, job.State)
	}
	ranges = nil
	if err = runImport(context.Background(), append(args, "--url", server.URL, "--resume", job.ID), io.Discard, io.Discard); err != nil || len(ranges) != 0 {
		t.Fatalf("repeated completed import uploaded data: %v, %v", ranges, err)
	}
}
