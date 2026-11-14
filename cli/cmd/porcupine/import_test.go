package main

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func nativeImportLine(id string) string {
	return fmt.Sprintf(`{"type":"track","message_id":%q,"name":"clicked","timestamp":"2020-01-01T23:15:00.123456789-02:00","properties":{"large":9007199254740993}}`+"\n", id)
}
func writeImportTestFile(t *testing.T, filename, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filename, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
}
func TestImportPreflightDirectoriesGzipAndFormatErrors(t *testing.T) {
	root := t.TempDir()
	writeImportTestFile(t, filepath.Join(root, "events", "part-1.jsonl"), nativeImportLine("one"))
	compressed := filepath.Join(root, "events", "part-2.jsonl.gz")
	file, err := os.Create(compressed)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	io.WriteString(writer, nativeImportLine("two"))
	writer.Close()
	file.Close()
	writeImportTestFile(t, filepath.Join(root, "README.txt"), "ignore non-data files")
	files, err := prepareImportFiles(t.Context(), root, "native", "auto")
	if err != nil || len(files) != 2 {
		t.Fatal(files, err)
	}
	if files[1].Size != int64(len(nativeImportLine("two"))) {
		t.Fatal("gzip offsets must count decompressed bytes")
	}
	sum := sha256.Sum256([]byte(nativeImportLine("two")))
	if files[1].SHA256 != hex.EncodeToString(sum[:]) {
		t.Fatal("compressed file manifest has wrong checksum")
	}
	for _, tc := range []struct{ format, row, field string }{
		{"native", `{"type":"track","timestamp":"2020-01-01T00:00:00Z"}`, "message_id"},
		{"segment", `{"type":"track","messageId":"one"}`, "timestamp"},
		{"native", `[{"type":"pageview"}]`, "one JSON object"},
		{"posthog", `{"uuid":"one","event":"x","distinct_id":"a","timestamp":"wrong"}`, "timestamp"},
	} {
		target := filepath.Join(t.TempDir(), "broken.jsonl")
		writeImportTestFile(t, target, tc.row+"\n")
		_, err := prepareImportFiles(t.Context(), target, tc.format, "events")
		if err == nil || !strings.Contains(err.Error(), "line 1") || !strings.Contains(err.Error(), tc.field) {
			t.Fatalf("missing useful validation error: %v", err)
		}
	}
	oversized := filepath.Join(t.TempDir(), "huge.jsonl")
	writeImportTestFile(t, oversized, strings.Repeat("x", importRowLimit+1))
	if _, err := prepareImportFiles(t.Context(), oversized, "native", "events"); err == nil {
		t.Fatal("unbounded JSONL row accepted")
	}
	if err := os.Symlink(files[0].filename, filepath.Join(root, "linked.jsonl")); err == nil {
		if _, err = prepareImportFiles(t.Context(), root, "native", "auto"); err == nil {
			t.Fatal("directory followed a symlink")
		}
	}
}

func TestImportPersonsSortAndOriginalByteHash(t *testing.T) {
	root := t.TempDir()
	person := "{\"person_id\":\"p\",\"distinct_id\":\"a\"}\r\n"
	event := "{\"uuid\":\"a\",\"event\":\"x\",\"distinct_id\":\"a\",\"timestamp\":\"2020-01-01 00:00:00\"}\r\n"
	writeImportTestFile(t, filepath.Join(root, "persons", "p.jsonl"), person)
	writeImportTestFile(t, filepath.Join(root, "events", "e.jsonl"), event)
	files, err := prepareImportFiles(t.Context(), root, "posthog", "auto")
	if err != nil {
		t.Fatal(err)
	}
	if files[0].Kind != "persons" || files[1].Kind != "events" {
		t.Fatal("persons must precede events")
	}
	sum := sha256.Sum256([]byte(person))
	if files[0].SHA256 != hex.EncodeToString(sum[:]) || files[0].Size != int64(len(person)) {
		t.Fatal("preflight changed CRLF source bytes")
	}
}

func TestImportCommandResumeLostAckAndBoundedChunks(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "private-import-key")
	source := filepath.Join(t.TempDir(), "events.jsonl")
	data := nativeImportLine("one") + nativeImportLine("two") + nativeImportLine("three")
	writeImportTestFile(t, source, data)
	var mu sync.Mutex
	var job importJob
	var accepted []byte
	var idem string
	var dropped bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer private-import-key" {
			t.Error("missing credential")
		}
		w.Header().Set("Content-Type", "application/json")
		base := "/api/v1/projects/project-a/imports"
		switch {
		case r.Method == "POST" && r.URL.Path == base:
			var body struct {
				Format string           `json:"format"`
				Files  []importFileSpec `json:"files"`
				Key    string           `json:"idempotency_key"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if idem != "" && idem != body.Key {
				t.Error("retry changed idempotency key")
			}
			idem = body.Key
			if job.ID == "" {
				job.ID = "job-one"
				job.Project = "project-a"
				job.Format = body.Format
				job.State = "uploading"
				job.Files = []importFileProgress{{importFileSpec: body.Files[0], ID: "file-one", State: "uploading"}}
				job.Limits.ChunkBytes = 211
				job.Limits.ChunkRows = 1
			}
			w.WriteHeader(201)
		case r.Method == "GET" && r.URL.Path == base+"/job-one":
		case r.Method == "PUT" && r.URL.Path == base+"/job-one/files/file-one":
			raw, _ := io.ReadAll(r.Body)
			if len(raw) > 211 || bytes.Count(raw, []byte{'\n'}) > 1 {
				t.Error("chunk exceeds discovered limits")
			}
			sum := sha256.Sum256(raw)
			if r.Header.Get("X-Chunk-SHA256") != hex.EncodeToString(sum[:]) {
				t.Error("bad chunk checksum")
			}
			offset := r.Header.Get("Upload-Offset")
			if offset == fmt.Sprint(len(accepted)) {
				accepted = append(accepted, raw...)
				job.Files[0].Offset = int64(len(accepted))
			} else if !bytes.Equal(accepted[len(accepted)-len(raw):], raw) {
				t.Error("retry altered acknowledged bytes")
			}
			if !dropped {
				dropped = true
				connection, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				connection.Close()
				return
			}
		case r.Method == "POST" && r.URL.Path == base+"/job-one/files/file-one/complete":
			if string(accepted) != data {
				t.Error("uploaded bytes differ")
			}
			job.Files[0].State = "completed"
		case r.Method == "POST" && r.URL.Path == base+"/job-one/complete":
			job.State = "completed"
			job.Counts.Accepted = 3
			job.Counts.Rows = 3
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(job)
	}))
	defer server.Close()
	args := []string{"import", "--project", "project-a", "--format", "native", "--no-wait", source}
	first, err := invoke(t, server.URL, args...)
	if err != nil {
		t.Fatal(err)
	}
	second, err := invoke(t, server.URL, args...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]byte(first), []byte(second)) {
		t.Fatal("repeating the command changed its completed job")
	}
	if string(accepted) != data || !dropped {
		t.Fatal("lost ACK or exact file upload was not exercised")
	}
	if strings.Contains(first, "private-import-key") || strings.Contains(first, "9007199254740993") {
		t.Fatal("summary exposed a secret or event content")
	}
}

func TestImportCommandRefusesManifestChangesAndRedirects(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "test-key")
	source := filepath.Join(t.TempDir(), "events.jsonl")
	writeImportTestFile(t, source, nativeImportLine("one"))
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("import followed a redirect") }))
	defer other.Close()
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 307) }))
	defer host.Close()
	if _, err := invoke(t, host.URL, "import", "--project", "p", "--format", "native", "--no-wait", source); err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatal("redirect accepted", err)
	}
	job := importJob{ID: "job", Project: "p", Format: "native", State: "uploading", Files: []importFileProgress{{importFileSpec: importFileSpec{Path: "events.jsonl", Kind: "events", Size: 1, SHA256: "changed"}, ID: "file", State: "uploading"}}}
	job.Limits.ChunkBytes = 256
	job.Limits.ChunkRows = 10
	if err := validateImportJob(job, "p", "native", []importFileSpec{{Path: "events.jsonl", Kind: "events", Size: 1, SHA256: "original"}}); err == nil {
		t.Fatal("resume accepted different source content")
	}
	for _, arg := range []string{"../p", "a/b", ".."} {
		if _, err := invoke(t, host.URL, "import", "--project", arg, "--format", "native", source); err == nil {
			t.Fatal("invalid project selector accepted")
		}
	}
}

func TestImportReadinessIsSeparateFromAcceptance(t *testing.T) {
	t.Setenv("PORCUPINE_API_KEY", "test-key")
	source := filepath.Join(t.TempDir(), "events.jsonl")
	writeImportTestFile(t, source, nativeImportLine("one"))
	// A completed upload with no analytical backend must report acceptance,
	// while default mode fails instead of claiming that SQL can see the rows.
	files, err := prepareImportFiles(t.Context(), source, "native", "events")
	if err != nil {
		t.Fatal(err)
	}
	var job importJob
	job.ID = "job"
	job.Project = "p"
	job.Format = "native"
	job.State = "completed"
	job.Files = []importFileProgress{{importFileSpec: files[0].importFileSpec, ID: "file", Offset: files[0].Size, State: "completed"}}
	job.Limits.ChunkBytes = 256
	job.Limits.ChunkRows = 10
	raw, _ := json.Marshal(map[string]any{"enabled": false, "pending": 1, "ready": false})
	json.Unmarshal(raw, &job.Delivery)
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(job)
	}))
	defer host.Close()
	output, err := invoke(t, host.URL, "import", "--project", "p", "--format", "native", source)
	if err == nil || !strings.Contains(err.Error(), "not configured") || !strings.Contains(output, `"completed"`) {
		t.Fatal("acceptance masquerades as queryability", output, err)
	}
	if _, err := invoke(t, host.URL, "import", "--project", "p", "--format", "native", "--no-wait", source); err != nil {
		t.Fatal(err)
	}
}
