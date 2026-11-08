package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/olup/porcupine/internal/exportstore"
)

type exportTestS3 struct {
	mu                         sync.Mutex
	seq                        int
	parts                      map[string][][]byte
	objects                    map[string][]byte
	aborts, deletes, completes int
	onComplete                 func()
	failComplete, failDelete   bool
}

func (f *exportTestS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	f.mu.Lock()
	if f.parts == nil {
		f.parts = map[string][][]byte{}
		f.objects = map[string][]byte{}
	}
	switch {
	case r.Method == "POST" && r.URL.Query().Has("uploads"):
		f.seq++
		id := fmt.Sprintf("upload-%d", f.seq)
		f.parts[id] = nil
		f.mu.Unlock()
		fmt.Fprintf(w, `<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, id)
	case r.Method == "PUT":
		body, _ := io.ReadAll(r.Body)
		id := r.URL.Query().Get("uploadId")
		f.parts[id] = append(f.parts[id], body)
		f.mu.Unlock()
		w.Header().Set("ETag", `"part"`)
	case r.Method == "POST":
		f.completes++
		id := r.URL.Query().Get("uploadId")
		f.objects[r.URL.Path] = bytes.Join(f.parts[id], nil)
		delete(f.parts, id)
		hook, fail := f.onComplete, f.failComplete
		f.mu.Unlock()
		if hook != nil {
			hook()
		}
		if fail {
			w.WriteHeader(403)
			io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>private store error</Message></Error>`)
			return
		}
		io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"completed"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == "DELETE" && r.URL.Query().Has("uploadId"):
		f.aborts++
		delete(f.parts, r.URL.Query().Get("uploadId"))
		f.mu.Unlock()
		w.WriteHeader(204)
	case r.Method == "DELETE":
		f.deletes++
		fail := f.failDelete
		if !fail {
			delete(f.objects, r.URL.Path)
		}
		f.mu.Unlock()
		if fail {
			w.WriteHeader(403)
			io.WriteString(w, `<Error><Code>AccessDenied</Code></Error>`)
		} else {
			w.WriteHeader(204)
		}
	default:
		f.mu.Unlock()
		w.WriteHeader(400)
	}
}

func exportWorkerFixture(t *testing.T, fake *exportTestS3) (*Service, storedExport) {
	t.Helper()
	s := service(t, 100)
	config, err := normalizeExportConfig(ExportConfig{BandwidthBytesPerSecond: 100 << 20, FileBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	store, err := exportstore.New(context.Background(), exportstore.Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "test-key", SecretAccessKey: "test-secret", UsePathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	s.exports = &exportRuntime{config: config, store: store, wake: make(chan struct{}, 1)}
	projectID, _ := project(t, s)
	_, token := key(t, s, []string{"events:read"}, []string{projectID})
	created, err := s.createEventExport(exportContext(t, s, token), exportInput(projectID, "worker-test"))
	if err != nil {
		t.Fatal(err)
	}
	queued, err := s.loadExport(context.Background(), projectID, created.Body.ID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.claimExport(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	return s, job
}

func TestExportSinkGzipChunksTypedJSONAndChecksums(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	s.exports.config.FileBytes = 42
	sink := newExportSink(context.Background(), s, job)
	columns := []AnalyticsResultColumn{{Name: "id", Type: "UInt64"}, {Name: "note", Type: "String"}}
	if err := sink.setColumns(columns); err != nil {
		t.Fatal(err)
	}
	for _, row := range [][]json.RawMessage{{json.RawMessage(`"18446744073709551615"`), json.RawMessage(`"<first>"`)}, {json.RawMessage(`"2"`), json.RawMessage(`"second"`)}} {
		if err := sink.writeRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.finish(); err != nil {
		t.Fatal(err)
	}
	artifacts, err := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts) != 2 || sink.rows != 2 {
		t.Fatal("sink did not rotate at row boundaries")
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	for _, artifact := range artifacts {
		body := fake.objects["/private-exports/isolated/"+artifact.Key]
		digest := sha256.Sum256(body)
		if artifact.SHA256 != hex.EncodeToString(digest[:]) || artifact.Bytes != int64(len(body)) || artifact.Rows != 1 || !artifact.Complete {
			t.Fatal("compressed manifest statistics mismatch")
		}
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if artifact.DecodedBytes != int64(len(decoded)) || !json.Valid(bytes.TrimSpace(decoded)) {
			t.Fatal("invalid JSONL or decoded byte count")
		}
	}
}

func TestExportSinkCSVHeaderEveryChunkAndEmptyResults(t *testing.T) {
	fake := &exportTestS3{}
	s, job := exportWorkerFixture(t, fake)
	job.Format = "csv"
	s.exports.config.FileBytes = 1
	sink := newExportSink(context.Background(), s, job)
	if err := sink.setColumns([]AnalyticsResultColumn{{Name: "value", Type: "String"}}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"comma, newline\nquote\""`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sink.finish(); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	bodies := []string{}
	for _, body := range fake.objects {
		reader, _ := gzip.NewReader(bytes.NewReader(body))
		data, _ := io.ReadAll(reader)
		reader.Close()
		bodies = append(bodies, string(data))
	}
	fake.mu.Unlock()
	if len(bodies) != 2 {
		t.Fatal("CSV files did not rotate")
	}
	for _, body := range bodies {
		records, err := csv.NewReader(strings.NewReader(body)).ReadAll()
		if err != nil || len(records) != 2 || records[0][0] != "value" || records[1][0] != "comma, newline\nquote\"" {
			t.Fatal("CSV header/escaping failed", err)
		}
	}
	// An empty export still produces one valid gzip file, containing the CSV header.
	job.AttemptID = newID()
	sink = newExportSink(context.Background(), s, job)
	sink.setColumns([]AnalyticsResultColumn{{Name: "value", Type: "String"}})
	if err := sink.finish(); err != nil {
		t.Fatal(err)
	}
	if sink.files != 1 || sink.rows != 0 || sink.decoded != int64(len("value\n")) {
		t.Fatal("empty CSV export lost its header")
	}
}

func TestExportSinkRejectsOutputAndColumnAmbiguity(t *testing.T) {
	s, job := exportWorkerFixture(t, &exportTestS3{})
	sink := newExportSink(context.Background(), s, job)
	if err := sink.setColumns([]AnalyticsResultColumn{{Name: "duplicate"}, {Name: "duplicate"}}); err == nil {
		t.Fatal("ambiguous JSONL columns accepted")
	}
	s.exports.config.MaxOutputBytes = 5
	sink = newExportSink(context.Background(), s, job)
	sink.setColumns([]AnalyticsResultColumn{{Name: "value"}})
	if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"long value"`)}); err != errExportOutputLimit {
		t.Fatal("output was truncated instead of rejected", err)
	}
	artifacts, _ := s.exportArtifacts(context.Background(), job.ID, job.AttemptID)
	if len(artifacts) != 1 || artifacts[0].Complete {
		t.Fatal("oversized output was published")
	}
}
