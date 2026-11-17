package porcupine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/olup/porcupine/internal/exportstore"
	"github.com/olup/porcupine/internal/output"
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
	s := duckLakeJourneyService(t)
	config, err := output.NormalizeExportConfig(output.ExportConfig{BandwidthBytesPerSecond: 100 << 20, FileBytes: 1024})
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
	config.S3 = exportstore.Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}
	if err := s.configureExports(&config); err != nil {
		t.Fatal(err)
	}
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
	job, err := queued, error(nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, job
}
