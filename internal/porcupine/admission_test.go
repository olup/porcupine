package porcupine

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type slowUpload struct {
	started chan struct{}
	once    sync.Once
	io.Reader
}

func (s *slowUpload) Read(p []byte) (int, error) {
	s.once.Do(func() { close(s.started) })
	return s.Reader.Read(p)
}
func TestCapturePressureLeavesTokenExchangeAvailable(t *testing.T) {
	f := newOAuthIntegration(t)
	f.s.captureSlots = make(chan struct{}, 1)
	h := f.s.Handler()
	reader, writer := io.Pipe()
	upload := &slowUpload{started: make(chan struct{}), Reader: reader}
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/capture/", upload))
	}()
	<-upload.started
	defer func() { writer.Close(); <-done; reader.Close() }()
	for _, path := range []string{"/capture/", "/e/", "/i/v0/e/", "/batch/"} {
		reply := httptest.NewRecorder()
		h.ServeHTTP(reply, httptest.NewRequest("POST", path, strings.NewReader("invalid")))
		if reply.Code != 503 || reply.Header().Get("Retry-After") == "" {
			t.Fatalf("%s saturated capture status %d", path, reply.Code)
		}
	}
	// Exchange using the actual HTTP server while the capture slot is occupied.
	f.token(t, "bootstrap", adminToken, f.host.URL+"/api/v1")
}
func TestCaptureBatchLimit(t *testing.T) {
	s := service(t, 2000)
	_, key := project(t, s)
	event := `{"event":"sample","distinct_id":"alice","api_key":"` + key + `"}`
	oversized := "[" + strings.Repeat(event+",", 1000) + event + "]"
	for _, body := range []string{oversized, `{"batch":` + oversized + `}`} {
		request(t, s, "POST", "/batch/", "", body, 400)
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM events").Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("oversized batch left %d events: %v", pending, err)
	}
	atLimit := "[" + strings.Repeat(event+",", 999) + event + "]"
	result := request(t, s, "POST", "/batch/", "", atLimit, 200)
	if result["accepted"] != float64(1000) {
		t.Fatal("batch at supported limit was not accepted")
	}
}

func FuzzCaptureBatch(f *testing.F) {
	for _, s := range []string{"null", "[]", `[{"properties":{"language":"fr"}}]`, "[{},1]", "[[[]]]"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > maxBodyBytes {
			return
		}
		var events eventBatch
		err := json.Unmarshal([]byte(raw), &events)
		if err == nil && len(events) > maxCaptureEvents {
			t.Fatal("accepted oversized capture batch")
		}
	})
}

func TestCaptureRejectsEmptyAuthEvenWithAValidPublicKey(t *testing.T) {
	s := service(t, 10)
	_, key := project(t, s)
	for _, headers := range [][]string{{""}, {"", "Bearer invalid"}} {
		req := httptest.NewRequest("POST", "/capture/", strings.NewReader(captureBody(key, "event")))
		req.Header["Authorization"] = headers
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, req)
		if w.Code != 401 || eventCount(t, s) != 0 {
			t.Fatalf("ambiguous authentication accepted: %d", w.Code)
		}
	}
}
