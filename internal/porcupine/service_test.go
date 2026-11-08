package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const adminToken = "test-admin-token-long-enough"

func TestBootstrapRequiresTokenAndCanRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	for _, token := range []string{"", "too-short"} {
		result := make(chan error, 1)
		go func() {
			s, err := Open(Config{DBPath: path, BootstrapToken: token})
			if s != nil {
				s.Close()
			}
			result <- err
		}()
		select {
		case err := <-result:
			if err == nil {
				t.Fatal("accepted invalid bootstrap token")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("bootstrap failure blocked")
		}
	}
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal("initialized database required bootstrap token", err)
	}
	s.Close()
}

func request(t *testing.T, s *Service, method, path, token, body string, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != status {
		t.Fatalf("%s %s: got %d, want %d: %s", method, path, w.Code, status, w.Body.String())
	}
	result := map[string]any{}
	if w.Body.Len() > 0 {
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
	}
	return result
}
func service(t *testing.T, max int) *Service {
	t.Helper()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, QueueMaxEvents: max})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func project(t *testing.T, s *Service) (string, string) {
	t.Helper()
	v := request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"app"}`, 201)
	return v["project"].(map[string]any)["id"].(string), v["ingest_token"].(string)
}
func key(t *testing.T, s *Service, permissions, projects []string) (string, string) {
	t.Helper()
	b, _ := json.Marshal(map[string]any{"name": "test key", "permissions": permissions, "projects": projects})
	v := request(t, s, "POST", "/api/v1/access-keys", adminToken, string(b), 201)
	return v["id"].(string), v["token"].(string)
}
func captureBody(token, id string) string {
	b, _ := json.Marshal(map[string]any{"api_key": token, "event": "purchase", "distinct_id": "user-1", "uuid": id})
	return string(b)
}
func eventCount(t *testing.T, s *Service) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRestartDedupAndBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	id, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "stable-id"), 200)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path, BootstrapToken: "replacement-token-long-enough"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	request(t, s, "GET", "/api/v1/projects", "replacement-token-long-enough", "", 401)
	request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)
	v := request(t, s, "POST", "/capture/", "", captureBody(ingest, "stable-id"), 200)
	if v["duplicates"] != float64(1) || eventCount(t, s) != 1 {
		t.Fatal("retry was not deduplicated")
	}
	_, reader := key(t, s, []string{"events:read"}, []string{id})
	v = request(t, s, "GET", "/api/v1/projects/"+id+"/events", reader, "", 200)
	events := v["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["delivered"] != false {
		t.Fatal("durable pending event missing")
	}
	var stored string
	if err = s.db.QueryRow(`SELECT token_hash FROM access_keys WHERE name='bootstrap admin'`).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored == adminToken || stored != hash(adminToken) {
		t.Fatal("bootstrap token not hashed")
	}
}
func TestScopesRevocationAndEscalation(t *testing.T) {
	s := service(t, 20)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "same"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "same"), 200)
	id, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	request(t, s, "GET", "/api/v1/projects/"+a+"/events", reader, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+b+"/events", reader, "", 403)
	v := request(t, s, "GET", "/api/v1/projects", reader, "", 200)
	if len(v["projects"].([]any)) != 1 {
		t.Fatal("project scope leaked")
	}
	request(t, s, "GET", "/api/v1/projects/"+a+"/events", ingestA, "", 403)
	_, manager := key(t, s, []string{"management:write", "events:read"}, []string{a})
	request(t, s, "POST", "/api/v1/access-keys", manager, `{"name":"bad","permissions":["instance:admin"],"projects":["*"]}`, 403)
	request(t, s, "POST", "/api/v1/access-keys", manager, `{"name":"bad","permissions":["events:read"],"projects":["`+b+`"]}`, 403)
	request(t, s, "POST", "/api/v1/access-keys", manager, `{"name":"bad","permissions":["events:read"],"projects":["*"]}`, 403)
	request(t, s, "POST", "/api/v1/projects", manager, `{"name":"bad"}`, 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+a+"/events", reader, "", 401)
	ingestID, revokable := key(t, s, []string{"events:write"}, []string{a})
	request(t, s, "DELETE", "/api/v1/access-keys/"+ingestID, adminToken, "", 204)
	request(t, s, "POST", "/capture/", "", captureBody(revokable, "revoked"), 401)
}
func TestAtomicBatchAndQueueCapacity(t *testing.T) {
	s := service(t, 2)
	_, ingest := project(t, s)
	request(t, s, "POST", "/batch/", "", `{"api_key":"`+ingest+`","batch":[{"uuid":"one","event":"ok","distinct_id":"user"},{"event":"bad"}]}`, 400)
	if eventCount(t, s) != 0 {
		t.Fatal("invalid batch partially committed")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "first"), 200)
	request(t, s, "POST", "/batch/", "", `{"api_key":"`+ingest+`","batch":[{"uuid":"second","event":"ok","distinct_id":"user"},{"uuid":"third","event":"ok","distinct_id":"user"}]}`, 503)
	if eventCount(t, s) != 1 {
		t.Fatal("full queue partially committed batch")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "second"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "second"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "third"), 503)
	if eventCount(t, s) != 2 {
		t.Fatal("queue capacity exceeded")
	}
}
func TestConcurrentCapacity(t *testing.T) {
	s := service(t, 1)
	_, ingest := project(t, s)
	var wg sync.WaitGroup
	codes := make(chan int, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/capture/", strings.NewReader(captureBody(ingest, string(rune('a'+i)))))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			codes <- w.Code
		}()
	}
	wg.Wait()
	close(codes)
	accepted := 0
	for c := range codes {
		if c == 200 {
			accepted++
		} else if c != 503 {
			t.Fatalf("unexpected response %d", c)
		}
	}
	if accepted != 1 || eventCount(t, s) != 1 {
		t.Fatal("concurrent queue exceeded capacity")
	}
}
func TestInputBoundaries(t *testing.T) {
	s := service(t, 20)
	_, ingest := project(t, s)
	for _, event := range []string{"$identify", "$create_alias", "$groupidentify", "$unknown"} {
		request(t, s, "POST", "/capture/", "", `{"api_key":"`+ingest+`","event":"`+event+`","distinct_id":"a"}`, 400)
	}
	request(t, s, "POST", "/capture/", "", `{"api_key":"`+ingest+`","event":"ok","distinct_id":"a","timestamp":"yesterday"}`, 400)
	request(t, s, "POST", "/capture/", "", `{"api_key":"`+ingest+`","event":"ok","distinct_id":"a","properties":{"big":"`+strings.Repeat("a", 1<<20)+`"}}`, 413)
	r := httptest.NewRequest("POST", "/capture/", strings.NewReader(`{}`))
	r.Header.Set("Content-Encoding", "gzip")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("invalid gzip payload accepted")
	}
	request(t, s, "GET", "/readyz", "", "", 200)
	request(t, s, "GET", "/healthz", "", "", 200)
	if eventCount(t, s) != 0 {
		t.Fatal("invalid input persisted")
	}
}
