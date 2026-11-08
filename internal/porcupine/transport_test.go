package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func transportRequest(t *testing.T, s *Service, method, path string, body []byte, headers map[string]string, want int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, bytes.NewReader(body))
	for key, value := range headers {
		r.Header.Set(key, value)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s got %d want %d: %s", method, path, w.Code, want, w.Body.String())
	}
	return w
}
func compressed(t *testing.T, body []byte) []byte {
	t.Helper()
	var b bytes.Buffer
	z := gzip.NewWriter(&b)
	if _, err := z.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func fixture(t *testing.T, name, key string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return bytes.ReplaceAll(b, []byte("{{KEY}}"), []byte(key))
}
func TestTransportFixturesAndStableRetries(t *testing.T) {
	for _, tc := range []struct{ name, path, distinct string }{
		{"capture.json", "/capture/", "root-user"},
		{"batch.json", "/batch/", "node-user"},
		{"browser-array.json", "/i/v0/e/", "browser-user"},
		{"browser-array.json", "/e/", "browser-user"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := service(t, 10)
			projectID, ingest := project(t, s)
			payload := fixture(t, tc.name, ingest)
			for _, retry := range []bool{false, true} {
				w := transportRequest(t, s, "POST", tc.path, payload, nil, 200)
				var v struct{ Accepted, Duplicates int }
				if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
					t.Fatal(err)
				}
				if retry && (v.Accepted != 0 || v.Duplicates != 1) || !retry && (v.Accepted != 1 || v.Duplicates != 0) {
					t.Fatalf("unexpected result %+v", v)
				}
			}
			result := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)
			events := result["events"].([]any)
			event := events[0].(map[string]any)
			if len(events) != 1 || event["project_id"] != projectID || event["distinct_id"] != tc.distinct {
				t.Fatalf("wrong persisted scope/identity: %+v", events)
			}
			props := event["properties"].(map[string]any)
			if _, ok := props["token"]; ok {
				t.Fatal("ingestion token leaked through event reader")
			}
			if tc.name == "capture.json" && (props["business_token"] != "keep-me" || props["amount"] != float64(42) || props["distinct_id"] != "property-user") {
				t.Fatal("business properties changed")
			}
			var stored string
			if err := s.db.QueryRow(`SELECT properties FROM events`).Scan(&stored); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(stored, ingest) {
				t.Fatal("raw ingestion token persisted")
			}
		})
	}
}
func TestIngestionGzipAndBodyLimits(t *testing.T) {
	s := service(t, 20)
	_, ingest := project(t, s)
	valid := fixture(t, "browser-array.json", ingest)
	gz := compressed(t, valid)
	for _, tc := range []struct {
		name, path string
		body       []byte
		headers    map[string]string
		code       int
	}{
		{"header gzip", "/i/v0/e/", gz, map[string]string{"Content-Encoding": "gzip"}, 200},
		{"query gzip", "/i/v0/e/?compression=gzip-js", gz, nil, 200},
		{"both markers", "/i/v0/e/?compression=gzip-js", gz, map[string]string{"Content-Encoding": "gzip"}, 200},
		{"browser unmarked gzip", "/e/", gz, map[string]string{"Content-Type": "text/plain;charset=UTF-8"}, 200},
		{"browser unmarked truncated gzip", "/e/", gz[:len(gz)-5], map[string]string{"Content-Type": "text/plain"}, 400},
		{"browser unmarked gzip bomb", "/e/", compressed(t, bytes.Repeat([]byte("a"), maxBodyBytes+1)), map[string]string{"Content-Type": "text/plain"}, 413},
		{"unknown encoding", "/capture/", valid, map[string]string{"Content-Encoding": "br"}, 415},
		{"unknown query compression", "/i/v0/e/?compression=lz64", valid, nil, 415},
		{"invalid gzip", "/capture/", []byte("invalid"), map[string]string{"Content-Encoding": "gzip"}, 400},
		{"truncated gzip", "/capture/", gz[:len(gz)-5], map[string]string{"Content-Encoding": "gzip"}, 400},
		{"gzip bomb", "/capture/", compressed(t, bytes.Repeat([]byte("a"), maxBodyBytes+1)), map[string]string{"Content-Encoding": "gzip"}, 413},
		{"compressed limit", "/capture/", bytes.Repeat([]byte("a"), maxBodyBytes+1), map[string]string{"Content-Encoding": "gzip"}, 413},
		{"raw limit", "/capture/", bytes.Repeat([]byte(" "), maxBodyBytes+1), nil, 413},
		{"trailing oversized whitespace", "/capture/", append(append([]byte{}, valid...), bytes.Repeat([]byte(" "), maxBodyBytes)...), nil, 413},
		{"multiple values", "/capture/", append(append([]byte{}, valid...), valid...), nil, 400},
	} {
		t.Run(tc.name, func(t *testing.T) { transportRequest(t, s, "POST", tc.path, tc.body, tc.headers, tc.code) })
	}
	if eventCount(t, s) != 1 {
		t.Fatal("failed input persisted or gzip retries not deduplicated")
	}
}
func TestTokenLocationsAndAtomicCredentials(t *testing.T) {
	s := service(t, 30)
	a, ingestA := project(t, s)
	_, ingestB := project(t, s)
	for _, body := range []string{
		`{"token":"` + ingestA + `","event":"ok","distinct_id":"root"}`,
		`{"event":"ok","properties":{"token":"` + ingestA + `","distinct_id":"property"}}`,
	} {
		request(t, s, "POST", "/capture/", "", body, 200)
	}
	for _, tc := range []struct {
		body string
		code int
	}{
		{`[{"properties":{"token":"` + ingestA + `"},"event":"ok","distinct_id":"a"},{"properties":{"token":"` + ingestB + `"},"event":"ok","distinct_id":"b"}]`, 400},
		{`{"api_key":"` + ingestA + `","batch":[{"event":"ok","distinct_id":"a"},{"event":"$identify","distinct_id":"b"}]}`, 400},
		{`{"api_key":"` + ingestA + `","batch":[{"event":"ok","distinct_id":"a"},{"event":"bad"}]}`, 400},
		{`{"api_key":"` + ingestA + `","batch":[{"token":"` + ingestB + `","event":"ok","distinct_id":"a"}]}`, 400},
		{`{"api_key":"wrong-key","event":"ok","distinct_id":"a"}`, 401},
		{`[{"properties":{"token":42},"event":"ok","distinct_id":"a"}]`, 400},
		{`{"api_key":"` + adminToken + `","event":"ok","distinct_id":"a"}`, 403},
	} {
		request(t, s, "POST", "/capture/", "", tc.body, tc.code)
	}
	id, revoked := key(t, s, []string{"events:write"}, []string{a})
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	request(t, s, "POST", "/i/v0/e/", "", `[{"properties":{"token":"`+revoked+`"},"event":"ok","distinct_id":"a"}]`, 401)
	if eventCount(t, s) != 2 {
		t.Fatal("invalid batch partially persisted")
	}
}
func TestIngestionCORS(t *testing.T) {
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, AllowedOrigins: []string{"https://app.example.test"}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_, ingest := project(t, s)
	allowed := map[string]string{"Origin": "https://app.example.test", "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "content-type, Content-Encoding"}
	w := transportRequest(t, s, "OPTIONS", "/i/v0/e/", nil, allowed, 204)
	if w.Body.Len() != 0 || w.Header().Get("Access-Control-Allow-Origin") != "https://app.example.test" || w.Header().Get("Access-Control-Allow-Credentials") != "" {
		t.Fatal("invalid preflight response")
	}
	for _, origin := range []string{"https://evil.test", "null", "https://app.example.test.evil.test", "http://app.example.test"} {
		for _, method := range []string{"POST", "OPTIONS"} {
			w := transportRequest(t, s, method, "/i/v0/e/", []byte(captureBody(ingest, "cors")), map[string]string{"Origin": origin}, 403)
			if w.Header().Get("Access-Control-Allow-Origin") != "" || strings.Contains(w.Body.String(), ingest) || strings.Contains(w.Body.String(), adminToken) {
				t.Fatal("disallowed origin leaked data")
			}
		}
	}
	transportRequest(t, s, "OPTIONS", "/i/v0/e/", nil, nil, 204)
	management := transportRequest(t, s, "OPTIONS", "/api/v1/projects", nil, allowed, 405)
	if management.Header().Get("Access-Control-Allow-Origin") != "" || strings.Contains(management.Body.String(), ingest) {
		t.Fatal("management endpoint exposed through CORS")
	}
	transportRequest(t, s, "OPTIONS", "/i/v0/e/", nil, map[string]string{"Origin": "https://app.example.test", "Access-Control-Request-Method": "DELETE"}, 403)
	transportRequest(t, s, "OPTIONS", "/i/v0/e/", nil, map[string]string{"Origin": "https://app.example.test", "Access-Control-Request-Headers": "authorization"}, 403)
	transportRequest(t, s, "POST", "/i/v0/e/", []byte(captureBody(ingest, "cors")), map[string]string{"Origin": "https://app.example.test"}, 200)
	if eventCount(t, s) != 1 {
		t.Fatal("OPTIONS or disallowed origin persisted events")
	}
	off := service(t, 10)
	transportRequest(t, off, "OPTIONS", "/capture/", nil, allowed, 403)
}
func TestAllowedOriginValidation(t *testing.T) {
	for _, origin := range []string{"*", "https://*.example.test", "null", "https://app.example.test/", "https://user:pass@app.example.test", "https://app.example.test?x=1", "https://app.example.test#x"} {
		s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, AllowedOrigins: []string{origin}})
		if s != nil {
			s.Close()
		}
		if err == nil {
			t.Fatalf("accepted origin %q", origin)
		}
	}
}
func TestReplacementConnectionRetainsSQLitePragmas(t *testing.T) {
	s := service(t, 10)
	s.db.SetMaxIdleConns(0)
	conn, err := s.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		name string
		want int
	}{{"foreign_keys", 1}, {"synchronous", 2}, {"busy_timeout", 5000}} {
		var got int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA "+tc.name).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Fatalf("PRAGMA %s = %d want %d", tc.name, got, tc.want)
		}
	}
	var mode string
	if err := conn.QueryRowContext(context.Background(), "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal mode = %q", mode)
	}
}

// Ensure bounded readers reject a read failure instead of acknowledging partial JSON.
func TestIngestionReadFailure(t *testing.T) {
	s := service(t, 10)
	_, ingest := project(t, s)
	r := httptest.NewRequest("POST", "/capture/", nil)
	r.Body = errorBody{Reader: strings.NewReader(captureBody(ingest, "read-error"))}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 400 || eventCount(t, s) != 0 {
		t.Fatal("read error was accepted")
	}
}

type errorBody struct{ io.Reader }

func (e errorBody) Read(p []byte) (int, error) {
	n, err := e.Reader.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}
func (errorBody) Close() error { return nil }

func TestExactBodyLimit(t *testing.T) {
	s := service(t, 10)
	_, ingest := project(t, s)
	payload := []byte(captureBody(ingest, "exact-limit"))
	payload = append(payload, bytes.Repeat([]byte(" "), maxBodyBytes-len(payload))...)
	transportRequest(t, s, "POST", "/capture/", payload, nil, 200)
	transportRequest(t, s, "POST", "/capture/", compressed(t, payload), map[string]string{"Content-Encoding": "gzip"}, 200)
	if eventCount(t, s) != 1 {
		t.Fatal("exact-boundary retry not deduplicated")
	}
}

func TestRecordedSDKFixtures(t *testing.T) {
	for _, name := range []string{"node-batch.json", "browser-capture.json", "browser-batch-gzip.json"} {
		t.Run(name, func(t *testing.T) {
			// These fixtures carry SDK/version and wire metadata from the real SDK suite.
			raw, err := os.ReadFile(filepath.Join("..", "..", "compat", "fixtures", name))
			if err != nil {
				t.Fatal(err)
			}
			var fixture struct {
				Provenance struct {
					SDK             string `json:"sdk"`
					Version         string `json:"version"`
					Path            string `json:"path"`
					ContentType     string `json:"content_type"`
					ContentEncoding string `json:"content_encoding"`
					BodyFormat      string `json:"body_format"`
				} `json:"provenance"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &fixture); err != nil {
				t.Fatal(err)
			}
			if fixture.Provenance.SDK == "" || fixture.Provenance.Version == "" {
				t.Fatal("SDK fixture lacks provenance")
			}
			s := service(t, 10)
			projectID, ingest := project(t, s)
			payload := bytes.ReplaceAll(fixture.Payload, []byte("FIXTURE_INGEST_TOKEN"), []byte(ingest))
			if fixture.Provenance.BodyFormat == "gzip" || fixture.Provenance.ContentEncoding == "gzip" {
				payload = compressed(t, payload)
			}
			headers := map[string]string{"Content-Type": fixture.Provenance.ContentType}
			if fixture.Provenance.ContentEncoding != "identity" {
				headers["Content-Encoding"] = fixture.Provenance.ContentEncoding
			}
			for i := 0; i < 2; i++ {
				transportRequest(t, s, "POST", fixture.Provenance.Path, payload, headers, 200)
			}
			result := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)
			events := result["events"].([]any)
			var original map[string]any
			if err := json.Unmarshal(fixture.Payload, &original); err != nil {
				t.Fatal(err)
			}
			expectedBatch := original["batch"].([]any)
			if len(events) != len(expectedBatch) {
				t.Fatalf("SDK fixture retry stored %d events, want %d", len(events), len(expectedBatch))
			}
			byUUID := map[string]map[string]any{}
			for _, value := range events {
				event := value.(map[string]any)
				byUUID[event["uuid"].(string)] = event
			}
			for _, value := range expectedBatch {
				expected := value.(map[string]any)
				event := byUUID[expected["uuid"].(string)]
				if event == nil {
					t.Fatal("SDK event UUID changed")
				}
				if event["project_id"] != projectID || event["timestamp"] != expected["timestamp"] {
					t.Fatal("SDK scope/timestamp changed")
				}
				expectedProps := expected["properties"].(map[string]any)
				delete(expectedProps, "token")
				expectedDistinct := expected["distinct_id"]
				if expectedDistinct == nil {
					expectedDistinct = expectedProps["distinct_id"]
				}
				props := event["properties"].(map[string]any)
				if event["event"] != expected["event"] || event["distinct_id"] != expectedDistinct || !reflect.DeepEqual(props, expectedProps) {
					t.Fatal("persisted SDK event does not match recorded fixture")
				}
				if _, exists := props["token"]; exists {
					t.Fatal("SDK token leaked into read API")
				}
				if nested, ok := props["nested"].(map[string]any); ok {
					if !strings.Contains(nested["unicode"].(string), "東京 🦔") || len(nested["list"].([]any)) != 3 {
						t.Fatal("SDK nested properties changed")
					}
				}
				var stored string
				if err := s.db.QueryRow(`SELECT properties FROM events WHERE uuid=?`, expected["uuid"]).Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if strings.Contains(stored, ingest) {
					t.Fatal("SDK token persisted")
				}
			}
		})
	}
}

func TestCookielessSentinelIsNotAVisitor(t *testing.T) {
	s := service(t, 100)
	_, key := project(t, s)
	body, _ := json.Marshal(map[string]any{"api_key": key, "batch": []map[string]any{
		{"event": "ordinary", "distinct_id": "visitor", "uuid": "ordinary"},
		{"event": "cookieless", "properties": map[string]any{"distinct_id": "$posthog_cookieless"}},
	}})
	transportRequest(t, s, "POST", "/batch/", body, nil, 200)
	var distinct string
	if err := s.db.QueryRow(`SELECT distinct_id FROM events WHERE event='cookieless'`).Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if eventCount(t, s) != 2 || !strings.HasPrefix(distinct, "cookieless_") {
		t.Fatal("cookieless sentinel was not resolved")
	}
}
