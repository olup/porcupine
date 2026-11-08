package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func testClickHouse(t *testing.T, handler http.HandlerFunc) *clickHouse {
	t.Helper()
	var initialized atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(strings.NewReader(string(body)))
		sql := string(body)
		if strings.HasPrefix(sql, migrationPrefix) {
			switch {
			case strings.Contains(sql, "SELECT legacy_uuid,version"):
				if initialized.Load() {
					io.WriteString(w, `{"legacy_uuid":"","version":"2"}`)
				}
			case strings.Contains(sql, "FROM system.tables"):
				if initialized.Load() {
					io.WriteString(w, `{"name":"events_v2","uuid":"test-v2"}`)
				}
			case strings.Contains(sql, "INSERT INTO analytics.schema_state"):
				initialized.Store(true)
			}
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	c, err := newClickHouse(ClickHouseConfig{URL: server.URL, Database: "analytics", User: "admin_user", Password: "admin_secret"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.client.CloseIdleConnections)
	return c
}

func TestClickHouseQueryCredentialsAndParameters(t *testing.T) {
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("X-ClickHouse-User") != "project_reader" || r.Header.Get("X-ClickHouse-Key") != "project_secret" {
			t.Error("query did not use project credentials")
		}
		if strings.Contains(r.URL.RawQuery, "secret") || r.URL.User != nil || r.Header.Get("Authorization") != "" {
			t.Error("credentials appeared outside ClickHouse headers")
		}
		if r.URL.Query().Get("database") != "project_database" || r.URL.Query().Get("param_person") != "a&b+雪" || r.URL.Query().Get("wait_end_of_query") != "1" || r.URL.Query().Get("default_format") != "JSON" {
			t.Error("query parameters were not forwarded correctly")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "SELECT {person:String}" {
			t.Errorf("unexpected query body %q", body)
		}
		io.WriteString(w, `{"data":[{"person":"a&b+雪"}]}`)
	})
	data, err := c.query(context.Background(), "project_reader", "project_secret", "project_database", "SELECT {person:String}", map[string]string{"person": "a&b+雪"})
	if err != nil || !json.Valid(data) {
		t.Fatalf("query failed: %v", err)
	}
}

func TestClickHouseInitializeIsIdempotentDDL(t *testing.T) {
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { t.Error("initialization escaped its control requests") })
	for range 2 {
		if err := c.initialize(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClickHouseInsertStableProjectionAndTimestampRange(t *testing.T) {
	var bodies []string
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "INSERT INTO analytics.events_v2 FORMAT JSONEachRow" || r.URL.Query().Get("async_insert") != "0" {
			t.Error("insert must be synchronous JSONEachRow")
		}
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
	})
	events := []Event{
		{ID: 42, ProjectID: "project", UUID: "stable", Event: "purchase", DistinctID: "person", Properties: json.RawMessage(`{"price":12.5}`), Timestamp: "0001-01-01T00:00:00Z", ReceivedAt: "2026-11-07T21:00:00.123456789Z"},
		{ID: 43, ProjectID: "project", UUID: "future", Event: "purchase", DistinctID: "person", Properties: json.RawMessage(`{}`), Timestamp: "9999-12-31T23:59:59.999999999Z", ReceivedAt: "2026-11-07T22:00:00+01:00"},
	}
	for range 2 {
		if err := c.insert(context.Background(), events); err != nil {
			t.Fatal(err)
		}
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatal("retry changed insertion projection")
	}
	lines := strings.Split(strings.TrimSpace(bodies[0]), "\n")
	if len(lines) != 2 {
		t.Fatal("expected two JSONEachRow records")
	}
	var first, last clickHouseEvent
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &last); err != nil {
		t.Fatal(err)
	}
	if first.EventID != 42 || first.Revision != 0 || first.OriginalSnapshot == "" || first.TimestampMS != -62135596800000 || first.Properties != `{"price":12.5}` || first.ReceivedAtMS != 1794085200123 {
		t.Fatalf("incorrect first projection: %+v", first)
	}
	if last.TimestampMS != 253402300799999 || last.ReceivedAt != "2026-11-07T21:00:00Z" {
		t.Fatalf("timestamp range or normalization failed: %+v", last)
	}
}

func TestClickHouseRedirectDoesNotForwardCredentials(t *testing.T) {
	var received atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { received.Add(1) }))
	defer target.Close()
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	})
	_, err := c.admin(context.Background(), "SELECT sensitive_sql")
	var failure *clickHouseError
	if !errors.As(err, &failure) || failure.Status != 307 || received.Load() != 0 {
		t.Fatalf("redirect followed or incorrect error: %v", err)
	}
}

func TestClickHouseSanitizedErrorsAndBoundedResponses(t *testing.T) {
	for _, tc := range []struct {
		name                string
		status, code, bytes int
		header              bool
	}{
		{"server header", 403, 497, 0, true},
		{"server body", 400, 62, 0, false},
		{"query too large", 200, 0, (2 << 20) + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
				if tc.header {
					w.Header().Set("X-ClickHouse-Exception-Code", strconv.Itoa(tc.code))
				}
				w.WriteHeader(tc.status)
				if tc.bytes > 0 {
					io.WriteString(w, strings.Repeat("a", tc.bytes))
					return
				}
				io.WriteString(w, "Code: "+strconv.Itoa(tc.code)+". DB::Exception: sensitive_sql secret_password")
			})
			data, err := c.query(context.Background(), "user", "secret_password", "analytics", "sensitive_sql", nil)
			var failure *clickHouseError
			if data != nil || !errors.As(err, &failure) || failure.Code != tc.code || failure.Status != tc.status {
				t.Fatalf("unexpected response or error: %v", err)
			}
			if strings.Contains(err.Error(), "sensitive_sql") || strings.Contains(err.Error(), "secret_password") {
				t.Fatal("ClickHouse error disclosed secrets")
			}
		})
	}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, strings.Repeat("a", (64<<10)+1)) })
	if data, err := c.admin(context.Background(), "DDL"); err == nil || data != nil {
		t.Fatal("oversized admin acknowledgment accepted")
	}
}

func TestClickHouseConfigurationAndCanceledRequest(t *testing.T) {
	for _, c := range []ClickHouseConfig{
		{URL: "file:///tmp/database"}, {URL: "http://user:password@localhost:8123"},
		{URL: "http://localhost:8123?password=secret"}, {URL: "http://localhost:8123#fragment"},
		{URL: "http://localhost:8123", Database: "bad; DROP DATABASE system"},
		{URL: "http://localhost:8123", Database: strings.Repeat("a", 64)},
		{URL: "http://localhost:8123", User: "user\r\nInjected: value"},
	} {
		if _, err := newClickHouse(c); err == nil {
			t.Fatalf("accepted invalid configuration %+v", c)
		}
	}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { t.Error("canceled request reached server") })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.admin(ctx, "private_sql"); err == nil || strings.Contains(err.Error(), "private_sql") {
		t.Fatal("cancellation was not safely reported")
	}
	if _, err := c.query(context.Background(), "u", "p", "bad database", "SELECT 1", nil); err == nil {
		t.Fatal("invalid database accepted")
	}
	if _, err := c.query(context.Background(), "u", "p", "analytics", "SELECT 1", map[string]string{"bad name": "value"}); err == nil {
		t.Fatal("invalid parameter accepted")
	}
}
