package porcupine

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
)

func operationalService(t *testing.T) *Service {
	t.Helper()
	s, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "journal.db"), BootstrapToken: adminToken,
		QueueMaxEvents: 1, MetricsEnabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.metrics.stopSampling()
	<-s.metrics.samplingDone
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func scrape(t *testing.T, s *Service) string {
	t.Helper()
	w := httptest.NewRecorder()
	s.MetricsHandler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != observability.ContentType {
		t.Fatalf("unexpected scrape response: %d %v", w.Code, w.Header())
	}
	return w.Body.String()
}

func requireMetric(t *testing.T, metrics, line string) {
	t.Helper()
	if !strings.Contains(metrics, line+"\n") {
		t.Errorf("missing sample %q", line)
	}
}

func TestOperationalMetricsPrivacyAdmissionAndMCP(t *testing.T) {
	t.Parallel()
	s := operationalService(t)
	projectID, ingest := project(t, s)
	_, reader := key(t, s, []string{"events:read", "management:read"}, []string{projectID})

	for _, view := range []struct {
		handler      http.Handler
		method, path string
		status       int
	}{
		{s.Handler(), "GET", "/metrics", 404},
		{s.MetricsHandler(), "GET", "/healthz", 404},
		{s.MetricsHandler(), "GET", "/api/v1/projects", 404},
		{s.MetricsHandler(), "POST", "/metrics", 405},
		{(&Service{}).MetricsHandler(), "GET", "/metrics", 404},
	} {
		w := httptest.NewRecorder()
		view.handler.ServeHTTP(w, httptest.NewRequest(view.method, view.path, nil))
		if w.Code != view.status {
			t.Errorf("%s %s: got %d, want %d", view.method, view.path, w.Code, view.status)
		}
	}
	public := s.Handler()
	for i := range 12 {
		r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/unregistered/private-identifier-%d?data=private-query", i), nil)
		w := httptest.NewRecorder()
		public.ServeHTTP(w, r)
		if w.Code != http.StatusNotFound {
			t.Fatal(w.Code)
		}
	}
	native := `{"type":"track","message_id":"private-marker","name":"private-event","properties":{"order":"private-payload"}}`
	nativeRequest(t, s, ingest, native, 200)
	nativeRequest(t, s, ingest, native, 200)
	nativeRequest(t, s, ingest, strings.Replace(native, "private-marker", "blocked-marker", 1), 503)
	nativeRequest(t, s, ingest, `{"type":"invalid-message"}`, 400)
	path := "/api/v1/projects/" + projectID + "/events"
	request(t, s, "GET", path+"?ignored=private-query", "private-bad-key", "", 401)

	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": projectID}}, 200)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "private-user-operation"}, 404)

	s.sampleJournalQueue(t.Context())
	metrics := scrape(t, s)
	for _, private := range []string{projectID, adminToken, ingest, reader, "private-identifier", "private-marker", "private-event", "private-payload", "private-query", "private-bad-key", "private-user-operation"} {
		if strings.Contains(metrics, private) {
			t.Errorf("private data appeared in scrape")
		}
	}
	for _, line := range []string{
		`porcupine_http_requests_total{profile="standalone",route="other",status_class="4xx"} 13`,
		`porcupine_http_requests_total{profile="standalone",route="ingestion",status_class="2xx"} 2`,
		`porcupine_http_requests_total{profile="standalone",route="ingestion",status_class="4xx"} 1`,
		`porcupine_http_requests_total{profile="standalone",route="ingestion",status_class="5xx"} 1`,
		`porcupine_ingestion_admissions_total{profile="standalone",durability="durable",outcome="accepted"} 2`,
		`porcupine_ingestion_admissions_total{profile="standalone",durability="durable",outcome="pressure"} 1`,
		`porcupine_ingestion_events_total{profile="standalone",durability="durable",outcome="accepted"} 1`,
		`porcupine_ingestion_events_total{profile="standalone",durability="durable",outcome="duplicate"} 1`,
		`porcupine_operations_total{profile="standalone",transport="http",operation="list-project-events",outcome="rejected"} 1`,
		`porcupine_operations_total{profile="standalone",transport="mcp",operation="porcupine_call",outcome="ok"} 1`,
		`porcupine_operations_total{profile="standalone",transport="mcp",operation="porcupine_call",outcome="rejected"} 1`,
		`porcupine_queue_events{profile="standalone"} 1`,
		`porcupine_queue_capacity_events{profile="standalone"} 1`,
	} {
		requireMetric(t, metrics, line)
	}
	if strings.Contains(metrics, `transport="http",operation="list-project-events",outcome="ok"`) {
		t.Error("internal MCP dispatch was also counted as a management HTTP operation")
	}
}

type waitingQueryLake struct {
	lakeapi.LakeStore
	started, cancelled, release chan struct{}
}

func (l *waitingQueryLake) Query(ctx context.Context, _, _ string, _ map[string]string, _ lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	close(l.started)
	<-ctx.Done()
	close(l.cancelled)
	<-l.release
	return lakeapi.AnalyticsResult{}, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
}

func (l *waitingQueryLake) Close() error { return nil }

func TestOperationalQueryCancellationWaitsForBackendReturn(t *testing.T) {
	t.Parallel()
	t.Run("request cancellation", func(t *testing.T) {
		t.Parallel()
		verifyQueryReturnsAfterCancellation(t, false, "cancelled")
	})
	t.Run("execution timeout", func(t *testing.T) {
		t.Parallel()
		verifyQueryReturnsAfterCancellation(t, true, "timeout")
	})
}

func verifyQueryReturnsAfterCancellation(t *testing.T, timeout bool, outcome string) {
	t.Helper()
	s := operationalService(t)
	backend := &waitingQueryLake{started: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	s.lake = backend
	projectID, _ := project(t, s)
	timeoutMS := 5000
	if timeout {
		timeoutMS = 100
	}
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1,hot_query_timeout_ms=$1 WHERE id=$2`, timeoutMS, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	_, reader := key(t, s, []string{"events:read"}, []string{projectID})
	post := func(ctx context.Context) int {
		r := httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/v1/projects/"+projectID+"/query", strings.NewReader(`{"sql":"SELECT event FROM events"}`))
		r.Header.Set("Authorization", "Bearer "+reader)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w.Code
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	var response int
	go func() {
		defer close(done)
		response = post(ctx)
	}()
	defer func() {
		cancel()
		close(backend.release)
		<-done
	}()
	wait := func(ch <-chan struct{}) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("query did not reach the expected stage")
		}
	}
	wait(backend.started)
	if !timeout {
		cancel()
	}
	wait(backend.cancelled)
	if status := post(t.Context()); status != http.StatusTooManyRequests {
		t.Errorf("saturated query slots returned %d", status)
	}
	metrics := scrape(t, s)
	requireMetric(t, metrics, `porcupine_query_active{profile="standalone",operation="sql"} 1`)
	requireMetric(t, metrics, `porcupine_query_slots_used{profile="standalone"} 1`)
	requireMetric(t, metrics, `porcupine_query_admissions_total{profile="standalone",operation="sql",outcome="admitted"} 1`)
	requireMetric(t, metrics, `porcupine_query_admissions_total{profile="standalone",operation="sql",outcome="limited"} 1`)
	if strings.Contains(metrics, "porcupine_query_backend_duration_seconds_count") {
		t.Error("in-flight cancelled query recorded a backend return")
	}
	select {
	case <-done:
		t.Fatal("request finished before the backend returned")
	default:
	}
	select {
	case backend.release <- struct{}{}:
	case <-time.After(3 * time.Second):
		t.Fatal("query did not wait for backend release")
	}
	wait(done)
	if response != http.StatusTooManyRequests {
		t.Errorf("interrupted query returned %d", response)
	}
	metrics = scrape(t, s)
	requireMetric(t, metrics, `porcupine_query_active{profile="standalone",operation="sql"} 0`)
	requireMetric(t, metrics, `porcupine_query_slots_used{profile="standalone"} 0`)
	requireMetric(t, metrics, `porcupine_query_backend_duration_seconds_count{profile="standalone",operation="sql",outcome="`+outcome+`"} 1`)
}
