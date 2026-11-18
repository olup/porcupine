package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

type cancellationIgnoringQuery struct {
	query.HostedReader
	project string
	entered chan struct{}
	resume  chan struct{}
}

func (s *cancellationIgnoringQuery) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	if project != s.project {
		return s.HostedReader.Query(ctx, project, sql, parameters, limits)
	}
	close(s.entered)
	<-s.resume // Simulate native I/O which has not returned after interruption.
	return lakeapi.AnalyticsResult{}, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
}

func TestSharedQueryExecutionRetainsRenewalUntilNativeReturns(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedNativeJourneyPair(t, nativeS3Fixture(t))
	tenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", adminToken, []byte(`{"name":"Hung query tenant"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	if _, err := a.service.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1,hot_query_timeout_ms=1000 WHERE id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	project, _ := sharedJourneyProject(t, a.host.URL, tenant, "Blocked native query")
	healthy, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Healthy native query")
	_, reader := sharedJourneyKey(t, a.host.URL, healthy, []string{"management:read", "events:read"})
	hostedCleanupWrite(t, a.service, healthy, "healthy")
	blocked := &cancellationIgnoringQuery{HostedReader: a.service.hostedReader, project: project, entered: make(chan struct{}), resume: make(chan struct{})}
	a.service.hostedReader = blocked
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(blocked.resume) }) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/api/v1/projects/"+project+"/query", strings.NewReader(`{"sql":"SELECT count(*) FROM events"}`)).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+adminToken)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.service.Handler().ServeHTTP(response, request)
	}()
	select {
	case <-blocked.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("query did not enter the simulated native call")
	}
	started := time.Now()
	cancel()
	// Keep the real production TTL: a fixed unreplenished permit would expire.
	deadline := started.Add(31 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatal("canceled HTTP context freed a still-running native scope")
		case <-time.After(100 * time.Millisecond):
		}
	}
	status := a.service.queries.Status()
	if status.Active != 1 || status.UncertainPermits != 0 || len(status.UnhealthyProjects) != 1 || status.UnhealthyProjects[0].ProjectID != project {
		t.Fatalf("hung query state incorrect: %+v", status)
	}
	var remaining float64
	if err := a.service.control.QueryRow(`SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&remaining); err != nil || remaining <= 0 {
		t.Fatal("active native scope lost its renewable permit", remaining, err)
	}
	if _, permit, err := b.service.holdSharedQuery(t.Context(), project); err == nil {
		permit()
		t.Fatal("another worker reused a healthy-renewed slot")
	} else if queryStatus(err) != 429 {
		t.Fatal(err)
	}
	sharedJourneyQuery(t, a.host.URL, healthy, reader, "SELECT count(*) FROM events", 200)
	denied := sharedJourneyQuery(t, a.host.URL, project, adminToken, "SELECT count(*) FROM events", 503)
	if denied["code"] != "analytics_engine_unhealthy" {
		t.Fatal("quarantined project received no actionable error", denied)
	}
	sharedJourneyHTTP(t, a.host.URL, "GET", "/healthz", "", nil, nil, 200)
	sharedJourneyHTTP(t, a.host.URL, "GET", "/readyz", "", nil, nil, 503)
	admin := sharedJourneyHTTP(t, a.host.URL, "GET", "/api/v1/status", adminToken, nil, nil, 200)
	if admin["checks"].(map[string]any)["query_execution"] != "unavailable" || admin["query_execution"] == nil {
		t.Fatal("operator status omitted unhealthy execution", admin)
	}
	private := sharedJourneyHTTP(t, a.host.URL, "GET", "/api/v1/status", reader, nil, nil, 200)
	raw, _ := json.Marshal(private)
	if private["query_execution"] != nil || bytes.Contains(raw, []byte(project)) {
		t.Fatal("project reader saw another project's execution identity", private)
	}
	if err := a.service.Close(); err == nil {
		t.Fatal("service closed the keeper while native execution was alive")
	}
	if err := a.service.control.PingContext(t.Context()); err != nil {
		t.Fatal("early Close discarded the keeper's PostgreSQL pool", err)
	}
	unblock.Do(func() { close(blocked.resume) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("returned native call retained its HTTP scope")
	}
	if response.Code != 429 || a.service.queries.UsedSlots() != 0 || a.service.queries.Status().Active != 0 {
		t.Fatal("native return did not release the scope", response.Code, response.Body.String())
	}
	var count int
	if err := a.service.control.QueryRow(`SELECT count(*) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&count); err != nil || count != 0 {
		t.Fatal("finished native scope retained its permit", count, err)
	}
}

func TestSharedQueryExecutionLostLeaseDoesNotTouchReplacement(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	ctx, _, finish, err := a.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err := a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	replacementID, replacement, err := b.holdSharedQuery(t.Context(), "b")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement()
	wait, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := waitQueryStatus(wait, a.queries, func(status query.QueryExecutionStatus) bool { return status.UncertainPermits == 1 }); err != nil {
		t.Fatal("permit was not marked uncertain", err)
	}
	if ctx.Err() == nil || a.queries.Status().UncertainPermits != 1 || a.queries.UsedSlots() != 1 {
		t.Fatal("lease loss did not cancel/quarantine while retaining the native slot")
	}
	if _, _, _, err := a.holdQuery(t.Context(), "a"); queryStatus(err) != 503 {
		t.Fatal("lost-permit project admitted another query", err)
	}
	finish()
	finish()
	var id string
	if err := a.control.QueryRow(`SELECT permit_id FROM shared_query_permits`).Scan(&id); err != nil || id != replacementID {
		t.Fatal("stale renewal or release changed another worker's replacement", id, err)
	}
}

func TestSharedQueryExecutionControlFailureRetainsLocalCapacity(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	ctx, _, finish, err := a.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := a.control.Close(); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := waitQueryStatus(wait, a.queries, func(status query.QueryExecutionStatus) bool { return status.UncertainPermits == 1 }); err != nil {
		t.Fatal("permit was not marked uncertain", err)
	}
	if status := a.queries.Status(); ctx.Err() == nil || status.UncertainPermits != 1 || status.Active != 1 || a.queries.UsedSlots() != 1 {
		t.Fatal("failed heartbeat freed local execution or hid uncertainty", status)
	}
	if _, _, _, err := a.holdQuery(t.Context(), "a"); queryStatus(err) != 503 {
		t.Fatal("unhealthy project reached the unavailable control store", err)
	}
	if _, err := b.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	_, replacement, err := b.holdSharedQuery(t.Context(), "b")
	if err != nil {
		t.Fatal("automatic expiry did not recover shared capacity", err)
	}
	defer replacement()
	if a.queries.UsedSlots() != 1 {
		t.Fatal("automatic shared expiry freed the still-live local call")
	}
	finish()
	if a.queries.UsedSlots() != 0 {
		t.Fatal("actual return retained local capacity")
	}
}

func TestSharedQueryExecutionDrainKeepsAllPermitsRenewable(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	first, _, finishFirst, err := a.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finishFirst()
	second, _, finishSecond, err := a.holdQuery(t.Context(), "c")
	if err != nil {
		t.Fatal(err)
	}
	defer finishSecond()
	if _, err := a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()+interval '10 seconds'`); err != nil {
		t.Fatal(err)
	}
	a.queries.Drain()
	a.queries.Drain()
	if first.Err() == nil || second.Err() == nil || a.queries.Status().Active != 2 {
		t.Fatal("draining did not cancel both requests while retaining executions")
	}
	wait, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := waitForRenewedPermits(wait, a.control, 2); err != nil {
		t.Fatal("draining stopped renewal before actual return", err)
	}
	if _, _, _, err := a.holdQuery(t.Context(), "b"); queryStatus(err) != 503 {
		t.Fatal("draining admitted new execution", err)
	}
	finishFirst()
	finishSecond()
	_, _, release, err := b.holdQuery(t.Context(), "b")
	if err != nil {
		t.Fatal("returned calls retained shared capacity", err)
	}
	release()
}

type blockedQueryExecutionLog struct{ entered chan struct{} }

func (w blockedQueryExecutionLog) Write([]byte) (int, error) {
	close(w.entered)
	select {}
}

func TestSharedQueryExecutionLoggerHelper(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("PORCUPINE_SHARED_QUERY_LOG_HELPER_DSN")
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	s := sharedQueryTestService(t, dsn, "blocked-logger")
	if _, err := s.control.Exec(`UPDATE tenants SET hot_query_timeout_ms=100 WHERE id='one'`); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	slog.SetDefault(slog.New(slog.NewTextHandler(blockedQueryExecutionLog{entered}, nil)))
	ctx, _, finish, err := s.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	<-entered
	if status := s.queries.Status(); ctx.Err() == nil || status.Active != 1 || status.UncertainPermits != 0 || !status.Unavailable() {
		t.Fatal("blocked log prevented quarantine", status)
	}
	if _, err := s.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()+interval '10 seconds'`); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	if err := waitForRenewedPermits(wait, s.control, 1); err != nil {
		t.Fatal("blocked log prevented renewal", err)
	}
	_, _, other, err := s.holdQuery(t.Context(), "c")
	if err != nil {
		t.Fatal("blocked log prevented another project's query", err)
	}
	other()
	finish()
	fmt.Println("blocked-log-query-scopes-released")
}

func TestSharedQueryExecutionBlockedLoggerDoesNotStopWorker(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	dsn, _, _ := sharedQueryTestFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSharedQueryExecutionLoggerHelper$", "-test.v=false")
	child.Env = append(os.Environ(), "PORCUPINE_SHARED_QUERY_LOG_HELPER_DSN="+dsn)
	output, err := child.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("blocked-log-query-scopes-released")) {
		t.Fatalf("blocked log stalled or terminated the worker: %v %s", err, output)
	}
}

func TestSharedQueryExecutionFinishedAndLocalScopesStayHealthy(t *testing.T) {
	t.Parallel()
	_, a, _ := sharedQueryTestFixture(t)
	_, _, finish, err := a.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	finish()
	finish()
	if status := a.queries.Status(); status.Active != 0 || status.Unavailable() || a.queries.UsedSlots() != 0 {
		t.Fatal("late timer quarantined a finished scope", status)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := a.holdQuery(canceled, "a"); queryStatus(err) != 429 || a.queries.UsedSlots() != 0 {
		t.Fatal("already-canceled request leaked a permit", err)
	}
	local := service(t, 20)
	project, _ := project(t, local)
	_, _, release, err := local.holdQuery(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	local.queries.Drain()
	release()
	if status := local.queries.Status(); status.Active != 0 || status.Unavailable() || status.Draining {
		t.Fatal("local mode inherited shared keeper/quarantine behavior", status)
	}
}
