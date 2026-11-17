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
)

func TestSharedQueryExecutionTimingRules(t *testing.T) {
	t.Parallel()
	if sharedQueryAdmissionTimeout+sharedQueryRenewInterval+sharedQueryControlTimeout >= sharedQueryPermitLifetime {
		t.Fatal("an initial acknowledgement could outlive the first renewal opportunity")
	}
	hard := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	for _, s3 := range []int{0, 5, 30} {
		for _, metadata := range []int{0, 10, 30} {
			deadline := sharedQueryUnresponsiveDeadline(hard, s3, metadata)
			if deadline.Sub(hard) != time.Duration(s3+metadata)*time.Second+sharedQueryUnresponsiveGrace {
				t.Fatal("quarantine omitted an I/O allowance", deadline)
			}
		}
	}
}

type cancellationIgnoringQuery struct {
	lakeapi.LakeStore
	project string
	entered chan struct{}
	resume  chan struct{}
}

func (s *cancellationIgnoringQuery) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	if project != s.project {
		return s.LakeStore.Query(ctx, project, sql, parameters, limits)
	}
	close(s.entered)
	<-s.resume // Simulate native I/O which has not returned after interruption.
	return lakeapi.AnalyticsResult{}, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
}

func TestSharedQueryExecutionRetainsRenewalUntilNativeReturns(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedJourneyPair(t, nil)
	attachCoreLake(t, a.service)
	tenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", adminToken, []byte(`{"name":"Hung query tenant"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	if _, err := a.service.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=1,hot_query_timeout_ms=1000 WHERE id=$1`, tenant); err != nil {
		t.Fatal(err)
	}
	project, _ := sharedJourneyProject(t, a.host.URL, tenant, "Blocked native query")
	healthy, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Healthy native query")
	_, reader := sharedJourneyKey(t, a.host.URL, healthy, []string{"management:read", "events:read"})
	blocked := &cancellationIgnoringQuery{LakeStore: a.service.lake, project: project, entered: make(chan struct{}), resume: make(chan struct{})}
	a.service.lake = blocked
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
	deadline := started.Add(sharedQueryPermitLifetime + time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-done:
			t.Fatal("canceled HTTP context freed a still-running native scope")
		case <-time.After(100 * time.Millisecond):
		}
	}
	status := a.service.sharedQueries.status()
	if status.Active != 1 || status.UncertainPermits != 0 || len(status.UnhealthyProjects) != 1 || status.UnhealthyProjects[0].ProjectID != project {
		t.Fatalf("hung query state incorrect: %+v", status)
	}
	var remaining float64
	if err := a.service.control.QueryRow(`SELECT extract(epoch FROM expires_at-clock_timestamp()) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&remaining); err != nil || remaining <= 0 {
		t.Fatal("active native scope lost its renewable permit", remaining, err)
	}
	if _, permit, err := b.service.acquireSharedQueryPermit(t.Context(), project, tenant); err == nil {
		permit.release()
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
	if response.Code != 429 || len(a.service.querySlots) != 0 || a.service.sharedQueries.status().Active != 0 {
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
	ctx, _, finish, err := a.beginInteractiveQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if _, err := a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	_, replacement, err := b.acquireSharedQueryPermit(t.Context(), "b", "one")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.release()
	a.sharedQueries.renew(a.control)
	if ctx.Err() == nil || a.sharedQueries.status().UncertainPermits != 1 || len(a.querySlots) != 1 {
		t.Fatal("lease loss did not cancel/quarantine while retaining the native slot")
	}
	if _, _, _, err := a.beginInteractiveQuery(t.Context(), "a"); queryStatus(err) != 503 {
		t.Fatal("lost-permit project admitted another query", err)
	}
	finish()
	finish()
	var id string
	if err := a.control.QueryRow(`SELECT permit_id FROM shared_query_permits`).Scan(&id); err != nil || id != replacement.id {
		t.Fatal("stale renewal or release changed another worker's replacement", id, err)
	}
}

func TestSharedQueryExecutionControlFailureRetainsLocalCapacity(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	ctx, _, finish, err := a.beginInteractiveQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if err := a.control.Close(); err != nil {
		t.Fatal(err)
	}
	a.sharedQueries.renew(a.control)
	if status := a.sharedQueries.status(); ctx.Err() == nil || status.UncertainPermits != 1 || status.Active != 1 || len(a.querySlots) != 1 {
		t.Fatal("failed heartbeat freed local execution or hid uncertainty", status)
	}
	if _, _, _, err := a.beginInteractiveQuery(t.Context(), "a"); queryStatus(err) != 503 {
		t.Fatal("unhealthy project reached the unavailable control store", err)
	}
	if _, err := b.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	_, replacement, err := b.acquireSharedQueryPermit(t.Context(), "b", "one")
	if err != nil {
		t.Fatal("automatic expiry did not recover shared capacity", err)
	}
	defer replacement.release()
	if len(a.querySlots) != 1 {
		t.Fatal("automatic shared expiry freed the still-live local call")
	}
	finish()
	if len(a.querySlots) != 0 {
		t.Fatal("actual return retained local capacity")
	}
}

func TestSharedQueryExecutionDrainKeepsAllPermitsRenewable(t *testing.T) {
	t.Parallel()
	_, a, b := sharedQueryTestFixture(t)
	first, _, finishFirst, err := a.beginInteractiveQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finishFirst()
	second, _, finishSecond, err := a.beginInteractiveQuery(t.Context(), "c")
	if err != nil {
		t.Fatal(err)
	}
	defer finishSecond()
	if _, err := a.control.Exec(`UPDATE shared_query_permits SET expires_at=clock_timestamp()+interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	a.BeginQueryDrain()
	a.BeginQueryDrain()
	a.sharedQueries.renew(a.control)
	if first.Err() == nil || second.Err() == nil || a.sharedQueries.status().Active != 2 {
		t.Fatal("draining did not cancel both requests while retaining executions")
	}
	var count int
	if err := a.control.QueryRow(`SELECT count(*) FROM shared_query_permits WHERE expires_at>clock_timestamp()+interval '20 seconds'`).Scan(&count); err != nil || count != 2 {
		t.Fatal("draining stopped renewal before actual return", count, err)
	}
	if _, _, _, err := a.beginInteractiveQuery(t.Context(), "b"); queryStatus(err) != 503 {
		t.Fatal("draining admitted new execution", err)
	}
	finishFirst()
	finishSecond()
	_, _, release, err := b.beginInteractiveQuery(t.Context(), "b")
	if err != nil {
		t.Fatal("returned calls retained shared capacity", err)
	}
	release()
}

type blockedQueryExecutionLog struct{}

func (blockedQueryExecutionLog) Write([]byte) (int, error) { select {} }

func TestSharedQueryExecutionLoggerHelper(t *testing.T) {
	t.Parallel()
	dsn := os.Getenv("PORCUPINE_SHARED_QUERY_LOG_HELPER_DSN")
	if dsn == "" {
		t.Skip("subprocess helper")
	}
	s := sharedQueryTestService(t, dsn, "blocked-logger")
	ctx, _, finish, err := s.beginInteractiveQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	var id string
	if err := s.control.QueryRow(`SELECT permit_id FROM shared_query_permits WHERE worker_id='blocked-logger'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(blockedQueryExecutionLog{}, nil)))
	s.sharedQueries.quarantine(id, "native_query_unresponsive", false)
	s.sharedQueries.renew(s.control)
	if ctx.Err() == nil || s.sharedQueries.status().Active != 1 || s.sharedQueries.status().UncertainPermits != 0 {
		t.Fatal("blocked log prevented quarantine or renewal")
	}
	_, _, other, err := s.beginInteractiveQuery(t.Context(), "c")
	if err != nil {
		t.Fatal("blocked log prevented another project's query", err)
	}
	other()
	finish()
	finish()
	fmt.Println("blocked-log-query-scopes-released")
}

func TestSharedQueryExecutionBlockedLoggerDoesNotStopWorker(t *testing.T) {
	t.Parallel()
	dsn, _, _ := sharedQueryTestFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
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
	_, _, finish, err := a.beginInteractiveQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	a.sharedQueries.mu.Lock()
	var id string
	for token := range a.sharedQueries.active {
		id = token
	}
	a.sharedQueries.mu.Unlock()
	finish()
	finish()
	a.sharedQueries.quarantine(id, "native_query_unresponsive", false)
	if status := a.sharedQueries.status(); status.Active != 0 || status.unavailable() || len(a.querySlots) != 0 {
		t.Fatal("late timer quarantined a finished scope", status)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := a.beginInteractiveQuery(canceled, "a"); queryStatus(err) != 429 || len(a.querySlots) != 0 {
		t.Fatal("already-canceled request leaked a permit", err)
	}
	local := service(t, 20)
	project, _ := project(t, local)
	_, _, release, err := local.beginInteractiveQuery(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	local.BeginQueryDrain()
	release()
	if status := local.sharedQueries.status(); status.Active != 0 || status.unavailable() || status.Draining {
		t.Fatal("local mode inherited shared keeper/quarantine behavior", status)
	}
}
