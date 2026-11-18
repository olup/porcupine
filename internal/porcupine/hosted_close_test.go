package porcupine

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/query"
)

func TestSharedHostedCloseDrainsBufferBeforeStuckAnalytics(t *testing.T) {
	t.Parallel()
	flushed := make(chan bufferedFlushBatch, 4)
	lifetime, stopBuffer := context.WithCancel(context.Background())
	t.Cleanup(stopBuffer)
	buffer, err := newBufferedAcceptance(lifetime, testBufferConfig(), liveFlusherFunc(func(ctx context.Context, batch bufferedFlushBatch) error {
		// A flush still in progress when Close returns is lost with the process.
		time.Sleep(200 * time.Millisecond)
		flushed <- batch
		return nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := buffer.Accept(t.Context(), testBufferBatch(t, "project", 3), ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	_, s, _ := sharedQueryTestFixture(t)
	s.hosted = &hostedRuntime{buffer: buffer, stopBuffer: stopBuffer, nativeGrace: 100 * time.Millisecond}
	// A read that ignores cancellation remains active during Close.
	native, _, release, err := s.holdQuery(t.Context(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	started := time.Now()
	err = s.Close()
	elapsed := time.Since(started)
	select {
	case batch := <-flushed:
		if batch.projectID != "project" || len(batch.events) != 3 {
			t.Fatal("drain flushed the wrong batch", batch.projectID, len(batch.events))
		}
	default:
		t.Fatal("close returned before flushing buffered events behind a stuck analytical call", err)
	}
	if err == nil || !strings.Contains(err.Error(), "analytics execution is still active") {
		t.Fatal("close hid a native call that is still running", err)
	}
	if elapsed > 5*time.Second {
		t.Fatal("close waited without a bound for a stuck analytical call", elapsed)
	}
	select {
	case <-native.Done():
	default:
		t.Fatal("drain did not cancel the analytical call")
	}
	if snapshot := buffer.snapshot(); snapshot.events != 0 || !snapshot.stopped {
		t.Fatal("buffer retained events after close", snapshot)
	}
	if _, err := buffer.Accept(t.Context(), testBufferBatch(t, "project", 1), ingestionPeer{}, false); err == nil {
		t.Fatal("closed worker admitted new events")
	}
}

// heldNativeQuery keeps a real project engine lease and DuckDB connection, as
// a native call blocked in I/O would, until the test resumes it.
type heldNativeQuery struct {
	query.HostedReader
	store   *lakeapi.DuckLakeStore
	project string
	entered chan struct{}
	resume  chan struct{}
}

func (q *heldNativeQuery) Query(ctx context.Context, project, sql string, parameters map[string]string, limits lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	if project != q.project {
		return q.HostedReader.Query(ctx, project, sql, parameters, limits)
	}
	p, release, err := q.store.LeaseProject(context.Background(), project)
	if err != nil {
		close(q.entered)
		return lakeapi.AnalyticsResult{}, err
	}
	defer release()
	conn, err := p.DB.Conn(context.Background())
	if err != nil {
		close(q.entered)
		return lakeapi.AnalyticsResult{}, err
	}
	defer conn.Close()
	close(q.entered)
	<-q.resume
	return lakeapi.AnalyticsResult{}, lakeapi.DuckLakeQueryError(ctx, ctx.Err())
}

func TestSharedHostedCloseFlushesBehindStuckNativeQuery(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedNativeJourneyPair(t, nativeS3Fixture(t))
	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Stuck query drain")
	other, otherIngest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "Second drained project")
	hostedCleanupWrite(t, a.service, project, "warm")
	_, reader := sharedJourneyKey(t, b.host.URL, project, []string{"events:read"})
	_, otherReader := sharedJourneyKey(t, b.host.URL, other, []string{"events:read"})
	stuck := &heldNativeQuery{HostedReader: a.service.hostedReader, store: a.service.hosted.native, project: project, entered: make(chan struct{}), resume: make(chan struct{})}
	a.service.hostedReader = stuck
	unblocked := false
	unblock := func() {
		if !unblocked {
			unblocked = true
			close(stuck.resume)
		}
	}
	t.Cleanup(unblock)
	// Keep accepted events in memory until shutdown starts the drain.
	buffer := a.service.hosted.buffer
	buffer.mu.Lock()
	buffer.config.flushInterval = time.Hour
	buffer.mu.Unlock()
	response := httptest.NewRecorder()
	request := httptest.NewRequest("POST", "/api/v1/projects/"+project+"/query", strings.NewReader(`{"sql":"SELECT count(*) FROM events"}`))
	request.Header.Set("Authorization", "Bearer "+adminToken)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.service.Handler().ServeHTTP(response, request)
	}()
	select {
	case <-stuck.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("query did not enter the simulated native call")
	}
	sharedJourneyNative(t, a.host.URL, ingest, `{"type":"track","message_id":"behind-stuck-query","name":"drained","actor_id":"test","timestamp":"2020-01-01T12:00:00Z"}`, 200)
	sharedJourneyNative(t, a.host.URL, otherIngest, `{"type":"track","message_id":"other-project","name":"drained","actor_id":"test","timestamp":"2020-01-01T12:00:00Z"}`, 200)
	if snapshot := buffer.snapshot(); snapshot.events != 2 {
		t.Fatal("events were not held in the buffer", snapshot)
	}
	started := time.Now()
	err := a.service.Close()
	// The process exits once Close returns, so the flush must be complete here.
	if snapshot := buffer.snapshot(); snapshot.events != 0 || !snapshot.stopped {
		t.Fatal("close returned before the buffer drain committed", snapshot, err)
	}
	if elapsed := time.Since(started); elapsed > hostedDrainTimeout {
		t.Fatal("close did not bound the stuck analytical call", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "analytics execution is still active") {
		t.Fatal("close hid a native call that is still running", err)
	}
	if err := a.service.control.PingContext(t.Context()); err != nil {
		t.Fatal("close discarded the permit keeper's PostgreSQL pool", err)
	}
	for scope, key := range map[string]string{project: reader, other: otherReader} {
		observed := sharedJourneyQuery(t, b.host.URL, scope, key, "SELECT count(*) AS n FROM events WHERE event='drained'", 200)
		if !reflect.DeepEqual(observed["result"].(map[string]any)["data"], []any{map[string]any{"n": "1"}}) {
			t.Fatalf("drain lost a buffered event behind the stuck query: %s %v", scope, observed)
		}
	}
	unblock()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("returned native call retained its HTTP scope")
	}
	if active := a.service.queries.Status().Active; active != 0 {
		t.Fatal("returned native call stayed active", active)
	}
}
