package porcupine

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestContextMutexZeroValueAndCanceledWaiter(t *testing.T) {
	var gate contextMutex
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- gate.Lock(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blocked waiter returned %v", err)
		}
	case <-time.After(time.Second):
		gate.Unlock()
		t.Fatal("canceled waiter remained blocked until unlock")
	}
	gate.Unlock()
	if err := gate.Lock(context.Background()); err != nil {
		t.Fatal("canceled waiter consumed gate", err)
	}
	gate.Unlock()
	canceled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	for range 100 {
		if err := gate.Lock(canceled); !errors.Is(err, context.Canceled) {
			t.Fatal("already canceled context acquired gate", err)
		}
	}
}

func TestContextMutexSerializesConcurrentCallers(t *testing.T) {
	var gate contextMutex
	var workers sync.WaitGroup
	counter := 0
	for range 16 {
		workers.Go(func() {
			for range 100 {
				if err := gate.Lock(context.Background()); err != nil {
					t.Error(err)
					return
				}
				counter++
				gate.Unlock()
			}
		})
	}
	workers.Wait()
	if counter != 1600 {
		t.Fatal("context gate did not serialize callers")
	}
}

func awaitProvisioningDeadline(t *testing.T, ctx context.Context, call func(context.Context) error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- call(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("provisioning returned %v, want context deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provisioning wait exceeded its context before owner unlocked")
	}
}

func TestAnalyticsReaderWaitHonorsContextDeadline(t *testing.T) {
	s := service(t, 20)
	var requests atomic.Int64
	configureAnalyticsTest(s, testClickHouse(t, func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	if err := s.queryMu.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.queryMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	awaitProvisioningDeadline(t, ctx, func(ctx context.Context) error { _, _, _, err := s.analyticsReader(ctx, "project"); return err })
	if requests.Load() != 0 {
		t.Fatal("timed out reader made a storage request")
	}
}

func TestSchemaInitializeWaitHonorsContextDeadline(t *testing.T) {
	var requests atomic.Int64
	ch := testClickHouse(t, func(http.ResponseWriter, *http.Request) { requests.Add(1) })
	if err := ch.schemaMu.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer ch.schemaMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	awaitProvisioningDeadline(t, ctx, ch.initialize)
	if requests.Load() != 0 {
		t.Fatal("timed out initializer made a storage request")
	}
}

func TestAnalyticsStaleCacheInvalidationHonorsContext(t *testing.T) {
	s := service(t, 20)
	s.queryReady = map[string]bool{"project": true}
	if err := s.queryMu.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.queryMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.analyticsStorageError(ctx, "project", &clickHouseError{Code: 516}) }()
	select {
	case err := <-done:
		if queryStatus(err) != 503 {
			t.Fatal("stale credential failure lost unavailable status", err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale cache invalidation ignored request deadline")
	}
	if !s.queryReady["project"] {
		t.Fatal("canceled invalidation mutated cache without owning gate")
	}
}

func contextTestExportConfig() ExportConfig {
	return ExportConfig{MemoryBytes: 512 << 20, MaxExecutionSeconds: 86400, MaxReadBytes: 1 << 40, TemporaryBytes: 1 << 30, BandwidthBytesPerSecond: 5 << 20}
}

func TestBlockedExportProvisioningDoesNotTrapInteractiveReader(t *testing.T) {
	s := service(t, 20)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	defer close(release)
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.HasPrefix(string(body), "CREATE USER") && strings.Contains(string(body), "_export ") {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	}))
	s.queryReady["project"] = true
	s.exports = &exportRuntime{config: contextTestExportConfig()}
	exportCtx, cancelExport := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, _, err := s.exportReader(exportCtx, "project"); done <- err }()
	defer func() {
		cancelExport()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("export provisioning did not stop")
		}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("export did not reach blocked DDL")
	}
	interactiveCtx, cancelInteractive := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelInteractive()
	awaitProvisioningDeadline(t, interactiveCtx, func(ctx context.Context) error { _, _, _, err := s.analyticsReader(ctx, "project"); return err })
	select {
	case <-done:
		t.Fatal("export released its gate before interactive cancellation was tested")
	default:
	}
}

type provisioningRoundTripper func(*http.Request) (*http.Response, error)

func (f provisioningRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestExportProvisioningUsesShortChildDeadline(t *testing.T) {
	s := service(t, 20)
	ch, err := newClickHouse(ClickHouseConfig{URL: "http://example.invalid"})
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	s.queryReady["project"] = true
	s.exports = &exportRuntime{config: contextTestExportConfig()}
	var provisioningContext context.Context
	calls := 0
	ch.client.Transport = provisioningRoundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) < 4*time.Second {
			t.Error("export DDL did not receive a five-second provisioning budget")
		}
		provisioningContext = r.Context()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
	})
	parent, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	if _, _, _, err := s.exportReader(parent, "project"); err != nil {
		t.Fatal(err)
	}
	if calls != 7 || provisioningContext == nil {
		t.Fatalf("expected seven export provisioning requests, got %d", calls)
	}
	if !errors.Is(provisioningContext.Err(), context.Canceled) || parent.Err() != nil {
		t.Fatal("short provisioning context did not close independently of export parent")
	}
}
