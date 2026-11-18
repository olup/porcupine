package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
	"github.com/olup/porcupine/internal/output"
)

type liveFlusherFunc func(context.Context, bufferedFlushBatch) error

func (f liveFlusherFunc) FlushLive(ctx context.Context, batch bufferedFlushBatch) error {
	return f(ctx, batch)
}

type cookielessKeysFunc func(context.Context, string) ([]byte, error)

func (f cookielessKeysFunc) CookielessKey(ctx context.Context, day string) ([]byte, error) {
	return f(ctx, day)
}
func testBufferConfig() bufferConfig {
	c := defaultBufferConfig()
	c.flushInterval = time.Hour
	return c
}
func newTestBuffer(t testing.TB, c bufferConfig, writer liveBatchFlusher) *bufferedAcceptance {
	t.Helper()
	buffer, err := newBufferedAcceptance(t.Context(), c, writer, nil)
	if err != nil {
		t.Fatal(err)
	}
	return buffer
}
func testBufferBatch(t testing.TB, project string, n int) preparedLiveBatch {
	t.Helper()
	inputs := make(eventBatch, n)
	for i := range inputs {
		inputs[i] = normalizedTrack("native", "stable-message")
	}
	batch, err := prepareLiveBatch(project, "source", inputs, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
func waitBuffer(t *testing.T, b *bufferedAcceptance, predicate func(bufferSnapshot) bool) bufferSnapshot {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s := b.snapshot()
		if predicate(s) {
			return s
		}
		select {
		case <-timer.C:
			t.Fatal("buffer state deadline", s)
			return s
		case <-tick.C:
		}
	}
}
func nextFlush(t *testing.T, calls <-chan bufferedFlushBatch) bufferedFlushBatch {
	t.Helper()
	select {
	case batch := <-calls:
		return batch
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not start")
		return bufferedFlushBatch{}
	}
}
func blockingTestWriter(calls chan<- bufferedFlushBatch, results <-chan error) liveBatchFlusher {
	return liveFlusherFunc(func(ctx context.Context, batch bufferedFlushBatch) error {
		select {
		case calls <- batch:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case err := <-results:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}
func TestBufferedAcceptanceAvailabilityAndConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := newBufferedAcceptance(t.Context(), testBufferConfig(), nil, nil); err == nil {
		t.Fatal("nil writer accepted")
	}
	for _, mode := range []AcceptanceMode{AcceptanceModeBuffered, "unknown"} {
		path := filepath.Join(t.TempDir(), "no-storage", "journal.db")
		if _, err := Open(Config{DBPath: path, AcceptanceMode: mode}); err == nil {
			t.Fatal("unavailable acceptance mode opened", mode)
		}
		if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
			t.Fatal("invalid mode created storage", err)
		}
	}
	c := testBufferConfig()
	c.flushBytes = c.projectMaxBytes + 1
	if _, err := newBufferedAcceptance(t.Context(), c, liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }), nil); err == nil {
		t.Fatal("unbounded configuration accepted")
	}
}
func TestBufferedAcceptanceAtomicCapacityAndScope(t *testing.T) {
	t.Parallel()
	batch := testBufferBatch(t, "p", 2)
	size := bufferedInputBytes(batch.events[0])
	c := testBufferConfig()
	c.maxBytes = 2*size + bufferedProjectOverhead + 1
	c.projectMaxBytes = 2 * size
	c.flushBytes = 2 * size
	c.maxEvents = 2
	c.projectMaxEvents = 2
	calls, results := make(chan bufferedFlushBatch, 1), make(chan error)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	outcome, err := b.Accept(t.Context(), batch, ingestionPeer{}, true)
	if err != nil || outcome.(bufferedLiveResult).received != 2 {
		t.Fatal(outcome, err)
	}
	before := b.snapshot()
	if before.bytes != c.maxBytes || before.events != 2 {
		t.Fatal("exact-fit accounting", before, c.maxBytes)
	}
	if _, err = b.Accept(t.Context(), testBufferBatch(t, "p", 2), ingestionPeer{}, true); err == nil {
		t.Fatal("overflow accepted")
	}
	after := b.snapshot()
	if after.bytes != before.bytes || after.events != before.events {
		t.Fatal("partial admission", before, after)
	}
	forged := testBufferBatch(t, "other", 1)
	forged.events[0].event.ProjectID = "p"
	if _, err = b.Accept(t.Context(), forged, ingestionPeer{}, true); err == nil {
		t.Fatal("forged project accepted")
	}
	if b.snapshot().events != 2 {
		t.Fatal("scope failure changed buffer")
	}
}
func TestBufferedAcceptanceConcurrentGlobalAndProjectBounds(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.maxEvents = 21
	c.projectMaxEvents = 11
	c.maxProjects = 2
	b := newTestBuffer(t, c, liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }))
	var wg sync.WaitGroup
	var admitted atomic.Int64
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			project := []string{"a", "b"}[i%2]
			if _, err := b.Accept(t.Context(), testBufferBatch(t, project, 2), ingestionPeer{}, false); err == nil {
				admitted.Add(2)
			}
		}()
	}
	wg.Wait()
	s := b.snapshot()
	if s.events != 20 || s.projects != 2 || admitted.Load() != 20 {
		t.Fatal("concurrent capacity mismatch", s, admitted.Load())
	}
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "c", 1), ingestionPeer{}, false); err == nil {
		t.Fatal("project count limit exceeded")
	}
}
func TestBufferedAcceptanceInflightRetainsCapacityAndGrace(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.flushBytes = 1
	c.maxEvents = 2
	c.projectMaxEvents = 2
	calls, results := make(chan bufferedFlushBatch, 2), make(chan error, 2)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	batch := testBufferBatch(t, "p", 1)
	if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, true); err != nil {
		t.Fatal(err)
	}
	nextFlush(t, calls)
	if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, true); err == nil {
		t.Fatal("inflight capacity was released early")
	}
	before := b.snapshot()
	if before.events != 2 || before.inflight != 1 || before.oldest.IsZero() {
		t.Fatal(before)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := b.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("grace expiry reported drained", err)
	}
	if s := b.snapshot(); s.events != 2 || !s.draining || s.bytes != before.bytes {
		t.Fatal("deadline dropped retained work", s)
	}
	if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, true); err == nil {
		t.Fatal("draining buffer admitted new data")
	}
	results <- nil
	nextFlush(t, calls)
	results <- nil
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s := b.snapshot(); s.events != 0 || s.bytes != 0 || s.projects != 0 {
		t.Fatal("successful drain retained capacity", s)
	}
}
func TestBufferedAcceptanceFailureBackpressureAndStableRetry(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.flushBytes = 1
	c.retryMin = 250 * time.Millisecond
	c.retryMax = c.retryMin
	calls, results := make(chan bufferedFlushBatch, 2), make(chan error, 2)
	blocked := blockingTestWriter(calls, results)
	healthy := make(chan struct{}, 1)
	b := newTestBuffer(t, c, liveFlusherFunc(func(ctx context.Context, batch bufferedFlushBatch) error {
		if batch.projectID == "healthy-project" {
			healthy <- struct{}{}
			return nil
		}
		return blocked.FlushLive(ctx, batch)
	}))
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	first := nextFlush(t, calls)
	results <- output.ErrPipelineCapacity
	failed := waitBuffer(t, b, func(s bufferSnapshot) bool { return s.failed })
	if failed.events != 1 || failed.oldest != first.oldest {
		t.Fatal(failed)
	}
	_, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false)
	var failure *acceptanceFailure
	if !errors.As(err, &failure) || !failure.retry {
		t.Fatal("flush failure did not backpressure its own project", err)
	}
	if !b.available() {
		t.Fatal("failed project made the worker unready")
	}
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "healthy-project", 1), ingestionPeer{}, false); err != nil {
		t.Fatal("failed project prevented healthy project admission", err)
	}
	select {
	case <-healthy:
	case <-time.After(2 * time.Second):
		t.Fatal("healthy project did not flush")
	}
	retry := nextFlush(t, calls)
	if retry.id != first.id || !reflect.DeepEqual(retry, first) {
		t.Fatal("retry changed batch identity or contents")
	}
	results <- nil
	waitBuffer(t, b, func(s bufferSnapshot) bool { return !s.failed && s.events == 0 })
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedAcceptanceSplitsAtRequestBoundaries(t *testing.T) {
	t.Parallel()
	for _, testcase := range []struct {
		name                       string
		sizes, attempts, committed []int
		droppedEvents              uint64
	}{
		{name: "both requests fit", sizes: []int{3, 3}, attempts: []int{6, 3, 3}, committed: []int{3, 3}},
		{name: "one oversized request", sizes: []int{5, 2, 1}, attempts: []int{8, 5, 3}, committed: []int{3}, droppedEvents: 5},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()
			var attempts, committed []int
			ids := map[string]bool{}
			writer := liveFlusherFunc(func(_ context.Context, batch bufferedFlushBatch) error {
				if ids[batch.id] {
					t.Errorf("split reused batch ID %q", batch.id)
				}
				ids[batch.id] = true
				attempts = append(attempts, len(batch.events))
				if len(batch.events) > 3 {
					return errHostedLiveBatchTooLarge
				}
				committed = append(committed, len(batch.events))
				return nil
			})
			b := newTestBuffer(t, testBufferConfig(), writer)
			for _, n := range testcase.sizes {
				if _, err := b.Accept(t.Context(), testBufferBatch(t, "project", n), ingestionPeer{}, false); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := b.Drain(ctx); err != nil {
				t.Fatal("capacity-limited batch did not drain", err)
			}
			if !reflect.DeepEqual(attempts, testcase.attempts) || !reflect.DeepEqual(committed, testcase.committed) {
				t.Fatal("unexpected attempts or commits", attempts, committed)
			}
			s := b.snapshot()
			var droppedRequests uint64
			if testcase.droppedEvents > 0 {
				droppedRequests = 1
			}
			if s.events != 0 || s.bytes != 0 || s.projects != 0 || s.droppedRequests != droppedRequests || s.droppedEvents != testcase.droppedEvents {
				t.Fatal("incorrect capacity or drop counts after drain", s)
			}
		})
	}
}

func TestBufferedAcceptanceOwnsCommandsAndPayload(t *testing.T) {
	t.Parallel()
	batch, err := prepareLiveBatch("p", "source", eventBatch{normalizedTrack("native", "track"), {Event: "$identify", DistinctID: "person", Properties: map[string]any{"$anon_distinct_id": "browser", "$set": map[string]any{"plan": "new"}}}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	calls := make(chan bufferedFlushBatch, 1)
	b := newTestBuffer(t, testBufferConfig(), liveFlusherFunc(func(_ context.Context, batch bufferedFlushBatch) error { calls <- batch; return nil }))
	if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, true); err != nil {
		t.Fatal(err)
	}
	batch.events[0].event.Properties[0] = '!'
	batch.events[0].event.Context[0] = '!'
	batch.events[1].identity.Set[0] = '!'
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	flushed := nextFlush(t, calls)
	if !json.Valid(flushed.events[0].input.event.Properties) || !json.Valid(flushed.events[0].input.event.Context) || string(flushed.events[1].input.identity.Set) != `{"plan":"new"}` {
		t.Fatal("caller mutated retained data")
	}
	identity := flushed.events[1].input
	if identity.event.DistinctID != "" || string(identity.event.Properties) != `{}` || identity.event.IdentitySnapshot != "" || identity.identity.Target != "person" || identity.identity.Anonymous != "browser" {
		t.Fatal("identity command mixed with committed event", identity)
	}
}
func TestBufferedAcceptanceCookielessDayAndPrivacy(t *testing.T) {
	t.Parallel()
	var clock atomic.Int64
	clock.Store(time.Date(2026, 11, 30, 23, 59, 59, 0, time.UTC).UnixNano())
	c := testBufferConfig()
	c.clock = func() time.Time { return time.Unix(0, clock.Load()) }
	calls := make(chan bufferedFlushBatch, 1)
	writer := liveFlusherFunc(func(_ context.Context, batch bufferedFlushBatch) error { calls <- batch; return nil })
	var resolved string
	keys := cookielessKeysFunc(func(_ context.Context, day string) ([]byte, error) {
		resolved = day
		clock.Add(int64(2 * time.Second))
		return bytes.Repeat([]byte("k"), 32), nil
	})
	b, err := newBufferedAcceptance(t.Context(), c, writer, keys)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := prepareLiveBatch("p", "source", eventBatch{{Event: "$pageview", DistinctID: ingestfmt.CookielessSentinel, Properties: map[string]any{"$ip": "192.0.2.1", "amount": json.Number("42")}}}, c.clock())
	if err != nil {
		t.Fatal(err)
	}
	peer := ingestionPeer{address: netip.MustParseAddr("192.0.2.1"), userAgent: "private-user-agent"}
	if _, err := b.Accept(t.Context(), batch, peer, true); err != nil {
		t.Fatal(err)
	}
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	flushed := nextFlush(t, calls)
	e := flushed.events[0]
	if resolved != "2026-11-30" || e.admittedAt.UTC().Format("2006-01-02") != resolved || !strings.HasPrefix(e.input.event.DistinctID, "cookieless_") || strings.Contains(string(e.input.event.Properties), "192.0.2.1") {
		t.Fatal("cookieless admission crossed day or retained identifying properties", resolved, e)
	}
	unavailable := newTestBuffer(t, testBufferConfig(), writer)
	if _, err := unavailable.Accept(t.Context(), batch, peer, true); err == nil || unavailable.snapshot().events != 0 {
		t.Fatal("cookieless admitted without durable key")
	}
}
func TestBufferedAcceptanceTimerAndConcurrentFlushLimit(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.flushInterval = 10 * time.Millisecond
	c.maxFlushes = 2
	calls, results := make(chan bufferedFlushBatch, 4), make(chan error, 4)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	for _, project := range []string{"a", "b", "c", "d"} {
		if _, err := b.Accept(t.Context(), testBufferBatch(t, project, 1), ingestionPeer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	nextFlush(t, calls)
	nextFlush(t, calls)
	if b.snapshot().inflight != 2 {
		t.Fatal("flush concurrency not bounded")
	}
	select {
	case <-calls:
		t.Fatal("too many concurrent flushes")
	case <-time.After(15 * time.Millisecond):
	}
	results <- nil
	results <- nil
	nextFlush(t, calls)
	nextFlush(t, calls)
	results <- nil
	results <- nil
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedAcceptanceProjectCountAndWholeBatchRejection(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.maxProjects = 2
	c.maxEvents = 3
	c.projectMaxEvents = 3
	b := newTestBuffer(t, c, liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }))
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "a", 2), ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	before := b.snapshot()
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "a", 2), ingestionPeer{}, false); err == nil {
		t.Fatal("oversized whole batch admitted")
	}
	if after := b.snapshot(); after.events != before.events || after.bytes != before.bytes {
		t.Fatal("partially admitted batch", before, after)
	}
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "b", 1), ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	// A separate roomy row budget isolates the active-project limit.
	c.maxEvents = 100
	countBuffer := newTestBuffer(t, c, liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }))
	for _, project := range []string{"a", "b"} {
		if _, err := countBuffer.Accept(t.Context(), testBufferBatch(t, project, 1), ingestionPeer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := countBuffer.Accept(t.Context(), testBufferBatch(t, "c", 1), ingestionPeer{}, false); err == nil {
		t.Fatal("active project limit ignored")
	}
	if countBuffer.snapshot().projects != 2 || countBuffer.snapshot().events != 2 {
		t.Fatal("project rejection changed queues")
	}
}

func TestBufferedLifetimeCancellationWaitsForActualWriterReturn(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		pending     bool
		writerError error
	}{
		{name: "committed writer"}, {name: "committed writer with pending batch", pending: true}, {name: "failed writer", writerError: errors.New("uncommitted")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lifetime, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, unblock := make(chan bufferedFlushBatch, 1), make(chan struct{})
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(unblock) }) })
			writer := liveFlusherFunc(func(_ context.Context, batch bufferedFlushBatch) error {
				entered <- batch
				<-unblock
				return test.writerError
			})
			c := testBufferConfig()
			c.flushBytes = 1
			b, err := newBufferedAcceptance(lifetime, c, writer, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false); err != nil {
				t.Fatal(err)
			}
			nextFlush(t, entered)
			if test.pending {
				if _, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			grace, stop := context.WithTimeout(t.Context(), 10*time.Millisecond)
			defer stop()
			if err := b.Drain(grace); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("cancellation claimed writer exit", err)
			}
			select {
			case <-b.done:
				t.Fatal("done closed while native writer still owned batch")
			default:
			}
			s := b.snapshot()
			if s.inflight != 1 || s.stopped {
				t.Fatal("lost in-flight ownership", s)
			}
			release.Do(func() { close(unblock) })
			select {
			case <-b.done:
			case <-time.After(2 * time.Second):
				t.Fatal("writer return was not observed")
			}
			remaining := 0
			if test.pending {
				remaining++
			}
			if test.writerError != nil {
				remaining++
			}
			s = b.snapshot()
			if s.inflight != 0 || !s.stopped || s.events != remaining || s.abandoned != remaining {
				t.Fatal("completion or abandoned accounting lost", s, remaining)
			}
			if err := b.Drain(t.Context()); (err == nil) != (remaining == 0) {
				t.Fatal("incorrect final drain outcome", s, err)
			}
			if remaining == 0 && (s.bytes != 0 || s.projects != 0) {
				t.Fatal("committed writer capacity leaked", s)
			}
		})
	}
}
func TestBufferedCookielessUnavailablePrecheckSkipsProvider(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"draining", "full", "failed"} {
		t.Run(state, func(t *testing.T) {
			t.Parallel()
			c := testBufferConfig()
			c.maxEvents = 1
			c.projectMaxEvents = 1
			c.retryMin = time.Hour
			c.retryMax = time.Hour
			if state == "failed" {
				c.flushBytes = 1
			}
			var calls atomic.Int64
			keys := cookielessKeysFunc(func(context.Context, string) ([]byte, error) { calls.Add(1); return bytes.Repeat([]byte("k"), 32), nil })
			writer := liveFlusherFunc(func(context.Context, bufferedFlushBatch) error {
				if state == "failed" {
					return errors.New("storage offline")
				}
				return nil
			})
			b, err := newBufferedAcceptance(t.Context(), c, writer, keys)
			if err != nil {
				t.Fatal(err)
			}
			if state == "draining" {
				if err := b.Drain(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false); err != nil {
					t.Fatal(err)
				}
				if state == "failed" {
					waitBuffer(t, b, func(s bufferSnapshot) bool { return s.failed })
				}
			}
			batch, err := prepareLiveBatch("p", "", eventBatch{{Event: "$pageview", DistinctID: ingestfmt.CookielessSentinel}}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, false); err == nil || calls.Load() != 0 {
				t.Fatal("unavailable admission contacted daily-key provider", state, calls.Load(), err)
			}
		})
	}
}
func TestBufferedCookielessKeyBoundaryAndFinalCapacityRecheck(t *testing.T) {
	t.Parallel()
	batch, err := prepareLiveBatch("p", "", eventBatch{{Event: "$pageview", DistinctID: ingestfmt.CookielessSentinel}}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 31, 33} {
		keys := cookielessKeysFunc(func(context.Context, string) ([]byte, error) { return bytes.Repeat([]byte("k"), size), nil })
		b, err := newBufferedAcceptance(t.Context(), testBufferConfig(), liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }), keys)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Accept(t.Context(), batch, ingestionPeer{}, false); err == nil || b.snapshot().events != 0 {
			t.Fatal("invalid daily key admitted data", size, err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	keys := cookielessKeysFunc(func(ctx context.Context, _ string) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return bytes.Repeat([]byte("k"), 32), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	c := testBufferConfig()
	c.maxEvents = 1
	c.projectMaxEvents = 1
	b, err := newBufferedAcceptance(t.Context(), c, liveFlusherFunc(func(context.Context, bufferedFlushBatch) error { return nil }), keys)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := b.Accept(t.Context(), batch, ingestionPeer{}, false); finished <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	if _, err := b.Accept(t.Context(), testBufferBatch(t, "p", 1), ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-finished:
		if err == nil || b.snapshot().events != 1 {
			t.Fatal("provider wait bypassed final capacity recheck", err, b.snapshot())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not finish")
	}
}

func TestBufferedAcceptanceDropsOnlyFencedProject(t *testing.T) {
	t.Parallel()
	calls := make(chan bufferedFlushBatch, 2)
	removed := make(chan struct{})
	kept := make(chan struct{})
	writer := liveFlusherFunc(func(ctx context.Context, batch bufferedFlushBatch) error {
		calls <- batch
		if batch.projectID == "removed" {
			select {
			case <-removed:
				return errHostedProjectFenced
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case <-kept:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cfg := testBufferConfig()
	cfg.flushBytes = 1
	first := testBufferBatch(t, "removed", 1)
	buffer := newTestBuffer(t, cfg, writer)
	if _, err := buffer.Accept(t.Context(), first, ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	if got := nextFlush(t, calls).projectID; got != "removed" {
		t.Fatal("unexpected first flush", got)
	}
	if _, err := buffer.Accept(t.Context(), first, ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	other := testBufferBatch(t, "kept", 1)
	if _, err := buffer.Accept(t.Context(), other, ingestionPeer{}, false); err != nil {
		t.Fatal(err)
	}
	if got := nextFlush(t, calls).projectID; got != "kept" {
		t.Fatal("unexpected second project", got)
	}
	close(removed)
	got := waitBuffer(t, buffer, func(s bufferSnapshot) bool { return s.projects == 1 })
	if got.events != 1 || got.inflight != 1 || got.failed {
		t.Fatalf("deletion affected the other project or retained payloads: %+v", got)
	}
	close(kept)
	waitBuffer(t, buffer, func(s bufferSnapshot) bool { return s.projects == 0 })
}
