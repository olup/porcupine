package acceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	"github.com/olup/porcupine/internal/observability"
)

func testBufferConfig() BufferConfig {
	c := DefaultBufferConfig()
	c.FlushInterval = time.Hour
	return c
}
func newTestBuffer(t testing.TB, c BufferConfig, writer FlushFunc) *Buffer {
	t.Helper()
	buffer, err := NewBuffer(t.Context(), c, writer, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return buffer
}
func testBufferBatch(project string, count int) Batch {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	uuid := "porcupine-ingest-v1:" + strings.Repeat("f", 128)
	events := make([]Input, count)
	for i := range events {
		events[i] = Input{Event: eventapi.Event{
			ProjectID: project, UUID: uuid, MessageID: "stable-message",
			Event: "purchase", DistinctID: "actor", SourceID: "source", EventType: "track",
			Timestamp: timestamp, ReceivedAt: timestamp,
			Properties: json.RawMessage(`{"cookieless":"business-marker","distinct_id":"business-distinct","token":"business"}`),
			Context:    json.RawMessage(`{"large":9007199254740993,"page":{"url":"https://example.test/checkout"}}`),
		}}
	}
	return Batch{ProjectID: project, Events: events}
}

func testCookielessBatch() Batch {
	return Batch{ProjectID: "p", Cookieless: true, Events: []Input{{Event: eventapi.Event{
		ProjectID: "p", UUID: "stable-uuid", Event: "$pageview",
		DistinctID: ingestfmt.CookielessSentinel, Properties: json.RawMessage(`{}`),
	}}}}
}

func waitBuffer(t *testing.T, b *Buffer, predicate func(BufferSnapshot) bool) BufferSnapshot {
	t.Helper()
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s := b.Snapshot()
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
func nextFlush(t *testing.T, calls <-chan FlushBatch) FlushBatch {
	t.Helper()
	select {
	case batch := <-calls:
		return batch
	case <-time.After(2 * time.Second):
		t.Fatal("flush did not start")
		return FlushBatch{}
	}
}
func blockingTestWriter(calls chan<- FlushBatch, results <-chan error) FlushFunc {
	return FlushFunc(func(ctx context.Context, batch FlushBatch) error {
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
	if _, err := NewBuffer(t.Context(), testBufferConfig(), nil, nil, nil); err == nil {
		t.Fatal("nil writer accepted")
	}
	c := testBufferConfig()
	c.FlushBytes = c.ProjectMaxBytes + 1
	if _, err := NewBuffer(t.Context(), c, FlushFunc(func(context.Context, FlushBatch) error { return nil }), nil, nil); err == nil {
		t.Fatal("unbounded configuration accepted")
	}
}
func TestBufferedAcceptanceAtomicCapacityAndScope(t *testing.T) {
	t.Parallel()
	batch := testBufferBatch("p", 2)
	size := bufferedInputBytes(batch.Events[0])
	c := testBufferConfig()
	c.MaxBytes = 2*size + bufferedProjectOverhead + 1
	c.ProjectMaxBytes = 2 * size
	c.FlushBytes = 2 * size
	c.MaxEvents = 2
	c.ProjectMaxEvents = 2
	calls, results := make(chan FlushBatch, 1), make(chan error)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	outcome, err := b.Accept(t.Context(), batch, Peer{}, true)
	if err != nil || outcome.(BufferedResult).Received != 2 {
		t.Fatal(outcome, err)
	}
	before := b.Snapshot()
	if before.Bytes != c.MaxBytes || before.Events != 2 {
		t.Fatal("exact-fit accounting", before, c.MaxBytes)
	}
	if _, err = b.Accept(t.Context(), testBufferBatch("p", 2), Peer{}, true); err == nil {
		t.Fatal("overflow accepted")
	}
	after := b.Snapshot()
	if after.Bytes != before.Bytes || after.Events != before.Events {
		t.Fatal("partial admission", before, after)
	}
	forged := testBufferBatch("other", 1)
	forged.Events[0].Event.ProjectID = "p"
	if _, err = b.Accept(t.Context(), forged, Peer{}, true); err == nil {
		t.Fatal("forged project accepted")
	}
	if b.Snapshot().Events != 2 {
		t.Fatal("scope failure changed buffer")
	}
}
func TestBufferedAcceptanceConcurrentGlobalAndProjectBounds(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.MaxEvents = 21
	c.ProjectMaxEvents = 11
	c.MaxProjects = 2
	b := newTestBuffer(t, c, blockingTestWriter(make(chan FlushBatch, 2), nil))
	var wg sync.WaitGroup
	var admitted atomic.Int64
	for i := range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			project := []string{"a", "b"}[i%2]
			if _, err := b.Accept(t.Context(), testBufferBatch(project, 2), Peer{}, false); err == nil {
				admitted.Add(2)
			}
		}()
	}
	wg.Wait()
	s := b.Snapshot()
	if s.Events != 20 || s.projects != 2 || admitted.Load() != 20 {
		t.Fatal("concurrent capacity mismatch", s, admitted.Load())
	}
	if _, err := b.Accept(t.Context(), testBufferBatch("c", 1), Peer{}, false); err == nil {
		t.Fatal("project count limit exceeded")
	}
}
func TestBufferedAcceptanceInflightRetainsCapacityAndGrace(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.FlushBytes = 1
	c.MaxEvents = 2
	c.ProjectMaxEvents = 2
	calls, results := make(chan FlushBatch, 2), make(chan error, 2)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	batch := testBufferBatch("p", 1)
	if _, err := b.Accept(t.Context(), batch, Peer{}, true); err != nil {
		t.Fatal(err)
	}
	nextFlush(t, calls)
	if _, err := b.Accept(t.Context(), batch, Peer{}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Accept(t.Context(), batch, Peer{}, true); err == nil {
		t.Fatal("inflight capacity was released early")
	}
	before := b.Snapshot()
	if before.Events != 2 || before.Inflight != 1 || before.Oldest.IsZero() {
		t.Fatal(before)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := b.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("grace expiry reported drained", err)
	}
	if s := b.Snapshot(); s.Events != 2 || !s.draining || s.Bytes != before.Bytes {
		t.Fatal("deadline dropped retained work", s)
	}
	if _, err := b.Accept(t.Context(), batch, Peer{}, true); err == nil {
		t.Fatal("draining buffer admitted new data")
	}
	results <- nil
	nextFlush(t, calls)
	results <- nil
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if s := b.Snapshot(); s.Events != 0 || s.Bytes != 0 || s.projects != 0 {
		t.Fatal("successful drain retained capacity", s)
	}
}
func TestBufferedAcceptanceFailureBackpressureAndStableRetry(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.FlushBytes = 1
	c.RetryMin = 250 * time.Millisecond
	c.RetryMax = c.RetryMin
	calls, results := make(chan FlushBatch, 2), make(chan error, 2)
	blocked := blockingTestWriter(calls, results)
	healthy := make(chan struct{}, 1)
	b := newTestBuffer(t, c, FlushFunc(func(ctx context.Context, batch FlushBatch) error {
		if batch.ProjectID == "healthy-project" {
			healthy <- struct{}{}
			return nil
		}
		return blocked(ctx, batch)
	}))
	if _, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	first := nextFlush(t, calls)
	results <- errors.New("pipeline capacity exceeded")
	failed := waitBuffer(t, b, func(s BufferSnapshot) bool { return s.failed })
	if failed.Events != 1 || failed.Oldest != first.oldest {
		t.Fatal(failed)
	}
	_, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false)
	var failure *Failure
	if !errors.As(err, &failure) || !failure.Retry {
		t.Fatal("flush failure did not backpressure its own project", err)
	}
	if !b.Available() {
		t.Fatal("failed project made the worker unready")
	}
	if _, err := b.Accept(t.Context(), testBufferBatch("healthy-project", 1), Peer{}, false); err != nil {
		t.Fatal("failed project prevented healthy project admission", err)
	}
	select {
	case <-healthy:
	case <-time.After(2 * time.Second):
		t.Fatal("healthy project did not flush")
	}
	retry := nextFlush(t, calls)
	if retry.ID != first.ID || !reflect.DeepEqual(retry, first) {
		t.Fatal("retry changed batch identity or contents")
	}
	results <- nil
	waitBuffer(t, b, func(s BufferSnapshot) bool { return !s.failed && s.Events == 0 })
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBufferedAcceptanceSplitsAtRequestBoundaries(t *testing.T) {
	t.Parallel()
	for _, testcase := range []struct {
		name                       string
		sizes, attempts, committed []int
	}{
		{name: "both requests fit", sizes: []int{3, 3}, attempts: []int{6, 3, 3}, committed: []int{3, 3}},
		{name: "one oversized request", sizes: []int{5, 2, 1}, attempts: []int{8, 5, 3}, committed: []int{5, 3}},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			t.Parallel()
			var attempts, committed []int
			ids := map[string]bool{}
			writer := FlushFunc(func(_ context.Context, batch FlushBatch) error {
				if ids[batch.ID] {
					t.Errorf("split reused batch ID %q", batch.ID)
				}
				ids[batch.ID] = true
				raw, prefixed := strings.CutPrefix(batch.ID, "live-")
				id, err := uuid.Parse(raw)
				if !prefixed || err != nil || id.Version() != 7 {
					t.Errorf("batch ID %q must have a live- prefix and a UUIDv7", batch.ID)
				}
				attempts = append(attempts, len(batch.Events))
				if len(batch.Events) > 3 && batch.CanSplit() {
					return ErrBatchTooLarge
				}
				committed = append(committed, len(batch.Events))
				return nil
			})
			b := newTestBuffer(t, testBufferConfig(), writer)
			for _, n := range testcase.sizes {
				if _, err := b.Accept(t.Context(), testBufferBatch("project", n), Peer{}, false); err != nil {
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
			s := b.Snapshot()
			if s.Events != 0 || s.Bytes != 0 || s.projects != 0 || s.DroppedRequests != 0 || s.DroppedEvents != 0 {
				t.Fatal("incorrect capacity or drop counts after drain", s)
			}
		})
	}
}

func TestBufferedAcceptanceOwnsCommandsAndPayload(t *testing.T) {
	t.Parallel()
	batch := testBufferBatch("p", 1)
	control, err := ValidateInput(eventapi.Event{
		ProjectID: "p", UUID: "identity", Event: "$identify", DistinctID: "person",
		Properties: json.RawMessage(`{"$anon_distinct_id":"browser","$set":{"plan":"new"}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	batch.Events = append(batch.Events, control)
	calls := make(chan FlushBatch, 1)
	b := newTestBuffer(t, testBufferConfig(), FlushFunc(func(_ context.Context, batch FlushBatch) error { calls <- batch; return nil }))
	if _, err := b.Accept(t.Context(), batch, Peer{}, true); err != nil {
		t.Fatal(err)
	}
	batch.Events[0].Event.Properties[0] = '!'
	batch.Events[0].Event.Context[0] = '!'
	batch.Events[1].Identity.Set[0] = '!'
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	flushed := nextFlush(t, calls)
	if !json.Valid(flushed.Events[0].Input.Event.Properties) || !json.Valid(flushed.Events[0].Input.Event.Context) || string(flushed.Events[1].Input.Identity.Set) != `{"plan":"new"}` {
		t.Fatal("caller mutated retained data")
	}
	identity := flushed.Events[1].Input
	if identity.Event.DistinctID != "" || string(identity.Event.Properties) != `{}` || identity.Event.IdentitySnapshot != "" || identity.Identity.Target != "person" || identity.Identity.Anonymous != "browser" {
		t.Fatal("identity command mixed with committed event", identity)
	}
}
func TestBufferedAcceptanceCookielessDayAndPrivacy(t *testing.T) {
	t.Parallel()
	var Clock atomic.Int64
	Clock.Store(time.Date(2026, 11, 30, 23, 59, 59, 0, time.UTC).UnixNano())
	c := testBufferConfig()
	c.Clock = func() time.Time { return time.Unix(0, Clock.Load()) }
	calls := make(chan FlushBatch, 1)
	writer := FlushFunc(func(_ context.Context, batch FlushBatch) error { calls <- batch; return nil })
	var resolved string
	keys := DailyKeyFunc(func(_ context.Context, day string) ([]byte, error) {
		resolved = day
		Clock.Add(int64(2 * time.Second))
		return bytes.Repeat([]byte("k"), 32), nil
	})
	b, err := NewBuffer(t.Context(), c, writer, nil, keys)
	if err != nil {
		t.Fatal(err)
	}
	batch := testCookielessBatch()
	batch.Events[0].Event.SourceID = "source"
	batch.Events[0].Event.Properties = json.RawMessage(`{"$ip":"192.0.2.1","amount":42}`)
	peer := Peer{Address: netip.MustParseAddr("192.0.2.1"), UserAgent: "private-user-agent"}
	if _, err := b.Accept(t.Context(), batch, peer, true); err != nil {
		t.Fatal(err)
	}
	if err := b.Drain(t.Context()); err != nil {
		t.Fatal(err)
	}
	flushed := nextFlush(t, calls)
	e := flushed.Events[0]
	if resolved != "2026-11-30" || e.AdmittedAt.UTC().Format("2006-01-02") != resolved || !strings.HasPrefix(e.Input.Event.DistinctID, "cookieless_") || strings.Contains(string(e.Input.Event.Properties), "192.0.2.1") {
		t.Fatal("cookieless admission crossed day or retained identifying properties", resolved, e)
	}
	unavailable := newTestBuffer(t, testBufferConfig(), writer)
	if _, err := unavailable.Accept(t.Context(), batch, peer, true); err == nil || unavailable.Snapshot().Events != 0 {
		t.Fatal("cookieless admitted without durable key")
	}
}
func TestBufferedAcceptanceTimerAndConcurrentFlushLimit(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.FlushInterval = 10 * time.Millisecond
	c.MaxFlushes = 2
	calls, results := make(chan FlushBatch, 4), make(chan error, 4)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	for _, project := range []string{"a", "b", "c", "d"} {
		if _, err := b.Accept(t.Context(), testBufferBatch(project, 1), Peer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	nextFlush(t, calls)
	nextFlush(t, calls)
	if b.Snapshot().Inflight != 2 {
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

func TestBufferedAcceptanceCountTriggerLeavesRoomForAnotherRequest(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := testBufferConfig()
		c.FlushInterval = 30 * time.Second
		c.ProjectMaxEvents = 9
		calls, results := make(chan FlushBatch, 2), make(chan error)
		b := newTestBuffer(t, c, blockingTestWriter(calls, results))
		batch := testBufferBatch("p", 2)
		admit := func() {
			t.Helper()
			if result, err := b.Accept(t.Context(), batch, Peer{}, false); err != nil || result.(BufferedResult).Received != 2 {
				t.Fatal("expected whole request to be admitted", result, err)
			}
		}
		assertWaiting := func() {
			t.Helper()
			synctest.Wait()
			if len(calls) != 0 {
				t.Fatal("flushed before either the count or timer was reached")
			}
		}
		admit()
		assertWaiting()
		admit()
		if first := nextFlush(t, calls); len(first.Events) != 4 || !first.Events[1].requestEnd || !first.Events[3].requestEnd {
			t.Fatal("count trigger failed to flush both requests intact", first.Events)
		}
		admit()
		if _, err := b.Accept(t.Context(), testBufferBatch("p", 4), Peer{}, false); err == nil {
			t.Fatal("admission ignored the events in flight")
		}
		if status := b.Snapshot(); status.Events != 6 || status.Inflight != 1 {
			t.Fatal("project capacity accounting changed", status)
		}
		results <- nil
		assertWaiting()
		time.Sleep(c.FlushInterval - time.Nanosecond)
		assertWaiting()
		time.Sleep(time.Nanosecond)
		if last := nextFlush(t, calls); len(last.Events) != 2 {
			t.Fatal("the timer did not flush the remaining whole request", last.Events)
		}
		results <- nil
		if err := b.Drain(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBufferedAcceptanceCountTriggerPrecedesDefaultByteTrigger(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := DefaultBufferConfig()
		c.FlushInterval = 30 * time.Second
		calls := make(chan FlushBatch, 1)
		b := newTestBuffer(t, c, func(_ context.Context, batch FlushBatch) error {
			calls <- batch
			return nil
		})
		for _, count := range []int{1000, 1000, 1000, 333} {
			if _, err := b.Accept(t.Context(), testBufferBatch("p", count), Peer{}, false); err != nil {
				t.Fatal(err)
			}
		}
		if batch := nextFlush(t, calls); len(batch.Events) != 3333 || batch.bytes >= c.FlushBytes {
			t.Fatalf("expected the event count to trigger a flush first, got %d events and %d bytes", len(batch.Events), batch.bytes)
		}
		if err := b.Drain(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBufferedAcceptanceProjectCountAndWholeBatchRejection(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.MaxProjects = 2
	c.MaxEvents = 3
	c.ProjectMaxEvents = 3
	b := newTestBuffer(t, c, blockingTestWriter(make(chan FlushBatch, 2), nil))
	if _, err := b.Accept(t.Context(), testBufferBatch("a", 2), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	before := b.Snapshot()
	if _, err := b.Accept(t.Context(), testBufferBatch("a", 2), Peer{}, false); err == nil {
		t.Fatal("oversized whole batch admitted")
	}
	if after := b.Snapshot(); after.Events != before.Events || after.Bytes != before.Bytes {
		t.Fatal("partially admitted batch", before, after)
	}
	if _, err := b.Accept(t.Context(), testBufferBatch("b", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	// A separate roomy row budget isolates the active-project limit.
	c.MaxEvents, c.ProjectMaxEvents = 100, 100
	countBuffer := newTestBuffer(t, c, FlushFunc(func(context.Context, FlushBatch) error { return nil }))
	for _, project := range []string{"a", "b"} {
		if _, err := countBuffer.Accept(t.Context(), testBufferBatch(project, 1), Peer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := countBuffer.Accept(t.Context(), testBufferBatch("c", 1), Peer{}, false); err == nil {
		t.Fatal("active project limit ignored")
	}
	if countBuffer.Snapshot().projects != 2 || countBuffer.Snapshot().Events != 2 {
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
			entered, unblock := make(chan FlushBatch, 1), make(chan struct{})
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(unblock) }) })
			writer := FlushFunc(func(_ context.Context, batch FlushBatch) error {
				entered <- batch
				<-unblock
				return test.writerError
			})
			c := testBufferConfig()
			c.FlushBytes = 1
			b, err := NewBuffer(lifetime, c, writer, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false); err != nil {
				t.Fatal(err)
			}
			nextFlush(t, entered)
			if test.pending {
				if _, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false); err != nil {
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
			s := b.Snapshot()
			if s.Inflight != 1 || s.Stopped {
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
			s = b.Snapshot()
			if s.Inflight != 0 || !s.Stopped || s.Events != remaining || s.abandoned != remaining {
				t.Fatal("completion or abandoned accounting lost", s, remaining)
			}
			if err := b.Drain(t.Context()); (err == nil) != (remaining == 0) {
				t.Fatal("incorrect final drain outcome", s, err)
			}
			if remaining == 0 && (s.Bytes != 0 || s.projects != 0) {
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
			c.MaxEvents = 1
			c.ProjectMaxEvents = 1
			c.RetryMin = time.Hour
			c.RetryMax = time.Hour
			if state == "failed" {
				c.FlushBytes = 1
			}
			var calls atomic.Int64
			keys := DailyKeyFunc(func(context.Context, string) ([]byte, error) { calls.Add(1); return bytes.Repeat([]byte("k"), 32), nil })
			writer := FlushFunc(func(ctx context.Context, _ FlushBatch) error {
				if state == "failed" {
					return errors.New("storage offline")
				}
				<-ctx.Done()
				return ctx.Err()
			})
			b, err := NewBuffer(t.Context(), c, writer, nil, keys)
			if err != nil {
				t.Fatal(err)
			}
			if state == "draining" {
				if err := b.Drain(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false); err != nil {
					t.Fatal(err)
				}
				if state == "failed" {
					waitBuffer(t, b, func(s BufferSnapshot) bool { return s.failed })
				}
			}
			batch := testCookielessBatch()
			if _, err := b.Accept(t.Context(), batch, Peer{}, false); err == nil || calls.Load() != 0 {
				t.Fatal("unavailable admission contacted daily-key provider", state, calls.Load(), err)
			}
		})
	}
}
func TestBufferedCookielessKeyBoundaryAndFinalCapacityRecheck(t *testing.T) {
	t.Parallel()
	batch := testCookielessBatch()
	for _, size := range []int{0, 1, 31, 33} {
		keys := DailyKeyFunc(func(context.Context, string) ([]byte, error) { return bytes.Repeat([]byte("k"), size), nil })
		b, err := NewBuffer(t.Context(), testBufferConfig(), FlushFunc(func(context.Context, FlushBatch) error { return nil }), nil, keys)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Accept(t.Context(), batch, Peer{}, false); err == nil || b.Snapshot().Events != 0 {
			t.Fatal("invalid daily key admitted data", size, err)
		}
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	keys := DailyKeyFunc(func(ctx context.Context, _ string) ([]byte, error) {
		close(entered)
		select {
		case <-release:
			return bytes.Repeat([]byte("k"), 32), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	c := testBufferConfig()
	c.MaxEvents = 1
	c.ProjectMaxEvents = 1
	b, err := NewBuffer(t.Context(), c, blockingTestWriter(make(chan FlushBatch, 1), nil), nil, keys)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := b.Accept(t.Context(), batch, Peer{}, false); finished <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("provider did not start")
	}
	if _, err := b.Accept(t.Context(), testBufferBatch("p", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-finished:
		if err == nil || b.Snapshot().Events != 1 {
			t.Fatal("provider wait bypassed final capacity recheck", err, b.Snapshot())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not finish")
	}
}

func TestBufferedAcceptanceDropsOnlyFencedProject(t *testing.T) {
	t.Parallel()
	calls := make(chan FlushBatch, 2)
	removed := make(chan struct{})
	kept := make(chan struct{})
	writer := FlushFunc(func(ctx context.Context, batch FlushBatch) error {
		calls <- batch
		if batch.ProjectID == "removed" {
			select {
			case <-removed:
				return ErrProjectFenced
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
	cfg.FlushBytes = 1
	first := testBufferBatch("removed", 1)
	buffer := newTestBuffer(t, cfg, writer)
	if _, err := buffer.Accept(t.Context(), first, Peer{}, false); err != nil {
		t.Fatal(err)
	}
	if got := nextFlush(t, calls).ProjectID; got != "removed" {
		t.Fatal("unexpected first flush", got)
	}
	if _, err := buffer.Accept(t.Context(), first, Peer{}, false); err != nil {
		t.Fatal(err)
	}
	other := testBufferBatch("kept", 1)
	if _, err := buffer.Accept(t.Context(), other, Peer{}, false); err != nil {
		t.Fatal(err)
	}
	if got := nextFlush(t, calls).ProjectID; got != "kept" {
		t.Fatal("unexpected second project", got)
	}
	close(removed)
	got := waitBuffer(t, buffer, func(s BufferSnapshot) bool { return s.projects == 1 })
	if got.Events != 1 || got.Inflight != 1 || got.failed {
		t.Fatalf("deletion affected the other project or retained payloads: %+v", got)
	}
	close(kept)
	waitBuffer(t, buffer, func(s BufferSnapshot) bool { return s.projects == 0 })
}

func TestOperationalHostedFlushRetriesAnIndivisibleBatch(t *testing.T) {
	t.Parallel()
	m := observability.NewRegistry("hosted")
	c := testBufferConfig()
	c.Metrics = m
	c.FlushBytes = 1
	calls, results := make(chan FlushBatch, 2), make(chan error, 2)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	if _, err := b.Accept(t.Context(), testBufferBatch("private-project", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	nextFlush(t, calls)
	results <- errors.New("private-backend-error")
	nextFlush(t, calls)
	results <- ErrBatchTooLarge
	nextFlush(t, calls)
	results <- nil
	waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 && s.DroppedEvents == 0 })
	var out strings.Builder
	m.Write(&out, nil)
	metrics := out.String()
	for _, expected := range []string{
		`porcupine_flush_attempts_total{profile="hosted",outcome="retry"} 2`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="ok"} 1`,
		`porcupine_flush_duration_seconds_count{profile="hosted"} 3`,
	} {
		if !strings.Contains(metrics, expected+"\n") {
			t.Fatalf("missing metric %q in %s", expected, metrics)
		}
	}
	if strings.Contains(metrics, "private-") {
		t.Fatal("flush telemetry exposed project or error detail")
	}
}
