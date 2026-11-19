package acceptance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/observability"
)

func TestBufferedAcceptanceQuarantinesDeterministicProjects(t *testing.T) {
	t.Parallel()
	const brokenProjects = 15
	c := testBufferConfig()
	c.FlushBytes = 1
	c.ProjectMaxEvents = 1
	c.MaxEvents = brokenProjects
	c.MaxProjects = brokenProjects
	c.RetryMin, c.RetryMax = time.Millisecond, time.Millisecond
	deterministic := errors.New("native effect history gap")
	c.DeterministicError = func(err error) string {
		if errors.Is(err, deterministic) {
			return "native_evidence_gap"
		}
		return ""
	}
	c.Metrics = observability.NewRegistry("hosted")
	var attempts [brokenProjects]atomic.Int32
	var flushed atomic.Int32
	b := newTestBuffer(t, c, func(_ context.Context, batch FlushBatch) error {
		if batch.ProjectID == "healthy" {
			flushed.Add(1)
			return nil
		}
		var i int
		if _, err := fmt.Sscanf(batch.ProjectID, "broken-%d", &i); err != nil {
			return err
		}
		attempts[i].Add(1)
		return deterministic
	})
	for i := range brokenProjects {
		if _, err := b.Accept(t.Context(), testBufferBatch(fmt.Sprintf("broken-%d", i), 1), Peer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	quarantined := waitBuffer(t, b, func(s BufferSnapshot) bool {
		return len(s.QuarantinedProjects) == brokenProjects
	})
	if quarantined.Events != 0 || quarantined.Bytes != 0 || quarantined.Inflight != 0 ||
		quarantined.DroppedEvents != brokenProjects || quarantined.DroppedRequests != brokenProjects || !b.Available() {
		t.Fatal("quarantine failed to release active capacity", quarantined)
	}
	for i := range attempts {
		project := fmt.Sprintf("broken-%d", i)
		if quarantined.QuarantinedProjects[project] != "native_evidence_gap" {
			t.Fatal("missing quarantine reason", project, quarantined.QuarantinedProjects)
		}
		var failure *Failure
		_, err := b.Accept(t.Context(), testBufferBatch(project, 1), Peer{}, false)
		if !errors.As(err, &failure) || !failure.Pressure || !failure.Retry {
			t.Fatal("quarantined project was not given retryable pressure", project, err)
		}
	}
	for i := range 3 {
		if _, err := b.Accept(t.Context(), testBufferBatch("healthy", 1), Peer{}, false); err != nil {
			t.Fatalf("15 broken projects blocked healthy admission: %v", err)
		}
		waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 && flushed.Load() == int32(i+1) })
	}
	for i := range attempts {
		if n := attempts[i].Load(); n != 3 {
			t.Fatalf("quarantined project %d received %d flushes", i, n)
		}
	}
	var out strings.Builder
	c.Metrics.Write(&out, nil)
	for _, metric := range []string{
		`porcupine_flush_quarantined_projects_total{profile="hosted",reason="native_evidence_gap"} 15`,
		`porcupine_flush_dropped_events_total{profile="hosted",reason="native_evidence_gap"} 15`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="retry"} 30`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="quarantined"} 15`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="ok"} 3`,
	} {
		if !strings.Contains(out.String(), metric+"\n") {
			t.Errorf("missing metric %q", metric)
		}
	}
}

func TestBufferedAcceptanceUncertainFlushPreservesBatch(t *testing.T) {
	t.Parallel()
	deterministic := errors.New("fixed bad projection")
	conflict := errors.New("fixed bad proof")
	c := testBufferConfig()
	c.FlushBytes = 1
	c.RetryMin, c.RetryMax = time.Millisecond, time.Millisecond
	c.DeterministicError = func(err error) string {
		switch err {
		case deterministic:
			return "invalid_live_projection"
		case conflict:
			return "pipeline_evidence_conflict"
		default:
			return ""
		}
	}
	calls, results := make(chan FlushBatch, 1), make(chan error)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	if _, err := b.Accept(t.Context(), testBufferBatch("project", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	first := nextFlush(t, calls)
	for _, err := range []error{
		deterministic, deterministic, context.DeadlineExceeded,
		deterministic, deterministic, conflict,
		deterministic, deterministic, context.Canceled,
		context.Canceled, context.Canceled,
		ErrBatchTooLarge, ErrBatchTooLarge, ErrBatchTooLarge,
	} {
		results <- err
		if got := nextFlush(t, calls); !reflect.DeepEqual(got, first) {
			t.Fatalf("retry of %v changed batch", err)
		}
	}
	results <- nil
	s := waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 })
	if len(s.QuarantinedProjects) != 0 || s.DroppedEvents != 0 || s.DroppedRequests != 0 {
		t.Fatal("temporary or changing failures discarded events", s)
	}
}

func TestBufferedAcceptanceQuarantineEvictsOldestMarker(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.FlushBytes = 1
	c.MaxProjects = 1
	c.RetryMin, c.RetryMax = time.Millisecond, time.Millisecond
	c.DeterministicError = func(error) string { return "invalid_live_projection" }
	calls, results := make(chan FlushBatch, 1), make(chan error)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	for i, project := range []string{"old", "new"} {
		if _, err := b.Accept(t.Context(), testBufferBatch(project, 1), Peer{}, false); err != nil {
			t.Fatal(err)
		}
		nextFlush(t, calls)
		// Admit a second request while a flush is in flight. Its events have
		// never reached the writer and are counted when the project is set aside.
		if _, err := b.Accept(t.Context(), testBufferBatch(project, 2), Peer{}, false); err != nil {
			t.Fatal(err)
		}
		for attempt := range 3 {
			if attempt > 0 && len(nextFlush(t, calls).Events) != 1 {
				t.Fatal("retry absorbed new input")
			}
			results <- errors.New("fixed")
		}
		s := waitBuffer(t, b, func(s BufferSnapshot) bool { return s.QuarantinedProjects[project] != "" })
		if len(s.QuarantinedProjects) != 1 || s.DroppedEvents != uint64(3*(i+1)) || s.DroppedRequests != uint64(2*(i+1)) || s.Bytes != 0 {
			t.Fatal("unbounded marker storage or incorrect buffered drop accounting", s)
		}
	}
	if _, err := b.Accept(t.Context(), testBufferBatch("old", 1), Peer{}, false); err != nil {
		t.Fatal("evicted marker still rejects admission", err)
	}
	nextFlush(t, calls)
	results <- nil
	waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 })
}
