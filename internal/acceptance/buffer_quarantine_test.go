package acceptance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
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
	c.MaxEvents = brokenProjects + 1
	c.MaxProjects = brokenProjects + 1
	c.RetryMin, c.RetryMax = 5*time.Millisecond, 40*time.Millisecond
	c.ProjectMaxBytes = 2 << 10
	deterministic := errors.New("native effect history gap")
	c.ClassifyFlushError = func(err error) (string, string) {
		if errors.Is(err, deterministic) {
			return "native_evidence_gap", ""
		}
		return "", ""
	}
	c.Metrics = observability.NewRegistry("hosted")
	var flushed, recovered atomic.Int32
	var repair atomic.Bool
	b := newTestBuffer(t, c, func(_ context.Context, batch FlushBatch) error {
		if batch.ProjectID == "healthy" {
			flushed.Add(1)
			return nil
		}
		if repair.Load() {
			recovered.Add(1)
			return nil
		}
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
	if quarantined.Events != brokenProjects || quarantined.Bytes == 0 ||
		quarantined.Bytes > brokenProjects*(c.ProjectMaxBytes+bufferedProjectOverhead+int64(len("broken-10"))) ||
		quarantined.DroppedEvents != 0 || quarantined.DroppedRequests != 0 || !b.Available() {
		t.Fatal("quarantine failed to retain acknowledged events", quarantined)
	}
	for i := range brokenProjects {
		project := fmt.Sprintf("broken-%d", i)
		if quarantined.QuarantinedProjects[project] != "native_evidence_gap" {
			t.Fatal("missing quarantine reason", project, quarantined.QuarantinedProjects)
		}
		_, err := b.Accept(t.Context(), testBufferBatch(project, 1), Peer{}, false)
		if !errors.Is(err, ErrProjectQuarantined) {
			t.Fatal("quarantined project was not given retryable pressure", project, err)
		}
	}
	for i := range 3 {
		if _, err := b.Accept(t.Context(), testBufferBatch("healthy", 1), Peer{}, false); err != nil {
			t.Fatalf("15 broken projects blocked healthy admission: %v", err)
		}
		waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == brokenProjects && flushed.Load() == int32(i+1) })
	}
	repair.Store(true)
	waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 && recovered.Load() == brokenProjects })
	if _, err := b.Accept(t.Context(), testBufferBatch("broken-0", 1), Peer{}, false); err != nil {
		t.Fatal("repaired project still rejected admission", err)
	}
	waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Bytes == 0 && len(s.QuarantinedProjects) == 0 })
	var out strings.Builder
	c.Metrics.Write(&out, nil)
	for _, metric := range []string{
		`porcupine_flush_quarantined_projects_total{profile="hosted",reason="native_evidence_gap"} 15`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="quarantined"} 15`,
		`porcupine_flush_attempts_total{profile="hosted",outcome="ok"} 19`,
	} {
		if !strings.Contains(out.String(), metric+"\n") {
			t.Errorf("missing metric %q", metric)
		}
	}
	if strings.Contains(out.String(), "porcupine_flush_dropped_events_total") {
		t.Fatal("a project failure produced a dropped-event metric")
	}
}

func TestBufferedAcceptanceUncertainFlushPreservesBatch(t *testing.T) {
	t.Parallel()
	deterministic := errors.New("fixed bad projection")
	conflict := errors.New("fixed bad proof")
	c := testBufferConfig()
	c.FlushBytes = 1
	c.RetryMin, c.RetryMax = time.Millisecond, time.Millisecond
	c.ClassifyFlushError = func(err error) (string, string) {
		switch err {
		case deterministic:
			return "", "invalid_live_projection"
		case conflict:
			return "pipeline_evidence_conflict", ""
		default:
			return "", ""
		}
	}
	calls, results := make(chan FlushBatch, 1), make(chan error)
	b := newTestBuffer(t, c, blockingTestWriter(calls, results))
	if _, err := b.Accept(t.Context(), testBufferBatch("project", 1), Peer{}, false); err != nil {
		t.Fatal(err)
	}
	first := nextFlush(t, calls)
	pending := testBufferBatch("project", 1)
	pending.Events[0].Event.UUID = "pending"
	if _, err := b.Accept(t.Context(), pending, Peer{}, false); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{
		errors.Join(deterministic, context.DeadlineExceeded),
		conflict, context.Canceled,
		ErrBatchTooLarge, ErrBatchTooLarge, ErrBatchTooLarge,
	} {
		results <- err
		if got := nextFlush(t, calls); !reflect.DeepEqual(got, first) {
			t.Fatalf("retry of %v changed batch", err)
		}
	}
	results <- nil
	if got := nextFlush(t, calls); got.ID == first.ID || len(got.Events) != 1 || got.Events[0].Input.Event.UUID != "pending" {
		t.Fatalf("pending request lost or merged into first batch: %+v", got)
	}
	results <- nil
	s := waitBuffer(t, b, func(s BufferSnapshot) bool { return s.Events == 0 })
	if len(s.QuarantinedProjects) != 0 || s.DroppedEvents != 0 || s.DroppedRequests != 0 {
		t.Fatal("temporary or changing failures discarded events", s)
	}
}

func TestBufferedAcceptanceIsolatesInvalidRequest(t *testing.T) {
	t.Parallel()
	c := testBufferConfig()
	c.Metrics = observability.NewRegistry("hosted")
	invalid := errors.New("invalid projection")
	c.ClassifyFlushError = func(err error) (string, string) {
		if errors.Is(err, invalid) {
			return "", "invalid_live_projection"
		}
		return "", ""
	}
	var committed []string
	b := newTestBuffer(t, c, func(_ context.Context, batch FlushBatch) error {
		for _, e := range batch.Events {
			if e.Input.Event.UUID == "bad" {
				return invalid
			}
		}
		for _, e := range batch.Events {
			committed = append(committed, e.Input.Event.UUID)
		}
		return nil
	})
	var expected []string
	for i := range 10 {
		if i == 5 {
			bad := testBufferBatch("project", 2)
			bad.Events[0].Event.UUID = "bad"
			bad.Events[1].Event.UUID = "same-request"
			if _, err := b.Accept(t.Context(), bad, Peer{}, false); err != nil {
				t.Fatal(err)
			}
		}
		batch := testBufferBatch("project", 1)
		batch.Events[0].Event.UUID = fmt.Sprintf("good-%d", i)
		expected = append(expected, batch.Events[0].Event.UUID)
		if _, err := b.Accept(t.Context(), batch, Peer{}, false); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if err := b.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(committed, expected) {
		t.Fatalf("committed %v, want %v", committed, expected)
	}
	s := b.Snapshot()
	if s.DroppedRequests != 1 || s.DroppedEvents != 2 || s.Bytes != 0 || len(s.QuarantinedProjects) != 0 {
		t.Fatal("discard escaped the isolated request", s)
	}
	var out strings.Builder
	c.Metrics.Write(&out, nil)
	if !strings.Contains(out.String(), `porcupine_flush_dropped_events_total{profile="hosted",reason="invalid_live_projection"} 2`+"\n") {
		t.Fatal("missing isolated rejection metric", out.String())
	}
}
