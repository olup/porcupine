package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockedReceiptLookup struct {
	lookup  func(context.Context, string, []string) (map[string]int64, error)
	project string
	entered chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (s *blockedReceiptLookup) KnownEvents(ctx context.Context, project string, uuids []string) (map[string]int64, error) {
	if project == s.project {
		s.once.Do(func() { close(s.entered) })
		select {
		case <-s.resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return s.lookup(ctx, project, uuids)
}

func asyncReceiptCapture(ctx context.Context, s *Service, key, body string) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		r := httptest.NewRequest("POST", "/ingest/v1/events", strings.NewReader(body)).WithContext(ctx)
		r.Header.Set("X-Porcupine-Key", key)
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		done <- w
	}()
	return done
}

func waitReceiptCapture(t *testing.T, done <-chan *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	select {
	case response := <-done:
		if response.Code != 200 {
			t.Fatalf("capture returned %d: %s", response.Code, response.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	case <-time.After(2 * time.Second):
		t.Fatal("another project's receipt lookup blocked healthy ingestion")
		return nil
	}
}

func TestReceiptLookupsDoNotBlockOtherProjectsOrRaceFirstPrune(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"first-prune", "capture-after-prune"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := duckLakeJourneyService(t)
			var clock atomic.Int64
			clock.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
			s.eventRules.clock = func() time.Time { return time.UnixMilli(clock.Load()).UTC() }
			s.receiptRetention = 48 * time.Hour
			if phase == "first-prune" {
				// Shared control uses project gates and SQLite transactions without
				// a global local-policy mutex. Exercise that same exclusion path.
				s.eventRules.admission = nil
			}
			a, writerA := project(t, s)
			b, writerB := project(t, s)
			original := nativeRequest(t, s, writerA, `{"type":"track","message_id":"stable","name":"original"}`, 200)
			originalID := receiptFrom(t, original, 0)["event_id"]
			duckLakeJourneyDrain(t, s)
			clock.Add((72 * time.Hour).Milliseconds())
			if phase == "capture-after-prune" {
				if err := s.pruneReceipts(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			resume := make(chan struct{})
			var release sync.Once
			defer release.Do(func() { close(resume) })
			blocked := &blockedReceiptLookup{lookup: s.lake.KnownEvents, project: a, entered: make(chan struct{}), resume: resume}
			s.receiptLookup = blocked.KnownEvents
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var pruned chan error
			var retry <-chan *httptest.ResponseRecorder
			if phase == "first-prune" {
				pruned = make(chan error, 1)
				go func() { pruned <- s.pruneReceipts(ctx) }()
			} else {
				retry = asyncReceiptCapture(ctx, s, writerA, `{"type":"track","message_id":"stable","name":"changed"}`)
			}
			select {
			case <-blocked.entered:
			case <-ctx.Done():
				t.Fatal("lookup did not start")
			}
			if retry == nil {
				retry = asyncReceiptCapture(ctx, s, writerA, `{"type":"track","message_id":"stable","name":"changed"}`)
			}
			// One blocked project cannot retain the instance-wide commit lock.
			healthy := waitReceiptCapture(t, asyncReceiptCapture(ctx, s, writerB, `{"type":"track","message_id":"healthy","name":"healthy"}`))
			if healthy["accepted"] != float64(1) {
				t.Fatal(healthy)
			}
			// Imports take the same project/instance gates in the same order.
			data := []byte("{\"type\":\"track\",\"message_id\":\"history\",\"name\":\"history\",\"timestamp\":\"2020-01-01T00:00:00Z\"}\n")
			job := createFormatImport(t, s, b, adminToken, "native", "healthy-import", data)
			finished := finishFormatImport(t, s, b, adminToken, job, data)
			if finished.Counts.Accepted != 1 {
				t.Fatal(finished.Counts)
			}
			release.Do(func() { close(resume) })
			if pruned != nil {
				if err := <-pruned; err != nil {
					t.Fatal(err)
				}
			}
			replayed := waitReceiptCapture(t, retry)
			if replayed["duplicates"] != float64(1) || receiptFrom(t, replayed, 0)["event_id"] != originalID {
				t.Fatal("prune race resurrected the event or changed its ID", replayed)
			}
			var pendingA int
			if err := s.db.QueryRow("SELECT count(*) FROM events WHERE project_id=?", a).Scan(&pendingA); err != nil || pendingA != 0 {
				t.Fatal("retry entered the queue", pendingA, err)
			}
			s.eventRules.receiptGates.mutex.Lock()
			gates := len(s.eventRules.receiptGates.projects)
			s.eventRules.receiptGates.mutex.Unlock()
			if gates != 0 {
				t.Fatal("completed requests retained project gates", gates)
			}
		})
	}
}

func TestReceiptPruningCanceledLookupKeepsReceiptAndMarker(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	a, writer := project(t, s)
	nativeRequest(t, s, writer, `{"type":"track","message_id":"stable","name":"original"}`, 200)
	duckLakeJourneyDrain(t, s)
	s.receiptRetention = time.Hour
	if _, err := s.db.Exec("UPDATE event_receipts SET recorded_at_ms=0 WHERE project_id=?", a); err != nil {
		t.Fatal(err)
	}
	blocked := &blockedReceiptLookup{lookup: s.lake.KnownEvents, project: a, entered: make(chan struct{}), resume: make(chan struct{})}
	s.receiptLookup = blocked.KnownEvents
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- s.pruneReceipts(ctx) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("pruning did not reach its remote lookup")
	}
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("canceled pruning succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pruning did not release its project gate after cancellation")
	}
	var marker bool
	if err := s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM metadata WHERE key='receipts_pruned:' || ?)", a).Scan(&marker); err != nil || marker || receiptCount(t, s) != 1 {
		t.Fatal("canceled lookup changed retention state", marker, err)
	}
}

func TestReceiptPruningRechecksCandidatesAfterRemoteLookup(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	a, writer := project(t, s)
	nativeRequest(t, s, writer, `{"type":"track","message_id":"stable","name":"original"}`, 200)
	duckLakeJourneyDrain(t, s)
	s.receiptRetention = time.Hour
	if _, err := s.db.Exec("UPDATE event_receipts SET recorded_at_ms=0 WHERE project_id=?", a); err != nil {
		t.Fatal(err)
	}
	resume := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(resume) })
	blocked := &blockedReceiptLookup{lookup: s.lake.KnownEvents, project: a, entered: make(chan struct{}), resume: resume}
	s.receiptLookup = blocked.KnownEvents
	finished := make(chan error, 1)
	go func() { finished <- s.pruneReceipts(context.Background()) }()
	select {
	case <-blocked.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("pruning did not reach its remote lookup")
	}
	// Simulate a changed local candidate while its analytical head is checked.
	// The deletion must use its current age, not the earlier candidate list.
	if _, err := s.db.Exec("UPDATE event_receipts SET recorded_at_ms=? WHERE project_id=?", time.Now().Add(time.Hour).UnixMilli(), a); err != nil {
		t.Fatal(err)
	}
	release.Do(func() { close(resume) })
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	var marker bool
	if err := s.db.QueryRow("SELECT EXISTS(SELECT 1 FROM metadata WHERE key='receipts_pruned:' || ?)", a).Scan(&marker); err != nil || marker || receiptCount(t, s) != 1 {
		t.Fatal("stale candidate changed retention state", marker, err)
	}
}

func TestReceiptProjectGateCancellationReleasesReferences(t *testing.T) {
	t.Parallel()
	var gates receiptProjectGates
	release, err := gates.lock(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gates.lock(canceled, "a"); err == nil {
		t.Fatal("canceled waiter acquired project gate")
	}
	release()
	if len(gates.projects) != 0 {
		t.Fatal("canceled waiter leaked an idle project gate")
	}
}
