package porcupine

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBoundedReceiptsFallBackToDuckLake(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	var now atomic.Int64
	now.Store(time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli())
	s.eventRules.clock = func() time.Time { return time.UnixMilli(now.Load()).UTC() }
	s.receiptRetention = 48 * time.Hour
	projectID, writer := project(t, s)
	ctx := context.Background()
	receipts := func() (n int) {
		if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE project_id=?`, projectID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	body := `{"type":"track","message_id":"old-retry","name":"purchase","timestamp":"2026-11-01T10:00:00Z"}`
	first := nativeRequest(t, s, writer, body, 200)
	id := receiptFrom(t, first, 0)["event_id"]
	duckLakeJourneyDrain(t, s)
	if receipts() != 1 {
		t.Fatal("projected event has no receipt")
	}
	if err := s.pruneReceipts(ctx); err != nil || receipts() != 1 {
		t.Fatal("receipt pruned inside the retention window", err)
	}
	now.Add((72 * time.Hour).Milliseconds())
	if err := s.pruneReceipts(ctx); err != nil || receipts() != 0 {
		t.Fatal("expired receipt kept", err)
	}
	replay := nativeRequest(t, s, writer, body, 200)
	if replay["duplicates"] != float64(1) || receiptFrom(t, replay, 0)["event_id"] != id {
		t.Fatalf("pruned receipt lost deduplication: %v", replay)
	}
	fresh := nativeRequest(t, s, writer, `{"type":"track","message_id":"old-new","name":"purchase","timestamp":"2026-11-01T10:00:00Z"}`, 200)
	if fresh["duplicates"] != float64(0) {
		t.Fatalf("new historical event treated as duplicate: %v", fresh)
	}
	duckLakeJourneyDrain(t, s)
	var total int
	if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts WHERE project_id=?`, projectID).Scan(&total); err != nil || total != 1 {
		t.Fatal("new event was not projected exactly once", total, err)
	}
}

func TestReceiptRetentionZeroKeepsReceiptsAndSkipsLookups(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	s.receiptRetention = 0
	_, writer := project(t, s)
	nativeRequest(t, s, writer, `{"type":"track","message_id":"kept","name":"purchase","timestamp":"2020-01-01T00:00:00Z"}`, 200)
	duckLakeJourneyDrain(t, s)
	if err := s.pruneReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts`).Scan(&n); err != nil || n != 1 {
		t.Fatal("retention 0 pruned a receipt", n, err)
	}
	known, err := s.eventRules.coldKnown(context.Background(), "any", []Event{{UUID: "u", Timestamp: "2020-01-01T00:00:00Z"}})
	if err != nil || known != nil {
		t.Fatal("retention 0 looked up DuckLake", known, err)
	}
}

func TestColdKnownLooksUpOnlyEventsOlderThanTheWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 30, 0, 0, 0, 0, time.UTC)
	var asked []string
	rules := &eventRules{clock: func() time.Time { return now }, receiptRetention: 30 * 24 * time.Hour,
		knownEvents: func(_ context.Context, _ string, uuids []string) (map[string]int64, error) {
			asked = append(asked, uuids...)
			return map[string]int64{}, nil
		}}
	events := []Event{
		{UUID: "recent", Timestamp: now.Add(-time.Hour).Format(time.RFC3339Nano)},
		{UUID: "inside-window", Timestamp: now.Add(-20 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "within-margin", Timestamp: now.Add(-25 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "old", Timestamp: now.Add(-40 * 24 * time.Hour).Format(time.RFC3339Nano)},
		{UUID: "unparsed", Timestamp: "not a time"},
	}
	if _, err := rules.coldKnown(context.Background(), "project", events); err != nil {
		t.Fatal(err)
	}
	// The 7-day margin checks events older than retention minus the margin.
	if len(asked) != 3 || asked[0] != "within-margin" || asked[1] != "old" || asked[2] != "unparsed" {
		t.Fatalf("looked up %v", asked)
	}
}
