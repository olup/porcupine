package porcupine

import (
	"context"
	"encoding/json"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDefaultReceiptRetentionAcceptsDuringLakeOutage(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	if s.receiptRetention != 0 {
		t.Fatal("default must retain local receipts")
	}
	projectID, writer := project(t, s)
	first := nativeRequest(t, s, writer, `[
 {"type":"track","message_id":"old","name":"original","timestamp":"2020-01-01T00:00:00Z"},
 {"type":"track","message_id":"recent","name":"original"},
 {"type":"identify","message_id":"identity","user_id":"known","anonymous_id":"browser","set":{"plan":"original"}}
]`, 200)
	ids := []any{receiptFrom(t, first, 0)["event_id"], receiptFrom(t, first, 1)["event_id"], receiptFrom(t, first, 2)["event_id"]}
	duckLakeJourneyDrain(t, s)
	if _, err := s.db.Exec(`UPDATE event_receipts SET recorded_at_ms=0`); err != nil {
		t.Fatal(err)
	}
	if err := s.pruneReceipts(context.Background()); err != nil || receiptCount(t, s) != 3 {
		t.Fatal("default pruned durable retry IDs", err)
	}
	store := s.lake.(*lakeapi.DuckLakeStore)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	retry := nativeRequest(t, s, writer, `[
 {"type":"track","message_id":"old","name":"changed"},
 {"type":"track","message_id":"recent","name":"changed","timestamp":"1999-01-01T00:00:00Z"},
 {"type":"identify","message_id":"identity","user_id":"attacker","anonymous_id":"browser","set":{"plan":"changed"}}
]`, 200)
	if retry["duplicates"] != float64(3) || retry["accepted"] != float64(0) {
		t.Fatal("outage changed retry classification", retry)
	}
	for i, id := range ids {
		if receiptFrom(t, retry, i)["event_id"] != id {
			t.Fatal("outage changed original receipt ID", retry)
		}
	}
	var identified, traits string
	if err := s.db.QueryRow(`SELECT identified_id FROM identity_links WHERE project_id=? AND anonymous_id='browser'`, projectID).Scan(&identified); err != nil || identified != "known" {
		t.Fatal("retry repeated identity side effects", identified, err)
	}
	if err := s.db.QueryRow(`SELECT traits FROM identity_profiles WHERE project_id=? AND identified_id='known'`, projectID).Scan(&traits); err != nil {
		t.Fatal(err)
	}
	var profile map[string]any
	if err := json.Unmarshal([]byte(traits), &profile); err != nil || profile["plan"] != "original" {
		t.Fatal("retry changed traits", profile, err)
	}
	fresh := nativeRequest(t, s, writer, `[
 {"type":"track","message_id":"new-current","name":"new"},
 {"type":"track","message_id":"new-history","name":"new","timestamp":"1980-01-01T00:00:00Z"}
]`, 200)
	if fresh["accepted"] != float64(2) || fresh["duplicates"] != float64(0) {
		t.Fatal("outage rejected new events", fresh)
	}
	var pending, usage int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=?`, projectID).Scan(&pending); err != nil || pending != 2 {
		t.Fatal("fresh events were not durably queued", pending, err)
	}
	if err := s.db.QueryRow(`SELECT sum(accepted_events) FROM tenant_usage`).Scan(&usage); err != nil || usage != 5 {
		t.Fatal("duplicate retry consumed ingestion budget", usage, err)
	}
}
