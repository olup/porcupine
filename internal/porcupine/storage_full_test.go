package porcupine

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// Exercise SQLite's real SQLITE_FULL path without consuming the operator's disk.
func TestCaptureRecoversFromSQLiteFull(t *testing.T) {
	s := service(t, 100)
	_, key := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(key, "acknowledged"), 200)
	var pages, oldLimit int
	if err := s.db.QueryRow("PRAGMA page_count").Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("PRAGMA max_page_count").Scan(&oldLimit); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages)); err != nil {
		t.Fatal(err)
	}
	batch := func(payload string) string {
		input := map[string]any{"api_key": key, "batch": []map[string]any{
			{"event": "small", "uuid": "small", "distinct_id": "user"},
			{"event": "large", "uuid": "large", "distinct_id": "user", "properties": map[string]any{"body": payload}},
		}}
		raw, err := json.Marshal(input)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	body := batch(strings.Repeat("x", 128<<10))
	request(t, s, "POST", "/batch/", "", body, 503)
	var count, charged int
	if err := s.db.QueryRow("SELECT (SELECT count(*) FROM events),pending_count FROM queue_usage").Scan(&count, &charged); err != nil || count != 1 || charged != 1 {
		t.Fatalf("partial acceptance after SQLITE_FULL: %d events %d charged: %v", count, charged, err)
	}
	if _, err := s.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", oldLimit)); err != nil {
		t.Fatal(err)
	}
	if result := request(t, s, "POST", "/batch/", "", body, 200); result["accepted"] != float64(2) {
		t.Fatal("storage recovery did not accept both events")
	}
	if result := request(t, s, "POST", "/capture/", "", captureBody(key, "acknowledged"), 200); result["duplicates"] != float64(1) {
		t.Fatal("lost previously acknowledged event")
	}
	if result := request(t, s, "POST", "/batch/", "", body, 200); result["duplicates"] != float64(2) {
		t.Fatal("retry not deduplicated")
	}
}
