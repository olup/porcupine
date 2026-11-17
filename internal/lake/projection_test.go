package lake

import (
	"encoding/json"
	"strings"
	"testing"
)

func projectionFixture(t *testing.T) EventProjection {
	t.Helper()
	event := Event{ID: 42, ProjectID: "project-a", UUID: "event-a", Event: "purchase", DistinctID: "person-a", Properties: json.RawMessage(`{"html":"<original>","price":5,"large":9007199254740993}`), Timestamp: "2026-01-01T12:00:00.123456789Z", ReceivedAt: "2026-11-07T21:00:00.123Z"}
	snapshot, err := OriginalSnapshot(event)
	if err != nil {
		t.Fatal(err)
	}
	return EventProjection{Event: event, OriginalSnapshot: snapshot}
}

func TestOriginalProjectionSnapshotPreservesOccurrenceAndBusinessJSON(t *testing.T) {
	t.Parallel()
	projection := projectionFixture(t)
	if strings.Contains(projection.OriginalSnapshot, `\\u003c`) || !strings.Contains(projection.OriginalSnapshot, `<original>`) || !strings.Contains(projection.OriginalSnapshot, `9007199254740993`) {
		t.Fatal("original snapshot changed HTML or large numeric JSON", projection.OriginalSnapshot)
	}
	var original originalEventSnapshot
	if err := json.Unmarshal([]byte(projection.OriginalSnapshot), &original); err != nil {
		t.Fatal(err)
	}
	if original.Event != projection.Event.Event || original.DistinctID != projection.DistinctID || original.Timestamp != projection.Timestamp {
		t.Fatal("original occurrence fields changed")
	}
	for _, private := range []string{"received_at", "project_id", "identity_snapshot", "revision"} {
		if strings.Contains(projection.OriginalSnapshot, private) {
			t.Fatal("snapshot mixed projection metadata with original data", private)
		}
	}
}

func TestOriginalProjectionSnapshotRejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	event := projectionFixture(t).Event
	event.Properties = json.RawMessage(`{"broken"`)
	if _, err := OriginalSnapshot(event); err == nil {
		t.Fatal("invalid JSON entered an immutable original snapshot")
	}
}
