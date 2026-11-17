package porcupine

import (
	"context"
	"strings"
	"testing"
)

// Unedited events store no original snapshot; the first correction stores the
// values it replaces, and later reads return them.
func TestOriginalSnapshotIsStoredOnlyOnceEdited(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "snap"), 200)
	duckLakeJourneyDrain(t, s)
	ctx := context.Background()
	stored, err := s.lake.Load(ctx, projectID, "snap")
	if err != nil {
		t.Fatal(err)
	}
	if stored.OriginalSnapshot != "" || stored.Event.Event != "purchase" {
		t.Fatalf("unedited event stored a snapshot: %+v", stored)
	}
	_, manager := key(t, s, []string{"events:read", "events:edit"}, []string{projectID})
	request(t, s, "PATCH", "/api/v1/projects/"+projectID+"/events/snap", manager,
		`{"expected_revision":"0","idempotency_key":"snap-edit","snapshot":{"event":"refund","distinct_id":"user-1","timestamp":"2026-11-13T00:00:00Z","properties":{"amount":1}}}`, 202)
	duckLakeJourneyDrain(t, s)
	edited, err := s.lake.Load(ctx, projectID, "snap")
	if err != nil {
		t.Fatal(err)
	}
	if edited.Event.Event != "refund" || edited.Revision != 1 || !strings.Contains(edited.OriginalSnapshot, `"event":"purchase"`) {
		t.Fatalf("first correction did not keep the original: %+v", edited)
	}
}
