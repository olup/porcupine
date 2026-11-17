package porcupine

import (
	"context"
	"encoding/json"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func metadataProperties(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMetadataNativeSQLColumnsAndProjectionRoundTrip(t *testing.T) {
	t.Parallel()
	s, _ := nativeCoreService(t)
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", `{"api_key":"`+ingest+`","uuid":"producer","event":"$pageview","distinct_id":"anonymous","timestamp":"2026-01-01T00:00:00Z","properties":{"$current_url":"https://example.test/"}}`, 200)
	duckLakeJourneyDrain(t, s)
	rows := duckLakeJourneyQuery(t, s, projectID, adminToken, "SELECT id,event_id,source_id,message_id,event_type,json_extract_string(context,'$.page.url') AS url FROM events", nil)
	if len(rows) != 1 {
		t.Fatal("missing native metadata projection")
	}
	row := rows[0].(map[string]any)
	if row["id"] != row["event_id"] || row["source_id"] != "posthog" || row["message_id"] != "producer" || row["event_type"] != "pageview" || row["url"] != "https://example.test/" {
		t.Fatal("metadata columns changed", row)
	}
	event, err := s.lake.Load(context.Background(), projectID, "producer")
	if err != nil {
		t.Fatal(err)
	}
	event.Revision = 1
	event.IsDeleted = true
	if err := s.lake.Apply(context.Background(), []lakeapi.EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	deleted, err := s.lake.Load(context.Background(), projectID, "producer")
	if err != nil || !deleted.IsDeleted || deleted.SourceID != "" || deleted.MessageID != "" || deleted.EventType != "" || len(deleted.Context) != 0 {
		t.Fatal("tombstone retained producer metadata", deleted, err)
	}
}
