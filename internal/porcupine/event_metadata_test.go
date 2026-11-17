package porcupine

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func metadataProperties(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func TestLegacyPostHogMetadataCanonicalContext(t *testing.T) {
	t.Parallel()
	original := Event{UUID: "sdk-message", Event: "$pageview", Properties: metadataProperties(t, map[string]any{
		"$current_url": "https://example.test/p?q=東京&n=1", "$referrer": "https://referrer.test/", "$title": "café 東京", "$pathname": "/p", "$search": "?q=東京&n=1",
		"$browser": "Chrome", "$browser_version": 132, "$os": "Mac OS X", "$os_version": "15.1", "$device_type": "Desktop", "$browser_language": "fr-FR", "$session_id": "session-a", "secret-property": "not-a-context-field",
	})}
	got := eventWithMetadata(original)
	if got.SourceID != "posthog" || got.MessageID != original.UUID || got.EventType != "pageview" {
		t.Fatal("legacy provenance defaults differ")
	}
	var object map[string]any
	if json.Unmarshal(got.Context, &object) != nil {
		t.Fatal("invalid canonical context")
	}
	expected := map[string]any{"page": map[string]any{"url": "https://example.test/p?q=東京&n=1", "referrer": "https://referrer.test/", "title": "café 東京", "path": "/p", "search": "?q=東京&n=1"}, "browser": map[string]any{"name": "Chrome", "version": "132"}, "os": map[string]any{"name": "Mac OS X", "version": "15.1"}, "device": map[string]any{"type": "Desktop"}, "locale": "fr-FR", "session_id": "session-a"}
	if !reflect.DeepEqual(object, expected) {
		t.Fatalf("canonical context mismatch: %s", got.Context)
	}
	if string(got.Properties) != string(original.Properties) {
		t.Fatal("original PostHog properties changed")
	}
	got.Properties = json.RawMessage(`{"$current_url":"https://edited.test"}`)
	got.Event = "renamed"
	frozen := eventWithMetadata(got)
	if string(frozen.Context) != string(got.Context) || frozen.EventType != "pageview" {
		t.Fatal("accepted provenance re-inferred after correction")
	}
	for name, typ := range map[string]string{"$screen": "track", "$identify": "identify", "$set": "identify", "business": "track"} {
		if actual := eventWithMetadata(Event{UUID: "id", Event: name}).EventType; actual != typ {
			t.Errorf("%s type %s", name, actual)
		}
	}
	explicit := Event{UUID: "physical", SourceID: "server", MessageID: "producer", EventType: "track", Context: json.RawMessage(`{"additional":{"kept":true}}`)}
	if got := eventWithMetadata(explicit); !reflect.DeepEqual(got, explicit) {
		t.Fatal("explicit native/Segment context or provenance changed")
	}
}
func TestLegacyMetadataCookielessPrivacy(t *testing.T) {
	t.Parallel()
	event := eventWithMetadata(Event{UUID: "id", Properties: json.RawMessage(`{"cookieless":true,"$session_id":"must-not-reappear","$ip":"192.0.2.1","$anon_distinct_id":"private","$current_url":"https://example.test/"}`)})
	if strings.Contains(string(event.Context), "must-not") || strings.Contains(string(event.Context), "192.0.2.1") || strings.Contains(string(event.Context), "private") {
		t.Fatal("known identifiers reintroduced through legacy context")
	}
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
	if err := s.lake.Apply(context.Background(), []eventProjection{event}); err != nil {
		t.Fatal(err)
	}
	deleted, err := s.lake.Load(context.Background(), projectID, "producer")
	if err != nil || !deleted.IsDeleted || deleted.SourceID != "" || deleted.MessageID != "" || deleted.EventType != "" || len(deleted.Context) != 0 {
		t.Fatal("tombstone retained producer metadata", deleted, err)
	}
}

func TestStoredContextRequiresJSONObject(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"null", "[]", "false", "1", `"text"`, "{"} {
		if validEventContext(json.RawMessage(raw)) {
			t.Errorf("invalid canonical context %s", raw)
		}
	}
	if !validEventContext(json.RawMessage(`{"additional":true}`)) {
		t.Fatal("valid context rejected")
	}
}
