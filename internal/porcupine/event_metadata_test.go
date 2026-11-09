package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
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
	event := eventWithMetadata(Event{UUID: "id", Properties: json.RawMessage(`{"cookieless":true,"$session_id":"must-not-reappear","$ip":"192.0.2.1","$anon_distinct_id":"private","$current_url":"https://example.test/"}`)})
	if strings.Contains(string(event.Context), "must-not") || strings.Contains(string(event.Context), "192.0.2.1") || strings.Contains(string(event.Context), "private") {
		t.Fatal("known identifiers reintroduced through legacy context")
	}
}
func TestMetadataSQLColumnsAndProjectionRoundTrip(t *testing.T) {
	if _, err := validateSQL("SELECT id,event_id,source_id,message_id,event_type,JSONExtractString(context,'page','url') FROM events"); err != nil {
		t.Fatal(err)
	}
	received := []clickHouseEvent{}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") == "" {
			t.Error("unexpected projection request")
		}
		decoder := json.NewDecoder(r.Body)
		for decoder.More() {
			var row clickHouseEvent
			if err := decoder.Decode(&row); err != nil {
				t.Error(err)
				return
			}
			received = append(received, row)
		}
	})
	event := Event{ID: 1, ProjectID: "p", UUID: "producer", Event: "$pageview", DistinctID: "anonymous", Properties: json.RawMessage(`{"$current_url":"https://example.test/"}`), Timestamp: "2026-01-01T00:00:00Z", ReceivedAt: "2026-01-01T00:00:00Z"}
	original, err := originalSnapshot(event)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.insertProjections(context.Background(), []eventProjection{{Event: event, OriginalSnapshot: original}}); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || received[0].SourceID != "posthog" || received[0].MessageID != "producer" || received[0].EventType != "pageview" || !strings.Contains(received[0].Context, "example.test") {
		t.Fatal("physical metadata projection missing")
	}
	event.SourceID, event.MessageID, event.EventType, event.Context = "native-source", "native-message", "track", json.RawMessage(`{"session_id":"private"}`)
	if err = c.insertProjections(context.Background(), []eventProjection{{Event: event, Revision: 1, IsDeleted: true}}); err != nil {
		t.Fatal(err)
	}
	deleted := received[1]
	if deleted.SourceID != "" || deleted.MessageID != "" || deleted.EventType != "" || deleted.Context != "" {
		t.Fatal("tombstone retains producer metadata")
	}
}

func TestStoredContextRequiresJSONObject(t *testing.T) {
	for _, raw := range []string{"null", "[]", "false", "1", `"text"`, "{"} {
		if validEventContext(json.RawMessage(raw)) {
			t.Errorf("invalid canonical context %s", raw)
		}
	}
	if !validEventContext(json.RawMessage(`{"additional":true}`)) {
		t.Fatal("valid context rejected")
	}
}
