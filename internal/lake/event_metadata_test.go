package lake

import (
	"encoding/json"
	"github.com/olup/porcupine/internal/event"
	"reflect"
	"strings"
	"testing"
)

func TestLegacyPostHogMetadataCanonicalContext(t *testing.T) {
	t.Parallel()
	original := event.Event{UUID: "sdk-message", Event: "$pageview", Properties: metadataProperties(t, map[string]any{
		"$current_url": "https://example.test/p?q=東京&n=1", "$referrer": "https://referrer.test/", "$title": "café 東京", "$pathname": "/p", "$search": "?q=東京&n=1",
		"$browser": "Chrome", "$browser_version": 132, "$os": "Mac OS X", "$os_version": "15.1", "$device_type": "Desktop", "$browser_language": "fr-FR", "$session_id": "session-a", "secret-property": "not-a-context-field",
	})}
	got := EventWithMetadata(original)
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
	frozen := EventWithMetadata(got)
	if string(frozen.Context) != string(got.Context) || frozen.EventType != "pageview" {
		t.Fatal("accepted provenance re-inferred after correction")
	}
	for name, typ := range map[string]string{"$screen": "track", "$identify": "identify", "$set": "identify", "business": "track"} {
		if actual := EventWithMetadata(event.Event{UUID: "id", Event: name}).EventType; actual != typ {
			t.Errorf("%s type %s", name, actual)
		}
	}
	explicit := event.Event{UUID: "physical", SourceID: "server", MessageID: "producer", EventType: "track", Context: json.RawMessage(`{"additional":{"kept":true}}`)}
	if got := EventWithMetadata(explicit); !reflect.DeepEqual(got, explicit) {
		t.Fatal("explicit native/Segment context or provenance changed")
	}
}

func TestLegacyMetadataCookielessPrivacy(t *testing.T) {
	t.Parallel()
	event := EventWithMetadata(event.Event{UUID: "id", Properties: json.RawMessage(`{"cookieless":true,"$session_id":"must-not-reappear","$ip":"192.0.2.1","$anon_distinct_id":"private","$current_url":"https://example.test/"}`)})
	if strings.Contains(string(event.Context), "must-not") || strings.Contains(string(event.Context), "192.0.2.1") || strings.Contains(string(event.Context), "private") {
		t.Fatal("known identifiers reintroduced through legacy context")
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
