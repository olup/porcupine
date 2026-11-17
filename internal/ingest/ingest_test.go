package ingest

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestLiveAdaptersNormalizeSameRecord(t *testing.T) {
	t.Parallel()
	native, err := DecodeNative([]byte(`{"type":"track","message_id":"source-1","name":"order_completed","actor_id":"account","timestamp":"2024-01-02T03:04:05.123456789Z","properties":{"token":"business-value","large":9007199254740993},"context":{"page":{"url":"https://example.test"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	object, err := DecodeObject([]byte(`{"type":"track","messageId":"source-1","event":"order_completed","userId":"account","timestamp":"2024-01-02T03:04:05.123456789Z","properties":{"token":"business-value","large":9007199254740993},"context":{"page":{"url":"https://example.test"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	segment, err := TranslateSegment(object, "/v1/track", "")
	if err != nil {
		t.Fatal(err)
	}
	a, err := native[0].Normalize("native")
	if err != nil {
		t.Fatal(err)
	}
	b, err := segment[0].Normalize("segment")
	if err != nil {
		t.Fatal(err)
	}
	if a.Metadata.Protocol != "native" || b.Metadata.Protocol != "segment" {
		t.Fatal("input provenance missing")
	}
	b.Metadata.Protocol = "native"
	if !reflect.DeepEqual(a, b) || a.Properties["large"] != json.Number("9007199254740993") {
		t.Fatalf("different normalized inputs: %#v %#v", a, b)
	}
}

func TestSegmentStrictObjectsAndNativeSelectors(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{`{"messageId":"a","messageId":"b"}`, `{"context":{"device":{"id":"a","id":"b"}}}`, `{"a":1} {}`, `[]`, strings.Repeat(`{"a":`, 66) + `1` + strings.Repeat(`}`, 66)} {
		if _, err := DecodeObject([]byte(raw)); err == nil {
			t.Fatalf("ambiguous JSON accepted: %s", raw)
		}
	}
	for _, raw := range []string{`{"type":"track","name":"valid","project_id":"forged"}`, `{"type":"pageview","source_id":"forged"}`, `[]`, `{"type":"track"} {}`} {
		if _, err := DecodeNative([]byte(raw)); err == nil {
			t.Fatalf("invalid native message accepted: %s", raw)
		}
	}
}

func TestNormalizeIdentityAndGeneratedMessageIDs(t *testing.T) {
	t.Parallel()
	identity, err := (Message{Type: "identify", MessageID: "identify-1", UserID: "person", AnonymousID: "browser", Set: map[string]any{"plan": "pro"}}).Normalize("native")
	if err != nil || identity.Event != "$identify" || identity.Metadata.EventType != "identify" || identity.Properties["$anon_distinct_id"] != "browser" {
		t.Fatal(identity, err)
	}
	a, err := (Message{Type: "track", Name: "backend"}).Normalize("native")
	if err != nil {
		t.Fatal(err)
	}
	b, err := (Message{Type: "track", Name: "backend"}).Normalize("native")
	if err != nil {
		t.Fatal(err)
	}
	if a.Metadata.MessageID == "" || a.Metadata.MessageID == b.Metadata.MessageID || a.DistinctID != "" {
		t.Fatal("invented an actor or reused a generated ID")
	}
	if _, err := (Message{Type: "track", Name: "$identify"}).Normalize("native"); err == nil {
		t.Fatal("reserved track accepted")
	}
	if _, err := (Message{Type: "set", ActorID: "browser"}).Normalize("native"); err == nil {
		t.Fatal("Segment-only identity update accepted")
	}
}

func TestCookielessContextScrubKeepsBusinessNumbers(t *testing.T) {
	t.Parallel()
	got := ScrubContext(json.RawMessage(`{"ip":"192.0.2.1","items":[{"userId":"private","large":9007199254740993}],"device":{"id":"private","type":"mobile"},"page":{"title":"kept"}}`))
	for _, secret := range []string{"private", "192.0.2.1"} {
		if strings.Contains(string(got), secret) {
			t.Fatalf("retained %q in %s", secret, got)
		}
	}
	if !strings.Contains(string(got), "9007199254740993") || !strings.Contains(string(got), "mobile") || !strings.Contains(string(got), "kept") {
		t.Fatal(string(got))
	}
}
