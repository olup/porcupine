package importer

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/ingest"
)

// All file adapters return historical records. None calls live capture or
// applies identity traits directly; acceptance owns historical side effects.
func ParseRow(raw []byte, kind, format string) (Row, error) {
	if format == "" || format == "posthog" {
		return ParsePostHog(raw, kind)
	}
	if kind != "events" || len(raw) == 0 || len(raw) > MaxRowBytes {
		return Row{}, ErrRow
	}
	object, err := ingest.DecodeObject(raw)
	if err != nil {
		return Row{}, ErrRow
	}
	var message ingest.Message
	switch format {
	case "native":
		messages, err := ingest.DecodeNative(raw)
		if err != nil || len(messages) != 1 {
			return Row{}, ErrRow
		}
		message = messages[0]
	case "segment":
		messageID, _ := object["messageId"].(string)
		timestamp, _ := object["timestamp"].(string)
		kind, _ := object["type"].(string)
		if strings.TrimSpace(messageID) == "" || timestamp == "" || kind != "track" && kind != "page" && kind != "identify" {
			return Row{}, ErrRow
		}
		if _, exists := object["writeKey"]; exists {
			return Row{}, ErrRow
		}
		messages, err := ingest.TranslateSegment(object, "/v1/"+kind, "", false)
		if err != nil || len(messages) != 1 {
			return Row{}, ErrRow
		}
		message = messages[0]
	default:
		return Row{}, ErrRow
	}
	if strings.TrimSpace(message.MessageID) == "" || message.Timestamp == "" {
		return Row{}, ErrRow
	}
	input, err := message.Normalize(format)
	if err != nil {
		return Row{}, ErrRow
	}
	timestamp, err := time.Parse(time.RFC3339Nano, message.Timestamp)
	if err != nil || strings.TrimSpace(input.Event) == "" || len(input.Event) > 500 || len(input.DistinctID) > 1000 || input.DistinctID == ingest.CookielessSentinel {
		return Row{}, ErrRow
	}
	properties, err := json.Marshal(input.Properties)
	if err != nil {
		return Row{}, ErrRow
	}
	if input.Properties == nil {
		properties = json.RawMessage(`{}`)
	}
	event := event.Event{UUID: message.MessageID, Event: input.Event, DistinctID: input.DistinctID, Timestamp: timestamp.UTC().Format(time.RFC3339Nano), Properties: properties}
	if strings.HasPrefix(event.DistinctID, "cookieless_") {
		input.Metadata.Context = ingest.ScrubContext(input.Metadata.Context)
		// Match the historical PostHog scrub without interpreting native business
		// properties as credentials or changing an archived daily actor.
		var props map[string]json.RawMessage
		json.Unmarshal(properties, &props)
		for _, key := range []string{"$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id", "$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for"} {
			delete(props, key)
		}
		event.Properties, _ = json.Marshal(props)
	}
	// A person snapshot is not represented by a native or Segment identify.
	// Historical acceptance can consume explicit link evidence, but never replay
	// set/set_once as current trait assignments.
	return Row{Kind: "events", Event: event, Ingestion: input.Metadata}, nil
}
