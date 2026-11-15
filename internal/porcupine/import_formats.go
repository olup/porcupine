package porcupine

import (
	"encoding/json"
	"strings"
	"time"
)

// All file adapters return historical records. None calls live capture or
// applies identity traits directly; acceptance owns historical side effects.
func parseHistoricalImportRow(raw []byte, kind, format string) (ImportRow, error) {
	if format == "" || format == "posthog" {
		return parsePostHogImportRow(raw, kind)
	}
	if kind != "events" || len(raw) == 0 || len(raw) > importMaxRowBytes {
		return ImportRow{}, errImportRow
	}
	object, err := decodeSegmentObject(raw)
	if err != nil {
		return ImportRow{}, errImportRow
	}
	var message nativeMessage
	switch format {
	case "native":
		messages, err := decodeNativeMessages(raw)
		if err != nil || len(messages) != 1 {
			return ImportRow{}, errImportRow
		}
		message = messages[0]
	case "segment":
		messageID, _ := object["messageId"].(string)
		timestamp, _ := object["timestamp"].(string)
		kind, _ := object["type"].(string)
		if strings.TrimSpace(messageID) == "" || timestamp == "" || kind != "track" && kind != "page" && kind != "identify" {
			return ImportRow{}, errImportRow
		}
		if _, exists := object["writeKey"]; exists {
			return ImportRow{}, errImportRow
		}
		messages, err := translateSegment(object, "/v1/"+kind, "")
		if err != nil || len(messages) != 1 {
			return ImportRow{}, errImportRow
		}
		message = messages[0]
	default:
		return ImportRow{}, errImportRow
	}
	if strings.TrimSpace(message.MessageID) == "" || message.Timestamp == "" {
		return ImportRow{}, errImportRow
	}
	input, err := message.eventInput(format)
	if err != nil {
		return ImportRow{}, errImportRow
	}
	timestamp, err := time.Parse(time.RFC3339Nano, message.Timestamp)
	if err != nil || strings.TrimSpace(input.Event) == "" || len(input.Event) > 500 || len(input.DistinctID) > 1000 || input.DistinctID == cookielessSentinel {
		return ImportRow{}, errImportRow
	}
	properties, err := json.Marshal(input.Properties)
	if err != nil {
		return ImportRow{}, errImportRow
	}
	if input.Properties == nil {
		properties = json.RawMessage(`{}`)
	}
	event := Event{UUID: message.MessageID, Event: input.Event, DistinctID: input.DistinctID, Timestamp: timestamp.UTC().Format(time.RFC3339Nano), Properties: properties}
	if strings.HasPrefix(event.DistinctID, "cookieless_") {
		input.ingestion.Context = scrubIngestionContext(input.ingestion.Context)
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
	return ImportRow{Kind: "events", Event: event, ingestion: input.ingestion}, nil
}
