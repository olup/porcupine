package porcupine

import (
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type storedIngestionMetadata struct {
	SourceID  string          `json:"source_id"`
	MessageID string          `json:"message_id"`
	EventType string          `json:"event_type"`
	Context   json.RawMessage `json:"context"`
}

func prepareIngestionMetadata(event *eventapi.Event, input *ingestfmt.Metadata, configuredSource string) error {
	if input.Protocol != "native" && input.Protocol != "segment" && !(input.Protocol == "posthog" && configuredSource != "" && configuredSource != "posthog") {
		return errors.New("invalid ingestion protocol")
	}
	messageID := input.MessageID
	if messageID == "" {
		messageID = eventapi.NewID()
	}
	if strings.TrimSpace(messageID) == "" || len(messageID) > 1024 {
		return errors.New("message_id must contain 1 to 1024 bytes")
	}
	if input.EventType != "track" && input.EventType != "pageview" && input.EventType != "identify" {
		return errors.New("invalid native event type")
	}
	source := configuredSource
	if source == "" {
		source = input.Protocol
	}
	if !validSourceID(source) {
		return errors.New("invalid configured ingestion source")
	}
	raw := input.Context
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var object map[string]any
	if len(raw) > 64<<10 || ingestfmt.DecodeJSON(raw, &object) != nil || object == nil {
		return errors.New("context must be an object of at most 64 KiB")
	}
	canonical, err := json.Marshal(object)
	if err != nil || len(canonical) > 64<<10 {
		return errors.New("invalid context or context exceeds 64 KiB")
	}
	event.SourceID, event.MessageID, event.EventType, event.Context = source, messageID, input.EventType, canonical
	digest := sha512.New()
	for _, value := range []string{event.ProjectID, source, messageID} {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		digest.Write(size[:])
		digest.Write([]byte(value))
	}
	// Every legacy PostHog UUID is at most 128 bytes. This namespace is longer,
	// so it remains disjoint even when an old producer chose a matching prefix.
	event.UUID = lakeapi.SourceEventUUIDPrefix + hex.EncodeToString(digest.Sum(nil))
	return nil
}

func encodeIngestionMetadata(event eventapi.Event) string {
	if event.SourceID == "" {
		return ""
	}
	raw, _ := json.Marshal(storedIngestionMetadata{event.SourceID, event.MessageID, event.EventType, event.Context})
	return string(raw)
}

func decodeIngestionMetadata(event *eventapi.Event, raw string) error {
	if raw == "" {
		return nil
	}
	var value storedIngestionMetadata
	if json.Unmarshal([]byte(raw), &value) != nil {
		return errors.New("invalid stored ingestion metadata")
	}
	event.SourceID, event.MessageID, event.EventType, event.Context = value.SourceID, value.MessageID, value.EventType, value.Context
	return nil
}
