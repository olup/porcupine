package porcupine

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// ingestionMetadata is supplied only by trusted in-process adapters. Public
// PostHog JSON cannot construct eventInput.ingestion or select its namespace.
type ingestionMetadata struct {
	MessageID string
	EventType string
	Context   json.RawMessage
	Protocol  string
}

type storedIngestionMetadata struct {
	SourceID  string          `json:"source_id"`
	MessageID string          `json:"message_id"`
	EventType string          `json:"event_type"`
	Context   json.RawMessage `json:"context"`
}

func (s *Service) initializeIngestionMetadata(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, column := range []struct{ table, name, definition string }{
		{"events", "ingestion_metadata", `TEXT NOT NULL DEFAULT ''`},
		{"event_receipts", "event_id", `INTEGER NOT NULL DEFAULT 0`},
		{"event_heads", "event_id", `INTEGER NOT NULL DEFAULT 0`},
	} {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name=?)`, column.table, column.name).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err := tx.ExecContext(ctx, `ALTER TABLE `+column.table+` ADD COLUMN `+column.name+` `+column.definition); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func prepareIngestionMetadata(event *Event, input *ingestionMetadata, configuredSource string) error {
	if input.Protocol != "native" && input.Protocol != "segment" && !(input.Protocol == "posthog" && configuredSource != "" && configuredSource != "posthog") {
		return errors.New("invalid ingestion protocol")
	}
	messageID := input.MessageID
	if messageID == "" {
		messageID = newID()
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
	if len(raw) > 64<<10 || decodeIngestionJSON(raw, &object) != nil || object == nil {
		return errors.New("context must be an object of at most 64 KiB")
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return errors.New("invalid context")
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
	event.UUID = sourceEventUUIDPrefix + hex.EncodeToString(digest.Sum(nil))
	return nil
}

func encodeIngestionMetadata(event Event) string {
	if event.SourceID == "" {
		return ""
	}
	raw, _ := json.Marshal(storedIngestionMetadata{event.SourceID, event.MessageID, event.EventType, event.Context})
	return string(raw)
}

func decodeIngestionMetadata(event *Event, raw string) error {
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

// Cookieless context retains presentation metadata, but removes known identity
// and address keys recursively. Arbitrary business values are not a PII scrub.
func scrubIngestionContext(raw json.RawMessage) json.RawMessage {
	var value any
	if decodeIngestionJSON(raw, &value) != nil {
		return json.RawMessage(`{}`)
	}
	var scrub func(any)
	scrub = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			for key, child := range node {
				switch strings.ToLower(key) {
				case "ip", "$ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for", "session_id", "sessionid", "$session_id", "anonymous_id", "anonymousid", "$anon_distinct_id", "user_id", "userid", "$user_id", "device_id", "deviceid", "$device_id", "window_id", "$window_id":
					delete(node, key)
				default:
					scrub(child)
				}
			}
		case []any:
			for _, child := range node {
				scrub(child)
			}
		}
	}
	scrub(value)
	result, _ := json.Marshal(value)
	return result
}

func decodeIngestionJSON(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("unexpected data after JSON value")
	}
	return nil
}

type ingestionReceipt struct {
	MessageID string `json:"message_id"`
	EventID   string `json:"event_id"`
	UUID      string `json:"uuid"`
	Duplicate bool   `json:"duplicate"`
}
