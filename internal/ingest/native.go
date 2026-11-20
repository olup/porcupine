package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/olup/porcupine/internal/event"
)

const MaxMessages = 1000
const CookielessSentinel = "$posthog_cookieless"

// Message is shared by the native HTTP API and the Segment adapter.
// Source and project are derived from the authenticated request.
type Message struct {
	Type        string         `json:"type"`
	MessageID   string         `json:"message_id,omitempty"`
	Timestamp   string         `json:"timestamp,omitempty"`
	ActorID     string         `json:"actor_id,omitempty"`
	Name        string         `json:"name,omitempty"`
	AnonymousID string         `json:"anonymous_id,omitempty"`
	UserID      string         `json:"user_id,omitempty"`
	Properties  map[string]any `json:"properties,omitempty"`
	Context     map[string]any `json:"context,omitempty"`
	Set         map[string]any `json:"set,omitempty"`
	SetOnce     map[string]any `json:"set_once,omitempty"`

	ClientTimestamp string `json:"-"`
	SentAt          string `json:"-"`
}

// Input is a protocol-neutral producer record. It contains no project or source selector.
type Input struct {
	Event, DistinctID, Timestamp string
	Properties                   map[string]any
	Metadata                     *Metadata
}

func DecodeNative(raw []byte) ([]Message, error) {
	raw = bytes.TrimSpace(raw)
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	batch := len(raw) > 0 && raw[0] == '['
	if batch {
		if _, err := decoder.Token(); err != nil {
			return nil, errors.New("invalid native batch")
		}
	}
	var messages []Message
	for !batch || decoder.More() {
		if len(messages) >= MaxMessages {
			return nil, errors.New("batch exceeds 1000 messages")
		}
		var message Message
		if err := decoder.Decode(&message); err != nil {
			return nil, errors.New("invalid native message or unknown field")
		}
		messages = append(messages, message)
		if !batch {
			break
		}
	}
	if batch {
		if _, err := decoder.Token(); err != nil || len(messages) == 0 {
			return nil, errors.New("expected a nonempty native batch")
		}
	}
	if decoder.Decode(new(json.RawMessage)) != io.EOF {
		return nil, errors.New("unexpected data after native message")
	}
	return messages, nil
}

func (m Message) Normalize(protocol string) (Input, error) {
	if m.MessageID != "" && strings.TrimSpace(m.MessageID) == "" {
		return Input{}, errors.New("message_id cannot contain only whitespace")
	}
	input := Input{DistinctID: m.ActorID, Properties: m.Properties, Timestamp: m.Timestamp}
	switch m.Type {
	case "track", "pageview":
		if m.UserID != "" || m.AnonymousID != "" || m.Set != nil || m.SetOnce != nil {
			return Input{}, errors.New("identity fields belong to identify messages")
		}
		input.Event = "$pageview"
		if m.Type == "track" {
			if strings.TrimSpace(m.Name) == "" || strings.HasPrefix(m.Name, "$") {
				return Input{}, errors.New("track requires a non-reserved event name")
			}
			input.Event = m.Name
		} else if m.Name != "" {
			return Input{}, errors.New("pageview names belong in context.page.name")
		}
	case "identify", "set":
		if m.Name != "" || m.Properties != nil {
			return Input{}, errors.New("use set or set_once for person traits")
		}
		input.Properties = map[string]any{}
		if m.Type == "identify" {
			if m.ActorID != "" || strings.TrimSpace(m.UserID) == "" {
				return Input{}, errors.New("identify requires user_id")
			}
			input.Event, input.DistinctID = "$identify", m.UserID
			if m.AnonymousID != "" {
				input.Properties["$anon_distinct_id"] = m.AnonymousID
			}
		} else {
			// Segment allows a traits update before a user ID is known.
			if protocol != "segment" || m.UserID != "" || m.AnonymousID != "" || strings.TrimSpace(m.ActorID) == "" {
				return Input{}, errors.New("unsupported identity update")
			}
			input.Event = "$set"
		}
		if m.Set != nil {
			input.Properties["$set"] = m.Set
		}
		if m.SetOnce != nil {
			input.Properties["$set_once"] = m.SetOnce
		}
	default:
		return Input{}, errors.New("unsupported message type")
	}
	var contextJSON json.RawMessage
	if m.Context != nil {
		var err error
		contextJSON, err = json.Marshal(m.Context)
		if err != nil {
			return Input{}, errors.New("invalid event context")
		}
	}
	eventType := m.Type
	if input.Event == "$identify" || input.Event == "$set" {
		eventType = "identify"
	}
	if m.MessageID == "" {
		m.MessageID = event.NewID()
	}
	input.Metadata = &Metadata{Protocol: protocol, MessageID: m.MessageID, EventType: eventType, Context: contextJSON}
	return input, nil
}
