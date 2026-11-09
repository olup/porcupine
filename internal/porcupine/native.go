package porcupine

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// nativeMessage is shared by the native HTTP API and the Segment adapter.
// Source and project are derived from the authenticated request.
type nativeMessage struct {
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
}

func (s *Service) captureNative(w http.ResponseWriter, r *http.Request) {
	raw, ok := readBody(w, r, true)
	if !ok {
		return
	}
	keys := r.Header.Values("X-Porcupine-Key")
	if len(keys) > 1 {
		problem(w, 400, "supply at most one project key")
		return
	}
	messages, err := decodeNativeMessages(raw)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	s.acceptMessages(w, r, r.Header.Get("X-Porcupine-Key"), messages, "native")
}

func decodeNativeMessages(raw []byte) ([]nativeMessage, error) {
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
	var messages []nativeMessage
	for !batch || decoder.More() {
		if len(messages) >= maxCaptureEvents {
			return nil, errors.New("batch exceeds 1000 messages")
		}
		var message nativeMessage
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

func (s *Service) acceptMessages(w http.ResponseWriter, r *http.Request, writeKey string, messages []nativeMessage, protocol string) {
	if len(messages) == 0 || len(messages) > maxCaptureEvents {
		problem(w, 400, "batch must contain 1 to 1000 messages")
		return
	}
	batch := make(eventBatch, 0, len(messages))
	for _, message := range messages {
		input, err := message.eventInput(protocol)
		if err != nil {
			problem(w, 400, err.Error())
			return
		}
		batch = append(batch, input)
	}
	s.captureNormalized(w, r, eventInput{APIKey: writeKey}, batch)
}

func (m nativeMessage) eventInput(protocol string) (eventInput, error) {
	if m.MessageID != "" && strings.TrimSpace(m.MessageID) == "" {
		return eventInput{}, errors.New("message_id cannot contain only whitespace")
	}
	input := eventInput{DistinctID: m.ActorID, Properties: m.Properties, Timestamp: m.Timestamp}
	switch m.Type {
	case "track", "pageview":
		if m.UserID != "" || m.AnonymousID != "" || m.Set != nil || m.SetOnce != nil {
			return eventInput{}, errors.New("identity fields belong to identify messages")
		}
		input.Event = "$pageview"
		if m.Type == "track" {
			if strings.TrimSpace(m.Name) == "" || strings.HasPrefix(m.Name, "$") {
				return eventInput{}, errors.New("track requires a non-reserved event name")
			}
			input.Event = m.Name
		} else if m.Name != "" {
			return eventInput{}, errors.New("pageview names belong in context.page.name")
		}
	case "identify", "set":
		if m.Name != "" || m.Properties != nil {
			return eventInput{}, errors.New("use set or set_once for person traits")
		}
		input.Properties = map[string]any{}
		if m.Type == "identify" {
			if m.ActorID != "" || strings.TrimSpace(m.UserID) == "" {
				return eventInput{}, errors.New("identify requires user_id")
			}
			input.Event, input.DistinctID = "$identify", m.UserID
			if m.AnonymousID != "" {
				input.Properties["$anon_distinct_id"] = m.AnonymousID
			}
		} else {
			// Segment allows a traits update before a user ID is known.
			if protocol != "segment" || m.UserID != "" || m.AnonymousID != "" || strings.TrimSpace(m.ActorID) == "" {
				return eventInput{}, errors.New("unsupported identity update")
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
		return eventInput{}, errors.New("unsupported message type")
	}
	var contextJSON json.RawMessage
	if m.Context != nil {
		var err error
		contextJSON, err = json.Marshal(m.Context)
		if err != nil {
			return eventInput{}, errors.New("invalid event context")
		}
	}
	eventType := m.Type
	if isIdentityControl(input.Event) {
		eventType = "identify"
	}
	if m.MessageID == "" {
		m.MessageID = newID()
	}
	input.ingestion = &ingestionMetadata{Protocol: protocol, MessageID: m.MessageID, EventType: eventType, Context: contextJSON}
	return input, nil
}
