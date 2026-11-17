package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

// Metadata is supplied only by trusted in-process adapters. Public
// PostHog JSON cannot construct Input.Metadata or select its namespace.
type Metadata struct {
	MessageID string
	EventType string
	Context   json.RawMessage
	Protocol  string
}

// Cookieless context retains presentation metadata, but removes known identity
// and address keys recursively. Arbitrary business values are not a PII scrub.
func ScrubContext(raw json.RawMessage) json.RawMessage {
	var value any
	if DecodeJSON(raw, &value) != nil {
		return json.RawMessage(`{}`)
	}
	var scrub func(any)
	scrub = func(value any) {
		switch node := value.(type) {
		case map[string]any:
			for key, child := range node {
				switch strings.ToLower(key) {
				case "ip", "$ip", "ip_address", "ipaddress", "client_ip", "clientip", "remote_addr", "remoteaddr", "x-forwarded-for", "session", "user", "anonymous", "window", "session_id", "sessionid", "$session_id", "$sessionid", "anonymous_id", "anonymousid", "$anon_distinct_id", "user_id", "userid", "$user_id", "device_id", "deviceid", "$device_id", "window_id", "$window_id":
					delete(node, key)
				default:
					if strings.EqualFold(key, "device") {
						if device, ok := child.(map[string]any); ok {
							delete(device, "id")
							delete(device, "Id")
							delete(device, "ID")
						}
					}
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

func DecodeJSON(raw []byte, value any) error {
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
