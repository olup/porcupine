package lake

import (
	"bytes"
	"encoding/json"
)

type contextField struct {
	name    string
	keys    []string
	number  bool
	session bool
}

var postHogContextFields = map[string][]contextField{
	"browser": {{name: "name", keys: []string{"$browser"}}, {name: "version", keys: []string{"$browser_version"}, number: true}},
	"device":  {{name: "type", keys: []string{"$device_type"}}},
	"os":      {{name: "name", keys: []string{"$os"}}, {name: "version", keys: []string{"$os_version"}, number: true}},
	"page":    {{name: "path", keys: []string{"$pathname", "$path"}}, {name: "referrer", keys: []string{"$referrer"}}, {name: "search", keys: []string{"$search"}}, {name: "title", keys: []string{"$title"}}, {name: "url", keys: []string{"$current_url"}}},
}
var postHogContextScalars = []contextField{{name: "locale", keys: []string{"$browser_language", "$locale", "locale"}}, {name: "session_id", keys: []string{"$session_id"}, session: true}}

// Missing metadata marks a legacy PostHog input. Explicit source metadata is
// immutable input provenance and is never inferred again after a correction.
func EventWithMetadata(event Event) Event {
	if event.SourceID == "" {
		event.SourceID = "posthog"
	}
	if event.MessageID == "" {
		event.MessageID = event.UUID
	}
	if event.EventType == "" {
		switch event.Event {
		case "$pageview":
			event.EventType = "pageview"
		case "$identify", "$set":
			event.EventType = "identify"
		default:
			event.EventType = "track"
		}
	}
	if len(event.Context) == 0 {
		var properties map[string]json.RawMessage
		_ = json.Unmarshal(event.Properties, &properties)
		context := map[string]any{}
		for group, fields := range postHogContextFields {
			values := map[string]string{}
			for _, field := range fields {
				if value := postHogContextValue(properties, field); value != "" {
					values[field.name] = value
				}
			}
			if len(values) > 0 {
				context[group] = values
			}
		}
		for _, field := range postHogContextScalars {
			if field.session && string(properties["cookieless"]) == "true" {
				continue
			}
			if value := postHogContextValue(properties, field); value != "" {
				context[field.name] = value
			}
		}
		event.Context, _ = json.Marshal(context)
	}
	return event
}
func postHogContextValue(properties map[string]json.RawMessage, field contextField) string {
	for _, key := range field.keys {
		raw := properties[key]
		var text string
		if json.Unmarshal(raw, &text) == nil && text != "" {
			return text
		}
		if field.number && len(raw) > 0 {
			var number json.Number
			decoder := json.NewDecoder(bytes.NewReader(raw))
			decoder.UseNumber()
			if decoder.Decode(&number) == nil && number != "" {
				return string(number)
			}
		}
	}
	return ""
}

// The same field specification drives old physical-row reads and new Go
// projections, so adding context does not require rewriting historical parts.
func validEventContext(raw json.RawMessage) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && object != nil
}
