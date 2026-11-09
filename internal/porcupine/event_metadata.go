package porcupine

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
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
func eventWithMetadata(event Event) Event {
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
func legacyContextSQL(properties string) string {
	values := map[string]string{}
	for group, fields := range postHogContextFields {
		nested := map[string]string{}
		for _, field := range fields {
			nested[field.name] = contextValueSQL(properties, field)
		}
		raw := contextObjectSQL(nested)
		values[group] = "if(" + raw + "='{}',''," + raw + ")"
	}
	for _, field := range postHogContextScalars {
		value := contextValueSQL(properties, field)
		if field.session {
			value = "if(JSONExtractBool(" + properties + ",'cookieless'),''," + value + ")"
		}
		values[field.name] = value
	}
	return contextObjectSQL(values)
}
func contextValueSQL(properties string, field contextField) string {
	value := "''"
	for i := len(field.keys) - 1; i >= 0; i-- {
		key := field.keys[i]
		raw := "JSONExtractRaw(" + properties + ",'" + key + "')"
		text := "JSONExtractString(" + properties + ",'" + key + "')"
		kind := "JSONType(" + properties + ",'" + key + "')"
		candidate := "if(" + kind + "='String',if(" + text + "='',''," + raw + "),'' )"
		if field.number {
			candidate = "if(" + kind + " IN ('Int64','UInt64','Float64'),toJSONString(" + raw + ")," + candidate + ")"
		}
		value = "if(" + candidate + "!=''," + candidate + "," + value + ")"
	}
	return value
}

// Values are already encoded JSON, with ” denoting an absent key.
func contextObjectSQL(values map[string]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	fields := make([]string, 0, len(keys))
	for _, key := range keys {
		value := values[key]
		fields = append(fields, "if("+value+"='','',concat('\""+key+"\":',"+value+"))")
	}
	return "concat('{',arrayStringConcat(arrayFilter(value -> value!='',[" + strings.Join(fields, ",") + "]),','),'}')"
}

const eventMetadataColumns = "source_id,message_id,event_type,context"
const eventMetadataFallbackSQL = "if(source_id='','posthog',source_id) AS source_id,if(message_id='',uuid,message_id) AS message_id,if(event_type='',multiIf(event='$pageview','pageview',event IN ('$identify','$set'),'identify','track'),event_type) AS event_type,"

func validEventContext(raw json.RawMessage) bool {
	var object map[string]json.RawMessage
	return json.Unmarshal(raw, &object) == nil && object != nil
}
