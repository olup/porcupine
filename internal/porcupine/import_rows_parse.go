package porcupine

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

var errImportRow = errors.New("invalid PostHog import row")

// PostHog S3 JSONLines uses object properties and ISO8601 UTC timestamps.
// JSON-encoded object strings and UTC ClickHouse timestamp text are accepted
// for older exports; no producer can supply the destination project or source.
func parsePostHogImportRow(raw []byte, kind string) (ImportRow, error) {
	var row ImportRow
	row.Kind = kind
	if len(raw) == 0 || len(raw) > 1<<20 {
		return row, errImportRow
	}
	var input map[string]json.RawMessage
	if _, err := decodeSegmentObject(raw); err != nil {
		return row, errImportRow
	}
	if json.Unmarshal(raw, &input) != nil || input == nil {
		return row, errImportRow
	}
	object := func(key string) (json.RawMessage, error) { return importProperties(input[key]) }
	text := func(key string, limit int) (string, error) {
		var value string
		if json.Unmarshal(input[key], &value) != nil || strings.TrimSpace(value) == "" || len(value) > limit {
			return "", errImportRow
		}
		return value, nil
	}
	if kind == "events" {
		var err error
		e := &row.Event
		if e.UUID, err = text("uuid", 128); err != nil {
			return row, err
		}
		if e.Event, err = text("event", 500); err != nil {
			return row, err
		}
		if e.DistinctID, err = text("distinct_id", 1000); err != nil {
			return row, err
		}
		if e.DistinctID == cookielessSentinel {
			return row, errImportRow
		} // A request sentinel cannot reconstruct an old daily ID.
		if e.Properties, err = object("properties"); err != nil {
			return row, err
		}
		var timestamp string
		if json.Unmarshal(input["timestamp"], &timestamp) != nil {
			return row, errImportRow
		}
		parsed, err := parseImportTimestamp(timestamp)
		if err != nil {
			return row, err
		}
		e.Timestamp = parsed.UTC().Format(time.RFC3339Nano)
		// Autocapture's DOM chain is a separate field in the actual S3 events model.
		if chain, ok := input["elements_chain"]; ok && string(chain) != "null" {
			var value string
			if json.Unmarshal(chain, &value) != nil {
				return row, errImportRow
			}
			if value != "" {
				var properties map[string]json.RawMessage
				json.Unmarshal(e.Properties, &properties)
				if _, exists := properties["$elements_chain"]; !exists {
					properties["$elements_chain"] = chain
					e.Properties, _ = json.Marshal(properties)
				}
			}
		}
		var properties map[string]json.RawMessage
		json.Unmarshal(e.Properties, &properties)
		for _, key := range []string{"token", "api_key", "$ip", "ip", "ip_address", "client_ip", "remote_addr", "x-forwarded-for"} {
			delete(properties, key)
		}
		if bytes.Equal(properties["cookieless"], []byte("true")) || strings.HasPrefix(e.DistinctID, "cookieless_") {
			// Keep an archived resolved daily ID; never derive one from an importer's IP.
			for _, key := range []string{"$device_id", "$user_id", "$anon_distinct_id", "$session_id", "$window_id"} {
				delete(properties, key)
			}
		}
		e.Properties, _ = json.Marshal(properties)
		return row, nil
	}
	if kind != "persons" {
		return row, errImportRow
	}
	p := &row.Person
	var err error
	if value, exists := input["is_deleted"]; exists && json.Unmarshal(value, &p.Deleted) != nil {
		return row, errImportRow
	}
	if p.InternalID, err = text("person_id", 1000); err != nil {
		return row, err
	}
	distinct, err := text("distinct_id", 1000)
	if err != nil {
		return row, err
	}
	p.IdentifiedID = distinct
	p.DistinctIDs = []string{distinct}
	if p.Deleted && (len(input["properties"]) == 0 || string(input["properties"]) == "null") {
		p.Properties = json.RawMessage(`{}`)
	} else if p.Properties, err = object("properties"); err != nil {
		return row, err
	}
	var objectProperties map[string]any
	decoder := json.NewDecoder(bytes.NewReader(p.Properties))
	decoder.UseNumber()
	decoder.Decode(&objectProperties)
	if _, valid := identityTraits(objectProperties); !valid {
		return row, errImportRow
	}
	if p.PersonVersion, err = importVersion(input["person_version"]); err != nil {
		return row, err
	}
	if p.MappingVersion, err = importVersion(input["person_distinct_id_version"]); err != nil {
		return row, err
	}
	if rawTime, exists := input["created_at"]; exists && string(rawTime) != "null" {
		var timestamp string
		if json.Unmarshal(rawTime, &timestamp) != nil {
			return row, errImportRow
		}
		p.Timestamp, err = parseImportTimestamp(timestamp)
		if err != nil {
			return row, err
		}
	}
	// This identifier is private and disjoint from every accepted live PostHog
	// UUID. It deduplicates an exact person-map snapshot without inventing links.
	canonical, _ := json.Marshal(p)
	digest := sha512.Sum512(canonical)
	row.Event.UUID = "porcupine-import-person-v1:" + hex.EncodeToString(digest[:])
	row.Event.Event, row.Event.DistinctID, row.Event.Properties = "$set", p.IdentifiedID, json.RawMessage(`{}`)
	return row, nil
}
func importProperties(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	if len(raw) > 0 && raw[0] == '"' {
		var value string
		if json.Unmarshal(raw, &value) != nil {
			return nil, errImportRow
		}
		raw = json.RawMessage(value)
	}
	if _, err := decodeSegmentObject(raw); err != nil {
		return nil, errImportRow
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, errImportRow
	}
	for _, value := range object {
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.UseNumber()
		var anyValue any
		if decoder.Decode(&anyValue) != nil {
			return nil, errImportRow
		}
		if decoder.Decode(&anyValue) != io.EOF {
			return nil, errImportRow
		}
	}
	encoded, err := json.Marshal(object)
	if err != nil {
		return nil, errImportRow
	}
	return encoded, nil
}
func importVersion(raw json.RawMessage) (uint64, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	var value string
	if raw[0] == '"' {
		if json.Unmarshal(raw, &value) != nil {
			return 0, errImportRow
		}
	} else {
		value = string(raw)
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, errImportRow
	}
	return parsed, nil
}
func parseImportTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999+00:00", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}
	return time.Time{}, errImportRow
}
