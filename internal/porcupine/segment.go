package porcupine

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
)

const segmentEventLimit = 32 << 10
const segmentBatchLimit = 500 << 10

// Segment's HTTP Tracking API wire format is defined at
// https://www.twilio.com/docs/segment/connections/sources/catalog/libraries/server/http-api.
func (s *Service) captureSegment(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		problem(w, 415, "Segment requests require application/json")
		return
	}
	raw, ok := readBody(w, r, true)
	if !ok {
		return
	}
	limit := segmentEventLimit
	if r.URL.Path == "/v1/batch" {
		limit = segmentBatchLimit
	}
	if len(raw) > limit {
		problem(w, 413, "Segment request exceeds its size limit")
		return
	}
	envelope, err := decodeSegmentObject(raw)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	writeKey, err := segmentString(envelope, "writeKey", false)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	if len(r.Header.Values("Authorization")) > 1 {
		problem(w, 401, "supply one authorization header")
		return
	}
	if authorization := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(authorization), "basic ") {
		key, password, valid := r.BasicAuth()
		if !valid || key == "" || password != "" || strings.TrimSpace(key) != key {
			problem(w, 401, "invalid Segment Basic credential")
			return
		}
		if writeKey != "" && writeKey != key {
			problem(w, 400, "Segment credentials must match")
			return
		}
		writeKey = key
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		r.Header.Del("Authorization")
	}
	messages, err := translateSegment(envelope, r.URL.Path, writeKey)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	s.acceptMessages(w, r, writeKey, messages, "segment")
}

// Token decoding preserves large JSON numbers and rejects duplicate selectors
// before their earlier value can be overwritten by encoding/json.
func decodeSegmentObject(raw []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := segmentJSONValue(decoder, 0)
	if err != nil {
		return nil, errors.New("invalid Segment JSON or duplicate object member")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return nil, errors.New("unexpected data after Segment JSON")
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("Segment request must be an object")
	}
	return object, nil
}
func segmentJSONValue(decoder *json.Decoder, depth int) (any, error) {
	if depth > 64 {
		return nil, errors.New("JSON nesting limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := map[string]any{}
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			name, ok := key.(string)
			if !ok {
				return nil, errors.New("object key")
			}
			if _, exists := object[name]; exists {
				return nil, errors.New("duplicate object member")
			}
			value, err := segmentJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			object[name] = value
		}
		_, err = decoder.Token()
		return object, err
	case '[':
		values := []any{}
		for decoder.More() {
			value, err := segmentJSONValue(decoder, depth+1)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		_, err = decoder.Token()
		return values, err
	default:
		return nil, errors.New("unexpected delimiter")
	}
}
func segmentString(object map[string]any, key string, nullable bool) (string, error) {
	value, exists := object[key]
	if !exists || nullable && value == nil {
		return "", nil
	}
	text, ok := value.(string)
	if !ok {
		return "", errors.New("Segment " + key + " must be a string")
	}
	return text, nil
}
func segmentObject(object map[string]any, key string) (map[string]any, error) {
	value, exists := object[key]
	if !exists || value == nil {
		return nil, nil
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("Segment " + key + " must be an object")
	}
	return result, nil
}
func segmentMerge(base, override map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range override {
		left, leftOK := merged[key].(map[string]any)
		right, rightOK := value.(map[string]any)
		if leftOK && rightOK {
			merged[key] = segmentMerge(left, right)
		} else {
			merged[key] = value
		}
	}
	return merged
}
func segmentNoSelectors(object map[string]any) error {
	for _, key := range []string{"source_id", "sourceId", "project_id", "projectId", "api_key", "token"} {
		if _, exists := object[key]; exists {
			return errors.New("Segment project credentials belong to the request")
		}
	}
	return nil
}
func translateSegment(envelope map[string]any, path, writeKey string) ([]nativeMessage, error) {
	if err := segmentNoSelectors(envelope); err != nil {
		return nil, err
	}
	kind := strings.TrimPrefix(path, "/v1/")
	switch kind {
	case "track", "page", "identify", "batch":
	default:
		return nil, errors.New("unsupported Segment endpoint")
	}
	batch := []any{envelope}
	var sharedContext map[string]any
	if kind == "batch" {
		var ok bool
		batch, ok = envelope["batch"].([]any)
		if !ok || len(batch) == 0 || len(batch) > maxCaptureEvents {
			return nil, errors.New("Segment batch must contain 1 to 1000 messages")
		}
		var err error
		if sharedContext, err = segmentObject(envelope, "context"); err != nil {
			return nil, err
		}
		if _, err = segmentObject(envelope, "integrations"); err != nil {
			return nil, err
		}
	} else if _, exists := envelope["batch"]; exists {
		return nil, errors.New("batch belongs to /v1/batch")
	}
	messages := make([]nativeMessage, 0, len(batch))
	expandedBytes := 0
	for _, raw := range batch {
		row, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("Segment batch entries must be objects")
		}
		if err := segmentNoSelectors(row); err != nil {
			return nil, err
		}
		if kind == "batch" {
			key, err := segmentString(row, "writeKey", false)
			if err != nil {
				return nil, err
			}
			if key != "" && (writeKey == "" || key != writeKey) {
				return nil, errors.New("Segment batch credentials must match request credentials")
			}
			encoded, err := json.Marshal(row)
			if err != nil || len(encoded) > segmentEventLimit {
				return nil, errors.New("Segment batch message exceeds 32 KiB")
			}
		}
		message, err := segmentNativeMessage(row, kind, sharedContext)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(message)
		if err != nil || len(encoded) > segmentEventLimit {
			return nil, errors.New("expanded Segment message exceeds 32 KiB")
		}
		expandedBytes += len(encoded)
		if expandedBytes > segmentBatchLimit {
			return nil, errors.New("expanded Segment batch exceeds 500 KiB")
		}
		messages = append(messages, message)
	}
	return messages, nil
}
func segmentNativeMessage(row map[string]any, endpoint string, sharedContext map[string]any) (nativeMessage, error) {
	message := nativeMessage{}
	kind, err := segmentString(row, "type", true)
	if err != nil {
		return message, err
	}
	if endpoint != "batch" {
		if kind != "" && kind != endpoint {
			return message, errors.New("Segment type does not match endpoint")
		}
		kind = endpoint
	}
	if kind != "track" && kind != "page" && kind != "identify" {
		return message, errors.New("unsupported Segment message type")
	}
	user, err := segmentString(row, "userId", true)
	if err != nil {
		return message, err
	}
	anonymous, err := segmentString(row, "anonymousId", true)
	if err != nil {
		return message, err
	}
	if user == "" && anonymous == "" || user != "" && strings.TrimSpace(user) == "" || anonymous != "" && strings.TrimSpace(anonymous) == "" || len(user) > 1000 || len(anonymous) > 1000 {
		return message, errors.New("Segment userId or anonymousId is required and must fit limits")
	}
	message.MessageID, err = segmentString(row, "messageId", true)
	if err != nil {
		return message, err
	}
	if len(message.MessageID) > 100 {
		return message, errors.New("Segment messageId exceeds 100 characters")
	}
	if message.MessageID == "" {
		message.MessageID = newID()
	}
	message.Timestamp, err = segmentString(row, "timestamp", true)
	if err != nil {
		return message, err
	}
	properties, err := segmentObject(row, "properties")
	if err != nil {
		return message, err
	}
	context, err := segmentObject(row, "context")
	if err != nil {
		return message, err
	}
	message.Context = segmentMerge(sharedContext, context)
	if _, err := segmentObject(row, "integrations"); err != nil {
		return message, err
	}
	switch kind {
	case "identify":
		traits, err := segmentObject(row, "traits")
		if err != nil {
			return message, err
		}
		message.Set = segmentMerge(nil, traits)
		if user != "" {
			message.Type = "identify"
			message.UserID = user
			message.AnonymousID = anonymous
		} else {
			message.Type = "set"
			message.ActorID = anonymous
		}
	case "track", "page":
		message.Properties = properties
		message.ActorID = user
		if user == "" {
			message.ActorID = anonymous
		}
		if kind == "track" {
			message.Type = "track"
			message.Name, err = segmentString(row, "event", true)
			if err != nil {
				return message, err
			}
			if strings.TrimSpace(message.Name) == "" {
				return message, errors.New("Segment track requires event")
			}
		} else {
			message.Type = "pageview"
			existing, err := segmentObject(message.Context, "page")
			if err != nil {
				return message, err
			}
			page := segmentMerge(nil, existing)
			for _, field := range []string{"url", "referrer", "title", "path", "search"} {
				if value, ok := properties[field].(string); ok {
					page[field] = value
				}
			}
			for _, field := range []string{"name", "category"} {
				value, err := segmentString(row, field, true)
				if err != nil {
					return message, err
				}
				if value != "" {
					page[field] = value
				}
			}
			if len(page) > 0 {
				message.Context["page"] = page
			}
		}
	}
	return message, nil
}
