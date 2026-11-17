package lake

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

var ErrEventNotFound = errors.New("event not found")

type EventProjection struct {
	Event
	Revision         uint64
	IsDeleted        bool
	OriginalSnapshot string
}

type originalEventSnapshot struct {
	Event      string          `json:"event"`
	DistinctID string          `json:"distinct_id"`
	Properties json.RawMessage `json:"properties"`
	Timestamp  string          `json:"timestamp"`
}

func OriginalSnapshot(event Event) (string, error) {
	var body bytes.Buffer
	encoder := json.NewEncoder(&body)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(originalEventSnapshot{event.Event, event.DistinctID, event.Properties, event.Timestamp}); err != nil {
		return "", errors.New("cannot encode original event")
	}
	return strings.TrimSuffix(body.String(), "\n"), nil
}
