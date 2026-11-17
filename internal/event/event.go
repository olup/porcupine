// Package event defines the canonical accepted event shared by ingestion and storage.
package event

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
)

func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Event struct {
	ID               int64           `json:"id"`
	ProjectID        string          `json:"project_id"`
	UUID             string          `json:"uuid"`
	Event            string          `json:"event"`
	DistinctID       string          `json:"distinct_id"`
	Properties       json.RawMessage `json:"properties"`
	Timestamp        string          `json:"timestamp"`
	ReceivedAt       string          `json:"received_at"`
	Delivered        bool            `json:"delivered"`
	Imported         bool            `json:"imported,omitempty"`
	IdentitySnapshot string          `json:"-"`
	SourceID         string          `json:"source_id,omitempty"`
	MessageID        string          `json:"message_id,omitempty"`
	EventType        string          `json:"event_type,omitempty"`
	Context          json.RawMessage `json:"context,omitempty"`
}
