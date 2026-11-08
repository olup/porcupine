package porcupine

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

// Keep concurrent decoding and SQLite waits bounded before caller authentication.
// Capture and credential exchange have separate pools so a slow event upload
// never consumes a slot needed for a service to renew its token.
func limitConcurrent(slots chan struct{}, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			next(w, r)
		default:
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Pragma", "no-cache")
			problem(w, http.StatusServiceUnavailable, "temporarily_unavailable")
		}
	}
}

const maxCaptureEvents = 1000

type eventBatch []eventInput

// A short JSON body can contain hundreds of thousands of empty objects.
// Decode incrementally so the event-count limit also bounds slice allocation.
func (b *eventBatch) UnmarshalJSON(raw []byte) error {
	if bytes.Equal(raw, []byte("null")) {
		*b = nil
		return nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('[') {
		return errors.New("expected event array")
	}
	events := make(eventBatch, 0)
	for d.More() {
		if len(events) == maxCaptureEvents {
			return errors.New("too many events")
		}
		var e eventInput
		if err := d.Decode(&e); err != nil {
			return err
		}
		events = append(events, e)
	}
	if _, err := d.Token(); err != nil {
		return err
	}
	*b = events
	return nil
}
