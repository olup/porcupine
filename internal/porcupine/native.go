package porcupine

import (
	"net/http"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

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
	messages, err := ingestfmt.DecodeNative(raw)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	s.acceptMessages(w, r, r.Header.Get("X-Porcupine-Key"), messages, "native")
}

func (s *Service) acceptMessages(w http.ResponseWriter, r *http.Request, writeKey string, messages []ingestfmt.Message, protocol string) {
	if len(messages) == 0 || len(messages) > maxCaptureEvents {
		problem(w, 400, "batch must contain 1 to 1000 messages")
		return
	}
	batch := make(eventBatch, 0, len(messages))
	for _, message := range messages {
		normalized, err := message.Normalize(protocol)
		if err != nil {
			problem(w, 400, err.Error())
			return
		}
		batch = append(batch, eventInput{Event: normalized.Event, DistinctID: normalized.DistinctID, Properties: normalized.Properties, Timestamp: normalized.Timestamp, ingestion: normalized.Metadata})
	}
	s.captureNormalized(w, r, eventInput{APIKey: writeKey}, batch)
}
