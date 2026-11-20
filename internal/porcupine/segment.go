package porcupine

import (
	"mime"
	"net/http"
	"strings"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

// Segment's HTTP Tracking API wire format is defined at
// https://www.twilio.com/docs/segment/connections/sources/catalog/libraries/server/http-api.
func (s *Service) captureSegment(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	base64Form := media == "application/x-www-form-urlencoded" && r.URL.Query().Get("compression") == "base64"
	if err != nil || media != "application/json" && !base64Form {
		problem(w, 415, "Segment requests require application/json")
		return
	}
	raw, ok := readBody(w, r)
	if !ok {
		return
	}
	limit := ingestfmt.SegmentEventLimit
	if r.URL.Path == "/v1/batch" {
		limit = ingestfmt.SegmentBatchLimit
	}
	if len(raw) > limit {
		problem(w, 413, "Segment request exceeds its size limit")
		return
	}
	envelope, err := ingestfmt.DecodeObject(raw)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	writeKey, err := ingestfmt.SegmentString(envelope, "writeKey", false)
	if err != nil {
		ingestionProblem(w, "invalid_credential", err.Error())
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
			ingestionProblem(w, "invalid_credential", "Segment credentials must match")
			return
		}
		writeKey = key
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		r.Header.Del("Authorization")
	}
	messages, err := ingestfmt.TranslateSegment(envelope, r.URL.Path, writeKey, true)
	if err != nil {
		ingestionProblem(w, "invalid_event", err.Error())
		return
	}
	s.acceptMessages(w, r, writeKey, messages, "segment")
}
