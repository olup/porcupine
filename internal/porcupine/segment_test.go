package porcupine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func segmentHeaders(key string) map[string]string {
	headers := map[string]string{"Content-Type": "application/json"}
	if key != "" {
		headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(key+":"))
	}
	return headers
}
func segmentEvent(t *testing.T, s *Service, project, messageID string) Event {
	t.Helper()
	var event Event
	var metadata, properties string
	if err := s.db.QueryRow(`SELECT id,uuid,event,distinct_id,properties,timestamp,ingestion_metadata FROM events WHERE project_id=? AND json_extract(ingestion_metadata,'$.message_id')=?`, project, messageID).Scan(&event.ID, &event.UUID, &event.Event, &event.DistinctID, &properties, &event.Timestamp, &metadata); err != nil {
		t.Fatal(err)
	}
	event.Properties = json.RawMessage(properties)
	if err := decodeIngestionMetadata(&event, metadata); err != nil {
		t.Fatal(err)
	}
	return event
}
func TestSegmentTrackAndPagePreserveBusinessValues(t *testing.T) {
	s := service(t, 20)
	projectID, ingest := project(t, s)
	track := []byte(`{"type":"track","messageId":"node-evt-1","userId":"account-7","anonymousId":"device-4","event":"Course Completed","timestamp":"2025-03-04T07:06:07.123+02:00","properties":{"token":"business-token","writeKey":"business-key","source_id":"custom-source","context":"custom-context","nested":{"unicode":"東京 🦔","items":[1,true,null]},"huge":9007199254740993},"context":{"library":{"name":"@segment/analytics-node","version":"3.1.0"},"locale":"fr-FR","browser":{"name":"Firefox","version":"132"},"custom":{"huge":9007199254740993}}}`)
	transportRequest(t, s, "POST", "/v1/track", track, segmentHeaders(ingest), 200)
	event := segmentEvent(t, s, projectID, "node-evt-1")
	if event.Event != "Course Completed" || event.DistinctID != "account-7" || event.Timestamp != "2025-03-04T05:06:07.123Z" || event.SourceID != "segment" || event.EventType != "track" {
		t.Fatalf("wrong translated event: %+v", event)
	}
	for _, part := range []string{`"token":"business-token"`, `"writeKey":"business-key"`, `"source_id":"custom-source"`, `"huge":9007199254740993`} {
		if !bytes.Contains(event.Properties, []byte(part)) {
			t.Fatalf("business value changed: %s", event.Properties)
		}
	}
	if !bytes.Contains(event.Context, []byte(`"huge":9007199254740993`)) {
		t.Fatalf("context number changed: %s", event.Context)
	}
	if bytes.Contains(event.Properties, []byte(ingest)) || bytes.Contains(event.Context, []byte(ingest)) {
		t.Fatal("write credential was persisted")
	}
	page := []byte(`{"messageId":"browser-page-1","anonymousId":"device-4","name":"Pricing","category":"Docs","properties":{"name":"custom-name","category":"custom-category","url":"https://example.test/pricing","referrer":"https://example.test/","title":"Pricing","path":"/pricing","search":"?plan=pro","custom":[null,false,3]},"context":{"page":{"campaign":"spring"},"session_id":"session-1"}}`)
	transportRequest(t, s, "POST", "/v1/page", page, segmentHeaders(ingest), 200)
	saved := segmentEvent(t, s, projectID, "browser-page-1")
	if saved.Event != "$pageview" || saved.DistinctID != "device-4" || saved.EventType != "pageview" {
		t.Fatalf("wrong page: %+v", saved)
	}
	var props, context map[string]any
	json.Unmarshal(saved.Properties, &props)
	json.Unmarshal(saved.Context, &context)
	if props["name"] != "custom-name" || props["category"] != "custom-category" {
		t.Fatal("page mapping overwrote custom properties")
	}
	info := context["page"].(map[string]any)
	if info["name"] != "Pricing" || info["category"] != "Docs" || info["url"] != "https://example.test/pricing" || info["campaign"] != "spring" || context["session_id"] != "session-1" {
		t.Fatalf("page context lost: %s", saved.Context)
	}
}
func TestSegmentAnonymousIdentifyAndAtomicMixedBatch(t *testing.T) {
	s := service(t, 20)
	projectID, ingest := project(t, s)
	body := map[string]any{"writeKey": ingest, "context": map[string]any{"locale": "en", "device": map[string]any{"type": "phone", "manufacturer": "example"}}, "integrations": map[string]any{"All": false, "Vendor": map[string]any{"apiKey": "vendor-secret-sensitive"}}, "batch": []any{
		map[string]any{"type": "identify", "messageId": "anon-traits", "anonymousId": "anon-7", "traits": map[string]any{"campaign": "docs"}},
		map[string]any{"type": "track", "messageId": "before-login", "anonymousId": "anon-7", "event": "Before Login"},
		map[string]any{"type": "identify", "messageId": "login", "userId": "account-7", "anonymousId": "anon-7", "traits": map[string]any{"plan": "pro"}},
		map[string]any{"type": "track", "messageId": "after-login", "userId": "account-7", "event": "After Login", "context": map[string]any{"locale": "fr", "device": map[string]any{"name": "Test Phone"}}, "integrations": map[string]any{"Example": true}},
	}}
	payload, _ := json.Marshal(body)
	result := transportRequest(t, s, "POST", "/v1/batch", payload, segmentHeaders(""), 200)
	var ack map[string]any
	json.Unmarshal(result.Body.Bytes(), &ack)
	if ack["accepted"] != float64(4) || ack["identity_ignored"] != float64(0) {
		t.Fatalf("wrong batch acknowledgement: %v", ack)
	}
	traits, identified, version := storedIdentityProfile(t, s, projectID, "account-7")
	if !identified || traits["plan"] != "pro" || traits["campaign"] != "docs" {
		t.Fatalf("identity mapping changed: %v", traits)
	}
	anonymous, known, _ := storedIdentityProfile(t, s, projectID, "anon-7")
	if known || anonymous["campaign"] != "docs" {
		t.Fatal("anonymous identify incorrectly identified a person")
	}
	var target string
	s.db.QueryRow(`SELECT identified_id FROM identity_links WHERE project_id=? AND anonymous_id=?`, projectID, "anon-7").Scan(&target)
	if target != "account-7" {
		t.Fatal("identify did not link the supplied anonymous ID")
	}
	event := segmentEvent(t, s, projectID, "after-login")
	var context map[string]any
	json.Unmarshal(event.Context, &context)
	device := context["device"].(map[string]any)
	if context["locale"] != "fr" || device["type"] != "phone" || device["name"] != "Test Phone" {
		t.Fatalf("batch context did not merge: %s", event.Context)
	}
	if _, exists := context["segment"]; exists {
		t.Fatal("destination routing metadata was retained")
	}
	var stored []byte
	all, err := s.db.Query(`SELECT properties,ingestion_metadata FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	for all.Next() {
		var props, metadata string
		if err := all.Scan(&props, &metadata); err != nil {
			t.Fatal(err)
		}
		stored = append(stored, []byte(props+metadata)...)
	}
	all.Close()
	if bytes.Contains(stored, []byte("vendor-secret-sensitive")) {
		t.Fatal("destination credentials were retained")
	}
	duplicate := transportRequest(t, s, "POST", "/v1/batch", payload, segmentHeaders(""), 200)
	json.Unmarshal(duplicate.Body.Bytes(), &ack)
	if ack["accepted"] != float64(0) || ack["duplicates"] != float64(4) || eventCount(t, s) != 4 {
		t.Fatal("Segment messageId retry was not stable")
	}
	got, _, afterVersion := storedIdentityProfile(t, s, projectID, "account-7")
	if version != afterVersion || !reflect.DeepEqual(traits, got) {
		t.Fatal("retry applied identity traits twice")
	}
}
func TestSegmentMalformedBatchAndCredentialConflictsAreAtomic(t *testing.T) {
	s := service(t, 100)
	_, a := project(t, s)
	_, b := project(t, s)
	cases := []string{
		`{"batch":[{"type":"track","messageId":"first","anonymousId":"anon","event":"ok"},{"type":"group","userId":"u"}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"ok"},{"type":"identify","traits":{"plan":"pro"}}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"ok"},{"type":"track","userId":7,"event":"bad"}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"ok"},{"type":"track","userId":"u","event":"bad","timestamp":"not-a-time"}]}`,
		`{"batch":[{"type":"identify","userId":"u","traits":{"plan":"pro"}},{"type":"track","userId":"u","event":"bad","properties":[]}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"ok"},{"type":"track","anonymousId":"anon","event":"bad","writeKey":"` + b + `"}]}`,
		`{"writeKey":"` + b + `","batch":[{"type":"track","anonymousId":"anon","event":"bad"}]}`,
		`{"writeKey":"` + a + `","writeKey":"` + b + `","batch":[{"type":"track","anonymousId":"anon","event":"bad"}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"bad","source_id":"forged"}]}`,
		`{"batch":[{"type":"track","anonymousId":"anon","event":"bad","writeKey":null}]}`,
		`{"batch":[]}`, `{"batch":[null]}`, `{"batch":[{"type":"page","anonymousId":"anon","context":[]}]}`,
	}
	for _, body := range cases {
		transportRequest(t, s, "POST", "/v1/batch", []byte(body), segmentHeaders(a), 400)
		if eventCount(t, s) != 0 {
			t.Fatal("malformed Segment batch partially committed")
		}
	}
	var profiles int
	s.db.QueryRow(`SELECT count(*) FROM identity_profiles`).Scan(&profiles)
	if profiles != 0 {
		t.Fatal("invalid batch partially committed traits")
	}
	transportRequest(t, s, "POST", "/v1/batch", []byte(`{"batch":[{"type":"track","anonymousId":"anon","event":"bad","writeKey":"`+a+`"}]}`), segmentHeaders(""), 400)
	for _, auth := range []string{"Basic not-base64", "Basic " + base64.StdEncoding.EncodeToString([]byte(a+":password")), "Bearer invalid"} {
		headers := segmentHeaders("")
		headers["Authorization"] = auth
		transportRequest(t, s, "POST", "/v1/track", []byte(`{"writeKey":"`+a+`","userId":"u","event":"bad"}`), headers, 401)
	}
	r := httptest.NewRequest("POST", "/v1/track", strings.NewReader(`{"userId":"u","event":"bad"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Add("Authorization", segmentHeaders(a)["Authorization"])
	r.Header.Add("Authorization", segmentHeaders(b)["Authorization"])
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("multiple Authorization headers accepted")
	}
	if eventCount(t, s) != 0 {
		t.Fatal("credential rejection stored an event")
	}
}
func TestSegmentBodyWriteKeyGzipLimitsAndGeneratedIDs(t *testing.T) {
	s := service(t, 20)
	projectID, ingest := project(t, s)
	body := []byte(`{"writeKey":"` + ingest + `","userId":"u","event":"HTTP direct","context":{"browser":{"name":"Test"}}}`)
	headers := segmentHeaders("")
	headers["Content-Encoding"] = "gzip"
	for range 2 {
		transportRequest(t, s, "POST", "/v1/track", compressed(t, body), headers, 200)
	}
	rows := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)["events"].([]any)
	if len(rows) != 2 || rows[0].(map[string]any)["message_id"] == rows[1].(map[string]any)["message_id"] {
		t.Fatal("omitted messageId did not generate distinct receipt IDs")
	}
	transportRequest(t, s, "POST", "/v1/track", body, map[string]string{"Content-Type": "text/plain"}, 415)
	transportRequest(t, s, "POST", "/v1/track", []byte(`{"userId":"u","event":"big","properties":{"pad":"`+strings.Repeat("x", segmentEventLimit)+`"}}`), segmentHeaders(ingest), 413)
	transportRequest(t, s, "POST", "/v1/batch", []byte(`{"batch":[{"type":"track","userId":"u","event":"big","properties":{"pad":"`+strings.Repeat("x", segmentEventLimit)+`"}}]}`), segmentHeaders(ingest), 400)
	transportRequest(t, s, "POST", "/v1/batch", bytes.Repeat([]byte(" "), segmentBatchLimit+1), segmentHeaders(ingest), 413)
	if eventCount(t, s) != 2 {
		t.Fatal("size or type rejection changed durable events")
	}
}

func TestSegmentSharedContextAmplificationIsAtomic(t *testing.T) {
	s := service(t, 1000)
	_, ingest := project(t, s)
	batch := make([]any, 64)
	for i := range batch {
		batch[i] = map[string]any{"type": "track", "anonymousId": "anon", "event": "small"}
	}
	body := map[string]any{"writeKey": ingest, "context": map[string]any{"custom": map[string]any{"padding": strings.Repeat("x", 24<<10)}}, "batch": batch}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) >= segmentBatchLimit {
		t.Fatal("test must exercise expanded context, not raw request limit")
	}
	transportRequest(t, s, "POST", "/v1/batch", payload, segmentHeaders(""), 400)
	if eventCount(t, s) != 0 {
		t.Fatal("amplified context partially committed")
	}
}
