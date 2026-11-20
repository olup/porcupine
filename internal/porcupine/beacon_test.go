package porcupine

import (
	"encoding/base64"
	"net/url"
	"strings"
	"testing"

	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

func beaconForm(encoding *base64.Encoding, payload string) []byte {
	return []byte(url.Values{"data": {encoding.EncodeToString([]byte(payload))}}.Encode())
}

func TestPostHogBeaconIngestion(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	_, ingest := project(t, s)
	_, other := project(t, s)
	event := `{"event":"beacon","distinct_id":"abc>>>???","properties":{"token":"` + ingest + `"}}`
	// Align the test payload so its standard encoding has both '+' and '/'.
	payload := `[ ` + event + `]`
	payload += strings.Repeat(" ", (4-len(payload)%3)%3)
	padded := beaconForm(base64.StdEncoding, payload)
	unpadded := beaconForm(base64.RawStdEncoding, payload)
	if !strings.Contains(string(padded), "%2B") || !strings.Contains(string(padded), "%2F") || !strings.HasSuffix(string(padded), "%3D%3D") || strings.Contains(string(unpadded), "%3D") {
		t.Fatalf("test payload does not exercise base64 character escaping and padding")
	}
	batch := `{"api_key":"` + ingest + `","batch":[` + event + `]}`
	headers := map[string]string{"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
	for _, tc := range []struct {
		name, route, compression string
		body                     []byte
		code                     int
		message                  string
	}{
		{"padded array", "/e/", "base64", padded, 200, ""},
		{"unpadded array", "/i/v0/e/", "base64", unpadded, 200, ""},
		{"object", "/capture/", "base64", beaconForm(base64.StdEncoding, `{"token":"`+ingest+`","event":"beacon","distinct_id":"a"}`), 200, ""},
		{"batch envelope", "/batch/", "base64", beaconForm(base64.StdEncoding, batch), 200, ""},
		{"missing data", "/e/", "base64", []byte("other=foo"), 400, "invalid base64 form"},
		{"empty data", "/e/", "base64", []byte("data="), 400, "invalid base64 form"},
		{"malformed form", "/e/", "base64", []byte("data=%z0"), 400, "invalid base64 form"},
		{"duplicate data", "/e/", "base64", []byte(string(padded) + "&d%61ta=YQ%3D%3D"), 400, "duplicate base64 form field"},
		{"duplicate other field", "/e/", "base64", []byte(string(padded) + "&x=1&x=2"), 400, "duplicate base64 form field"},
		{"invalid base64", "/e/", "base64", []byte("data=!!!!"), 400, "invalid base64 body"},
		{"URL-safe base64", "/e/", "base64", beaconForm(base64.URLEncoding, payload), 400, "invalid base64 body"},
		{"oversized JSON", "/e/", "base64", beaconForm(base64.StdEncoding, payload+strings.Repeat(" ", maxBodyBytes)), 413, "body exceeds 1 MiB"},
		{"unsupported compression", "/e/", "lz64", padded, 415, "unsupported body encoding"},
		{"invalid key", "/e/", "base64", beaconForm(base64.StdEncoding, captureBody("wrong-key", "invalid")), 401, "invalid ingestion credential"},
		{"conflicting keys", "/batch/", "base64", beaconForm(base64.StdEncoding, strings.Replace(batch, ingest, other, 1)), 400, "batch credentials must match"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := transportRequest(t, s, "POST", tc.route+"?compression="+tc.compression, tc.body, headers, tc.code)
			if !strings.Contains(w.Body.String(), tc.message) {
				t.Errorf("response %q does not contain %q", w.Body.String(), tc.message)
			}
		})
	}
	transportRequest(t, s, "POST", "/e/?compression=base64", padded, nil, 415)
	if count := eventCount(t, s); count != 4 {
		t.Fatalf("stored %d events, want 4 accepted beacon events", count)
	}
}

func TestNativeAndSegmentBase64Transport(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	projectID, ingest := project(t, s)
	_, other := project(t, s)
	const formType = "application/x-www-form-urlencoded; charset=UTF-8"
	native := beaconForm(base64.StdEncoding, `{"type":"track","name":"pageleave","actor_id":"native-user","message_id":"native-form"}`)
	nativeRoute := "/ingest/v1/events?compression=base64"
	for range 2 {
		transportRequest(t, s, "POST", nativeRoute, native, map[string]string{"Content-Type": formType, "X-Porcupine-Key": ingest}, 200)
	}
	formHeader := map[string]string{"Content-Type": formType}
	transportRequest(t, s, "POST", nativeRoute, native, formHeader, 401)

	segmentEvent := `{"userId":"segment-user","event":"pageleave","messageId":"segment-form","writeKey":"` + ingest + `"}`
	segment := beaconForm(base64.RawStdEncoding, segmentEvent)
	segmentRoute := "/v1/track?compression=base64"
	for range 2 {
		transportRequest(t, s, "POST", segmentRoute, segment, formHeader, 200)
	}
	wrongSegmentKey := segmentHeaders(other)
	wrongSegmentKey["Content-Type"] = formType
	transportRequest(t, s, "POST", segmentRoute, segment, wrongSegmentKey, 400)
	transportRequest(t, s, "POST", "/v1/track", segment, formHeader, 415)
	transportRequest(t, s, "POST", segmentRoute, []byte(segmentEvent), segmentHeaders(ingest), 415)
	large := `{"userId":"segment-user","event":"large","writeKey":"` + ingest + `","properties":{"pad":"` + strings.Repeat("x", ingestfmt.SegmentEventLimit) + `"}}`
	transportRequest(t, s, "POST", segmentRoute, beaconForm(base64.StdEncoding, large), formHeader, 413)

	result := request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", adminToken, "", 200)
	events := result["events"].([]any)
	if len(events) != 2 {
		t.Fatalf("stored %d events, want only the two accepted IDs once", len(events))
	}
	users := map[string]bool{"native-user": true, "segment-user": true}
	for _, value := range events {
		event := value.(map[string]any)
		user := event["distinct_id"].(string)
		if event["event"] != "pageleave" || !users[user] {
			t.Errorf("unexpected event and distinct ID %v, %q", event["event"], user)
		}
		delete(users, user)
	}
}

func TestIngestionRejectionMetrics(t *testing.T) {
	t.Parallel()
	s := operationalService(t)
	_, ingest := project(t, s)
	header := map[string]string{"Content-Type": "application/x-www-form-urlencoded"}
	transportRequest(t, s, "POST", "/e/?compression=lz64", nil, header, 415)
	transportRequest(t, s, "POST", "/e/?compression=base64", []byte("data=!!!!"), header, 400)
	transportRequest(t, s, "POST", "/e/?compression=base64", beaconForm(base64.StdEncoding, captureBody("wrong-key", "unauthenticated")), header, 401)
	transportRequest(t, s, "POST", "/e/?compression=base64", beaconForm(base64.StdEncoding, captureBody(adminToken, "forbidden")), header, 403)
	transportRequest(t, s, "POST", "/e/?compression=base64", beaconForm(base64.StdEncoding, `{"api_key":"`+ingest+`"}`), header, 400)
	transportRequest(t, s, "POST", "/e/?compression=base64", make([]byte, maxBodyBytes+1), header, 413)
	transportRequest(t, s, "POST", "/e/?compression=base64", beaconForm(base64.StdEncoding, captureBody(ingest, "accepted")), header, 200)
	transportRequest(t, s, "POST", "/e/?compression=base64", beaconForm(base64.StdEncoding, captureBody(ingest, "over-capacity")), header, 503)
	metrics := scrape(t, s)
	for _, reason := range []string{"unsupported_encoding", "invalid_body", "body_too_large", "invalid_credential", "forbidden", "invalid_event", "unavailable"} {
		requireMetric(t, metrics, `porcupine_ingestion_rejected_total{profile="standalone",route="/e/",reason="`+reason+`"} 1`)
	}
}
