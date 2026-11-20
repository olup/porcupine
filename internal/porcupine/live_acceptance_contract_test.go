package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
)

func TestLiveAcceptanceNativeAndVendorContractsByMode(t *testing.T) {
	t.Parallel()
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "durable", true: "buffered"}[buffered], func(t *testing.T) {
			t.Parallel()
			s := service(t, 100)
			projectID, _ := project(t, s)
			writer := sourceKey(t, s, projectID, "checkout", "events:write")
			if buffered {
				s.acceptance = newTestBuffer(t, testBufferConfig(), func(context.Context, acceptance.FlushBatch) error { return nil })
			}
			mode := string(s.acceptance.Durability())
			schema := resolveNativeContract(t, "native-response-v1.schema.json")
			const raw = `[{"type":"track","message_id":"one","name":"purchase","actor_id":"user"},{"type":"pageview","message_id":"two","actor_id":"user"}]`
			first := nativeRequest(t, s, writer, raw, 200)
			retry := nativeRequest(t, s, writer, raw, 200)
			for _, body := range []map[string]any{first, retry} {
				if err := schema.Validate(body); err != nil {
					t.Fatal("response schema rejected mode", mode, err, body)
				}
				if body["durability"] != mode || body["status"] != float64(1) {
					t.Fatal(body)
				}
				receipts := body["receipts"].([]any)
				for i, id := range []string{"one", "two"} {
					receipt := receipts[i].(map[string]any)
					if receipt["message_id"] != id || receipt["uuid"] == "" {
						t.Fatal("receipt order or identity lost", receipt)
					}
					if buffered && len(receipt) != 2 {
						t.Fatal("buffered receipt claimed commit knowledge", receipt)
					}
				}
				if buffered {
					if len(body) != 4 || body["received"] != float64(2) {
						t.Fatal("buffered response fabricated committed counts", body)
					}
				} else if len(body) != 6 {
					t.Fatal("durable response gained fields beyond durability", body)
				}
			}
			if receiptFrom(t, first, 0)["uuid"] != receiptFrom(t, retry, 0)["uuid"] {
				t.Fatal("retry UUID changed")
			}
			if !buffered && (retry["duplicates"] != float64(2) || receiptFrom(t, retry, 0)["event_id"] != receiptFrom(t, first, 0)["event_id"]) {
				t.Fatal("durable stable retry changed", retry)
			}
			// The native producer schema never accepts a client-selected durability contract.
			nativeRequest(t, s, writer, `{"type":"pageview","durability":"durable"}`, 400)
			segment := transportRequest(t, s, "POST", "/v1/batch", []byte(`{"batch":[{"type":"track","messageId":"one","event":"purchase","userId":"user"}]}`), segmentHeaders(writer), 200)
			var segmentBody map[string]any
			if err := json.Unmarshal(segment.Body.Bytes(), &segmentBody); err != nil {
				t.Fatal(err)
			}
			if segmentBody["status"] != float64(1) || segmentBody["durability"] != mode {
				t.Fatal("Segment success compatibility changed", segmentBody)
			}
			if err := schema.Validate(segmentBody); err != nil {
				t.Fatal("Segment receipt shape diverged", err, segmentBody)
			}
			posthog := request(t, s, "POST", "/capture/", "", captureBody(writer, "one"), 200)
			if posthog["status"] != float64(1) || posthog["durability"] != mode || posthog["receipts"] != nil {
				t.Fatal("PostHog response flavor changed", posthog)
			}
			if buffered {
				if len(posthog) != 3 || posthog["received"] != float64(1) || eventCount(t, s) != 0 {
					t.Fatal("buffered SDK response claimed durable counts or wrote SQLite", posthog)
				}
			} else if len(posthog) != 5 || posthog["duplicates"] != float64(1) {
				t.Fatal("durable PostHog fields changed", posthog)
			}
			_, reader := key(t, s, []string{"events:read"}, []string{projectID})
			discovery := request(t, s, "GET", "/api/v1/projects/"+projectID+"/input-contract", reader, "", 200)
			ack := discovery["live_acknowledgment"].(map[string]any)
			if ack["durability"] != mode || ack["meaning"] == "" {
				t.Fatal("instance discovery omits actual acknowledgment", discovery)
			}
			public := transportRequest(t, s, "GET", "/ingest/v1/schema", nil, nil, 200)
			var published map[string]any
			if err := json.Unmarshal(public.Body.Bytes(), &published); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(published, discovery["live_schema"]) || !reflect.DeepEqual(published["x-porcupine-acknowledgment"], ack) {
				t.Fatal("published instance schema disagrees with discovery", published)
			}
			if buffered && !strings.Contains(ack["received_meaning"].(string), "including retries") {
				t.Fatal("received count meaning absent")
			}
			// A reader from another project cannot discover or select the producer's scope.
			other, _ := project(t, s)
			request(t, s, "GET", "/api/v1/projects/"+other+"/input-contract", reader, "", 403)
			otherKey := sourceKey(t, s, other, "checkout", "events:write")
			otherReceipt := receiptFrom(t, nativeRequest(t, s, otherKey, `{"type":"track","message_id":"one","name":"other"}`, 200), 0)
			if otherReceipt["uuid"] == receiptFrom(t, first, 0)["uuid"] {
				t.Fatal("same source/message ID crossed project scope")
			}
		})
	}
}

func TestBufferedHTTPBackpressureAndNoAcknowledgment(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	_, key := project(t, s)
	c := testBufferConfig()
	c.MaxEvents = 1
	c.ProjectMaxEvents = 1
	b := newTestBuffer(t, c, func(ctx context.Context, _ acceptance.FlushBatch) error {
		<-ctx.Done()
		return ctx.Err()
	})
	s.acceptance = b
	nativeRequest(t, s, key, `{"type":"pageview","message_id":"one"}`, 200)
	req := httptest.NewRequest("POST", "/ingest/v1/events", strings.NewReader(`{"type":"pageview","message_id":"two"}`))
	req.Header.Set("X-Porcupine-Key", key)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	var body map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != 503 || w.Header().Get("Retry-After") != "1" || body["received"] != nil || body["receipts"] != nil || b.Snapshot().Events != 1 {
		t.Fatal("rejected request acknowledged or partially admitted", w.Code, w.Header(), body)
	}
}
func TestLivePreparationIdentityNoopAndMalformedBatch(t *testing.T) {
	t.Parallel()
	s := service(t, 10)
	_, key := project(t, s)
	invalid := request(t, s, "POST", "/capture/", "", `{"api_key":"`+key+`","event":"$identify","uuid":"invalid-control","distinct_id":"person","properties":{"$set":null}}`, 200)
	if invalid["accepted"] != float64(1) || invalid["identity_ignored"] != float64(1) {
		t.Fatal("invalid SDK control lost canonical no-op", invalid)
	}
	nativeRequest(t, s, key, `[{"type":"pageview","message_id":"valid-row"},{"type":"identify","user_id":"person","set":[]}]`, 400)
	if eventCount(t, s) != 1 {
		t.Fatal("malformed outer batch partially committed")
	}
	batch, err := prepareLiveBatch("p", "", eventBatch{{UUID: "no-op", Event: "$identify", DistinctID: "person", Properties: map[string]any{"$set": nil}}}, time.Now(), nil)
	if err != nil || batch.Events[0].Identity.Operation != identityControlIgnored || batch.Events[0].Event.DistinctID != "" || string(batch.Events[0].Event.Properties) != `{}` {
		t.Fatal("pending invalid control was not parsed as a canonical no-op", batch, err)
	}
}

func TestHistoricalUploadsBypassLiveMemoryAdmission(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	project, _ := project(t, s)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{project})
	buffer := newTestBuffer(t, testBufferConfig(), func(context.Context, acceptance.FlushBatch) error {
		t.Error("historical import reached live flusher")
		return nil
	})
	s.acceptance = buffer
	data := historicalJourney(t, "native")
	job := createFormatImport(t, s, project, token, "native", "historical-durable", data)
	done := finishFormatImport(t, s, project, token, job, data)
	if done.Counts.Accepted != 3 || eventCount(t, s) != 3 || buffer.Snapshot().Events != 0 {
		t.Fatal("historical checkpoint was replaced with volatile admission", done, buffer.Snapshot())
	}
}

func TestLiveAcceptanceUnknownResultFailsClosed(t *testing.T) {
	t.Parallel()
	for _, result := range []acceptance.Result{nil, (*acceptance.BufferedResult)(nil)} {
		if response, err := liveAcceptanceResponse(result, true); err == nil || response != nil {
			t.Fatal("unknown result fabricated success", response, err)
		}
	}
}
