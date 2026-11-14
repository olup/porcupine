package porcupine

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func resolveNativeContract(t *testing.T, name string) *jsonschema.Resolved {
	t.Helper()
	load := func(name string) *jsonschema.Schema {
		t.Helper()
		raw, err := nativeContracts.ReadFile("contracts/" + name)
		if err != nil {
			t.Fatal(err)
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		return &schema
	}
	resolved, err := load(name).Resolve(&jsonschema.ResolveOptions{Loader: func(uri *url.URL) (*jsonschema.Schema, error) {
		if uri.String() != "https://olup.github.io/porcupine/schemas/native-message-v1.schema.json" {
			return nil, fmt.Errorf("unexpected schema reference: %s", uri)
		}
		return load(path.Base(uri.Path)), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func contractExample(t *testing.T, adapter string) []byte {
	t.Helper()
	raw, err := os.ReadFile("../../examples/events/" + adapter + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestNativeContractSchemasAndExamples(t *testing.T) {
	live := resolveNativeContract(t, "native-message-v1.schema.json")
	history := resolveNativeContract(t, "native-history-event-v1.draft.schema.json")
	var examples []map[string]any
	decoder := json.NewDecoder(bytes.NewReader(contractExample(t, "native")))
	decoder.UseNumber()
	if err := decoder.Decode(&examples); err != nil {
		t.Fatal(err)
	}
	for _, message := range examples {
		if err := live.Validate(message); err != nil {
			t.Fatalf("example violates live contract: %v", err)
		}
		err := history.Validate(message)
		if (message["type"] == "identify") != (err != nil) {
			t.Fatalf("ordinary history draft accepted identity or rejected event: %v", err)
		}
	}
	for _, raw := range []string{
		`{"type":"track"}`, `{"type":"track","name":"$identify"}`,
		`{"type":"identify","actor_id":"a","user_id":"u"}`,
		`{"type":"pageview","name":"Home"}`, `{"type":"set","actor_id":"a"}`,
		`{"type":"track","name":"ping","message_id":" "}`,
		`{"type":"track","name":"ping","context":[]}`,
	} {
		var message any
		if err := json.Unmarshal([]byte(raw), &message); err != nil {
			t.Fatal(err)
		}
		if live.Validate(message) == nil {
			t.Fatalf("schema accepted %s", raw)
		}
	}
	for _, field := range []string{"project_id", "source_id", "event_id", "uuid", "received_at", "revision", "is_imported"} {
		message := map[string]any{"type": "pageview", field: "forged"}
		if live.Validate(message) == nil {
			t.Fatalf("schema accepted trusted field %s", field)
		}
	}
	for _, raw := range []string{`{"type":"track","name":"ping","timestamp":"2026-01-10T08:00:00Z"}`, `{"type":"pageview","message_id":"one"}`} {
		var message any
		json.Unmarshal([]byte(raw), &message)
		if live.Validate(message) != nil || history.Validate(message) == nil {
			t.Fatal("history must require both occurrence time and stable producer ID")
		}
	}
}

func TestNativeContractDiscoveryAndProjectIsolation(t *testing.T) {
	s := service(t, 10)
	a, writer := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	route := "/api/v1/projects/" + a + "/input-contract"
	body := request(t, s, "GET", route, reader, "", 200)
	if body["version"] != "1" || body["native_historical_import_available"] != false || body["historical_schema_status"] != "draft" || body["live_endpoint"] != "/ingest/v1/events" {
		t.Fatalf("misleading contract: %v", body)
	}
	live, _ := nativeContractSchema("native-message-v1.schema.json")
	if !reflect.DeepEqual(body["live_schema"], live) {
		t.Fatal("discovery schema diverged from embedded source")
	}
	request(t, s, "GET", route, "", "", 401)
	request(t, s, "GET", route, writer, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+b+"/input-contract", reader, "", 403)
	public := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/ingest/v1/schema", nil)
	req.Header.Set("Origin", "https://producer.example")
	s.Handler().ServeHTTP(public, req)
	raw, _ := nativeContracts.ReadFile("contracts/native-message-v1.schema.json")
	if public.Code != 200 || public.Header().Get("Content-Type") != "application/schema+json" || public.Header().Get("Access-Control-Allow-Origin") != "*" || !bytes.Equal(public.Body.Bytes(), raw) {
		t.Fatalf("public schema unavailable: %d %s", public.Code, public.Body.String())
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "")
	detail := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: "get-event-input-contract"}, false)
	encoded, _ := json.Marshal(detail)
	if !bytes.Contains(encoded, []byte("/input-contract")) {
		t.Fatal("MCP did not discover input contract")
	}
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-input-contract", Path: map[string]string{"id": a}}, 200)
	if !reflect.DeepEqual(body, result) {
		t.Fatal("MCP and HTTP contracts differ")
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-input-contract", Path: map[string]string{"id": b}}, 403)
}

func TestNativeContractAdaptersAndCrossProtocolRetry(t *testing.T) {
	for _, adapter := range []string{"native", "segment", "posthog"} {
		t.Run(adapter, func(t *testing.T) {
			s := service(t, 20)
			projectID, ingest := project(t, s)
			bindIngestionSource(t, s, ingest, "checkout")
			send := func(adapter string) {
				raw := contractExample(t, adapter)
				switch adapter {
				case "native":
					nativeRequest(t, s, ingest, string(raw), 200)
				case "segment":
					transportRequest(t, s, "POST", "/v1/batch", raw, segmentHeaders(ingest), 200)
				case "posthog":
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(raw, &envelope); err != nil {
						t.Fatal(err)
					}
					envelope["api_key"], _ = json.Marshal(ingest)
					raw, _ = json.Marshal(envelope)
					request(t, s, "POST", "/batch/", "", string(raw), 200)
				}
			}
			send(adapter)
			var uuid, timestamp, properties, actor string
			var id int64
			if err := s.db.QueryRow(`SELECT id,uuid,timestamp,properties,distinct_id FROM events WHERE project_id=? AND event='order_completed'`, projectID).Scan(&id, &uuid, &timestamp, &properties, &actor); err != nil {
				t.Fatal(err)
			}
			if timestamp != "2026-01-10T08:02:00Z" || actor != "user-42" || !strings.Contains(properties, `"external_id":9007199254740993`) {
				t.Fatalf("adapter changed event meaning: %s %s %s", timestamp, actor, properties)
			}
			var target, pageActor string
			if err := s.db.QueryRow(`SELECT identified_id FROM identity_links WHERE project_id=? AND anonymous_id='browser-7'`, projectID).Scan(&target); err != nil || target != "user-42" {
				t.Fatalf("identify not applied: %s %v", target, err)
			}
			if err := s.db.QueryRow(`SELECT distinct_id FROM events WHERE project_id=? AND event='$pageview'`, projectID).Scan(&pageActor); err != nil || pageActor != "browser-7" {
				t.Fatalf("original actor overwritten: %s %v", pageActor, err)
			}
			traits, identified, _ := storedIdentityProfile(t, s, projectID, "user-42")
			if !identified || traits["plan"] != "pro" {
				t.Fatalf("traits not normalized: %v", traits)
			}
			count := eventCount(t, s)
			for _, retryAdapter := range []string{"native", "segment", "posthog"} {
				send(retryAdapter)
			}
			retry := nativeRequest(t, s, ingest, string(contractExample(t, "native")), 200)
			receipt := receiptFrom(t, retry, 2)
			if eventCount(t, s) != count || receipt["uuid"] != uuid || receipt["event_id"] != fmt.Sprint(id) || receipt["duplicate"] != true {
				t.Fatalf("cross-adapter replay changed event identity: %v", receipt)
			}
		})
	}
}

func TestNativeContractSemanticValidationAndActorlessEvents(t *testing.T) {
	s := service(t, 10)
	projectID, ingest := project(t, s)
	for _, message := range []map[string]any{
		{"type": "track", "name": "ping", "timestamp": "not-a-date"},
		{"type": "track", "name": "ping", "message_id": strings.Repeat("é", 513)},
		{"type": "track", "name": strings.Repeat("é", 251)},
		{"type": "track", "name": "ping", "actor_id": strings.Repeat("é", 501)},
		{"type": "track", "name": "ping", "context": map[string]any{"value": strings.Repeat("x", 65536)}},
	} {
		raw, _ := json.Marshal(message)
		nativeRequest(t, s, ingest, string(raw), 400)
	}
	if eventCount(t, s) != 0 {
		t.Fatal("invalid messages reached the queue")
	}
	first := nativeRequest(t, s, ingest, `{"type":"track","name":"stock_updated","properties":{"quantity":3}}`, 200)
	second := nativeRequest(t, s, ingest, `{"type":"track","name":"stock_updated","properties":{"quantity":3}}`, 200)
	if receiptFrom(t, first, 0)["event_id"] == receiptFrom(t, second, 0)["event_id"] {
		t.Fatal("omitted message ID deduplicated legitimate identical events")
	}
	var actors int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=? AND distinct_id!=''`, projectID).Scan(&actors); err != nil || actors != 0 {
		t.Fatal("actorless event invented an identity", err)
	}
}

func TestPostHogContractPreservesNumbersInEveryEnvelope(t *testing.T) {
	for _, envelope := range []string{"single", "array", "batch"} {
		t.Run(envelope, func(t *testing.T) {
			s := service(t, 10)
			projectID, ingest := project(t, s)
			event := fmt.Sprintf(`{"api_key":%q,"event":"precision","uuid":"number","distinct_id":"actor","properties":{"large":9007199254740993,"nested":{"negative":-9007199254740993}}}`, ingest)
			body := event
			switch envelope {
			case "array":
				body = "[" + event + "]"
			case "batch":
				body = `{"batch":[` + event + `]}`
			}
			request(t, s, "POST", "/capture/", "", body, 200)
			var properties string
			if err := s.db.QueryRow(`SELECT properties FROM events WHERE project_id=? AND uuid='number'`, projectID).Scan(&properties); err != nil {
				t.Fatal(err)
			}
			for _, fragment := range []string{`"large":9007199254740993`, `"negative":-9007199254740993`} {
				if !strings.Contains(properties, fragment) {
					t.Fatalf("numeric JSON token changed: %s", properties)
				}
			}
			request(t, s, "POST", "/capture/", "", body+` {}`, 400)
			if eventCount(t, s) != 1 {
				t.Fatal("trailing JSON changed durable acceptance")
			}
		})
	}
}
