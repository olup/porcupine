package porcupine

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type pipelineBatchContractTransport func(*http.Request) (*http.Response, error)

func (transport pipelineBatchContractTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestPipelineBatchHTTPAndMCPContract(t *testing.T) {
	start := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	var clockMS atomic.Int64
	clockMS.Store(start.UnixMilli())
	service, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, QueueMaxEvents: 20,
		Clock:     func() time.Time { return time.UnixMilli(clockMS.Load()).UTC() },
		Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{0x37}, 32), WebhookMaxAttempts: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { service.Close() })

	const endpoint = "https://batch-contract.example.test/private-destination"
	const bearerSecret = "batch-contract-bearer-secret"
	const headerSecret = "batch-contract-header-secret"
	type outbound struct {
		body    []byte
		headers http.Header
		url     string
		method  string
		sentMS  int64
	}
	observed := make(chan outbound, 8)
	var calls atomic.Int32
	// No destinations exist yet. Join the initial workers before replacing their
	// outbound client, then run the real workers for the entire API contract.
	service.stopPipelines()
	service.pipelines.webhookClient = &http.Client{Transport: pipelineBatchContractTransport(func(request *http.Request) (*http.Response, error) {
		defer request.Body.Close()
		body, err := io.ReadAll(io.LimitReader(request.Body, 2<<20))
		if err != nil {
			return nil, err
		}
		observed <- outbound{body: body, headers: request.Header.Clone(), url: request.URL.String(), method: request.Method, sentMS: clockMS.Load()}
		status, response := http.StatusNoContent, ""
		if calls.Add(1) == 1 {
			status = http.StatusServiceUnavailable
			// An untrusted receiver may reflect secrets. Public run errors must not.
			response = bearerSecret + " " + headerSecret + " " + endpoint
		}
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response)), Request: request}, nil
	}), Timeout: 2 * time.Second}
	service.startPipelines()
	host := httptest.NewServer(service.Handler())
	t.Cleanup(host.Close)
	client := &http.Client{Timeout: 3 * time.Second}
	call := func(method, path, token string, body any, want int) map[string]any {
		t.Helper()
		var input io.Reader
		if body != nil {
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			input = bytes.NewReader(encoded)
		}
		request, err := http.NewRequest(method, host.URL+path, input)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("%s %s returned %d, want %d", method, path, response.StatusCode, want)
		}
		output := map[string]any{}
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&output); err != nil && err != io.EOF {
			t.Fatal(err)
		}
		return output
	}
	createProject := func(name string) (string, string) {
		created := call("POST", "/api/v1/projects", adminToken, map[string]any{"name": name}, 201)
		return created["project"].(map[string]any)["id"].(string), created["ingest_token"].(string)
	}
	projectA, ingestA := createProject("batch contract A")
	projectB, ingestB := createProject("batch contract B")
	createKey := func(project string, permissions ...string) string {
		created := call("POST", "/api/v1/access-keys", adminToken, map[string]any{"name": "batch contract", "permissions": permissions, "projects": []string{project}}, 201)
		return created["token"].(string)
	}
	writer := createKey(projectA, "pipelines:write", "pipelines:read", "events:read")
	readerA := createKey(projectA, "pipelines:read", "events:read")
	readerB := createKey(projectB, "pipelines:read", "events:read")
	assertPrivate := func(value any) {
		t.Helper()
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{endpoint, "batch-contract.example.test", bearerSecret, headerSecret, ingestA, ingestB, writer, readerA, readerB, "batch-event-data"} {
			if bytes.Contains(encoded, []byte(secret)) {
				t.Fatal("public pipeline metadata exposed a destination or credential")
			}
		}
	}
	base := "/api/v1/projects/" + projectA + "/pipelines"
	created := call("POST", base, writer, map[string]any{
		"name": "minute purchases", "kind": "webhook",
		"webhook":     map[string]any{"endpoint": endpoint, "include_events": []string{"purchase", "blocked"}, "exclude_events": []string{"blocked"}, "delivery_mode": "batch", "interval_seconds": 60, "start_at": start.Format(time.RFC3339)},
		"credentials": map[string]any{"bearer_token": bearerSecret, "headers": map[string]string{"X-Contract-Key": headerSecret}},
	}, 201)
	assertPrivate(created)
	publicSpec, ok := created["webhook"].(map[string]any)
	if !ok || publicSpec["delivery_mode"] != "batch" || publicSpec["interval_seconds"] != float64(60) {
		t.Fatal("created pipeline did not expose its nonsecret batch configuration")
	}
	pipelineID := created["id"].(string)
	pipelinePath := base + "/" + pipelineID
	for _, route := range []string{base, pipelinePath, pipelinePath + "/runs"} {
		assertPrivate(call("GET", route, readerA, nil, 200))
	}

	event := func(uuid, name string) map[string]any {
		return map[string]any{"uuid": uuid, "event": name, "distinct_id": "batch-user", "timestamp": "2026-11-01T00:00:00Z", "properties": map[string]any{"unicode": "東京 🦔", "private_marker": "batch-event-data"}}
	}
	accepted := call("POST", "/batch/", "", map[string]any{"api_key": ingestA, "batch": []any{event("batch-a", "purchase"), event("batch-b", "purchase"), event("not-included", "ignored"), event("explicitly-excluded", "blocked")}}, 200)
	if accepted["accepted"] != float64(4) {
		t.Fatal("capture batch was not durably accepted")
	}
	foreign := event("foreign-project", "purchase")
	foreign["api_key"] = ingestB
	call("POST", "/capture/", "", foreign, 200)
	select {
	case <-observed:
		t.Fatal("batch webhook was sent before its receipt-time window closed")
	case <-time.After(1100 * time.Millisecond):
	}
	clockMS.Store(start.Add(time.Minute).UnixMilli())
	waitOutbound := func() outbound {
		t.Helper()
		select {
		case request := <-observed:
			return request
		case <-time.After(5 * time.Second):
			t.Fatal("batch worker did not send its closed window")
			return outbound{}
		}
	}
	first := waitOutbound()
	if first.method != "POST" || first.url != endpoint || first.sentMS < start.Add(time.Minute).UnixMilli() || first.headers.Get("Authorization") != "Bearer "+bearerSecret || first.headers.Get("X-Contract-Key") != headerSecret {
		t.Fatal("outbound batch request did not preserve its HTTPS destination and credentials")
	}
	var payload struct {
		DeliveryID string  `json:"delivery_id"`
		PipelineID string  `json:"pipeline_id"`
		ProjectID  string  `json:"project_id"`
		Events     []Event `json:"events"`
	}
	if err := json.Unmarshal(first.body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.DeliveryID == "" || payload.PipelineID != pipelineID || payload.ProjectID != projectA || len(payload.Events) != 2 || first.headers.Get("X-Porcupine-Delivery-ID") != payload.DeliveryID || first.headers.Get("Idempotency-Key") != payload.DeliveryID {
		t.Fatal("batch envelope or stable delivery headers are incorrect")
	}
	wantUUIDs := map[string]bool{"batch-a": true, "batch-b": true}
	for _, captured := range payload.Events {
		if !wantUUIDs[captured.UUID] || captured.ProjectID != projectA || captured.Event != "purchase" || captured.DistinctID != "batch-user" {
			t.Fatal("batch included a duplicate, excluded, nonmatching or foreign event")
		}
		delete(wantUUIDs, captured.UUID)
		if !bytes.Contains(captured.Properties, []byte("batch-event-data")) || !bytes.Contains(captured.Properties, []byte("東京")) {
			t.Fatal("batch did not preserve captured custom properties")
		}
	}
	if len(wantUUIDs) != 0 {
		t.Fatal("batch omitted an accepted matching event")
	}

	waitRun := func(state string, attempts float64) map[string]any {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			result := call("GET", pipelinePath+"/runs", readerA, nil, 200)
			assertPrivate(result)
			runs := result["runs"].([]any)
			if len(runs) == 1 {
				run := runs[0].(map[string]any)
				if run["state"] == state && run["attempts"] == attempts {
					return run
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("batch run did not reach %s after %.0f attempts", state, attempts)
		return nil
	}
	retrying := waitRun("queued", 1)
	if retrying["id"] != payload.DeliveryID || retrying["last_status"] != float64(503) {
		t.Fatal("transient receiver failure did not retain the same run")
	}
	clockMS.Store(start.Add(time.Minute + 2*time.Second).UnixMilli())
	second := waitOutbound()
	if !bytes.Equal(first.body, second.body) || first.headers.Get("Idempotency-Key") != second.headers.Get("Idempotency-Key") || first.headers.Get("X-Porcupine-Delivery-ID") != second.headers.Get("X-Porcupine-Delivery-ID") {
		t.Fatal("transient retry changed its batch body or stable delivery identity")
	}
	delivered := waitRun("delivered", 2)
	if delivered["id"] != payload.DeliveryID || delivered["last_status"] != float64(204) || delivered["window_start_ms"] != float64(start.UnixMilli()) || delivered["window_end_ms"] != float64(start.Add(time.Minute).UnixMilli()) || delivered["event_count"] != float64(2) {
		t.Fatal("completed batch run metadata lost its window or event count")
	}
	if calls.Load() != 2 {
		t.Fatal("worker sent unexpected deliveries for nonmatching or foreign events")
	}
	call("GET", pipelinePath+"/runs", readerB, nil, 403)
	call("GET", "/api/v1/projects/"+projectB+"/pipelines/"+pipelineID+"/runs", readerB, nil, 404)

	ctx, session := mcpTestSession(t, host.URL, readerA, "")
	for _, operation := range []string{"get-pipeline", "list-pipeline-runs", "get-pipeline-run"} {
		assertPrivate(mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: operation}, false))
	}
	path := map[string]string{"id": projectA, "pipeline_id": pipelineID}
	assertPrivate(mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-pipeline", Path: path}, 200))
	mcpRuns := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-pipeline-runs", Path: path}, 200)
	assertPrivate(mcpRuns)
	if runs := mcpRuns["runs"].([]any); len(runs) != 1 || runs[0].(map[string]any)["id"] != payload.DeliveryID {
		t.Fatal("MCP run listing disagreed with the delivered HTTP batch")
	}
	path["run_id"] = payload.DeliveryID
	inspected := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-pipeline-run", Path: path}, 200)
	assertPrivate(inspected)
	if inspected["state"] != "delivered" || inspected["attempts"] != float64(2) || inspected["event_count"] != float64(2) {
		t.Fatal("MCP could not inspect the completed batch run")
	}
	if encoded, _ := json.Marshal(inspected); bytes.Contains(encoded, []byte("batch-event-data")) {
		t.Fatal("run inspection exposed retained event payloads")
	}
}
