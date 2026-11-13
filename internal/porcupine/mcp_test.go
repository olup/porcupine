package porcupine

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type mcpTestBearer struct{ key string }

func (b mcpTestBearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.key)
	return http.DefaultTransport.RoundTrip(r)
}

func mcpTestSession(t *testing.T, endpoint, key, version string) (context.Context, *mcp.ClientSession) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	client := mcp.NewClient(&mcp.Implementation{Name: "porcupine-tests", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint: endpoint + "/mcp", HTTPClient: &http.Client{Transport: mcpTestBearer{key: key}, Timeout: 5 * time.Second},
		DisableStandaloneSSE: true, MaxRetries: -1,
	}, &mcp.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return ctx, session
}

func mcpTestTool(t *testing.T, ctx context.Context, session *mcp.ClientSession, name string, args any, wantError bool) map[string]any {
	t.Helper()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError != wantError {
		if result.IsError {
			t.Fatalf("tool %s failed: %+v", name, result.StructuredContent)
		}
		t.Fatalf("tool %s isError=%v, want %v: %+v", name, result.IsError, wantError, result.Content)
	}
	if len(result.Content) == 0 {
		t.Fatal("missing text fallback")
	}
	encoded, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		t.Fatal(err)
	}
	return output
}

func mcpTestCall(t *testing.T, ctx context.Context, session *mcp.ClientSession, input mcpCallInput, status int) map[string]any {
	t.Helper()
	result := mcpTestTool(t, ctx, session, "porcupine_call", input, status >= 400)
	if result["status"] != float64(status) {
		t.Fatalf("API call %s returned %+v, want %d", input.OperationID, result, status)
	}
	if result["body"] == nil {
		return nil
	}
	return result["body"].(map[string]any)
}

func TestMCPRealClientDiscoveryAndLifecycle(t *testing.T) {
	for _, version := range []string{"2026-07-28", "2025-11-25"} {
		t.Run("protocol-"+version, func(t *testing.T) {
			s := service(t, 20)
			host := httptest.NewServer(s.Handler())
			defer host.Close()
			ctx, session := mcpTestSession(t, host.URL, adminToken, version)
			tools, err := session.ListTools(ctx, nil)
			if err != nil || len(tools.Tools) != 2 {
				t.Fatalf("expected two tools: %+v, %v", tools, err)
			}
			for _, tool := range tools.Tools {
				if tool.OutputSchema == nil || tool.Annotations.OpenWorldHint == nil || *tool.Annotations.OpenWorldHint {
					t.Fatalf("missing output schema or closed-world annotation: %+v", tool)
				}
				if tool.Name == "porcupine_docs" && !tool.Annotations.ReadOnlyHint {
					t.Fatal("docs must be read-only")
				}
				if tool.Name == "porcupine_call" && (tool.Annotations.ReadOnlyHint || tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint) {
					t.Fatal("generic API call must advertise possible mutation and destruction")
				}
			}
			catalog := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 20}, false)
			if len(catalog["operations"].([]any)) != 20 || catalog["contract"] != nil || catalog["next_offset"] != float64(20) {
				t.Fatal("catalog must contain a bounded first page of summaries")
			}
			tail := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 20, Offset: 20}, false)
			if len(tail["operations"].([]any)) != 18 || tail["next_offset"] != nil {
				t.Fatal("full catalog pagination failed")
			}
			allOperations := append(catalog["operations"].([]any), tail["operations"].([]any)...)
			for _, item := range allOperations {
				op := item.(map[string]any)
				wantReadOnly := op["method"] == "GET" || op["operation_id"] == "query-events"
				if op["read_only"] != wantReadOnly {
					t.Fatalf("incorrect side-effect hint for %s", op["operation_id"])
				}
			}
			page := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 2}, false)
			if len(page["operations"].([]any)) != 2 || page["next_offset"] != float64(2) {
				t.Fatal("catalog pagination failed")
			}
			filtered := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Query: "recent events"}, false)
			if len(filtered["operations"].([]any)) != 1 {
				t.Fatal("word search failed")
			}
			detail := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: "create-project"}, false)
			contract := detail["contract"].(map[string]any)
			paths := contract["paths"].(map[string]any)
			if len(paths) != 1 || len(paths["/api/v1/projects"].(map[string]any)) != 1 {
				t.Fatalf("detail leaked other operations: %+v", paths)
			}
			schemas := contract["components"].(map[string]any)["schemas"].(map[string]any)
			if schemas["AccessKeyCreated"] != nil || schemas["ProjectCreateInputBody"] == nil {
				t.Fatal("detail should include only reachable schemas")
			}
			created := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "MCP"}}, 201)
			project := created["project"].(map[string]any)["id"].(string)
			request(t, s, "POST", "/capture/", "", captureBody(created["ingest_token"].(string), "mcp-event"), 200)
			rows := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": project}, Query: map[string]string{"limit": "1"}}, 200)
			if len(rows["events"].([]any)) != 1 {
				t.Fatal("MCP could not read captured event")
			}
			reader := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-access-key", Body: map[string]any{
				"name": "agent", "permissions": []string{"events:read"}, "projects": []string{project},
			}}, 201)
			_, readerSession := mcpTestSession(t, host.URL, reader["token"].(string), version)
			mcpTestCall(t, ctx, readerSession, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": project}}, 200)
			mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "revoke-access-key", Path: map[string]string{"id": reader["id"].(string)}}, 204)
			if _, err := readerSession.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
				t.Fatalf("revoked key retained MCP access: %v", err)
			}
			var bootstrapID string
			if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&bootstrapID); err != nil {
				t.Fatal(err)
			}
			mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "revoke-access-key", Path: map[string]string{"id": bootstrapID}}, 409)
		})
	}
}

func TestMCPProjectIsolationAndEscalation(t *testing.T) {
	s := service(t, 20)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "a"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "b"), 200)
	hiddenKey, _ := key(t, s, []string{"events:read"}, []string{a, b})
	_, reader := key(t, s, []string{"management:read", "events:read"}, []string{a})
	_, manager := key(t, s, []string{"management:write", "events:read"}, []string{a})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "")
	catalog := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 20}, false)
	names := []string{}
	for _, item := range catalog["operations"].([]any) {
		names = append(names, item.(map[string]any)["operation_id"].(string))
	}
	if !slices.Equal(names, []string{"cancel-event-export", "create-event-export", "get-event", "get-event-catalog", "get-event-export", "get-event-mutation", "get-event-schema", "get-instance-status", "list-access-keys", "list-event-export-files", "list-event-exports", "list-oauth-clients", "list-project-events", "list-projects", "list-quarantined-events", "query-events"}) {
		t.Fatalf("discovery exceeded reader permissions: %v", names)
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "porcupine_docs", Arguments: mcpDocsInput{OperationID: "create-project"}})
	if err != nil || !result.IsError {
		t.Fatal("unauthorized contract was exposed")
	}
	projects := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-projects"}, 200)
	if len(projects["projects"].([]any)) != 1 || projects["projects"].([]any)[0].(map[string]any)["id"] != a {
		t.Fatal("MCP project listing leaked another project")
	}
	keys := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-access-keys"}, 200)
	encoded, _ := json.Marshal(keys)
	if strings.Contains(string(encoded), hiddenKey) || strings.Contains(string(encoded), b) || strings.Contains(string(encoded), "token") {
		t.Fatal("MCP key listing leaked a secret or cross-project key")
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": a}}, 200)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": b}}, 403)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "bad"}}, 403)
	_, writer := mcpTestSession(t, host.URL, manager, "")
	writerDocs := mcpTestTool(t, ctx, writer, "porcupine_docs", mcpDocsInput{}, false)
	for _, item := range writerDocs["operations"].([]any) {
		if item.(map[string]any)["operation_id"] == "create-project" {
			t.Fatal("project-scoped manager cannot create instance projects")
		}
	}
	for _, body := range []map[string]any{
		{"name": "bad", "permissions": []string{"events:read"}, "projects": []string{b}},
		{"name": "bad", "permissions": []string{"events:read"}, "projects": []string{"*"}},
		{"name": "bad", "permissions": []string{"instance:admin"}, "projects": []string{"*"}},
	} {
		mcpTestCall(t, ctx, writer, mcpCallInput{OperationID: "create-access-key", Body: body}, 403)
	}
}

func TestMCPValidationAndRoutingBoundaries(t *testing.T) {
	s := service(t, 20)
	project, _ := project(t, s)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, adminToken, "")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "https://example.test/"}, 404)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "/capture/"}, 404)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-projects", Query: map[string]string{"url": "http://example.test/"}}, 400)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-projects", Path: map[string]string{"id": project}}, 400)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-projects", Body: map[string]any{"unexpected": true}}, 400)
	for _, id := range []string{"", "..", "../access-keys", "%2e%2e", project + "?limit=1000", project + "/events"} {
		mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": id}}, 400)
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": project}, Query: map[string]string{"limit": "1001"}}, 422)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": false}}, 422)
	for _, args := range []any{
		map[string]any{"operation_id": "list-projects", "url": "https://example.test/"},
		map[string]any{"operation_id": "list-projects", "headers": map[string]string{"Authorization": "Bearer other"}},
	} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "porcupine_call", Arguments: args})
		if err != nil || !result.IsError {
			t.Fatal("unknown tool arguments must be rejected")
		}
	}
	for _, args := range []mcpDocsInput{{Limit: 21}, {Offset: -1}, {OperationID: "list-projects", Query: "projects"}} {
		result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "porcupine_docs", Arguments: args})
		if err != nil || !result.IsError {
			t.Fatal("invalid discovery parameters accepted")
		}
	}
}

func TestMCPConcurrentCredentials(t *testing.T) {
	s := service(t, 20)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "a"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "b"), 200)
	_, keyA := key(t, s, []string{"events:read"}, []string{a})
	_, keyB := key(t, s, []string{"events:read"}, []string{b})
	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	for _, tc := range []struct{ project, key string }{{a, keyA}, {b, keyB}} {
		t.Run(tc.project, func(t *testing.T) {
			t.Parallel()
			ctx, session := mcpTestSession(t, host.URL, tc.key, "")
			for range 5 {
				catalog := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{}, false)
				if projects := catalog["projects"].([]any); len(projects) != 1 || projects[0] != tc.project {
					t.Fatal("another credential contaminated the request context")
				}
				result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": tc.project}}, 200)
				if events := result["events"].([]any); len(events) != 1 || events[0].(map[string]any)["project_id"] != tc.project {
					t.Fatal("another project's event was returned")
				}
			}
		})
	}
}

func TestMCPHTTPBoundaries(t *testing.T) {
	s := service(t, 20)
	handler := s.Handler()
	for _, tc := range []struct {
		name, method, key, origin, encoding, body string
		status                                    int
	}{
		{"missing key", "POST", "", "", "", `{}`, 401},
		{"invalid key", "POST", "invalid", "", "", `{}`, 401},
		{"cross origin", "POST", adminToken, "https://other.test", "", `{}`, 403},
		{"compression", "POST", adminToken, "", "gzip", `{}`, 415},
		{"oversized", "POST", adminToken, "", "", strings.Repeat(" ", maxBodyBytes+1), 413},
		{"no SSE listener", "GET", adminToken, "", "", "", 405},
		{"no sessions", "DELETE", adminToken, "", "", "", 405},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "http://porcupine.test/mcp", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			if tc.key != "" {
				r.Header.Set("Authorization", "Bearer "+tc.key)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.encoding != "" {
				r.Header.Set("Content-Encoding", tc.encoding)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("got %d, want %d: %s", w.Code, tc.status, w.Body.String())
			}
			if w.Header().Get("Access-Control-Allow-Origin") != "" || strings.Contains(w.Body.String(), adminToken) {
				t.Fatal("MCP leaked credential or ingestion CORS")
			}
		})
	}
	r := httptest.NewRequest("POST", "http://attacker.test/mcp", strings.NewReader(`{}`))
	r.Header.Set("Authorization", "Bearer "+adminToken)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	r = r.WithContext(context.WithValue(r.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8080}))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("localhost DNS rebinding protection returned %d", w.Code)
	}
}

func TestMCPResponseBound(t *testing.T) {
	s := service(t, 20)
	a, ingest := project(t, s)
	for _, id := range []string{"large-a", "large-b", "large-c"} {
		body := `{"api_key":"` + ingest + `","event":"large","distinct_id":"a","uuid":"` + id + `","properties":{"data":"` + strings.Repeat("x", 750000) + `"}}`
		request(t, s, "POST", "/capture/", "", body, 200)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, adminToken, "")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": a}}, 413)
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": a}, Query: map[string]string{"limit": "1"}}, 200)
	if len(result["events"].([]any)) != 1 {
		t.Fatal("bounded retry failed")
	}
	w := &mcpAPIResponse{header: http.Header{}}
	if _, err := w.Write(make([]byte, maxMCPResponseBytes+1)); err != nil || !w.overflow || w.body.Len() != 0 {
		t.Fatal("oversized API output was buffered")
	}
}
