package porcupine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInstanceStatusHTTPPermissionsAndPrivacy(t *testing.T) {
	s := service(t, 20)
	a, public := project(t, s)
	b, _ := project(t, s)
	id, reader := key(t, s, []string{"management:read"}, []string{a})
	_, eventsOnly := key(t, s, []string{"events:read"}, []string{a})
	request(t, s, "GET", "/api/v1/status", "", "", 401)
	request(t, s, "GET", "/api/v1/status", public, "", 403)
	request(t, s, "GET", "/api/v1/status", eventsOnly, "", 403)
	result := request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if result["status"] != "ok" || result["version"] != Version {
		t.Fatalf("unexpected instance status: %+v", result)
	}
	if _, err := time.Parse(time.RFC3339, result["checked_at"].(string)); err != nil {
		t.Fatal("missing check timestamp")
	}
	checks := result["checks"].(map[string]any)
	if checks["sqlite"] != "ok" || checks["clickhouse"] != "not_configured" {
		t.Fatalf("unexpected local checks: %+v", checks)
	}
	for name, value := range result["configuration"].(map[string]any) {
		if value != false {
			t.Fatalf("optional service %s reported configured", name)
		}
	}
	encoded, _ := json.Marshal(result)
	for _, private := range []string{a, b, public, reader, adminToken, "app", "projects", "pending"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("status disclosed project data or a credential")
		}
	}
	r := httptest.NewRequest("GET", "/api/v1/status", nil)
	r.Header.Set("Authorization", "Bearer "+reader)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("instance status must not be cached")
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	request(t, s, "GET", "/api/v1/status", reader, "", 401)
}

func TestInstanceStatusClickHouseChecks(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       DependencyStatus
	}{
		{"healthy", "1\n", 200, dependencyOK},
		{"auth_failure", "secret upstream error", 401, dependencyUnavailable},
		{"database_failure", "secret upstream error", 500, dependencyUnavailable},
		{"unexpected_success_body", "<html>proxy login</html>", 200, dependencyUnavailable},
		{"oversized_success_body", strings.Repeat("1", 129), 200, dependencyUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if string(body) != "SELECT 1" || r.URL.Query().Get("database") != "private_database" || r.URL.Query().Get("max_execution_time") != "1" {
					t.Error("status did not use a bounded fixed probe of the configured database")
				}
				if r.Header.Get("X-ClickHouse-Key") != "private-password" {
					t.Error("ClickHouse probe was not authenticated")
				}
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			s := service(t, 20)
			var err error
			s.ch, err = newClickHouse(ClickHouseConfig{URL: upstream.URL, Database: "private_database", User: "private_user", Password: "private-password"})
			if err != nil {
				t.Fatal(err)
			}
			result := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
			if result["checks"].(map[string]any)["clickhouse"] != string(tc.want) {
				t.Fatalf("unexpected ClickHouse state: %+v", result)
			}
			if (result["status"] == "ok") != (tc.want == dependencyOK) {
				t.Fatal("overall health disagrees with the dependency check")
			}
			encoded, _ := json.Marshal(result)
			for _, private := range []string{upstream.URL, "private_database", "private_user", "private-password", "secret upstream error", "proxy login"} {
				if strings.Contains(string(encoded), private) {
					t.Fatal("status disclosed upstream details")
				}
			}
		})
	}
}

func TestInstanceStatusConfigurationDoesNotProbeDestinations(t *testing.T) {
	s := service(t, 20)
	var sent atomic.Int32
	s.exports = &exportRuntime{}
	s.pipelines = &pipelineRuntime{}
	s.oauth = &oauthRuntime{config: OAuthConfig{Email: &EmailConfig{SendCode: func(context.Context, string, string) error {
		sent.Add(1)
		return nil
	}}}}
	defer func() { s.exports, s.pipelines, s.oauth = nil, nil, nil }()
	result, err := s.getInstanceStatus(context.Background(), &struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	c := result.Body.Configuration
	if !c.QueryExports || !c.WebhookPipelines || !c.EmailSignIn || c.S3QueryPipelines || c.S3Tiering || c.OIDCSignIn || result.Body.Status != "ok" {
		t.Fatalf("incorrect configuration state: %+v", result.Body)
	}
	if sent.Load() != 0 {
		t.Fatal("a status check sent an email")
	}
	// An uninitialized export store and destination client would fail if probed.
	s.oauth.config.Email = nil
	s.oauth.oidc = &interactiveOIDC{}
	result, err = s.getInstanceStatus(context.Background(), &struct{}{})
	if err != nil || result.Body.Configuration.EmailSignIn || !result.Body.Configuration.OIDCSignIn {
		t.Fatal("OIDC and email configuration were conflated")
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "1\n") }))
	defer upstream.Close()
	s.ch, err = newClickHouse(ClickHouseConfig{URL: upstream.URL, StoragePolicy: "private_policy"})
	if err != nil {
		t.Fatal(err)
	}
	result, err = s.getInstanceStatus(context.Background(), &struct{}{})
	if err != nil || !result.Body.Configuration.S3QueryPipelines || !result.Body.Configuration.S3Tiering {
		t.Fatal("configured ClickHouse did not enable S3 pipeline and storage declarations")
	}
}

func TestInstanceStatusBoundedProbesAndConcurrency(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer upstream.Close()
	defer close(release)
	s := service(t, 20)
	var err error
	s.ch, err = newClickHouse(ClickHouseConfig{URL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan *InstanceStatusOutput, 1)
	started := time.Now()
	go func() {
		output, _ := s.getInstanceStatus(context.Background(), &struct{}{})
		done <- output
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("probe did not reach ClickHouse")
	}
	request(t, s, "GET", "/api/v1/status", adminToken, "", 429)
	select {
	case result := <-done:
		if result == nil || result.Body.Checks.ClickHouse != dependencyUnavailable || result.Body.Status != "degraded" {
			t.Fatal("timed-out dependency did not degrade the status")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("status probe exceeded its budget")
	}
	if time.Since(started) > 3*time.Second || len(s.statusSlots) != 0 {
		t.Fatal("status check did not release its bounded slot")
	}
}

func TestInstanceStatusSQLiteFailure(t *testing.T) {
	s := service(t, 20)
	s.db.Close()
	result, err := s.getInstanceStatus(context.Background(), &struct{}{})
	if err != nil || result.Body.Checks.SQLite != dependencyUnavailable || result.Body.Status != "degraded" {
		t.Fatal("failed local storage was reported healthy")
	}
}

func TestInstanceStatusMCPDiscoveryAndRevocation(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	id, reader := key(t, s, []string{"management:read"}, []string{a})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "2025-11-25")
	docs := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: "get-instance-status"}, false)
	if docs["contract"] == nil {
		t.Fatal("instance status contract missing from MCP")
	}
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["status"] != "ok" || result["configuration"] == nil {
		t.Fatal("MCP did not expose the instance status")
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	r.Header.Set("Authorization", "Bearer "+reader)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal("revoked MCP reader retained access")
	}
}

func TestInstanceStatusOAuthAudiences(t *testing.T) {
	f := newOAuthIntegration(t)
	apiResource, mcpResource := f.host.URL+"/api/v1", f.host.URL+"/mcp"
	admin := f.token(t, "bootstrap", adminToken, apiResource)
	a, public := f.project(t, admin.AccessToken)
	b, _ := f.project(t, admin.AccessToken)
	client := f.createClient(t, admin.AccessToken, []string{"management:read"}, []string{a})
	api := f.token(t, client.ClientID, client.ClientSecret, apiResource, "management:read")
	mcpToken := f.token(t, client.ClientID, client.ClientSecret, mcpResource, "management:read")
	var report InstanceStatus
	f.call(t, "GET", "/api/v1/status", api.AccessToken, nil, 200, &report)
	if report.Status != "ok" {
		t.Fatal("scoped OAuth reader could not inspect status")
	}
	f.call(t, "GET", "/api/v1/status", mcpToken.AccessToken, nil, 401, nil)
	f.call(t, "GET", "/api/v1/status", public, nil, 401, nil)
	ctx, session := mcpTestSession(t, f.host.URL, mcpToken.AccessToken, "2025-11-25")
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), a) || strings.Contains(string(encoded), b) {
		t.Fatal("global status leaked project identifiers")
	}
	f.call(t, "DELETE", "/api/v1/oauth-clients/"+client.ClientID, admin.AccessToken, nil, 204, nil)
	f.call(t, "GET", "/api/v1/status", api.AccessToken, nil, 401, nil)
	f.call(t, "POST", "/mcp", mcpToken.AccessToken, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}, 401, nil)
}
