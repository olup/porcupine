package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestInstanceStatusHTTPPermissionsAndPrivacy(t *testing.T) {
	t.Parallel()
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
	if checks["sqlite"] != "ok" || checks["ducklake"] != "not_configured" || len(checks) != 2 {
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

func instanceStatusReaderContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, actorContextKey{}, auth.Principal{ID: "status-reader", Permissions: []string{"management:read"}, Projects: []string{"*"}})
}

func instanceStatusNativeStore(t *testing.T, s *Service) *lakeapi.DuckLakeStore {
	t.Helper()
	store := openLakeStorageTest(t, filepath.Join(t.TempDir(), "private-native-archive"), 1)
	s.lake = store
	s.lakeMemoryMB = 128
	return store
}

func TestInstanceStatusNativeChecks(t *testing.T) {
	t.Parallel()
	for _, state := range []string{"healthy", "closed", "missing_registry"} {
		t.Run(state, func(t *testing.T) {
			s := service(t, 20)
			store := instanceStatusNativeStore(t, s)
			source, err := store.SourceID(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch state {
			case "closed":
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			case "missing_registry":
				filename := filepath.Join(store.Root, "registry.sqlite")
				moved := filename + ".saved"
				if err := os.Rename(filename, moved); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Rename(moved, filename); err != nil {
						t.Error(err)
					}
				}()
			}
			result := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
			checks := result["checks"].(map[string]any)
			want := string(dependencyUnavailable)
			if state == "healthy" {
				want = string(dependencyOK)
			}
			if checks["sqlite"] != "ok" || checks["ducklake"] != want || len(checks) != 2 {
				t.Fatalf("wrong native dependency state: %+v", result)
			}
			if (result["status"] == "ok") != (state == "healthy") {
				t.Fatal("overall health disagrees with native check")
			}
			if result["configuration"].(map[string]any)["analytics"] != true {
				t.Fatal("configured native store disappeared from configuration after a failed probe")
			}
			encoded, _ := json.Marshal(result)
			for _, private := range []string{store.Root, source, "registry.sqlite", "DuckLake is closed", "source registry changed"} {
				if strings.Contains(string(encoded), private) {
					t.Fatal("status disclosed private storage details")
				}
			}
			if state == "missing_registry" {
				if _, err := os.Stat(filepath.Join(store.Root, "registry.sqlite")); !os.IsNotExist(err) {
					t.Fatal("status recreated a missing source registry")
				}
			}
		})
	}
}

func TestInstanceStatusConfigurationDoesNotProbeDestinations(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	var sent atomic.Int32
	s.exports = &exportRuntime{}
	s.pipelines = &pipelineRuntime{}
	s.oauth = &oauthRuntime{config: OAuthConfig{Email: &EmailConfig{SendCode: func(context.Context, string, string) error {
		sent.Add(1)
		return nil
	}}}}
	defer func() { s.exports, s.pipelines, s.oauth = nil, nil, nil }()
	result, err := s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	c := result.Body.Configuration
	if !c.QueryExports || !c.WebhookPipelines || !c.EmailSignIn || c.S3QueryPipelines || c.Analytics || c.NativeS3Storage || c.OIDCSignIn || result.Body.Status != "ok" {
		t.Fatalf("incorrect configuration state: %+v", result.Body)
	}
	if sent.Load() != 0 {
		t.Fatal("a status check sent an email")
	}
	// An uninitialized export store and destination client would fail if probed.
	s.oauth.config.Email = nil
	s.oauth.oidc = &interactiveOIDC{}
	result, err = s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil || result.Body.Configuration.EmailSignIn || !result.Body.Configuration.OIDCSignIn {
		t.Fatal("OIDC and email configuration were conflated")
	}
	instanceStatusNativeStore(t, s)
	result, err = s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil || !result.Body.Configuration.Analytics || !result.Body.Configuration.S3QueryPipelines || result.Body.Configuration.NativeS3Storage {
		t.Fatal("native analytics configuration did not enable S3 query pipelines independently of analytical object storage", err)
	}
	if sent.Load() != 0 {
		t.Fatal("native status check sent email")
	}
}

func TestInstanceStatusBoundedProbesAndConcurrency(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	store := instanceStatusNativeStore(t, s)
	if err := store.Mu.Lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			store.Mu.Unlock()
		}
	}()
	done := make(chan *InstanceStatusOutput, 1)
	started := time.Now()
	go func() {
		output, _ := s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
		done <- output
	}()
	enteredDeadline := time.Now().Add(time.Second)
	for len(s.statusSlots) == 0 {
		if time.Now().After(enteredDeadline) {
			t.Fatal("status probe did not take its admission slot")
		}
		time.Sleep(time.Millisecond)
	}
	request(t, s, "GET", "/api/v1/status", adminToken, "", 429)
	select {
	case result := <-done:
		if result == nil || result.Body.Checks.DuckLake != dependencyUnavailable || result.Body.Checks.SQLite != dependencyOK || result.Body.Status != "degraded" {
			t.Fatal("timed-out native source did not degrade status")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("native source probe exceeded its budget")
	}
	if time.Since(started) > 3*time.Second || len(s.statusSlots) != 0 {
		t.Fatal("status did not release its bounded slot")
	}
	store.Mu.Unlock()
	locked = false
	result, err := s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil || result.Body.Checks.DuckLake != dependencyOK || result.Body.Status != "ok" {
		t.Fatal("cancelled probe consumed the native gate", err)
	}
}

func TestInstanceStatusCanceledContext(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	instanceStatusNativeStore(t, s)
	ctx, cancel := context.WithCancel(instanceStatusReaderContext(context.Background()))
	cancel()
	started := time.Now()
	result, err := s.getInstanceStatus(ctx, &struct{}{})
	if err != nil || result.Body.Checks.SQLite != dependencyUnavailable || result.Body.Checks.DuckLake != dependencyUnavailable || result.Body.Status != "degraded" {
		t.Fatal("cancelled context reported healthy dependencies", err)
	}
	if time.Since(started) > time.Second || len(s.statusSlots) != 0 {
		t.Fatal("cancelled status check exceeded its budget or retained admission")
	}
}

func TestInstanceStatusSQLiteFailure(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	s.db.Close()
	result, err := s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil || result.Body.Checks.SQLite != dependencyUnavailable || result.Body.Status != "degraded" {
		t.Fatal("failed local storage was reported healthy")
	}
}

func TestInstanceStatusMCPDiscoveryAndRevocation(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
