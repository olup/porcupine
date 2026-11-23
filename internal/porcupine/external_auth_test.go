package porcupine

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
	"github.com/olup/porcupine/internal/auth"
)

type externalAuthFixture struct {
	s      *Service
	key    *rsa.PrivateKey
	issuer string
	broken atomic.Bool
	clock  time.Time
	now    atomic.Int64
}

func newExternalAuthFixture(t *testing.T) *externalAuthFixture {
	t.Helper()
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	f := &externalAuthFixture{key: key, clock: time.Now().UTC().Truncate(time.Second)}
	f.now.Store(f.clock.UnixMilli())
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.broken.Load() {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "one", Algorithm: "RS256", Use: "sig"}}})
	}))
	t.Cleanup(issuer.Close)
	f.issuer = issuer.URL + "/realm"
	f.s, e = Open(Config{DBPath: filepath.Join(t.TempDir(), "journal.db"), Clock: func() time.Time { return time.UnixMilli(f.now.Load()) }, Auth: &AuthConfig{Mode: "external", PublicURL: "https://data.test", External: auth.ExternalConfig{Issuer: f.issuer, JWKSURL: issuer.URL + "/keys", APIAudience: "https://data.test/api/v1", MCPAudience: "https://data.test/mcp"}}})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = f.s.Close() })
	return f
}
func (f *externalAuthFixture) token(t *testing.T, subject, tenant, scope, audience string, projects ...string) string {
	t.Helper()
	authority := map[string]any{"tenant_id": tenant, "project_scope": "selected", "projects": projects}
	if tenant == "" {
		authority = map[string]any{"operator": true}
	} else if projects == nil {
		authority = map[string]any{"tenant_id": tenant, "project_scope": "all"}
	}
	now := time.UnixMilli(f.now.Load())
	c := jwt.MapClaims{"iss": f.issuer, "sub": subject, "aud": audience, "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "scope": scope, "porcupine": authority}
	j := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	j.Header["typ"] = "at+jwt"
	j.Header["kid"] = "one"
	raw, e := j.SignedString(f.key)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func (f *externalAuthFixture) operator(t *testing.T) string {
	return f.token(t, "operator", "", "instance:admin", f.s.resources.apiResource)
}
func externalBody(t *testing.T, value any) string {
	t.Helper()
	data, e := json.Marshal(value)
	if e != nil {
		t.Fatal(e)
	}
	return string(data)
}

func TestExternalAuthRoutesTenantIsolationAndMachineKeys(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	enableTestPipelines(t, s)
	if s.oauth != nil || !s.externalAuthEnabled() {
		t.Fatal("external mode initialized builtin authority")
	}
	var accounts int
	if e := s.control.QueryRow(`SELECT count(*) FROM access_keys`).Scan(&accounts); e != nil || accounts != 0 {
		t.Fatalf("fresh external instance has local identities: %d %v", accounts, e)
	}
	for _, route := range []struct{ method, path string }{{"POST", "/oauth/token"}, {"POST", "/oauth/revoke"}, {"GET", "/oauth/authorize"}, {"GET", "/oauth/consent"}, {"POST", "/oauth/consent"}, {"POST", "/oauth/email/request"}, {"POST", "/oauth/email/verify"}, {"POST", "/oauth/oidc/start"}, {"GET", "/oauth/oidc/callback"}, {"GET", "/.well-known/oauth-authorization-server"}, {"GET", "/api/v1/humans"}, {"GET", "/api/v1/oauth-grants"}} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(route.method, route.path, nil))
		if w.Code != 404 {
			t.Fatalf("builtin route remains: %s %s %d", route.method, route.path, w.Code)
		}
	}
	for _, resource := range []string{"/api/v1", "/mcp"} {
		metadata := request(t, s, "GET", "/.well-known/oauth-protected-resource"+resource, "", "", 200)
		if metadata["resource"] != "https://data.test"+resource || metadata["authorization_servers"].([]any)[0] != f.issuer {
			t.Fatal("wrong external metadata", metadata)
		}
	}
	schema := request(t, s, "GET", "/openapi.json", "", "", 200)
	paths := schema["paths"].(map[string]any)
	for path := range paths {
		if strings.Contains(path, "/humans") || strings.Contains(path, "/members") || strings.Contains(path, "/oauth-grants") {
			t.Fatal("builtin management leaked", path)
		}
	}
	operator := f.operator(t)
	tenant := func(name string) string {
		return request(t, s, "POST", "/api/v1/tenants", operator, externalBody(t, map[string]any{"name": name}), 201)["tenant"].(map[string]any)["id"].(string)
	}
	a, b := tenant("A"), tenant("B")
	project := func(tenant string) map[string]any {
		return request(t, s, "POST", "/api/v1/projects", operator, externalBody(t, map[string]any{"name": "project", "tenant_id": tenant}), 201)
	}
	pa, peer, pb := project(a), project(a), project(b)
	idA := pa["project"].(map[string]any)["id"].(string)
	idB := pb["project"].(map[string]any)["id"].(string)
	peerID := peer["project"].(map[string]any)["id"].(string)
	multiReader := f.token(t, "multi-reader", a, "events:read", s.resources.apiResource, idA, peerID)
	for _, id := range []string{idA, peerID} {
		request(t, s, "GET", "/api/v1/projects/"+id+"/events", multiReader, "", 200)
	}
	reader := f.token(t, "reader", a, "management:read events:read", s.resources.apiResource, idA)
	request(t, s, "GET", "/api/v1/projects/"+idA+"/events", reader, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+idB+"/events", reader, "", 403)
	forged := f.token(t, "reader", a, "management:read events:read", s.resources.apiResource, idB)
	request(t, s, "GET", "/api/v1/projects/"+idB+"/events", forged, "", 401)
	mcp := f.token(t, "reader", a, "management:read events:read", s.resources.mcpResource, idA)
	request(t, s, "GET", "/api/v1/projects/"+idA+"/events", mcp, "", 401)
	if _, e := s.authenticateResource(t.Context(), reader, s.resources.mcpResource); e == nil {
		t.Fatal("API token used at MCP")
	}
	admin := f.token(t, "admin-a", a, "tenant:admin management:read management:write events:read events:write pipelines:write", s.resources.apiResource)
	request(t, s, "POST", "/api/v1/projects", admin, externalBody(t, map[string]any{"tenant_id": b, "name": "wrong tenant"}), 403)
	projectWriter := f.token(t, "project-manager", a, "management:write events:write", s.resources.apiResource, idA)
	ingestionGrant := externalBody(t, map[string]any{"name": "ingestion key", "permissions": []string{"events:write"}, "projects": []string{idA}})
	request(t, s, "POST", "/api/v1/access-keys", projectWriter, ingestionGrant, 403)
	request(t, s, "POST", "/api/v1/oauth-clients", projectWriter, ingestionGrant, 403)
	request(t, s, "POST", "/api/v1/oauth-clients", admin, ingestionGrant, 403)
	for _, grant := range []map[string]any{
		{"name": "reader", "permissions": []string{"events:read"}, "projects": []string{idA}},
		{"name": "wildcard writer", "permissions": []string{"events:write"}, "projects": []string{"*"}},
		{"name": "multi-project writer", "permissions": []string{"events:write"}, "projects": []string{idA, peerID}},
	} {
		request(t, s, "POST", "/api/v1/access-keys", operator, externalBody(t, grant), 400)
	}
	ingestionKey := request(t, s, "POST", "/api/v1/access-keys", admin, ingestionGrant, 201)["token"].(string)
	request(t, s, "GET", "/api/v1/projects", ingestionKey, "", 401)
	request(t, s, "POST", "/mcp", ingestionKey, "", 401)
	client := request(t, s, "POST", "/api/v1/tenants/"+a+"/service-accounts", admin, externalBody(t, map[string]any{"name": "Team", "permissions": []string{"events:read", "events:write", "pipelines:write"}, "projects": []string{idA}}), 201)
	if client["client_secret"] != nil {
		t.Fatal("unusable builtin OAuth secret exposed in external mode")
	}
	clientID := client["client_id"].(string)
	key := request(t, s, "POST", "/api/v1/tenants/"+a+"/service-accounts/"+clientID+"/api-keys", admin, externalBody(t, map[string]any{"name": "worker", "audience": "api", "permissions": []string{"events:read", "events:write", "pipelines:write"}, "projects": []string{idA}}), 201)
	native := key["token"].(string)
	request(t, s, "GET", "/api/v1/projects/"+idA+"/events", native, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+peerID+"/events", native, "", 403)
	rejected := request(t, s, "POST", "/api/v1/projects/"+idA+"/pipelines", admin, `{"name":"schedule","kind":"webhook","credentials":{},"webhook":{"endpoint":"https://example.test"}}`, 403)
	if rejected["code"] != "execution_credential_required" {
		t.Fatal("missing execution credential guidance", rejected)
	}
	created := request(t, s, "POST", "/api/v1/projects/"+idA+"/pipelines", native, pipelineBody("https://example.test"), 201)
	stored, err := loadTestPipeline(s, t.Context(), idA, created["id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := s.pipelines.AuthorizeCreator(t.Context(), stored.ActorID, idA); err != nil || !allowed {
		t.Fatal("native execution credential cannot authorize its pipeline", err)
	}
	request(t, s, "DELETE", "/api/v1/tenants/"+a+"/service-accounts/"+clientID+"/api-keys/"+key["id"].(string), admin, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+idA+"/events", native, "", 401)
	if allowed, err := s.pipelines.AuthorizeCreator(t.Context(), stored.ActorID, idA); err != nil || allowed {
		t.Fatal("revoked execution credential still authorizes pipeline", err)
	}
	// An external-AS outage never enters the public SDK acceptance path.
	f.broken.Store(true)
	request(t, s, "POST", "/capture/", "", fmt.Sprintf(`{"api_key":%q,"event":"click","distinct_id":"person"}`, pa["ingest_token"]), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestionKey, "external-ingestion-key"), 200)
	var humans int
	if e := s.control.QueryRow(`SELECT count(*) FROM email_humans`).Scan(&humans); e != nil || humans != 0 {
		t.Fatal("external mode persisted human account", e)
	}
}

func TestExternalAuthConfigIsOfflineAndCannotAdoptBuiltin(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	c := Config{DBPath: filepath.Join(t.TempDir(), "unused.db"), Auth: &AuthConfig{Mode: "external", PublicURL: "https://data.test", External: auth.ExternalConfig{Issuer: "https://offline.test/realm", JWKSURL: "https://offline.test/keys", APIAudience: "https://data.test/api/v1", MCPAudience: "https://data.test/mcp"}}}
	if e := ValidateConfig(c); e != nil {
		t.Fatal(e)
	}
	c.BootstrapToken = adminToken
	if e := ValidateConfig(c); e == nil {
		t.Fatal("external bootstrap secret accepted")
	}
	if e := checkControlAuthMode(t.Context(), f.s.control, false); e == nil {
		t.Fatal("external database adopted by builtin")
	}
	builtin := service(t, 10)
	if e := checkControlAuthMode(t.Context(), builtin.control, true); e == nil {
		t.Fatal("builtin identities adopted by external")
	}
}

func TestExternalMCPReauthenticatesEveryRequest(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	p := request(t, s, "POST", "/api/v1/projects", f.operator(t), `{"name":"MCP"}`, 201)
	id := p["project"].(map[string]any)["id"].(string)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	token := f.token(t, "reader", defaultTenantID, "management:read events:read", s.resources.mcpResource, id)
	ctx, session := mcpTestSession(t, host.URL, token, "")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": id}}, 200)
	docs := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{Limit: 20}, false)
	for _, op := range docs["operations"].([]any) {
		id := op.(map[string]any)["operation_id"].(string)
		if strings.Contains(id, "human") || strings.Contains(id, "oauth-grant") || strings.Contains(id, "tenant-member") {
			t.Fatal("MCP exposed disabled builtin authority", id)
		}
	}
	f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli())
	if _, err := session.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatal("MCP session retained expired external authority", err)
	}
	f.broken.Store(true)
	// Past the stale-key grace, an outage fails closed.
	f.now.Store(f.clock.Add(2 * time.Hour).UnixMilli())
	fresh := f.token(t, "reader", defaultTenantID, "management:read events:read", s.resources.mcpResource, id)
	r := httptest.NewRequest("POST", "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"test","version":"1"},"capabilities":{}}}`))
	r.Header.Set("Authorization", "Bearer "+fresh)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", "application/json, text/event-stream")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 503 {
		t.Fatalf("external outage should be retryable: status %d", w.Code)
	}
	request(t, s, "POST", "/capture/", "", captureBody(p["ingest_token"].(string), "public-during-outage"), 200)
}

func TestExternalAuthLoopbackDevelopmentURLs(t *testing.T) {
	t.Parallel()
	for _, origin := range []string{"http://localhost:9080", "http://127.0.0.2:9080", "http://[::1]:9080", "https://data.example.eu"} {
		c := Config{Auth: &AuthConfig{Mode: "external", PublicURL: origin, External: auth.ExternalConfig{Issuer: origin + "/realm", JWKSURL: origin + "/keys", APIAudience: "api", MCPAudience: "mcp"}}}
		if err := ValidateConfig(c); err != nil {
			t.Fatal("explicit loopback development URL rejected", origin, err)
		}
	}
	for _, field := range []string{"public", "issuer", "jwks"} {
		c := Config{Auth: &AuthConfig{Mode: "external", PublicURL: "https://data.test", External: auth.ExternalConfig{Issuer: "https://identity.test/realm", JWKSURL: "https://identity.test/keys", APIAudience: "api", MCPAudience: "mcp"}}}
		switch field {
		case "public":
			c.Auth.PublicURL = "http://data.example.eu"
		case "issuer":
			c.Auth.External.Issuer = "http://identity.example.eu/realm"
		case "jwks":
			c.Auth.External.JWKSURL = "http://identity.example.eu/keys"
		}
		if err := ValidateConfig(c); err == nil {
			t.Fatal("remote HTTP accepted", field)
		}
	}
}

func TestExternalNativeOperatorSurvivesIssuerOutage(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	operator := f.operator(t)
	account := request(t, s, "POST", "/api/v1/oauth-clients", operator, `{"name":"Operator","permissions":["instance:admin"],"projects":["*"]}`, 201)
	if account["client_secret"] != nil {
		t.Fatal("external machine account exposes local OAuth secret")
	}
	native := request(t, s, "POST", "/api/v1/oauth-clients/"+account["client_id"].(string)+"/api-keys", operator, `{"name":"Operator key","audience":"api","permissions":["instance:admin"],"projects":["*"]}`, 201)["token"].(string)
	f.broken.Store(true)
	f.now.Store(f.clock.Add(6 * time.Minute).UnixMilli())
	// The last good key set covers a short outage past cache expiry, then fails closed.
	request(t, s, "GET", "/api/v1/status", f.operator(t), "", 200)
	f.now.Store(f.clock.Add(2 * time.Hour).UnixMilli())
	fresh := f.operator(t)
	request(t, s, "GET", "/api/v1/status", fresh, "", 503)
	status := request(t, s, "GET", "/api/v1/status", native, "", 200)
	if status["status"] != "degraded" || status["checks"].(map[string]any)["external_authentication"] != "unavailable" {
		t.Fatal("external key outage hidden from component status", status)
	}
	request(t, s, "POST", "/api/v1/tenants", native, `{"name":"Native operator while issuer unavailable"}`, 201)
	if _, err := s.authenticateResource(t.Context(), native, s.resources.mcpResource); err == nil {
		t.Fatal("native operator API key crossed into MCP")
	}
}

func TestOfflineSchemaKeepsNativeCredentialOperations(t *testing.T) {
	t.Parallel()
	operations := managementOperations(ManagementOpenAPI())
	for _, id := range []string{"create-service-api-key", "create-tenant-service-account", "create-tenant-service-api-key", "list-tenant-service-api-keys", "update-tenant-service-account", "revoke-tenant-service-account"} {
		if operations[id] == nil {
			t.Fatal("offline CLI contract lost native credential operation", id)
		}
	}
}
