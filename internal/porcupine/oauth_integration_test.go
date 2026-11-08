package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/time/rate"
)

type oauthIntegration struct {
	s      *Service
	host   *httptest.Server
	now    atomic.Int64
	client *http.Client
	ctx    context.Context
}

func newOAuthIntegration(t *testing.T) *oauthIntegration {
	t.Helper()
	fixture := &oauthIntegration{host: httptest.NewUnstartedServer(nil), client: &http.Client{Timeout: 5 * time.Second}}
	fixture.now.Store(time.Now().UTC().UnixNano())
	issuer := "http://" + fixture.host.Listener.Addr().String()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "oauth.db"), BootstrapToken: adminToken, Clock: func() time.Time { return time.Unix(0, fixture.now.Load()).UTC() }, OAuth: &OAuthConfig{Issuer: issuer, AccessTokenLifetimeSeconds: 60}})
	if err != nil {
		fixture.host.Close()
		t.Fatal(err)
	}
	fixture.s = s
	// Protocol unit tests cover brute-force limits. Integration exercises distinct grants.
	s.oauth.limiter = rate.NewLimiter(rate.Inf, 1)
	fixture.host.Config.Handler = s.Handler()
	fixture.host.Start()
	fixture.ctx = context.WithValue(context.Background(), oauth2.HTTPClient, fixture.client)
	t.Cleanup(func() { fixture.host.Close(); fixture.client.CloseIdleConnections(); s.Close() })
	if fixture.host.URL != issuer {
		t.Fatal("OAuth test issuer differs from real HTTP origin")
	}
	return fixture
}

func (f *oauthIntegration) credentials(id, secret, resource string, scopes ...string) *clientcredentials.Config {
	return &clientcredentials.Config{ClientID: id, ClientSecret: secret, TokenURL: f.host.URL + "/oauth/token", AuthStyle: oauth2.AuthStyleInHeader, Scopes: scopes, EndpointParams: url.Values{"resource": {resource}}}
}
func (f *oauthIntegration) token(t *testing.T, id, secret, resource string, scopes ...string) *oauth2.Token {
	t.Helper()
	token, err := f.credentials(id, secret, resource, scopes...).Token(f.ctx)
	if err != nil {
		t.Fatal("standard client_credentials token exchange failed")
	}
	if token.AccessToken == "" || !strings.EqualFold(token.TokenType, "Bearer") || token.RefreshToken != "" {
		t.Fatal("invalid machine access token response")
	}
	return token
}
func (f *oauthIntegration) call(t *testing.T, method, path, bearer string, body any, status int, output any) http.Header {
	t.Helper()
	var encoded []byte
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal("encode OAuth test request")
		}
	}
	request, err := http.NewRequest(method, f.host.URL+path, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal("create OAuth test request")
	}
	request.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		request.Header.Set("Authorization", "Bearer "+bearer)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal("OAuth test HTTP request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		t.Fatalf("%s %s returned %d, want %d", method, path, response.StatusCode, status)
	}
	if output != nil && json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(output) != nil {
		t.Fatal("invalid OAuth test HTTP JSON")
	}
	return response.Header.Clone()
}
func (f *oauthIntegration) project(t *testing.T, admin string) (string, string) {
	t.Helper()
	var response struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		Token string `json:"ingest_token"`
	}
	f.call(t, "POST", "/api/v1/projects", admin, map[string]string{"name": "OAuth project"}, 201, &response)
	if response.Project.ID == "" || response.Token == "" {
		t.Fatal("project creation missing ingestion key")
	}
	return response.Project.ID, response.Token
}
func (f *oauthIntegration) createClient(t *testing.T, admin string, permissions, projects []string) OAuthClientCreated {
	t.Helper()
	var created OAuthClientCreated
	headers := f.call(t, "POST", "/api/v1/oauth-clients", admin, map[string]any{"name": "OAuth integration client", "permissions": permissions, "projects": projects}, 201, &created)
	if created.ClientID == "" || created.ClientSecret == "" || headers.Get("Cache-Control") != "no-store" {
		t.Fatal("OAuth client creation omitted one-time credential or cache protection")
	}
	return created
}

func TestOAuthIntegrationClientCredentialsHTTPAndMCP(t *testing.T) {
	f := newOAuthIntegration(t)
	apiResource, mcpResource := f.host.URL+"/api/v1", f.host.URL+"/mcp"
	admin := f.token(t, "bootstrap", adminToken, apiResource)
	a, publicA := f.project(t, admin.AccessToken)
	b, publicB := f.project(t, admin.AccessToken)
	created := f.createClient(t, admin.AccessToken, []string{"events:read", "events:write", "management:read", "management:write"}, []string{a})
	read := f.token(t, created.ClientID, created.ClientSecret, apiResource, "events:read")
	write := f.token(t, created.ClientID, created.ClientSecret, apiResource, "events:write")
	broad := f.token(t, created.ClientID, created.ClientSecret, apiResource)
	path := "/api/v1/projects/" + a + "/events"
	f.call(t, "GET", path, read.AccessToken, nil, 200, nil)
	f.call(t, "GET", "/api/v1/projects/"+b+"/events", read.AccessToken, nil, 403, nil)
	f.call(t, "GET", "/api/v1/projects", read.AccessToken, nil, 403, nil)
	f.call(t, "GET", "/api/v1/projects", broad.AccessToken, nil, 200, nil)
	f.call(t, "GET", path, write.AccessToken, nil, 403, nil)
	// Explicit OAuth configuration rejects long-lived secrets as API/MCP bearers.
	for _, secret := range []string{adminToken, created.ClientSecret, publicA} {
		f.call(t, "GET", path, secret, nil, 401, nil)
		f.call(t, "GET", "/mcp", secret, nil, 401, nil)
	}
	f.call(t, "POST", "/api/v1/oauth-clients", read.AccessToken, map[string]any{"name": "forbidden", "permissions": []string{"events:read"}, "projects": []string{a}}, 403, nil)
	f.call(t, "POST", "/api/v1/oauth-clients", broad.AccessToken, map[string]any{"name": "escalation", "permissions": []string{"instance:admin"}, "projects": []string{"*"}}, 403, nil)
	f.call(t, "POST", "/api/v1/oauth-clients", broad.AccessToken, map[string]any{"name": "wrong project", "permissions": []string{"events:read"}, "projects": []string{b}}, 403, nil)
	if _, err := f.credentials(created.ClientID, created.ClientSecret, apiResource, "instance:admin").Token(f.ctx); err == nil {
		t.Fatal("OAuth scope widened client grants")
	}
	if _, err := f.credentials(a, publicA, apiResource).Token(f.ctx); err == nil {
		t.Fatal("public ingestion key exchanged for OAuth access")
	}
	if _, err := f.credentials("bootstrap", publicA, apiResource).Token(f.ctx); err == nil {
		t.Fatal("public ingestion key authenticated bootstrap alias")
	}

	event := func(id string) map[string]any {
		return map[string]any{"uuid": id, "event": "backend capture", "distinct_id": "visitor-not-client-id", "properties": map[string]string{"unicode": "東京 🦔"}}
	}
	f.call(t, "POST", "/capture/", write.AccessToken, event("oauth-backend"), 200, nil)
	withSelector := event("oauth-selected")
	withSelector["api_key"] = publicA
	f.call(t, "POST", "/capture/", write.AccessToken, withSelector, 200, nil)
	wrongSelector := event("wrong-selector")
	wrongSelector["api_key"] = publicB
	f.call(t, "POST", "/capture/", write.AccessToken, wrongSelector, 401, nil)
	f.call(t, "POST", "/capture/", read.AccessToken, event("read-cannot-write"), 403, nil)
	publicEvent := event("public-browser")
	publicEvent["api_key"] = publicA
	f.call(t, "POST", "/capture/", "", publicEvent, 200, nil)
	secretEvent := event("secret-not-ingestion")
	secretEvent["api_key"] = created.ClientSecret
	f.call(t, "POST", "/capture/", "", secretEvent, 401, nil)
	multi := f.createClient(t, admin.AccessToken, []string{"events:write"}, []string{a, b})
	multiToken := f.token(t, multi.ClientID, multi.ClientSecret, apiResource)
	f.call(t, "POST", "/capture/", multiToken.AccessToken, event("ambiguous-project"), 403, nil)
	selected := event("multi-selected")
	selected["api_key"] = publicB
	f.call(t, "POST", "/capture/", multiToken.AccessToken, selected, 200, nil)
	var captured struct {
		Events []Event `json:"events"`
	}
	f.call(t, "GET", path, read.AccessToken, nil, 200, &captured)
	if len(captured.Events) != 3 {
		t.Fatal("OAuth ingestion persisted wrong project event count")
	}
	for _, saved := range captured.Events {
		if saved.DistinctID != "visitor-not-client-id" || saved.ProjectID != a {
			t.Fatal("OAuth client identity replaced event distinct ID or project scope")
		}
	}

	// The official MCP client obtains its token through the standard OAuth HTTP client.
	mcpHTTP := f.credentials(created.ClientID, created.ClientSecret, mcpResource, "events:read").Client(f.ctx)
	mcpClient := mcp.NewClient(&mcp.Implementation{Name: "oauth-integration", Version: "1"}, nil)
	session, err := mcpClient.Connect(f.ctx, &mcp.StreamableClientTransport{Endpoint: f.host.URL + "/mcp", HTTPClient: mcpHTTP, DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatal("real MCP client failed with MCP-scoped OAuth token")
	}
	defer session.Close()
	result := mcpTestCall(t, f.ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": a}}, 200)
	if len(result["events"].([]any)) != 3 {
		t.Fatal("MCP OAuth internal dispatch lost events:read grants")
	}
	mcpTestCall(t, f.ctx, session, mcpCallInput{OperationID: "list-project-events", Path: map[string]string{"id": b}}, 403)
	mcpToken := f.token(t, created.ClientID, created.ClientSecret, mcpResource, "events:read")
	f.call(t, "GET", path, mcpToken.AccessToken, nil, 401, nil)
	f.call(t, "GET", "/mcp", read.AccessToken, nil, 401, nil)
	f.call(t, "POST", "/capture/", mcpToken.AccessToken, event("mcp-audience-write"), 401, nil)
	wrongClient := mcp.NewClient(&mcp.Implementation{Name: "wrong-audience", Version: "1"}, nil)
	wrongSession, err := wrongClient.Connect(f.ctx, &mcp.StreamableClientTransport{Endpoint: f.host.URL + "/mcp", HTTPClient: oauth2.NewClient(f.ctx, oauth2.StaticTokenSource(read)), DisableStandaloneSSE: true, MaxRetries: -1}, nil)
	if err == nil {
		wrongSession.Close()
		t.Fatal("MCP SDK accepted API-audience access token")
	}

	// Tokens and one-time client secrets are persisted only as hashes.
	var secretHash, bearerHash string
	if err := f.s.db.QueryRow(`SELECT c.secret_hash,k.token_hash FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id WHERE c.client_id=?`, created.ClientID).Scan(&secretHash, &bearerHash); err != nil {
		t.Fatal(err)
	}
	if secretHash != hash(created.ClientSecret) || secretHash == created.ClientSecret || bearerHash == hash(created.ClientSecret) {
		t.Fatal("client secret persisted in plaintext or reused as bearer key")
	}
	var rawTokens, hashedTokens int
	if err := f.s.db.QueryRow(`SELECT count(*) FROM oauth_tokens WHERE token_hash=?`, read.AccessToken).Scan(&rawTokens); err != nil {
		t.Fatal(err)
	}
	if err := f.s.db.QueryRow(`SELECT count(*) FROM oauth_tokens WHERE token_hash=?`, hash(read.AccessToken)).Scan(&hashedTokens); err != nil {
		t.Fatal(err)
	}
	if rawTokens != 0 || hashedTokens != 1 {
		t.Fatal("access token hash storage contract failed")
	}
	var listed map[string]any
	f.call(t, "GET", "/api/v1/oauth-clients", admin.AccessToken, nil, 200, &listed)
	encoded, _ := json.Marshal(listed)
	if bytes.Contains(encoded, []byte(created.ClientSecret)) || bytes.Contains(encoded, []byte("client_secret")) || bytes.Contains(encoded, []byte("secret_hash")) {
		t.Fatal("OAuth client metadata exposed secret material")
	}

	// A queued export records the durable principal, independent of any token's lifespan.
	f.s.exports = &exportRuntime{config: ExportConfig{MaxQueuedJobs: 3}, wake: make(chan struct{}, 1)}
	var job ExportJob
	f.call(t, "POST", "/api/v1/projects/"+a+"/exports", read.AccessToken, ExportCreateBody{IdempotencyKey: "oauth-owner", SQL: "SELECT count() FROM events", Format: "jsonl"}, 202, &job)
	var owner string
	if err := f.s.db.QueryRow(`SELECT actor_id FROM export_jobs WHERE id=?`, job.ID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != created.ClientID || owner == read.AccessToken {
		t.Fatal("export creator is not the stable OAuth principal")
	}

	// Current grants can remove permissions immediately from previously issued tokens.
	if _, err := f.s.db.Exec(`UPDATE access_keys SET permissions='["events:read"]' WHERE id=?`, created.ClientID); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", "/capture/", write.AccessToken, event("removed-write"), 403, nil)
	f.call(t, "GET", path, read.AccessToken, nil, 200, nil)
	projects, _ := json.Marshal([]string{b})
	if _, err := f.s.db.Exec(`UPDATE access_keys SET projects=? WHERE id=?`, string(projects), created.ClientID); err != nil {
		t.Fatal(err)
	}
	f.call(t, "GET", path, read.AccessToken, nil, 403, nil)
	projects, _ = json.Marshal([]string{a})
	if _, err := f.s.db.Exec(`UPDATE access_keys SET projects=? WHERE id=?`, string(projects), created.ClientID); err != nil {
		t.Fatal(err)
	}
	f.call(t, "DELETE", "/api/v1/oauth-clients/"+created.ClientID, admin.AccessToken, nil, 204, nil)
	f.call(t, "GET", path, read.AccessToken, nil, 401, nil)
	if _, err := f.credentials(created.ClientID, created.ClientSecret, apiResource).Token(f.ctx); err == nil {
		t.Fatal("revoked OAuth client obtained new token")
	}
	if _, err := session.ListTools(f.ctx, nil); err == nil {
		t.Fatal("revoked OAuth client retained active MCP access")
	}
}

func TestOAuthIntegrationExpiryAndDiscovery(t *testing.T) {
	f := newOAuthIntegration(t)
	resource := f.host.URL + "/api/v1"
	admin := f.token(t, "bootstrap", adminToken, resource)
	a, _ := f.project(t, admin.AccessToken)
	created := f.createClient(t, admin.AccessToken, []string{"events:read"}, []string{a})
	token := f.token(t, created.ClientID, created.ClientSecret, resource)
	f.call(t, "GET", "/api/v1/projects/"+a+"/events", token.AccessToken, nil, 200, nil)
	f.s.exports = &exportRuntime{config: ExportConfig{MaxQueuedJobs: 3}, wake: make(chan struct{}, 1)}
	var queued ExportJob
	f.call(t, "POST", "/api/v1/projects/"+a+"/exports", token.AccessToken, ExportCreateBody{IdempotencyKey: "expiring-token-owner", SQL: "SELECT count() FROM events", Format: "jsonl"}, 202, &queued)
	f.now.Add(int64(61 * time.Second))
	f.call(t, "GET", "/api/v1/projects/"+a+"/events", token.AccessToken, nil, 401, nil)
	var owner string
	if err := f.s.db.QueryRow(`SELECT actor_id FROM export_jobs WHERE id=?`, queued.ID).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != created.ClientID {
		t.Fatal("queued export principal changed when bearer token expired")
	}
	fresh := f.token(t, created.ClientID, created.ClientSecret, resource)
	f.call(t, "GET", "/api/v1/projects/"+a+"/events", fresh.AccessToken, nil, 200, nil)
	for path, field := range map[string]string{"/.well-known/oauth-authorization-server": "issuer", "/.well-known/oauth-protected-resource/api/v1": "resource", "/.well-known/oauth-protected-resource/mcp": "resource"} {
		request, _ := http.NewRequest("GET", f.host.URL+path, nil)
		request.Host = "poisoned.example"
		request.Header.Set("X-Forwarded-Host", "poisoned.example")
		request.Header.Set("X-Forwarded-Proto", "https")
		response, err := f.client.Do(request)
		if err != nil {
			t.Fatal("OAuth discovery request failed")
		}
		var body map[string]any
		decodeErr := json.NewDecoder(response.Body).Decode(&body)
		response.Body.Close()
		if response.StatusCode != 200 || decodeErr != nil || !strings.HasPrefix(body[field].(string), f.host.URL) || strings.Contains(body[field].(string), "poisoned") {
			t.Fatal("discovery trusted request host instead of configured issuer")
		}
		if field == "issuer" && body["token_endpoint"] != f.host.URL+"/oauth/token" {
			t.Fatal("discovery token endpoint differs from pinned issuer")
		}
	}
	request, _ := http.NewRequest("GET", f.host.URL+"/api/v1/projects", nil)
	request.Host = "poisoned.example"
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal("OAuth resource challenge request failed")
	}
	response.Body.Close()
	challenge := response.Header.Get("WWW-Authenticate")
	if response.StatusCode != 401 || !strings.Contains(challenge, f.host.URL+"/.well-known/oauth-protected-resource/api/v1") || strings.Contains(challenge, "poisoned") {
		t.Fatal("resource challenge used untrusted host")
	}
}
