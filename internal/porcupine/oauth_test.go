package porcupine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/time/rate"
)

func oauthProtocolService(t *testing.T) (*Service, *atomic.Int64) {
	t.Helper()
	clock := &atomic.Int64{}
	clock.Store(time.Now().UTC().UnixMilli())
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "oauth.db"), BootstrapToken: adminToken, Clock: func() time.Time { return time.UnixMilli(clock.Load()) }, OAuth: &OAuthConfig{Issuer: "https://oauth.example", AllowLegacyKeys: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.oauth.limiter = rate.NewLimiter(rate.Inf, 1000)
	return s, clock
}

func oauthProtocolClient(t *testing.T, s *Service, permissions, projects []string) (string, string) {
	t.Helper()
	id := newID()
	secret := "secret+percent:% reserved-character-value"
	encodedPermissions, _ := json.Marshal(permissions)
	encodedProjects, _ := json.Marshal(projects)
	_, err := s.db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'OAuth fixture',?,?,?,?)`, id, hash("unreturned-principal-"+id), string(encodedPermissions), string(encodedProjects), now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO oauth_clients(client_id,secret_hash) VALUES(?,?)`, id, hash(secret)); err != nil {
		t.Fatal(err)
	}
	return id, secret
}

func oauthProtocolRequest(t *testing.T, s *Service, id, secret string, form url.Values, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest("POST", "/oauth/token", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetBasicAuth(url.QueryEscape(id), url.QueryEscape(secret))
	w := httptest.NewRecorder()
	s.oauthToken(w, r)
	if w.Code != status {
		t.Fatalf("token endpoint got %d want %d: %s", w.Code, status, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
		t.Fatal("OAuth response was cacheable")
	}
	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("token endpoint granted browser CORS")
	}
	if strings.Contains(w.Body.String(), secret) || strings.Contains(w.Body.String(), "token_hash") {
		t.Fatal("OAuth response exposed credentials")
	}
	if status == 401 && !strings.HasPrefix(w.Header().Get("WWW-Authenticate"), "Basic ") {
		t.Fatal("invalid client response missing Basic challenge")
	}
	var result map[string]any
	if json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatal("invalid OAuth JSON")
	}
	return result
}

func oauthGrantForm() url.Values { return url.Values{"grant_type": {"client_credentials"}} }

func TestOAuthStandardClientCredentialsAndHashOnlyPersistence(t *testing.T) {
	s, _ := oauthProtocolService(t)
	projectID, _ := project(t, s)
	clientID, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{projectID})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	config := clientcredentials.Config{ClientID: clientID, ClientSecret: secret, TokenURL: host.URL + "/oauth/token", Scopes: []string{"events:read"}, AuthStyle: oauth2.AuthStyleInHeader}
	token, err := config.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if token.TokenType != "Bearer" || !strings.HasPrefix(token.AccessToken, "pc_at_") || token.RefreshToken != "" {
		t.Fatal("invalid client credentials token shape")
	}
	random, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token.AccessToken, "pc_at_"))
	if err != nil || len(random) != 32 {
		t.Fatal("access token lacks 256 random bits")
	}
	principal, err := s.authenticateOAuthToken(context.Background(), token.AccessToken, s.oauth.apiResource)
	if err != nil || principal.ID != clientID || !authorize(principal, "events:read", projectID) || principal.admin() {
		t.Fatal("OAuth token lost original scoped principal", err)
	}
	request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", token.AccessToken, "", 200)
	other, _ := project(t, s)
	request(t, s, "GET", "/api/v1/projects/"+other+"/events", token.AccessToken, "", 403)
	if _, err = s.authenticateResource(context.Background(), secret, s.oauth.apiResource); err == nil {
		t.Fatal("client secret worked as a bearer key")
	}
	var storedHash, storedClient, resource, scope string
	var issued, expires int64
	if err = s.db.QueryRow(`SELECT token_hash,client_id,resource,scope,issued_at_ms,expires_at_ms FROM oauth_tokens`).Scan(&storedHash, &storedClient, &resource, &scope, &issued, &expires); err != nil {
		t.Fatal(err)
	}
	if storedHash != hash(token.AccessToken) || storedHash == token.AccessToken || storedClient != clientID || resource != s.oauth.apiResource || scope != "events:read" || expires-issued != 900000 {
		t.Fatal("unsafe or incorrect persisted token")
	}
	var secretHash, principalHash string
	if err = s.db.QueryRow(`SELECT c.secret_hash,k.token_hash FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id WHERE c.client_id=?`, clientID).Scan(&secretHash, &principalHash); err != nil {
		t.Fatal(err)
	}
	if secretHash != hash(secret) || principalHash == secretHash {
		t.Fatal("OAuth secret reused as legacy credential")
	}
}

func TestOAuthResourceExpiryRevocationAndLivePermissionIntersection(t *testing.T) {
	s, clock := oauthProtocolService(t)
	projectID, _ := project(t, s)
	otherProject, _ := project(t, s)
	id, secret := oauthProtocolClient(t, s, []string{"events:read", "events:write"}, []string{projectID})
	form := oauthGrantForm()
	form.Set("scope", "events:read")
	issued := oauthProtocolRequest(t, s, id, secret, form, 200)
	raw := issued["access_token"].(string)
	if _, err := s.authenticateOAuthToken(context.Background(), raw, s.oauth.mcpResource); err == nil {
		t.Fatal("API token accepted at MCP resource")
	}
	form.Set("resource", s.oauth.mcpResource)
	mcp := oauthProtocolRequest(t, s, id, secret, form, 200)["access_token"].(string)
	if _, err := s.authenticateOAuthToken(context.Background(), mcp, s.oauth.mcpResource); err != nil {
		t.Fatal(err)
	}
	if _, err := s.authenticateOAuthToken(context.Background(), mcp, s.oauth.apiResource); err == nil {
		t.Fatal("MCP token accepted at API resource")
	}
	if _, err := s.db.Exec(`UPDATE access_keys SET permissions='["instance:admin"]',projects='["*"]' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	actor, err := s.authenticateOAuthToken(context.Background(), raw, s.oauth.apiResource)
	if err != nil || actor.admin() || !actor.can("events:read") || actor.can("events:write") {
		t.Fatal("current admin grant escalated a downscoped token", err)
	}
	if !authorize(actor, "events:read", projectID) || authorize(actor, "events:read", otherProject) {
		t.Fatal("new admin project grants widened an old token snapshot")
	}
	request(t, s, "GET", "/api/v1/projects/"+projectID+"/events", raw, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+otherProject+"/events", raw, "", 403)
	newIssued := oauthProtocolRequest(t, s, id, secret, oauthGrantForm(), 200)["access_token"].(string)
	newActor, err := s.authenticateOAuthToken(context.Background(), newIssued, s.oauth.apiResource)
	if err != nil || !authorize(newActor, "events:read", otherProject) {
		t.Fatal("new token did not capture newly authorized scope", err)
	}
	if _, err = s.db.Exec(`UPDATE access_keys SET permissions='["events:write"]' WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	actor, err = s.authenticateOAuthToken(context.Background(), raw, s.oauth.apiResource)
	if err != nil || len(actor.Permissions) != 0 {
		t.Fatal("removed grant remained effective")
	}
	clock.Add(900000)
	if _, err = s.authenticateResource(context.Background(), raw, s.oauth.apiResource); err == nil {
		t.Fatal("expired OAuth token fell back to legacy auth")
	}
	clock.Add(-900000)
	if _, err = s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), id); err != nil {
		t.Fatal(err)
	}
	if _, err = s.authenticateOAuthToken(context.Background(), raw, s.oauth.apiResource); err == nil {
		t.Fatal("revoked client retained access")
	}
	oauthProtocolRequest(t, s, id, secret, form, 401)
}

func TestOAuthBootstrapAliasAndAdminDownscope(t *testing.T) {
	s, _ := oauthProtocolService(t)
	form := oauthGrantForm()
	form.Set("scope", "events:write events:read")
	issued := oauthProtocolRequest(t, s, "bootstrap", adminToken, form, 200)
	if issued["scope"] != "events:read events:write" {
		t.Fatal("scope was not normalized and sorted")
	}
	var bootstrapID string
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&bootstrapID); err != nil {
		t.Fatal(err)
	}
	principal, err := s.authenticateOAuthToken(context.Background(), issued["access_token"].(string), s.oauth.apiResource)
	if err != nil || principal.ID != bootstrapID || principal.admin() || !principal.can("events:read") || principal.can("management:write") {
		t.Fatal("bootstrap downscope changed actor identity or retained administration")
	}
	full := oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 200)["access_token"].(string)
	if _, err = s.db.Exec(`UPDATE access_keys SET permissions='["events:read"]' WHERE id=?`, bootstrapID); err != nil {
		t.Fatal(err)
	}
	principal, err = s.authenticateOAuthToken(context.Background(), full, s.oauth.apiResource)
	if err != nil || principal.admin() || !principal.can("events:read") || principal.can("events:delete") {
		t.Fatal("old admin token bypassed current grant reduction")
	}
	if _, err = s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), bootstrapID); err != nil {
		t.Fatal(err)
	}
	oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 401)
}

func TestOAuthRejectsMixedDuplicateAndUnsupportedRequests(t *testing.T) {
	s, _ := oauthProtocolService(t)
	id, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	cases := []struct {
		body, path, auth string
		status           int
		modify           func(*http.Request)
	}{
		{"grant_type=client_credentials&grant_type=client_credentials", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&scope=events%3Aread&%73cope=events%3Aread", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&resource=a&resource=b", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&client_id=other", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&client_secret=private", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials", "/oauth/token?scope=events:read", "", 400, nil},
		{"grant_type=password", "/oauth/token", "", 400, nil},
		{"grant_type=refresh_token&refresh_token=unused", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&scope=instance%3Aadmin", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&scope=events%3Aunknown", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials&resource=https%3A%2F%2Fevil.example%2Fmcp", "/oauth/token", "", 400, nil},
		{"grant_type=client_credentials", "/oauth/token", "Bearer private", 401, nil},
		{"grant_type=client_credentials", "/oauth/token", "", 401, func(r *http.Request) { r.Header.Add("Authorization", "Basic duplicate") }},
		{"grant_type=client_credentials", "/oauth/token", "", 400, func(r *http.Request) { r.Header.Set("Content-Type", "application/json") }},
		{"grant_type=client_credentials&bad=%zz", "/oauth/token", "", 400, nil},
	}
	for _, test := range cases {
		r := httptest.NewRequest("POST", test.path, strings.NewReader(test.body))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.SetBasicAuth(url.QueryEscape(id), url.QueryEscape(secret))
		if test.auth != "" {
			r.Header.Set("Authorization", test.auth)
		}
		if test.modify != nil {
			test.modify(r)
		}
		w := httptest.NewRecorder()
		s.oauthToken(w, r)
		if w.Code != test.status {
			t.Fatalf("request %s got %d: %s", test.body, w.Code, w.Body.String())
		}
		for _, private := range []string{secret, "evil.example", "private", "token_hash"} {
			if strings.Contains(w.Body.String(), private) {
				t.Fatal("OAuth error reflected private input")
			}
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("OAuth error cacheable")
		}
	}
	oauthProtocolRequest(t, s, id, "wrong-password", oauthGrantForm(), 401)
	large := httptest.NewRequest("POST", "/oauth/token", strings.NewReader(strings.Repeat("a", (16<<10)+1)))
	large.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.oauthToken(w, large)
	if w.Code != 413 {
		t.Fatal("large OAuth form accepted")
	}
}

func TestOAuthEndpointRateLimitAndPublicIngestClientRejection(t *testing.T) {
	s, _ := oauthProtocolService(t)
	_, public := project(t, s)
	var id string
	if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(public)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	oauthProtocolRequest(t, s, id, public, oauthGrantForm(), 401)
	s.oauth.limiter = rate.NewLimiter(0, 1)
	oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 200)
	oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 429)
}

func TestOAuthIgnoresUnknownParametersWithoutLibrarySideEffects(t *testing.T) {
	s, _ := oauthProtocolService(t)
	id, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	form := oauthGrantForm()
	form.Set("extension_future", "private-extension-value")
	form.Set("redirect_uri", "https://evil.example/steal")
	form.Set("code", "not-an-authorization-code")
	form.Set("client_id", id)
	issued := oauthProtocolRequest(t, s, id, secret, form, 200)
	if issued["resource"] != s.oauth.apiResource || issued["scope"] != "events:read" {
		t.Fatal("unrecognized fields changed token authority")
	}
	if _, err := s.authenticateOAuthToken(context.Background(), issued["access_token"].(string), s.oauth.apiResource); err != nil {
		t.Fatal(err)
	}
	form["extension_future"] = []string{"one", "two"}
	oauthProtocolRequest(t, s, id, secret, form, 400)
	form.Del("extension_future")
	form.Set("client_id", "different-client")
	oauthProtocolRequest(t, s, id, secret, form, 400)
	form.Set("client_id", id)
	form.Set("client_assertion", "unsigned-assertion")
	oauthProtocolRequest(t, s, id, secret, form, 400)
}

func TestOAuthInvalidTrafficCannotConsumeIssuanceBudget(t *testing.T) {
	s, _ := oauthProtocolService(t)
	id, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	s.oauth.limiter = rate.NewLimiter(0, 1)
	for range 20 {
		oauthProtocolRequest(t, s, id, "invalid-secret", oauthGrantForm(), 401)
		oauthProtocolRequest(t, s, "unknown-client", "invalid-secret", oauthGrantForm(), 401)
		badScope := oauthGrantForm()
		badScope.Set("scope", "instance:admin")
		oauthProtocolRequest(t, s, id, secret, badScope, 400)
		badResource := oauthGrantForm()
		badResource.Set("resource", "https://other.example/api/v1")
		oauthProtocolRequest(t, s, id, secret, badResource, 400)
		duplicate := oauthGrantForm()
		duplicate["grant_type"] = []string{"client_credentials", "client_credentials"}
		oauthProtocolRequest(t, s, id, secret, duplicate, 400)
	}
	oauthProtocolRequest(t, s, id, secret, oauthGrantForm(), 200)
	oauthProtocolRequest(t, s, id, secret, oauthGrantForm(), 429)
}
