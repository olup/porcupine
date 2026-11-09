package porcupine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
)

func oauthManagementActor(id string, permissions, projects []string) context.Context {
	return context.WithValue(context.Background(), actorContextKey{}, actor{ID: id, Permissions: permissions, Projects: projects})
}

func oauthManagementAdmin(t *testing.T, s *Service) context.Context {
	t.Helper()
	var id string
	if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return oauthManagementActor(id, []string{"instance:admin"}, []string{"*"})
}

func enableOAuthManagementTest(t *testing.T, s *Service) {
	t.Helper()
	if err := s.initializeOAuthClients(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.oauth = &oauthRuntime{}
}

func oauthClientInput(name string, permissions, projects []string) *OAuthClientCreateInput {
	input := &OAuthClientCreateInput{}
	input.Body.Name = name
	input.Body.Permissions = permissions
	input.Body.Projects = projects
	return input
}

func TestOAuthClientSchemaPinsOriginalBootstrap(t *testing.T) {
	s := service(t, 20)
	var original string
	if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE access_keys SET created_at='2000-01-01',revoked_at='2001-01-01' WHERE id=?`, original); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`DELETE FROM metadata WHERE key='oauth_bootstrap_id'`); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ id, permissions, created string }{
		{newID(), `["events:read"]`, "1990-01-01"},
		{newID(), `["instance:admin"]`, "2002-01-01"},
	} {
		if _, err := s.db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'bootstrap admin',?,?,?,?)`, row.id, hash(row.id), row.permissions, `["*"]`, row.created); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.initializeOAuthClients(context.Background()); err != nil {
		t.Fatal(err)
	}
	var alias string
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&alias); err != nil || alias != original {
		t.Fatal("bootstrap alias did not retain the original revoked administrator", err)
	}
	older := newID()
	if _, err := s.db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'bootstrap admin',?,'["instance:admin"]','["*"]','1980-01-01')`, older, hash(older)); err != nil {
		t.Fatal(err)
	}
	if err := s.initializeOAuthClients(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&alias); err != nil || alias != original {
		t.Fatal("existing alias rotated", err)
	}
	if _, err := s.db.Exec(`INSERT INTO oauth_clients(client_id,secret_hash) VALUES('missing-principal','hash')`); err == nil {
		t.Fatal("OAuth client accepted without a principal")
	}
}

func TestOAuthClientSecretIsIndependentAndListingScoped(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	admin := oauthManagementAdmin(t, s)
	enableOAuthManagementTest(t, s)
	created, err := s.managementCreateOAuthClient(admin, oauthClientInput("reader", []string{"events:read"}, []string{a}))
	if err != nil {
		t.Fatal(err)
	}
	secret := created.Body.ClientSecret
	if raw, err := base64.RawURLEncoding.DecodeString(secret); err != nil || len(raw) != 32 {
		t.Fatal("OAuth secret does not contain 256 bits")
	}
	if created.CacheControl != "no-store" {
		t.Fatal("one-time client secret response is cacheable")
	}
	var tokenHash, secretHash string
	if err := s.db.QueryRow(`SELECT k.token_hash,c.secret_hash FROM oauth_clients c JOIN access_keys k ON k.id=c.client_id WHERE c.client_id=?`, created.Body.ClientID).Scan(&tokenHash, &secretHash); err != nil {
		t.Fatal(err)
	}
	if secretHash != hash(secret) || tokenHash == secretHash || tokenHash == secret || len(tokenHash) != 64 {
		t.Fatal("client secret was stored raw or reused as principal bearer secret")
	}
	hidden, err := s.managementCreateOAuthClient(admin, oauthClientInput("both projects", []string{"events:read"}, []string{a, b}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := oauthManagementActor("metadata-reader", []string{"management:read"}, []string{a})
	listed, err := s.managementListOAuthClients(ctx, &struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if !listed.Body.Enabled || len(listed.Body.OAuthClients) != 1 || listed.Body.OAuthClients[0].ClientID != created.Body.ClientID {
		t.Fatal("OAuth metadata project scope incorrect")
	}
	encoded, _ := json.Marshal(listed)
	for _, private := range []string{secret, secretHash, tokenHash, hidden.Body.ClientID, b, `"client_secret":`, `"secret_hash":`, `"token_hash":`} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("client secret or another project's credential metadata leaked")
		}
	}
	for _, item := range listed.Body.OAuthClients {
		if item.ClientID == "bootstrap" {
			t.Fatal("bootstrap alias appeared as registered client")
		}
	}
}

func TestOAuthClientGrantChecksAndAtomicCreation(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	admin := oauthManagementAdmin(t, s)
	enableOAuthManagementTest(t, s)
	manager := oauthManagementActor("manager", []string{"management:write", "events:read"}, []string{a})
	for _, test := range []struct {
		ctx    context.Context
		input  *OAuthClientCreateInput
		status int
	}{
		{manager, oauthClientInput("elevated", []string{"events:write"}, []string{a}), 403},
		{manager, oauthClientInput("admin", []string{"instance:admin"}, []string{"*"}), 403},
		{manager, oauthClientInput("outside", []string{"events:read"}, []string{b}), 403},
		{manager, oauthClientInput("wildcard", []string{"events:read"}, []string{"*"}), 403},
		{manager, oauthClientInput("unknown", []string{"unknown"}, []string{a}), 400},
		{admin, oauthClientInput("unknown project", []string{"events:read"}, []string{"missing"}), 400},
		{admin, oauthClientInput("scoped admin", []string{"instance:admin"}, []string{a}), 400},
		{admin, oauthClientInput(" ", []string{"events:read"}, []string{a}), 400},
	} {
		if _, err := s.managementCreateOAuthClient(test.ctx, test.input); queryStatus(err) != test.status {
			t.Errorf("grant status=%d, want=%d", queryStatus(err), test.status)
		}
	}
	if _, err := s.managementCreateOAuthClient(manager, oauthClientInput("valid", []string{"events:read"}, []string{a})); err != nil {
		t.Fatal("manager could not create subordinate client", err)
	}
	var before, after int
	if err := s.db.QueryRow(`SELECT count(*) FROM access_keys`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`CREATE TRIGGER reject_oauth_client BEFORE INSERT ON oauth_clients BEGIN SELECT RAISE(ABORT,'forced client insert failure'); END`); err != nil {
		t.Fatal(err)
	}
	if output, err := s.managementCreateOAuthClient(admin, oauthClientInput("must rollback", []string{"events:read"}, []string{a})); output != nil || queryStatus(err) != 500 {
		t.Fatal("failed creation returned client credentials", err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM access_keys`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatal("OAuth insert failure left an orphan principal")
	}
}

func TestOAuthClientRevocationAndLastAdministrator(t *testing.T) {
	s := service(t, 20)
	admin := oauthManagementAdmin(t, s)
	bootstrap := principal(admin).ID
	enableOAuthManagementTest(t, s)
	created, err := s.managementCreateOAuthClient(admin, oauthClientInput("oauth administrator", []string{"instance:admin"}, []string{"*"}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := oauthManagementActor(created.Body.ClientID, []string{"instance:admin"}, []string{"*"})
	if _, err := s.managementRevokeKey(ctx, &AccessKeyDeleteInput{ID: bootstrap}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.managementRevokeOAuthClient(ctx, &OAuthClientDeleteInput{ID: created.Body.ClientID}); queryStatus(err) != 409 {
		t.Fatal("last administrator could revoke itself", err)
	}
	second, err := s.managementCreateOAuthClient(ctx, oauthClientInput("replacement", []string{"instance:admin"}, []string{"*"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.managementRevokeOAuthClient(ctx, &OAuthClientDeleteInput{ID: created.Body.ClientID}); err != nil {
		t.Fatal(err)
	}
	var revoked *string
	if err := s.db.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=?`, created.Body.ClientID).Scan(&revoked); err != nil || revoked == nil {
		t.Fatal("revocation did not disable the shared principal", err)
	}
	secondContext := oauthManagementActor(second.Body.ClientID, []string{"instance:admin"}, []string{"*"})
	if _, err := s.managementRevokeOAuthClient(secondContext, &OAuthClientDeleteInput{ID: created.Body.ClientID}); queryStatus(err) != 404 {
		t.Fatal("repeated revocation did not report inactive principal", err)
	}
	if _, err := s.managementRevokeOAuthClient(secondContext, &OAuthClientDeleteInput{ID: bootstrap}); queryStatus(err) != 404 {
		t.Fatal("OAuth route revoked an unregistered principal", err)
	}
	if _, err := s.managementRevokeOAuthClient(secondContext, &OAuthClientDeleteInput{ID: second.Body.ClientID}); queryStatus(err) != 409 {
		t.Fatal("last replacement administrator was not protected", err)
	}
}

func TestOAuthClientManagementDisabledDiscovery(t *testing.T) {
	s := service(t, 20)
	admin := oauthManagementAdmin(t, s)
	if err := s.initializeOAuthClients(context.Background()); err != nil {
		t.Fatal(err)
	}
	if output, err := s.managementCreateOAuthClient(admin, oauthClientInput("disabled", []string{"events:read"}, []string{"*"})); output != nil || queryStatus(err) != 503 {
		t.Fatal("disabled OAuth accepted client creation", err)
	}
	listed, err := s.managementListOAuthClients(admin, &struct{}{})
	if err != nil || listed.Body.Enabled || len(listed.Body.OAuthClients) != 0 {
		t.Fatal("disabled discovery was incorrect", err)
	}
}

func TestOAuthClientHTTPManagementLifecycle(t *testing.T) {
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "oauth.db"), BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: "http://localhost", AllowLegacyKeys: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	a, _ := project(t, s)
	b, _ := project(t, s)
	_, metadataReader := key(t, s, []string{"management:read"}, []string{a})
	_, manager := key(t, s, []string{"management:write", "events:read"}, []string{a})
	input, _ := json.Marshal(map[string]any{"name": "OAuth HTTP reader", "permissions": []string{"events:read"}, "projects": []string{a}})
	request(t, s, "POST", "/api/v1/oauth-clients", metadataReader, string(input), 403)
	w := httptest.NewRecorder()
	r := httptest.NewRequest("POST", "/api/v1/oauth-clients", strings.NewReader(string(input)))
	r.Header.Set("Authorization", "Bearer "+manager)
	s.Handler().ServeHTTP(w, r)
	if w.Code != 201 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("client creation failed or exposed a cacheable secret", w.Code)
	}
	var created OAuthClientCreated
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	request(t, s, "GET", "/api/v1/projects", created.ClientSecret, "", 401)
	request(t, s, "POST", "/capture/", "", captureBody(created.ClientSecret, "secret-is-not-ingestion-key"), 401)
	request(t, s, "DELETE", "/api/v1/oauth-clients/"+created.ClientID, metadataReader, "", 403)
	hiddenBody, _ := json.Marshal(map[string]any{"name": "cross-project client", "permissions": []string{"events:read"}, "projects": []string{a, b}})
	hidden := request(t, s, "POST", "/api/v1/oauth-clients", adminToken, string(hiddenBody), 201)
	listed := request(t, s, "GET", "/api/v1/oauth-clients", metadataReader, "", 200)
	encoded, _ := json.Marshal(listed)
	if len(listed["oauth_clients"].([]any)) != 1 || strings.Contains(string(encoded), created.ClientSecret) || strings.Contains(string(encoded), hidden["client_id"].(string)) || strings.Contains(string(encoded), `"client_secret":`) {
		t.Fatal("OAuth HTTP list leaked secrets or cross-project metadata")
	}
	form := url.Values{"grant_type": {"client_credentials"}, "resource": {"http://localhost/api/v1"}}
	tokenRequest := httptest.NewRequest("POST", "/oauth/token", strings.NewReader(form.Encode()))
	tokenRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenRequest.SetBasicAuth(created.ClientID, created.ClientSecret)
	tokenResponse := httptest.NewRecorder()
	s.Handler().ServeHTTP(tokenResponse, tokenRequest)
	if tokenResponse.Code != 200 {
		t.Fatalf("created client token exchange returned %d", tokenResponse.Code)
	}
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if json.Unmarshal(tokenResponse.Body.Bytes(), &token) != nil || token.AccessToken == "" {
		t.Fatal("invalid token response")
	}
	request(t, s, "GET", "/api/v1/projects/"+a+"/events", token.AccessToken, "", 200)
	request(t, s, "DELETE", "/api/v1/oauth-clients/"+created.ClientID, adminToken, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+a+"/events", token.AccessToken, "", 401)
}
