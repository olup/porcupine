package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/query"
)

func tenantOAuthJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func tenantOAuthSetup(t *testing.T) (*emailJourney, string, string, string, string, string) {
	t.Helper()
	j := newEmailJourney(t)
	operator := accountTestToken(t, j.service)
	create := func(name string) string {
		return request(t, j.service, "POST", "/api/v1/tenants", operator, tenantOAuthJSON(t, map[string]any{"name": name}), 201)["tenant"].(map[string]any)["id"].(string)
	}
	a, b := create("Team A"), create("Team B")
	project := request(t, j.service, "POST", "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": "B project", "tenant_id": b}), 201)["project"].(map[string]any)["id"].(string)
	human := request(t, j.service, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"lea@example.test","role":"admin"}`, 201)["human_id"].(string)
	request(t, j.service, "POST", "/api/v1/tenants/"+b+"/members", operator, `{"email":"lea@example.test","role":"member","project_scope":"all","project_role":"reader"}`, 201)
	return j, operator, a, b, project, human
}

func TestTenantAdminEmailOAuthEmptyTenantAndRoleBounds(t *testing.T) {
	t.Parallel()
	j, operator, a, b, projectB, human := tenantOAuthSetup(t)
	s := j.service
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	token := j.browserLoginSelection(t, app, "lea@example.test", "/api/v1", "tenant", a, b, 200)
	if token.Extra("scope") != tenantAdminScope {
		t.Fatal("tenant consent issued another OAuth scope")
	}
	current, err := s.authenticateResource(t.Context(), token.AccessToken, s.resources.apiResource)
	if err != nil || current.Admin() || len(current.TenantAdmins) != 1 || current.TenantAdmins[0] != a || len(current.Projects) != 0 {
		t.Fatalf("empty tenant did not yield bounded administration: %+v %v", current, err)
	}
	request(t, s, "GET", "/api/v1/tenants/"+a, token.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/tenants/"+b, token.AccessToken, "", 403)
	request(t, s, "GET", "/api/v1/tenants/"+a+"/usage", token.AccessToken, "", 200)
	request(t, s, "PUT", "/api/v1/tenants/"+a+"/budgets", token.AccessToken, "{}", 403)
	request(t, s, "GET", "/api/v1/humans", token.AccessToken, "", 403)
	request(t, s, "POST", "/api/v1/projects", token.AccessToken, tenantOAuthJSON(t, map[string]any{"name": "Forbidden", "tenant_id": b}), 403)
	projectA := request(t, s, "POST", "/api/v1/projects", token.AccessToken, tenantOAuthJSON(t, map[string]any{"name": "First A project", "tenant_id": a}), 201)["project"].(map[string]any)["id"].(string)
	request(t, s, "GET", "/api/v1/projects/"+projectA+"/schema", token.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+projectB+"/schema", token.AccessToken, "", 403)
	listed := request(t, s, "GET", "/api/v1/projects", token.AccessToken, "", 200)
	if strings.Contains(tenantOAuthJSON(t, listed), projectB) {
		t.Fatal("tenant admin project list leaked another tenant")
	}
	future := request(t, s, "POST", "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": "Future A project", "tenant_id": a}), 201)["project"].(map[string]any)["id"].(string)
	request(t, s, "GET", "/api/v1/projects/"+future+"/schema", token.AccessToken, "", 200)
	token.Expiry = time.Now().Add(-time.Minute)
	refreshed, err := app.TokenSource(context.Background(), token).Token()
	if err != nil || refreshed.RefreshToken == token.RefreshToken || refreshed.Extra("scope") != tenantAdminScope {
		t.Fatalf("tenant role refresh failed: %v", err)
	}
	request(t, s, "GET", "/api/v1/projects/"+future+"/schema", refreshed.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+projectB+"/schema", refreshed.AccessToken, "", 403)
	request(t, s, "DELETE", "/api/v1/projects/"+projectB, refreshed.AccessToken, "", 403)
	request(t, s, "DELETE", "/api/v1/projects/"+future, refreshed.AccessToken, "", 204)
	request(t, s, "DELETE", "/api/v1/projects/"+future, refreshed.AccessToken, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+future+"/schema", refreshed.AccessToken, "", 403)

	request(t, s, "PUT", "/api/v1/tenants/"+a+"/members/"+human, refreshed.AccessToken, `{"role":"member","project_scope":"all","project_role":"reader"}`, 409)
	request(t, s, "PUT", "/api/v1/tenants/"+a+"/members/"+human, operator, `{"role":"member","project_scope":"all","project_role":"reader"}`, 409)
	request(t, s, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"backup@example.test","role":"admin"}`, 201)
	request(t, s, "PUT", "/api/v1/tenants/"+a+"/members/"+human, operator, `{"role":"member","project_scope":"all","project_role":"reader"}`, 200)
	request(t, s, "GET", "/api/v1/tenants/"+a, refreshed.AccessToken, "", 401)
	if _, err := s.exchangeInteractiveRefresh(t.Context(), mustTenantOAuthClient(t, s), url.Values{"refresh_token": {refreshed.RefreshToken}}); err == nil {
		t.Fatal("downgraded membership refreshed an administrative grant")
	}
	readerApp := j.client("porcupine-local", localOAuthCallback, auth.InteractiveReadScopes())
	reader := j.browserLogin(t, readerApp, "lea@example.test", "/api/v1", projectB, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+projectB+"/schema", reader.AccessToken, "", 200)
	request(t, s, "PATCH", "/api/v1/projects/"+projectB, reader.AccessToken, `{"name":"Cannot rename"}`, 403)
	request(t, s, "GET", "/api/v1/projects/"+projectA+"/schema", reader.AccessToken, "", 403)
}

func mustTenantOAuthClient(t *testing.T, s *Service) *oauthClient {
	t.Helper()
	client, err := loadOAuthClient(t.Context(), s.control, "porcupine-local-tenant-admin")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestTenantAdminEmailOAuthMCPIsolationAndRevocation(t *testing.T) {
	t.Parallel()
	j, operator, a, b, _, human := tenantOAuthSetup(t)
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	token := j.browserLoginSelection(t, app, "lea@example.test", "/mcp", "tenant", a, b, 200)
	request(t, j.service, "GET", "/api/v1/tenants/"+a, token.AccessToken, "", 401)
	ctx, client := mcpTestSession(t, j.issuer, token.AccessToken, "")
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "get-tenant", Path: map[string]string{"tenant_id": a}}, 200)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "get-tenant", Path: map[string]string{"tenant_id": b}}, 403)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "list-tenant-members", Path: map[string]string{"tenant_id": a}}, 200)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "list-tenant-members", Path: map[string]string{"tenant_id": b}}, 403)
	created := mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "Via MCP", "tenant_id": a}}, 201)
	project := created["project"].(map[string]any)["id"].(string)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "Forbidden", "tenant_id": b}}, 403)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "delete-project", Path: map[string]string{"id": project}}, 204)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "delete-project", Path: map[string]string{"id": project}}, 204)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": project}}, 403)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "revoke-tenant-member", Path: map[string]string{"tenant_id": a, "human_id": human}}, 409)
	request(t, j.service, "DELETE", "/api/v1/tenants/"+a+"/members/"+human, operator, "", 409)
	request(t, j.service, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"backup@example.test","role":"admin"}`, 201)
	request(t, j.service, "DELETE", "/api/v1/tenants/"+a+"/members/"+human, operator, "", 204)
	if _, err := j.service.authenticateResource(t.Context(), token.AccessToken, j.service.resources.mcpResource); err == nil {
		t.Fatal("revoked tenant membership retained MCP authority")
	}
	result, err := client.CallTool(ctx, &mcp.CallToolParams{Name: "porcupine_call", Arguments: mcpCallInput{OperationID: "get-tenant", Path: map[string]string{"tenant_id": a}}})
	if err == nil && result != nil && !result.IsError {
		t.Fatal("already connected MCP client retained revoked tenant administration")
	}
}

func TestTenantAdminInteractiveScopeRequiresExplicitDedicatedClient(t *testing.T) {
	t.Parallel()
	j := newEmailJourney(t)
	for _, tc := range []struct{ client, scope string }{
		{"porcupine-local-tenant-admin", ""},
		{"porcupine-local-tenant-admin", "events:read"},
		{"porcupine-local-tenant-admin", "tenant:admin events:read"},
		{"porcupine-local", "tenant:admin"},
		{"porcupine-local-admin", "tenant:admin"},
	} {
		query := url.Values{
			"response_type": {"code"}, "client_id": {tc.client}, "redirect_uri": {localOAuthCallback},
			"code_challenge_method": {"S256"}, "code_challenge": {strings.Repeat("A", 43)},
		}
		if tc.scope != "" {
			query.Set("scope", tc.scope)
		}
		result := request(t, j.service, "GET", "/oauth/authorize?"+query.Encode(), "", "", 400)
		if result["error"] != "invalid_scope" {
			t.Fatalf("unexpected OAuth scope error for client %s: %v", tc.client, result)
		}
	}
}

func TestTenantMemberLocalOAuthExplicitWriteScopes(t *testing.T) {
	t.Parallel()
	j := newEmailJourney(t)
	s, operator := j.service, accountTestToken(t, j.service)
	project := func(name string) string {
		return request(t, s, "POST", "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": name}), 201)["project"].(map[string]any)["id"].(string)
	}
	a, b := project("Contribute here"), project("Read here")
	request(t, s, "POST", "/api/v1/tenants/default/members", operator, tenantOAuthJSON(t, map[string]any{
		"email": "member@example.test", "role": "member", "project_scope": "selected",
		"grants": []map[string]any{{"project_id": a, "role": "contributor"}, {"project_id": b, "role": "reader"}},
	}), 201)
	app := j.client("porcupine-local", localOAuthCallback, []string{"events:read", "events:write", "imports:write"})
	token := j.browserLogin(t, app, "member@example.test", "/api/v1", a, b, 200)
	if token.Extra("scope") != "events:read events:write imports:write" {
		t.Fatal("explicit project login issued different scopes")
	}
	request(t, s, "GET", "/api/v1/projects/"+a+"/schema", token.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+b+"/schema", token.AccessToken, "", 403)
	accepted := request(t, s, "POST", "/ingest/v1/events", token.AccessToken, `{"type":"track","message_id":"member-write-1","actor_id":"test-user","name":"member_event"}`, 200)
	if accepted["accepted"] != float64(1) {
		t.Fatal("contributor could not use explicitly consented ingestion")
	}
	request(t, s, "PATCH", "/api/v1/projects/"+a, token.AccessToken, `{"name":"Forbidden settings edit"}`, 403)
	request(t, s, "DELETE", "/api/v1/projects/"+a+"/events/not-granted", token.AccessToken, "", 403)
	request(t, s, "GET", "/api/v1/tenants/default/members", token.AccessToken, "", 403)
	token.Expiry = time.Now().Add(-time.Minute)
	refreshed, err := app.TokenSource(context.Background(), token).Token()
	if err != nil || refreshed.Extra("scope") != "events:read events:write imports:write" {
		t.Fatalf("explicit project scopes did not survive bounded refresh: %v", err)
	}
	request(t, s, "GET", "/api/v1/projects/"+b+"/schema", refreshed.AccessToken, "", 403)
}

func TestTenantAdminOAuthRevocationKeepsOtherTenantGrant(t *testing.T) {
	t.Parallel()
	j, operator, a, b, _, human := tenantOAuthSetup(t)
	s := j.service
	request(t, s, "PUT", "/api/v1/tenants/"+b+"/members/"+human, operator, `{"role":"admin"}`, 200)
	request(t, s, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"backup@example.test","role":"admin"}`, 201)
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	first := j.browserLoginSelection(t, app, "lea@example.test", "/api/v1", "tenant", a, "", 200)
	second := j.browserLoginSelection(t, app, "lea@example.test", "/api/v1", "tenant", b, "", 200)
	grants := request(t, s, "GET", "/api/v1/oauth-grants", operator, "", 200)["grants"].([]any)
	if len(grants) != 2 {
		t.Fatal("tenant consent grants were not listed")
	}
	seen := map[string]bool{}
	for _, raw := range grants {
		grant := raw.(map[string]any)
		seen[grant["tenant_id"].(string)] = true
		if grant["issuer"] != "email" || grant["subject"] != "lea@example.test" {
			t.Fatal("grant metadata lost the common durable human identity")
		}
	}
	if !seen[a] || !seen[b] {
		t.Fatal("grant metadata lost the selected tenant context")
	}
	request(t, s, "DELETE", "/api/v1/tenants/"+a+"/members/"+human, operator, "", 204)
	request(t, s, "GET", "/api/v1/tenants/"+a, first.AccessToken, "", 401)
	request(t, s, "GET", "/api/v1/tenants/"+b, second.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/tenants/"+a, second.AccessToken, "", 403)
}

func TestSharedTenantAdminOAuthCrossWorkerRevocation(t *testing.T) {
	t.Parallel()
	control, dsn := sharedControlFixture(t)
	ctx := context.Background()
	queries, err := query.Normalize(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := initializeSharedControl(ctx, control, adminToken, queries, false); err != nil {
		t.Fatal(err)
	}
	other, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { other.Close() })
	if _, err := control.Exec(`INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES('other','Other',$1,1,5000,1000,1048576)`, now()); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewUnstartedServer(nil)
	issuer := "http://" + host.Listener.Addr().String()
	mail := make(chan journeyMail, 8)
	worker := func(database *sql.DB) *Service {
		t.Helper()
		local, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "journal.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { local.Close() })
		if _, err := local.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		oauth, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer, Email: &EmailConfig{SendCode: func(ctx context.Context, to, code string) error {
			select {
			case mail <- journeyMail{to: to, code: code}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}})
		if err != nil {
			t.Fatal(err)
		}
		s := &Service{db: local, control: database, shared: &sharedRuntime{}, oauth: oauth, resources: newResourceRuntime(oauth.issuer), eventRules: &eventRules{clock: time.Now}, tokenSlots: make(chan struct{}, 8)}
		if err := s.initializeOAuthClients(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.initializeOAuth(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := worker(control), worker(other)
	var rootID string
	if err := control.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&rootID); err != nil {
		t.Fatal(err)
	}
	operator, err := a.loadCredentialActor(ctx, rootID)
	if err != nil {
		t.Fatal(err)
	}
	operatorContext := context.WithValue(ctx, actorContextKey{}, operator)
	invite := func(email string) string {
		t.Helper()
		input := &TenantMemberInviteInput{TenantID: defaultTenantID}
		input.Body.Email, input.Body.Role = email, "admin"
		result, err := a.inviteTenantMember(operatorContext, input)
		if err != nil {
			t.Fatal(err)
		}
		return result.Body.HumanID
	}
	human := invite("shared-admin@example.test")
	invite("backup-admin@example.test")
	muxes := []*http.ServeMux{http.NewServeMux(), http.NewServeMux()}
	a.registerOAuth(muxes[0])
	b.registerOAuth(muxes[1])
	var requests atomic.Uint32
	host.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		muxes[requests.Add(1)%2].ServeHTTP(w, r)
	})
	host.Start()
	t.Cleanup(host.Close)
	j := &emailJourney{service: a, issuer: issuer, mail: mail}
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	token := j.browserLoginSelection(t, app, "shared-admin@example.test", "/mcp", "tenant", defaultTenantID, "other", 200)
	for _, s := range []*Service{a, b} {
		current, err := s.authenticateResource(ctx, token.AccessToken, s.resources.mcpResource)
		if err != nil || current.Admin() || !current.CanAdminTenant(defaultTenantID) || current.CanAdminTenant("other") {
			t.Fatalf("shared tenant administration lost its boundary: %+v %v", current, err)
		}
	}
	if _, err := b.revokeTenantMember(operatorContext, &TenantMemberIDInput{TenantID: defaultTenantID, HumanID: human}); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*Service{a, b} {
		if _, err := s.authenticateResource(ctx, token.AccessToken, s.resources.mcpResource); err == nil {
			t.Fatal("tenant revocation remained authorized on another worker")
		}
	}
}
