package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"time"

	"slices"
	"strings"
	"testing"

	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
)

func tenantServiceFixture(t *testing.T) (*oauthIntegration, string, string, string, string, string) {
	t.Helper()
	f := &oauthIntegration{host: httptest.NewUnstartedServer(nil), client: &http.Client{Timeout: 5 * time.Second}}
	f.now.Store(time.Now().UTC().UnixNano())
	issuer := "http://" + f.host.Listener.Addr().String()
	var err error
	f.s, err = Open(Config{DBPath: filepath.Join(t.TempDir(), "tenant-services.db"), BootstrapToken: adminToken, Clock: func() time.Time { return time.Unix(0, f.now.Load()).UTC() }, OAuth: &OAuthConfig{Issuer: issuer, AccessTokenLifetimeSeconds: 60, Email: &EmailConfig{SendCode: func(context.Context, string, string) error { return nil }}}})
	if err != nil {
		f.host.Close()
		t.Fatal(err)
	}
	f.s.oauth.limiter = rate.NewLimiter(rate.Inf, 1)
	f.host.Config.Handler = f.s.Handler()
	f.host.Start()
	f.ctx = context.WithValue(context.Background(), oauth2.HTTPClient, f.client)
	t.Cleanup(func() { f.host.Close(); f.client.CloseIdleConnections(); f.s.Close() })
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	tenant := func(name string) string {
		var out struct {
			Tenant Tenant `json:"tenant"`
		}
		f.call(t, "POST", "/api/v1/tenants", admin, map[string]string{"name": name}, 201, &out)
		return out.Tenant.ID
	}
	a, b := tenant("Team A"), tenant("Team B")
	project := func(tenant string) string {
		var out ProjectCreated
		f.call(t, "POST", "/api/v1/projects", admin, map[string]string{"name": "app", "tenant_id": tenant}, 201, &out)
		return out.Project.ID
	}
	return f, admin, a, b, project(a), project(b)
}

func TestTenantServiceAccountsLifecycleAndScope(t *testing.T) {
	t.Parallel()
	f, admin, a, b, projectA, projectB := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + a + "/service-accounts"
	body := TenantServiceAccountSpec{Name: "reports", Permissions: []string{"events:read", "events:write", "management:read"}, Projects: []string{projectA}}
	var account OAuthClientCreated
	f.call(t, "POST", base, admin, body, 201, &account)
	if account.ClientID == "" || account.ClientSecret == "" || !slices.Equal(account.GrantTypes, []string{"client_credentials"}) {
		t.Fatal("machine account receipt missing")
	}
	var owner, stored string
	if err := f.s.control.QueryRow(`SELECT t.tenant_id,c.secret_hash FROM tenant_credentials t JOIN oauth_clients c ON c.client_id=t.principal_id WHERE t.principal_id=?`, account.ClientID).Scan(&owner, &stored); err != nil || owner != a || stored != hash(account.ClientSecret) {
		t.Fatal("machine ownership or secret storage incorrect")
	}
	var listed struct {
		ServiceAccounts []OAuthClient `json:"service_accounts"`
	}
	f.call(t, "GET", base, admin, nil, 200, &listed)
	if len(listed.ServiceAccounts) != 1 || listed.ServiceAccounts[0].ClientID != account.ClientID {
		t.Fatal("tenant account discovery missing")
	}
	raw, _ := json.Marshal(listed)
	if strings.Contains(string(raw), account.ClientSecret) || strings.Contains(string(raw), stored) {
		t.Fatal("account listing leaked a secret")
	}
	f.call(t, "GET", "/api/v1/tenants/"+b+"/service-accounts", admin, nil, 200, &listed)
	if len(listed.ServiceAccounts) != 0 {
		t.Fatal("another tenant's service account leaked")
	}
	reader := f.token(t, account.ClientID, account.ClientSecret, f.s.oauth.apiResource, "events:read").AccessToken
	f.call(t, "GET", "/api/v1/projects/"+projectA+"/events", reader, nil, 200, nil)
	f.call(t, "GET", "/api/v1/projects/"+projectB+"/events", reader, nil, 403, nil)
	f.call(t, "POST", "/capture/", reader, map[string]string{"event": "forbidden", "distinct_id": "test"}, 403, nil)
	f.call(t, "GET", base, reader, nil, 403, nil)
	for _, bad := range []TenantServiceAccountSpec{
		{Name: "operator", Permissions: []string{"instance:admin"}, Projects: []string{projectA}},
		{Name: "tenant admin", Permissions: []string{"tenant:admin"}, Projects: []string{projectA}},
		{Name: "wildcard", Permissions: []string{"events:read"}, Projects: []string{"*"}},
	} {
		f.call(t, "POST", base, admin, bad, 400, nil)
	}
	f.call(t, "POST", base, admin, TenantServiceAccountSpec{Name: "cross tenant", Permissions: []string{"events:read"}, Projects: []string{projectB}}, 403, nil)
	f.call(t, "PUT", "/api/v1/tenants/"+b+"/service-accounts/"+account.ClientID, admin, body, 404, nil)
	f.call(t, "DELETE", "/api/v1/tenants/"+b+"/service-accounts/"+account.ClientID, admin, nil, 404, nil)
	f.call(t, "DELETE", base+"/"+account.ClientID, admin, nil, 204, nil)
	f.call(t, "DELETE", base+"/"+account.ClientID, admin, nil, 204, nil)
	f.call(t, "GET", "/api/v1/projects/"+projectA+"/events", reader, nil, 401, nil)
}

func TestTenantServiceKeysParentContainmentAndIndependentRevocation(t *testing.T) {
	t.Parallel()
	f, admin, tenant, other, project, otherProject := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + tenant + "/service-accounts"
	var account OAuthClientCreated
	spec := TenantServiceAccountSpec{Name: "backend", Projects: []string{project}, Permissions: []string{"events:read", "events:write"}}
	f.call(t, "POST", base, admin, spec, 201, &account)
	keys := base + "/" + account.ClientID + "/api-keys"
	makeKey := func(audience string) AccessKeyCreated {
		t.Helper()
		var key AccessKeyCreated
		f.call(t, "POST", keys, admin, ServiceKeySpec{Name: "analytics", Audience: audience, Permissions: []string{"events:read"}, Projects: []string{project}}, 201, &key)
		if key.Token == "" || key.ServiceClientID != account.ClientID || key.Audience != audience {
			t.Fatal("service key receipt invalid")
		}
		var owner string
		if err := f.s.control.QueryRow(`SELECT tenant_id FROM tenant_credentials WHERE principal_id=?`, key.ID).Scan(&owner); err != nil || owner != tenant {
			t.Fatal("key not bound to team")
		}
		return key
	}
	api, mcp := makeKey("api"), makeKey("mcp")
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", api.Token, nil, 200, nil)
	f.call(t, "GET", "/api/v1/projects/"+otherProject+"/events", api.Token, nil, 403, nil)
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", mcp.Token, nil, 401, nil)
	f.call(t, "POST", keys, api.Token, ServiceKeySpec{Name: "more", Audience: "api", Permissions: []string{"events:read"}, Projects: []string{project}}, 403, nil)
	ctx, session := mcpTestSession(t, f.host.URL, mcp.Token, "2025-11-25")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": project}}, 200)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": otherProject}}, 403)
	var page struct {
		Keys []AccessKey `json:"access_keys"`
		Next *int        `json:"next_offset"`
	}
	f.call(t, "GET", keys+"?limit=1", admin, nil, 200, &page)
	if len(page.Keys) != 1 || page.Next == nil || *page.Next != 1 {
		t.Fatal("key pagination missing")
	}
	page.Next = nil
	f.call(t, "GET", keys+"?limit=1&offset=1", admin, nil, 200, &page)
	if len(page.Keys) != 1 || page.Next != nil {
		t.Fatal("key pagination lost keys")
	}
	f.call(t, "DELETE", "/api/v1/tenants/"+other+"/service-accounts/"+account.ClientID+"/api-keys/"+api.ID, admin, nil, 404, nil)
	f.call(t, "DELETE", keys+"/"+api.ID, admin, nil, 204, nil)
	f.call(t, "DELETE", keys+"/"+api.ID, admin, nil, 204, nil)
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", api.Token, nil, 401, nil)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": project}}, 200)
	var updated OAuthClientCreated
	spec.Permissions = []string{"events:read"}
	f.call(t, "PUT", base+"/"+account.ClientID, admin, spec, 200, &updated)
	if updated.CreatedAt == "" || updated.ClientSecret != "" {
		t.Fatal("update must preserve creation metadata without returning a secret")
	}
	if _, err := f.s.authenticateResource(f.ctx, mcp.Token, f.s.oauth.mcpResource); err == nil {
		t.Fatal("parent grant reduction left its MCP key active")
	}
}

func TestTenantServiceCredentialCreationIsAtomicWithOwnership(t *testing.T) {
	t.Parallel()
	f, admin, tenant, _, project, _ := tenantServiceFixture(t)
	var before, after int
	if err := f.s.control.QueryRow(`SELECT count(*) FROM oauth_clients`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.control.Exec(`CREATE TRIGGER fail_tenant_binding BEFORE INSERT ON tenant_credentials BEGIN SELECT RAISE(ABORT,'fixture interruption'); END`); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", "/api/v1/tenants/"+tenant+"/service-accounts", admin, TenantServiceAccountSpec{Name: "atomic", Projects: []string{project}, Permissions: []string{"events:read"}}, 503, nil)
	if err := f.s.control.QueryRow(`SELECT count(*) FROM oauth_clients`).Scan(&after); err != nil || after != before {
		t.Fatal("partially unowned service account escaped rollback")
	}
}

func TestTenantServiceActorRejectsChangedOwnershipAndParent(t *testing.T) {
	t.Parallel()
	f, admin, tenant, other, project, _ := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + tenant + "/service-accounts"
	var account OAuthClientCreated
	f.call(t, "POST", base, admin, TenantServiceAccountSpec{Name: "fixed", Projects: []string{project}, Permissions: []string{"events:read"}}, 201, &account)
	var key AccessKeyCreated
	f.call(t, "POST", base+"/"+account.ClientID+"/api-keys", admin, ServiceKeySpec{Name: "fixed key", Audience: "api", Projects: []string{project}, Permissions: []string{"events:read"}}, 201, &key)
	token := f.token(t, account.ClientID, account.ClientSecret, f.s.oauth.apiResource).AccessToken
	if _, err := f.s.control.Exec(`UPDATE tenant_credentials SET tenant_id=? WHERE principal_id=?`, other, account.ClientID); err != nil {
		t.Fatal(err)
	}
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", key.Token, nil, 401, nil)
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", token, nil, 401, nil)
	if _, err := f.s.loadCredentialActor(context.Background(), key.ID); err == nil {
		t.Fatal("background key authority ignored changed service ownership")
	}
}

func TestTenantServiceAccountCreatorDoesNotOwnItsLifetime(t *testing.T) {
	t.Parallel()
	f, _, tenant, other, project, _ := tenantServiceFixture(t)
	ctx := f.ctx
	root := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	var member struct {
		HumanID string `json:"human_id"`
	}
	f.call(t, "POST", "/api/v1/tenants/"+tenant+"/members", root, map[string]any{"email": "team-admin@example.test", "role": "admin"}, 201, &member)
	id := member.HumanID
	admin, err := loadEmailHuman(ctx, f.s.control, id)
	if err != nil || !admin.canAdminTenant(tenant) {
		t.Fatalf("tenant admin fixture unavailable: %v", err)
	}
	ctx = context.WithValue(ctx, actorContextKey{}, admin)
	if _, err := f.s.createTenantServiceAccount(ctx, &TenantServiceAccountCreateInput{TenantID: other, Body: TenantServiceAccountSpec{Name: "other team", Permissions: []string{"events:read"}, Projects: []string{project}}}); err == nil {
		t.Fatal("tenant administrator created an account in another team")
	}
	limited := admin
	limited.Permissions = []string{"events:read"}
	if _, err := f.s.createTenantServiceAccount(context.WithValue(ctx, actorContextKey{}, limited), &TenantServiceAccountCreateInput{TenantID: tenant, Body: TenantServiceAccountSpec{Name: "scope escalation", Permissions: []string{"events:read"}, Projects: []string{project}}}); err == nil {
		t.Fatal("fresh administrator authority escaped the original token's scope")
	}
	out, err := f.s.createTenantServiceAccount(ctx, &TenantServiceAccountCreateInput{TenantID: tenant, Body: TenantServiceAccountSpec{Name: "shared team agent", Permissions: []string{"events:read"}, Projects: []string{project}}})
	if err != nil {
		t.Fatal(err)
	}
	account := out.Body
	machine := f.token(t, account.ClientID, account.ClientSecret, f.s.oauth.apiResource).AccessToken
	if _, err := f.s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), id); err != nil {
		t.Fatal(err)
	}
	f.call(t, http.MethodGet, "/api/v1/projects/"+project+"/events", machine, nil, 200, nil)
	if _, err := f.s.createTenantServiceAccount(ctx, &TenantServiceAccountCreateInput{TenantID: tenant, Body: TenantServiceAccountSpec{Name: "stale admin", Permissions: []string{"events:read"}, Projects: []string{project}}}); err == nil {
		t.Fatal("stale tenant admin created an independent account after revocation")
	}
}

func TestTenantServiceProducerSourceAllowsReadOnlyChild(t *testing.T) {
	t.Parallel()
	f, admin, tenant, _, project, _ := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + tenant + "/service-accounts"
	spec := TenantServiceAccountSpec{Name: "backend", SourceID: "backend", Permissions: []string{"events:write", "events:read"}, Projects: []string{project}}
	var account OAuthClientCreated
	f.call(t, "POST", base, admin, spec, 201, &account)
	var key AccessKeyCreated
	f.call(t, "POST", base+"/"+account.ClientID+"/api-keys", admin, ServiceKeySpec{Name: "reader", Audience: "api", Permissions: []string{"events:read"}, Projects: []string{project}}, 201, &key)
	if key.SourceID != "" || key.TenantID != tenant || account.TenantID != tenant {
		t.Fatal("read-only key must omit producer authority and retain team ownership")
	}
	f.call(t, "GET", "/api/v1/projects/"+project+"/events", key.Token, nil, 200, nil)
	spec.SourceID = "another-source"
	f.call(t, "PUT", base+"/"+account.ClientID, admin, spec, 400, nil)
	var owner, source string
	if err := f.s.control.QueryRow(`SELECT t.tenant_id,k.source_id FROM tenant_credentials t JOIN access_keys k ON k.id=t.principal_id WHERE k.id=?`, account.ClientID).Scan(&owner, &source); err != nil || owner != tenant || source != "backend" {
		t.Fatal("failed source update changed service account")
	}
}
