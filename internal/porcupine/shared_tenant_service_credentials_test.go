package porcupine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
)

func TestSharedTenantServiceCredentialsAcrossWorkers(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	fixture := nativeS3Fixture(t)
	firstHost := httptest.NewUnstartedServer(nil)
	issuer := "http://" + firstHost.Listener.Addr().String()
	openWorker := func(host *httptest.Server) *oauthIntegration {
		t.Helper()
		config := hostedJourneyConfig(t, dsn, fixture)
		config.OAuth = &OAuthConfig{Issuer: issuer, Email: &EmailConfig{SendCode: func(context.Context, string, string) error { return nil }}}
		s, err := Open(config)
		if err != nil {
			host.Close()
			t.Fatal(err)
		}
		s.oauth.limiter = rate.NewLimiter(rate.Inf, 1)
		host.Config.Handler = s.Handler()
		host.Start()
		client := &http.Client{Timeout: 10 * time.Second}
		f := &oauthIntegration{s: s, host: host, client: client, ctx: context.WithValue(context.Background(), oauth2.HTTPClient, client)}
		t.Cleanup(func() { host.Close(); client.CloseIdleConnections(); s.Close() })
		return f
	}
	a := openWorker(firstHost)
	b := openWorker(httptest.NewUnstartedServer(nil))
	admin := a.token(t, "bootstrap", adminToken, a.s.resources.apiResource).AccessToken
	var tenant struct {
		Tenant Tenant `json:"tenant"`
	}
	a.call(t, "POST", "/api/v1/tenants", admin, map[string]string{"name": "shared team"}, 201, &tenant)
	var project ProjectCreated
	a.call(t, "POST", "/api/v1/projects", admin, map[string]string{"name": "shared app", "tenant_id": tenant.Tenant.ID}, 201, &project)
	base := "/api/v1/tenants/" + tenant.Tenant.ID + "/service-accounts"
	var machine OAuthClientCreated
	a.call(t, "POST", base, admin, TenantServiceAccountSpec{Name: "shared agent", Projects: []string{project.Project.ID}, Permissions: []string{"events:read"}}, 201, &machine)
	var key AccessKeyCreated
	b.call(t, "POST", base+"/"+machine.ClientID+"/api-keys", admin, ServiceKeySpec{Name: "shared key", Audience: "api", Projects: []string{project.Project.ID}, Permissions: []string{"events:read"}}, 201, &key)
	hostedCleanupWrite(t, a.s, project.Project.ID, "tenant event")
	path := "/api/v1/projects/" + project.Project.ID + "/events"
	issued := a.token(t, machine.ClientID, machine.ClientSecret, a.s.resources.apiResource).AccessToken
	for _, worker := range []*oauthIntegration{a, b} {
		worker.call(t, "GET", path, key.Token, nil, 200, nil)
		worker.call(t, "GET", path, issued, nil, 200, nil)
		worker.call(t, "GET", "/api/v1/tenants/"+tenant.Tenant.ID+"/usage", key.Token, nil, 403, nil)
	}
	b.call(t, "DELETE", base+"/"+machine.ClientID, admin, nil, 204, nil)
	for _, worker := range []*oauthIntegration{a, b} {
		worker.call(t, "GET", path, key.Token, nil, 401, nil)
		worker.call(t, "GET", path, issued, nil, 401, nil)
		if _, err := worker.s.loadCredentialActor(worker.ctx, key.ID); err == nil {
			t.Fatal("revoked team account retained background key authority on another worker")
		}
	}
}
