package porcupine

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSharedExternalAuthAcrossWorkersAndRevocation(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	f := newExternalAuthFixture(t)
	makeWorker := func(id string) *Service {
		host := httptest.NewUnstartedServer(nil)
		s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "worker.db"), Clock: func() time.Time { return time.UnixMilli(f.now.Load()) }, Shared: &SharedConfig{DatabaseURL: dsn, WorkerID: id, RouteURL: "http://" + host.Listener.Addr().String(), RoutingSecret: []byte(strings.Repeat("shared-test-secret", 3))}, Auth: &AuthConfig{Mode: "external", PublicURL: "https://data.test", External: f.s.resources.external.Config()}})
		if err != nil {
			host.Close()
			t.Fatal(err)
		}
		host.Config.Handler = s.Handler()
		host.Start()
		t.Cleanup(func() { host.Close(); _ = s.Close() })
		return s
	}
	first, second := makeWorker("first"), makeWorker("second")
	operator := f.operator(t)
	p := request(t, first, "POST", "/api/v1/projects", operator, `{"name":"Shared external"}`, 201)
	id := p["project"].(map[string]any)["id"].(string)
	reader := f.token(t, "reader", defaultTenantID, "events:read", first.resources.apiResource, id)
	request(t, second, "GET", "/api/v1/projects/"+id+"/events", reader, "", 200)
	request(t, second, "POST", "/capture/", "", captureBody(p["ingest_token"].(string), "shared-external"), 200)
	if eventCount(t, first) != 1 || eventCount(t, second) != 0 {
		t.Fatal("shared metadata became an event queue or routing failed")
	}
	admin := f.token(t, "tenant-admin", defaultTenantID, "tenant:admin management:read", first.resources.apiResource)
	account := request(t, second, "POST", "/api/v1/tenants/"+defaultTenantID+"/service-accounts", admin, externalBody(t, map[string]any{"name": "team", "permissions": []string{"events:read"}, "projects": []string{id}}), 201)
	cid := account["client_id"].(string)
	key := request(t, second, "POST", "/api/v1/tenants/"+defaultTenantID+"/service-accounts/"+cid+"/api-keys", admin, externalBody(t, map[string]any{"name": "worker", "permissions": []string{"events:read"}, "projects": []string{id}, "audience": "api"}), 201)["token"].(string)
	request(t, first, "GET", "/api/v1/projects/"+id+"/events", key, "", 200)
	request(t, second, "DELETE", "/api/v1/tenants/"+defaultTenantID+"/service-accounts/"+cid, admin, "", 204)
	request(t, first, "GET", "/api/v1/projects/"+id+"/events", key, "", 401)
	for _, s := range []*Service{first, second} {
		var count int
		if err := s.control.QueryRow(`SELECT count(*) FROM email_humans`).Scan(&count); err != nil || count != 0 {
			t.Fatal("external shared runtime depends on native humans", err)
		}
	}
}
