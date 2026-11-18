package porcupine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/query"
)

func (f *externalAuthFixture) withHostedAuth(config Config) Config {
	config.BootstrapToken = ""
	config.Clock = func() time.Time { return time.UnixMilli(f.now.Load()) }
	config.Auth = &AuthConfig{Mode: "external", PublicURL: "https://data.test", External: f.s.resources.external.Config()}
	return config
}

func (f *externalAuthFixture) sharedWorker(t *testing.T, dsn string, fixture duckLakeS3Fixture) *Service {
	t.Helper()
	config := f.withHostedAuth(hostedJourneyConfig(t, dsn, fixture))
	s, _, _ := hostedCleanupWorker(t, config)
	return s
}

func TestSharedExternalAuthAcrossWorkersAndRevocation(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	f := newExternalAuthFixture(t)
	fixture := nativeS3Fixture(t)
	first, second := f.sharedWorker(t, dsn, fixture), f.sharedWorker(t, dsn, fixture)
	operator := f.operator(t)
	p := request(t, first, "POST", "/api/v1/projects", operator, `{"name":"Shared external"}`, 201)
	id := p["project"].(map[string]any)["id"].(string)
	reader := f.token(t, "reader", defaultTenantID, "events:read", first.resources.apiResource, id)
	hostedCleanupWrite(t, first, id, "shared-external")
	events := request(t, second, "GET", "/api/v1/projects/"+id+"/events", reader, "", 200)
	if rows, _ := events["events"].([]any); len(rows) != 1 {
		t.Fatal("second hosted worker did not see the published event", events)
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

func TestSharedExternalAuthorityExpiresWhileWaitingForWriteLock(t *testing.T) {
	t.Parallel()
	dsn := sharedPostgresDSN(t)
	f := newExternalAuthFixture(t)
	ctx := t.Context()
	queries, err := query.Normalize(nil)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newServiceRuntime(f.withHostedAuth(Config{}), queries)
	if err != nil {
		t.Fatal(err)
	}
	s.control, err = openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.control.Close() })
	s.shared = &sharedRuntime{}
	s.hosted = &hostedRuntime{}
	if err := initializeSharedControl(ctx, s.control, "", queries, true); err != nil {
		t.Fatal(err)
	}
	const project = "lock-target"
	if _, err := s.control.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at) VALUES($1,$2,'lock target',$3)`, project, defaultTenantID, now()); err != nil {
		t.Fatal(err)
	}
	operator := f.operator(t)
	account := request(t, s, "POST", "/api/v1/oauth-clients", operator, externalBody(t, map[string]any{"name": "machine", "permissions": []string{"events:read"}, "projects": []string{project}}), 201)["client_id"].(string)
	key := request(t, s, "POST", "/api/v1/access-keys", operator, externalBody(t, map[string]any{"name": "revoke target", "permissions": []string{"events:write"}, "projects": []string{project}}), 201)["id"].(string)
	tenant := request(t, s, "POST", "/api/v1/tenants", operator, `{"name":"delete target"}`, 201)["tenant"].(map[string]any)["id"].(string)
	for _, operation := range []struct{ name, method, path, body string }{
		{"create-ingest", "POST", "/api/v1/access-keys", externalBody(t, map[string]any{"name": "expired ingest", "permissions": []string{"events:write"}, "projects": []string{project}})},
		{"create-service-key", "POST", "/api/v1/oauth-clients/" + account + "/api-keys", externalBody(t, map[string]any{"name": "expired service key", "audience": "api", "permissions": []string{"events:read"}, "projects": []string{project}})},
		{"revoke-key", "DELETE", "/api/v1/access-keys/" + key, ""},
		{"delete-tenant", "DELETE", "/api/v1/tenants/" + tenant, ""},
	} {
		t.Run(operation.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			blocker, err := s.control.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback()
			var pid int
			if err := blocker.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			if _, err := blocker.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest(operation.method, operation.path, strings.NewReader(operation.body)).WithContext(ctx)
			r.Header.Set("Authorization", "Bearer "+f.operator(t))
			r.Header.Set("Content-Type", "application/json")
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, r)
				done <- w
			}()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				var blocked bool
				if err := s.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("request never waited for the actual PostgreSQL write lock")
				case <-ticker.C:
				}
			}
			f.now.Add(int64(6 * time.Minute / time.Millisecond))
			if err := blocker.Rollback(); err != nil {
				t.Fatal(err)
			}
			select {
			case w := <-done:
				if w.Code != http.StatusUnauthorized {
					t.Fatalf("expired operator acted after waiting: %d %s", w.Code, w.Body.String())
				}
			case <-ctx.Done():
				t.Fatal("expired mutation did not terminate after lock release")
			}
		})
	}
	var changes int
	if err := s.control.QueryRow(`SELECT (SELECT count(*) FROM access_keys WHERE name IN ('expired ingest','expired service key'))+(SELECT count(*) FROM access_keys WHERE id=$1 AND revoked_at IS NOT NULL)+(SELECT count(*) FROM tenant_deletions WHERE tenant_id=$2)`, key, tenant).Scan(&changes); err != nil || changes != 0 {
		t.Fatal("expired mutations changed durable state", changes, err)
	}
}
