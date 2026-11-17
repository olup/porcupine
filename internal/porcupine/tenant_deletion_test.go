package porcupine

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/olup/porcupine/internal/auth"
)

func waitTenantDeletion(t *testing.T, s *Service, tenant, state string) TenantDeletion {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := s.tenantDeletionStatus(t.Context(), tenant)
		if err != nil {
			t.Fatal(err)
		}
		if status.State == state {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	status, _ := s.tenantDeletionStatus(t.Context(), tenant)
	t.Fatalf("deletion did not reach %s: %+v", state, status)
	return status
}

func TestTenantDeletionIsolationInterruptionAndRestart(t *testing.T) {
	t.Parallel()
	config := Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	a, b := tenantTestCreate(t, s, "A private name"), tenantTestCreate(t, s, "B")
	pa, ka := tenantJourneyProject(t, s, a)
	_, _ = tenantJourneyProject(t, s, a)
	pb, kb := tenantJourneyProject(t, s, b)
	request(t, s, "POST", "/capture/", "", captureBody(ka, "a-private-payload"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "b-kept"), 200)
	if _, err := s.db.Exec(`INSERT INTO metadata(key,value) VALUES('ducklake_source_id','temporarily-disconnected')`); err != nil {
		t.Fatal(err)
	}
	request(t, s, "DELETE", "/api/v1/tenants/"+a, "", "", 401)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, kb, "", 403)
	first := request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	retry := request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	if first["started_at"] != retry["started_at"] {
		t.Fatal("retry replaced the durable job")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ka, "late"), 401)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "b-still-accepts"), 200)
	request(t, s, "PATCH", "/api/v1/projects/"+pa, adminToken, `{"name":"resurrect"}`, 404)
	request(t, s, "POST", "/api/v1/projects", adminToken, tenantTestBody(t, map[string]any{"name": "late", "tenant_id": a}), 404)
	request(t, s, "POST", "/api/v1/access-keys", adminToken, tenantTestBody(t, map[string]any{"name": "late", "permissions": []string{"events:write"}, "projects": []string{pa}}), 404)
	request(t, s, "GET", "/api/v1/tenants/"+a, adminToken, "", 404)
	request(t, s, "GET", "/api/v1/tenants/"+a+"/deletion", kb, "", 403)
	request(t, s, "GET", "/api/v1/tenants/"+b, adminToken, "", 200)
	api := s.registerManagement(http.NewServeMux())
	for path, item := range api.OpenAPI().Paths {
		for _, op := range []*huma.Operation{item.Get, item.Post, item.Put, item.Patch, item.Delete} {
			if op == nil || op.OperationID == "delete-project" {
				continue
			}
			access := managementOperationAccess(op)
			if access.ProjectParameter == "" {
				continue
			}
			target := path
			for _, parameter := range op.Parameters {
				if parameter.In == "path" {
					value := "arbitrary-resource"
					if parameter.Name == access.ProjectParameter {
						value = pa
					}
					target = strings.ReplaceAll(target, "{"+parameter.Name+"}", value)
				}
			}
			request(t, s, op.Method, target, adminToken, `{}`, 404)
		}
	}
	// The disconnected archive keeps the durable cleanup intent pending.
	s.resumeProjectDeletions(t.Context())
	status, err := s.tenantDeletionStatus(t.Context(), a)
	if err != nil || status.State != "deleting" || status.ProjectsRemaining != 2 || status.PhysicalPurge != "pending_verification" {
		t.Fatalf("incorrect interrupted status: %+v %v", status, err)
	}
	if _, err := s.db.Exec(`DELETE FROM metadata WHERE key='ducklake_source_id'`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	status = waitTenantDeletion(t, s, a, "logically_deleted")
	if status.ProjectsRemaining != 0 || status.ProjectsTotal != 2 || status.PhysicalPurge != "pending_verification" {
		t.Fatalf("cleanup claimed physical purge: %+v", status)
	}
	for _, db := range []*sql.DB{s.db, s.control} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM tenants WHERE id=$1`, a).Scan(&count); err != nil || count != 0 {
			t.Fatal("deleted tenant row survived", count, err)
		}
	}
	var keys, usage int64
	if err := s.control.QueryRow(`SELECT count(*) FROM access_keys WHERE token_hash=$1`, hash(ka)).Scan(&keys); err != nil || keys != 0 {
		t.Fatal("tenant ingestion credential survived", keys, err)
	}
	if err := s.control.QueryRow(`SELECT accepted_events FROM tenant_deletion_usage WHERE tenant_id=$1`, a).Scan(&usage); err != nil || usage != 1 {
		t.Fatal("cumulative usage was refunded or lost", usage, err)
	}
	if err := s.publishLocalUsage(t.Context()); err != nil {
		t.Fatal("late usage publication failed", err)
	}
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", adminToken, "", 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "b-after-restart"), 200)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	request(t, s, "DELETE", "/api/v1/tenants/never-existed", adminToken, "", 404)
}

func TestTenantDeletionDefaultNeverReappears(t *testing.T) {
	t.Parallel()
	config := Config{DBPath: filepath.Join(t.TempDir(), "default.db"), BootstrapToken: adminToken}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	request(t, s, "DELETE", "/api/v1/tenants/default", adminToken, "", 202)
	waitTenantDeletion(t, s, defaultTenantID, "logically_deleted")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, db := range []*sql.DB{s.db, s.control} {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM tenants WHERE id='default'`).Scan(&n); err != nil || n != 0 {
			t.Fatal("default tenant resurrected on restart", n, err)
		}
	}
	request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"implicit default"}`, 404)
	other := tenantTestCreate(t, s, "Explicit replacement")
	_, _ = tenantJourneyProject(t, s, other)
}

func TestTenantDeletionMixedHumanGrantKeepsBTokenAndFutureConsent(t *testing.T) {
	t.Parallel()
	j := newEmailJourney(t)
	s := j.service
	operator := accountTestToken(t, s)
	createTenant := func(name string) string {
		return request(t, s, "POST", "/api/v1/tenants", operator, `{"name":"`+name+`"}`, 201)["tenant"].(map[string]any)["id"].(string)
	}
	a, b := createTenant("A"), createTenant("B")
	createProject := func(tenant string) string {
		return request(t, s, "POST", "/api/v1/projects", operator, `{"name":"Project","tenant_id":"`+tenant+`"}`, 201)["project"].(map[string]any)["id"].(string)
	}
	pa, pb := createProject(a), createProject(b)
	human := request(t, s, "POST", "/api/v1/humans", operator, tenantTestBody(t, map[string]any{"email": "mixed@example.test", "projects": []string{pa, pb}, "permissions": []string{"events:read", "management:read"}}), 201)["id"].(string)
	app := j.register(t, operator, "mixed", []string{"events:read", "management:read"}, []string{pa, pb})
	aOnly := j.browserLogin(t, app, "mixed@example.test", "/api/v1", pa, "", 200)
	token := j.browserLogin(t, app, "mixed@example.test", "/api/v1", pb, "", 200)
	// The same granted connection has both A and B scopes, including its frozen
	// refresh scope. This models existing mixed-tenant consent from older clients.
	encoded := tenantTestBody(t, []string{pa, pb})
	for _, statement := range []struct {
		sql  string
		hash string
	}{
		{`UPDATE access_keys SET projects=$1 WHERE id=(SELECT g.principal_id FROM oauth_interactive_grants g JOIN oauth_tokens t ON t.interactive_grant_id=g.id WHERE t.token_hash=$2)`, hash(token.AccessToken)},
		{`UPDATE oauth_tokens SET projects_json=$1 WHERE token_hash=$2`, hash(token.AccessToken)},
		{`UPDATE oauth_refresh_tokens SET projects_json=$1 WHERE token_hash=$2`, hash(token.RefreshToken)},
	} {
		if _, err := s.control.Exec(statement.sql, encoded, statement.hash); err != nil {
			t.Fatal(err)
		}
	}
	request(t, s, "GET", "/api/v1/projects/"+pa+"/schema", token.AccessToken, "", 200)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, token.AccessToken, "", 403)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, operator, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	request(t, s, "GET", "/api/v1/projects/"+pa+"/schema", aOnly.AccessToken, "", 401)
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", token.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+pa+"/schema", token.AccessToken, "", 403)
	actor, err := loadEmailHuman(t.Context(), s.control, human)
	if err != nil || slices.Contains(actor.Projects, pa) || !auth.Authorize(actor, "events:read", pb) {
		t.Fatal("historical global IDs conferred effective authority", actor, err)
	}
	var historical string
	if err := s.control.QueryRow(`SELECT project_grants FROM email_humans WHERE principal_id=$1`, human).Scan(&historical); err != nil || !strings.Contains(historical, pa) {
		t.Fatal("test did not preserve historical global identity metadata", historical, err)
	}
	client, err := loadOAuthClient(t.Context(), s.control, app.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	refreshed, err := s.exchangeInteractiveRefresh(t.Context(), client, url.Values{"refresh_token": {token.RefreshToken}, "resource": {s.oauth.apiResource}})
	if err != nil {
		t.Fatal("B refresh was unnecessarily revoked", err)
	}
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", refreshed["access_token"].(string), "", 200)
	fresh := j.browserLogin(t, app, "mixed@example.test", "/api/v1", pb, pa, 200)
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", fresh.AccessToken, "", 200)
	for _, path := range []string{"/api/v1/humans", "/api/v1/access-keys", "/api/v1/oauth-clients", "/api/v1/oauth-grants"} {
		body := request(t, s, "GET", path, operator, "", 200)
		if strings.Contains(tenantTestBody(t, body), pa) {
			t.Fatal("public view exposed historical deleted-project scope", path, body)
		}
	}
}

func TestTenantDeletionFenceDoesNotRequireNativeIdentity(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a := tenantTestCreate(t, s, "A")
	pa, _ := tenantJourneyProject(t, s, a)
	ctx := context.WithValue(t.Context(), actorContextKey{}, auth.Principal{ID: "external-subject-no-native-row", Permissions: []string{"instance:admin"}, Projects: []string{"*"}})
	if err := s.beginTenantDeletion(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := s.requireProjectActive(ctx, pa); err == nil {
		t.Fatal("an external operator principal bypassed the authoritative fence")
	}
	request := httptest.NewRequest("GET", "/api/v1/projects/"+pa+"/schema", nil)
	request.Header.Set("Authorization", "Bearer "+adminToken)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != 404 {
		t.Fatal("operator bypassed fence", response.Code)
	}
}

func TestTenantDeletionRemovesOwnedMachinesAndTenantAdminGrantOnly(t *testing.T) {
	t.Parallel()
	j, operator, a, b, pb, human := tenantOAuthSetup(t)
	s := j.service
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	admin := j.browserLoginSelection(t, app, "lea@example.test", "/api/v1", "tenant", a, "", 200)
	pa := request(t, s, "POST", "/api/v1/projects", admin.AccessToken, `{"name":"A","tenant_id":"`+a+`"}`, 201)["project"].(map[string]any)["id"].(string)
	base := "/api/v1/tenants/" + a + "/service-accounts"
	machine := request(t, s, "POST", base, admin.AccessToken, tenantTestBody(t, TenantServiceAccountSpec{Name: "A machine", Permissions: []string{"events:read"}, Projects: []string{pa}}), 201)
	machineID := machine["client_id"].(string)
	static := request(t, s, "POST", base+"/"+machineID+"/api-keys", admin.AccessToken, tenantTestBody(t, ServiceKeySpec{Name: "A static", Audience: "api", Permissions: []string{"events:read"}, Projects: []string{pa}}), 201)
	machineToken := oauthProtocolRequest(t, s, machineID, machine["client_secret"].(string), url.Values{"grant_type": {"client_credentials"}, "resource": {s.oauth.apiResource}}, 200)["access_token"].(string)
	readerApp := j.register(t, operator, "reader-b", []string{"events:read", "management:read"}, []string{pb})
	bToken := j.browserLogin(t, readerApp, "lea@example.test", "/api/v1", pb, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+pa+"/schema", static["token"].(string), "", 200)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, operator, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	for _, credential := range []string{admin.AccessToken, machineToken, static["token"].(string)} {
		request(t, s, "GET", "/api/v1/projects/"+pa+"/schema", credential, "", 401)
	}
	request(t, s, "GET", "/api/v1/projects/"+pb+"/schema", bToken.AccessToken, "", 200)
	for _, table := range []string{"tenant_memberships", "tenant_credentials", "oauth_grant_tenants"} {
		var count int
		if err := s.control.QueryRow(`SELECT count(*) FROM `+table+` WHERE tenant_id=$1`, a).Scan(&count); err != nil || count != 0 {
			t.Fatal("tenant control records survived", table, count, err)
		}
	}
	var memberships int
	if err := s.control.QueryRow(`SELECT count(*) FROM tenant_memberships WHERE tenant_id=$1 AND human_id=$2 AND revoked_at IS NULL`, b, human).Scan(&memberships); err != nil || memberships != 1 {
		t.Fatal("B membership was removed", memberships, err)
	}
	for _, db := range []*sql.DB{s.db, s.control} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM access_keys WHERE id=$1 OR id=$2`, machineID, static["id"].(string)).Scan(&count); err != nil || count != 0 {
			t.Fatal("owned machine credential survived", count, err)
		}
	}
}

func TestTenantDeletionStartRollsBackEveryFenceAndRevocation(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	a := tenantTestCreate(t, s, "A")
	pa, ka := tenantJourneyProject(t, s, a)
	if _, err := s.control.Exec(`CREATE TRIGGER fail_tenant_revocation BEFORE UPDATE OF revoked_at ON access_keys BEGIN SELECT RAISE(ABORT,'fixture revocation failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginTenantDeletion(t.Context(), a); err == nil {
		t.Fatal("deletion ignored a failed revocation")
	}
	if err := s.requireProjectActive(t.Context(), pa); err != nil {
		t.Fatal("rolled back deletion left an access fence", err)
	}
	var n int
	if err := s.control.QueryRow(`SELECT count(*) FROM tenant_deletions WHERE tenant_id=$1`, a).Scan(&n); err != nil || n != 0 {
		t.Fatal("partial job committed", n, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ka, "after-rollback"), 200)
}

func TestSharedTenantDeletionFencesAndReportsLocalCleanupPending(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	s.clock = time.Now
	if _, err := s.control.Exec(`INSERT INTO tenants SELECT 'B','B',created_at,ingestion_allowed,ingestion_monthly_events,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes FROM tenants WHERE id='default'; UPDATE projects SET tenant_id='B' WHERE id='kept'`); err != nil {
		t.Fatal(err)
	}
	sharedDeletionPrincipal(t, s.control, "tenant-key", []string{"removed"})
	if _, err := s.control.Exec(`INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES('tenant-key','default')`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginTenantDeletion(t.Context(), defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if err := s.beginTenantDeletion(t.Context(), defaultTenantID); err != nil {
		t.Fatal("cross-worker retry failed", err)
	}
	if err := s.requireProjectActive(t.Context(), "removed"); err == nil {
		t.Fatal("shared deletion did not fence A")
	}
	if err := s.requireProjectActive(t.Context(), "kept"); err != nil {
		t.Fatal("shared deletion fenced B", err)
	}
	var revoked sql.NullString
	if err := s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id='tenant-key'`).Scan(&revoked); err != nil || !revoked.Valid {
		t.Fatal("shared owned credential not revoked", revoked, err)
	}
	s.resumeTenantDeletions(t.Context())
	status, err := s.tenantDeletionStatus(t.Context(), defaultTenantID)
	if err != nil || status.State != "deleting" || status.ErrorCode != "shared_local_cleanup_pending" || status.PhysicalPurge != "pending_verification" {
		t.Fatal("owner-local backend claimed full cleanup", status, err)
	}
}

func TestTenantDeletionMCPUsesOperatorContract(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	a := tenantTestCreate(t, s, "A")
	pa, _ := tenantJourneyProject(t, s, a)
	_, reader := key(t, s, []string{"events:read", "management:read"}, []string{pa})
	ctx, client := mcpTestSession(t, host.URL, reader, "")
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "delete-tenant", Path: map[string]string{"tenant_id": a}}, 403)
	mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "get-tenant-deletion", Path: map[string]string{"tenant_id": a}}, 403)
	ctx, operator := mcpTestSession(t, host.URL, adminToken, "")
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "delete-tenant", Path: map[string]string{"tenant_id": a}}, 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "get-tenant-deletion", Path: map[string]string{"tenant_id": a}}, 200)
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": pa}}, 404)
}

func TestTenantDeletionRemovesNativeDataWithoutResurrection(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	a, b := tenantTestCreate(t, s, "A"), tenantTestCreate(t, s, "B")
	pa, ka := tenantJourneyProject(t, s, a)
	pb, kb := tenantJourneyProject(t, s, b)
	request(t, s, "POST", "/capture/", "", captureBody(ka, "a-native-private-payload"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "b-native-kept"), 200)
	duckLakeJourneyDrain(t, s)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	if _, err := s.lake.Load(t.Context(), pa, "a-native-private-payload"); err == nil {
		t.Fatal("native archive reopened a deleted tenant project")
	}
	kept, err := s.lake.Load(t.Context(), pb, "b-native-kept")
	if err != nil || kept.ProjectID != pb {
		t.Fatal("native deletion changed tenant B", kept, err)
	}
	if err := s.lake.Maintain(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.lake.Load(t.Context(), pa, "a-native-private-payload"); err == nil {
		t.Fatal("maintenance resurrected the deleted project")
	}
	rows := duckLakeJourneyQuery(t, s, pb, adminToken, "SELECT count(*) AS n FROM events", nil)
	if len(rows) != 1 {
		t.Fatal("B query failed after A cleanup", rows)
	}
}
