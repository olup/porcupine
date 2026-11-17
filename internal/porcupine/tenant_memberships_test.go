package porcupine

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
)

func TestTenantMembershipPerProjectPermissionsAndIndependentTeams(t *testing.T) {
	t.Parallel()
	f, operator, a, b, projectA, projectB := tenantServiceFixture(t)
	invite := func(tenant, body string) string {
		t.Helper()
		return request(t, f.s, "POST", "/api/v1/tenants/"+tenant+"/members", operator, body, 201)["human_id"].(string)
	}
	human := invite(a, tenantTestBody(t, map[string]any{"email": "analyst@example.test", "role": "member", "project_scope": "selected", "grants": []map[string]string{{"project_id": projectA, "role": "reader"}}}))
	if other := invite(b, tenantTestBody(t, map[string]any{"email": "analyst@example.test", "role": "member", "project_scope": "selected", "grants": []map[string]string{{"project_id": projectB, "role": "contributor"}}})); other != human {
		t.Fatal("one mailbox created separate human identities for two tenants")
	}
	load := func() auth.Principal {
		t.Helper()
		a, err := loadEmailHuman(t.Context(), f.s.control, human)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	current := load()
	for _, tc := range []struct {
		permission, project string
		want                bool
	}{{"events:read", projectA, true}, {"events:write", projectA, false}, {"events:write", projectB, true}, {"events:delete", projectB, false}, {"management:write", projectA, false}} {
		if got := auth.Authorize(current, tc.permission, tc.project); got != tc.want {
			t.Fatalf("permission union crossed a project boundary: %s %s=%v", tc.permission, tc.project, got)
		}
	}
	if err := f.s.validateCredentialGrant(t.Context(), current, "escalation", []string{"events:write"}, []string{projectA}); err == nil {
		t.Fatal("a permission on project B could be delegated for project A")
	}
	var future ProjectCreated
	f.call(t, "POST", "/api/v1/projects", operator, map[string]string{"name": "future", "tenant_id": a}, 201, &future)
	if auth.Authorize(load(), "events:read", future.Project.ID) {
		t.Fatal("selected-project membership included a future project")
	}
	request(t, f.s, "DELETE", "/api/v1/projects/"+projectA, operator, "", 204)
	listed := request(t, f.s, "GET", "/api/v1/tenants/"+a+"/members", operator, "", 200)["members"].([]any)
	if len(listed) != 1 || len(listed[0].(map[string]any)["grants"].([]any)) != 0 {
		t.Fatal("project deletion retained a dangling membership grant")
	}
	request(t, f.s, "PUT", "/api/v1/tenants/"+a+"/members/"+human, operator, `{"role":"member","project_scope":"all","project_role":"reader"}`, 200)
	if !auth.Authorize(load(), "events:read", future.Project.ID) || auth.Authorize(load(), "events:write", future.Project.ID) {
		t.Fatal("all-project membership lost its default read-only permissions")
	}
	request(t, f.s, "DELETE", "/api/v1/tenants/"+a+"/members/"+human, operator, "", 204)
	current = load()
	if auth.Authorize(current, "events:read", projectA) || !auth.Authorize(current, "events:write", projectB) {
		t.Fatal("revoking one membership changed another tenant's authority")
	}
	var revoked *string
	if err := f.s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=?`, human).Scan(&revoked); err != nil || revoked != nil {
		t.Fatal("revoking a membership disabled the global human identity")
	}
}

func TestTenantMembershipInputValidationAndAtomicInvitation(t *testing.T) {
	t.Parallel()
	f, operator, tenant, _, project, otherProject := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + tenant + "/members"
	for _, body := range []map[string]any{
		{"role": "owner"},
		{"role": "admin", "project_scope": "selected"},
		{"role": "admin", "permissions": []string{"events:read"}},
		{"role": "member", "project_scope": "all", "permissions": []string{"instance:admin"}},
		{"role": "member", "project_scope": "all", "permissions": []string{"tenant:admin"}},
		{"role": "member", "project_scope": "all", "permissions": []string{"unknown"}},
		{"role": "member", "project_scope": "all", "project_role": "reader", "permissions": []string{"events:write"}},
		{"role": "member", "grants": []map[string]string{{"project_id": "*"}}},
		{"role": "member", "grants": []map[string]string{{"project_id": otherProject}}},
		{"role": "member", "grants": []map[string]string{{"project_id": project}, {"project_id": project}}},
	} {
		body["email"] = "invalid@example.test"
		want := 400
		if body["role"] == "owner" {
			want = 422
		}
		request(t, f.s, "POST", base, operator, tenantTestBody(t, body), want)
	}
	if _, err := f.s.control.Exec(`CREATE TRIGGER reject_tenant_member BEFORE INSERT ON tenant_memberships BEGIN SELECT RAISE(ABORT,'fixture failure'); END`); err != nil {
		t.Fatal(err)
	}
	body := `{"email":"atomic@example.test","role":"admin"}`
	request(t, f.s, "POST", base, operator, body, 503)
	var count int
	if err := f.s.control.QueryRow(`SELECT count(*) FROM email_humans WHERE email='atomic@example.test'`).Scan(&count); err != nil || count != 0 {
		t.Fatal("membership insertion failure left an authorized human identity")
	}
	if _, err := f.s.control.Exec(`DROP TRIGGER reject_tenant_member`); err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	f.s.oauth.config.Email.SendInvitation = func(context.Context, string, bool) error {
		sends.Add(1)
		return nil
	}
	first := request(t, f.s, "POST", base, operator, body, 201)
	second := request(t, f.s, "POST", base, operator, body, 201)
	if first["human_id"] != second["human_id"] || sends.Load() != 1 {
		t.Fatal("an invitation retry duplicated an identity or its acknowledged notice")
	}
	request(t, f.s, "POST", base, operator, `{"email":"atomic@example.test","role":"member","project_scope":"all"}`, 409)
	if sends.Load() != 1 {
		t.Fatal("conflicting invitation sent another notice")
	}
}

func TestTenantMembershipConcurrentLastAdminProtectionAndScopeAttenuation(t *testing.T) {
	t.Parallel()
	f, operator, tenant, _, _, _ := tenantServiceFixture(t)
	base := "/api/v1/tenants/" + tenant + "/members"
	ids := []string{}
	for _, email := range []string{"first@example.test", "second@example.test"} {
		ids = append(ids, request(t, f.s, "POST", base, operator, tenantTestBody(t, map[string]string{"email": email, "role": "admin"}), 201)["human_id"].(string))
	}
	results := make(chan int, 2)
	for _, id := range ids {
		go func() {
			r := httptest.NewRequest("DELETE", base+"/"+id, nil)
			r.Header.Set("Authorization", "Bearer "+operator)
			w := httptest.NewRecorder()
			f.s.Handler().ServeHTTP(w, r)
			results <- w.Code
		}()
	}
	statuses := map[int]int{}
	for range 2 {
		select {
		case status := <-results:
			statuses[status]++
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent administrator removal stalled")
		}
	}
	if statuses[204] != 1 || statuses[409] != 1 {
		t.Fatalf("concurrent requests removed the final tenant administrator: %v", statuses)
	}
	var active int
	if err := f.s.control.QueryRow(`SELECT count(*) FROM tenant_memberships WHERE tenant_id=? AND role='admin' AND revoked_at IS NULL`, tenant).Scan(&active); err != nil || active != 1 {
		t.Fatal("tenant lost its last active administrator")
	}
	current := auth.Principal{Permissions: []string{"events:read", "management:read"}, TenantAdmins: []string{tenant}, TenantIDs: []string{tenant}, Interactive: true}
	if current.CanAdminTenant(tenant) || current.CanProvisionCredentials() {
		t.Fatal("a read token retained administrative authority from role metadata")
	}
	page := request(t, f.s, "GET", base+"?limit=1", operator, "", 200)
	raw, _ := json.Marshal(page)
	if len(page["members"].([]any)) != 1 || !strings.Contains(string(raw), "next_cursor") {
		t.Fatal("membership pagination did not return its continuation")
	}
	var remaining string
	if err := f.s.control.QueryRow(`SELECT human_id FROM tenant_memberships WHERE tenant_id=? AND revoked_at IS NULL`, tenant).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	request(t, f.s, "DELETE", "/api/v1/humans/"+remaining, operator, "", 204)
	request(t, f.s, "DELETE", base+"/"+remaining, operator, "", 204)
}

func TestTenantAdministratorPublicIngestionKeyLifecycle(t *testing.T) {
	t.Parallel()
	j, operator, tenant, otherTenant, _, _ := tenantOAuthSetup(t)
	app := j.client("porcupine-local-tenant-admin", localOAuthCallback, []string{tenantAdminScope})
	admin := j.browserLoginSelection(t, app, "lea@example.test", "/api/v1", "tenant", tenant, otherTenant, 200).AccessToken
	created := request(t, j.service, "POST", "/api/v1/projects", admin, tenantTestBody(t, map[string]string{"name": "browser app", "tenant_id": tenant}), 201)
	project := created["project"].(map[string]any)["id"].(string)
	key := request(t, j.service, "POST", "/api/v1/access-keys", admin, tenantTestBody(t, map[string]any{"name": "rotated browser key", "permissions": []string{"events:write"}, "projects": []string{project}}), 201)
	if key["tenant_id"] != tenant {
		t.Fatal("public ingestion key lost authoritative team ownership")
	}
	raw := key["token"].(string)
	nativeRequest(t, j.service, raw, `{"type":"track","name":"before-revoke","actor_id":"visitor"}`, 200)
	metadata := request(t, j.service, "GET", "/api/v1/access-keys", admin, "", 200)
	encoded, _ := json.Marshal(metadata)
	if strings.Contains(string(encoded), raw) || !strings.Contains(string(encoded), key["id"].(string)) {
		t.Fatal("tenant key discovery leaked a secret or hid an owned key")
	}
	var global string
	if err := j.service.control.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&global); err != nil {
		t.Fatal(err)
	}
	request(t, j.service, "DELETE", "/api/v1/access-keys/"+global, admin, "", 403)
	foreign := request(t, j.service, "POST", "/api/v1/projects", operator, tenantTestBody(t, map[string]string{"name": "foreign", "tenant_id": otherTenant}), 201)
	var foreignKey string
	if err := j.service.control.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(foreign["ingest_token"].(string))).Scan(&foreignKey); err != nil {
		t.Fatal(err)
	}
	request(t, j.service, "DELETE", "/api/v1/access-keys/"+foreignKey, admin, "", 403)
	request(t, j.service, "DELETE", "/api/v1/access-keys/"+key["id"].(string), admin, "", 204)
	nativeRequest(t, j.service, raw, `{"type":"track","name":"after-revoke","actor_id":"visitor"}`, 401)
}
