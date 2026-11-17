package porcupine

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"testing"

	"github.com/olup/porcupine/internal/auth"
)

type identityRecoveryTokens struct{ access, refresh string }

func identityRecoveryCode(t *testing.T, f *oidcFixture, project string) url.Values {
	t.Helper()
	csrf := f.login(t)
	redirect := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {project}}, f.host.URL, http.StatusSeeOther)
	target, err := url.Parse(redirect)
	if err != nil || target.Query().Get("code") == "" {
		t.Fatal("missing authorization code before recovery")
	}
	return url.Values{"grant_type": {"authorization_code"}, "client_id": {f.clientID}, "redirect_uri": {f.callback}, "code": {target.Query().Get("code")}, "code_verifier": {f.verifier}, "resource": {f.resource}}
}

func identityRecoveryExchange(t *testing.T, f *oidcFixture, form url.Values, status int) identityRecoveryTokens {
	t.Helper()
	response, err := http.PostForm(f.host.URL+"/oauth/token", form)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || response.StatusCode != status {
		t.Fatalf("token exchange after recovery: HTTP %d want %d, error %q, decode %v", response.StatusCode, status, result.Error, err)
	}
	if status == http.StatusOK && (result.Access == "" || result.Refresh == "") {
		t.Fatal("successful interactive exchange omitted access or refresh token")
	}
	return identityRecoveryTokens{result.Access, result.Refresh}
}

func identityRecoveryIssue(t *testing.T, f *oidcFixture, project string) identityRecoveryTokens {
	t.Helper()
	return identityRecoveryExchange(t, f, identityRecoveryCode(t, f, project), http.StatusOK)
}

func identityRecoveryList(t *testing.T, f *oidcFixture, operator, human string) []any {
	t.Helper()
	return request(t, f.s, http.MethodGet, "/api/v1/humans/"+human+"/identities", operator, "", http.StatusOK)["identities"].([]any)
}

func identityRecoveryBinding(t *testing.T, f *oidcFixture, operator string) string {
	t.Helper()
	identities := identityRecoveryList(t, f, operator, f.humanID)
	if len(identities) != 1 {
		t.Fatalf("expected one linked provider identity, got %d", len(identities))
	}
	return identities[0].(map[string]any)["id"].(string)
}

func identityRecoveryVersion(t *testing.T, f *oidcFixture) int64 {
	t.Helper()
	var version int64
	if err := f.s.control.QueryRow(`SELECT login_version FROM email_humans WHERE principal_id=?`, f.humanID).Scan(&version); err != nil {
		t.Fatal(err)
	}
	return version
}

func identityRecoveryOtherHuman(t *testing.T, f *oidcFixture) string {
	t.Helper()
	human := EmailHuman{ID: newID(), Email: "unaffected@example.test", CreatedAt: now(), Projects: []string{"project-b"}, Permissions: []string{"events:read", "management:read"}, Grants: []HumanProjectGrant{{ProjectID: "project-b", Permissions: []string{"events:read", "management:read"}}}}
	tx, err := f.s.control.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertEmailHuman(t.Context(), tx, human); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return human.ID
}

func TestOIDCIdentityManagementHTTPAuthorizationAndPagination(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	f.login(t)
	binding := identityRecoveryBinding(t, f, operator)
	base := "/api/v1/humans/" + f.humanID + "/identities"
	tenant := request(t, f.s, http.MethodPost, "/api/v1/tenants", operator, `{"name":"Identity recovery team"}`, 201)["tenant"].(map[string]any)["id"].(string)
	request(t, f.s, http.MethodPost, "/api/v1/tenants/"+tenant+"/members", operator, `{"email":"operator@example.test","role":"admin"}`, 201)
	f.resource = f.host.URL + "/api/v1"
	reader := f.issueToken(t, "project-a")
	f.clientID, f.callback, f.scope = "porcupine-local-tenant-admin", localOAuthCallback, tenantAdminScope
	tenantAdmin := f.issueSelectionToken(t, "tenant", tenant)
	for _, tc := range []struct {
		name, credential string
		status           int
	}{{"absent", "", 401}, {"invalid", "not-a-valid-credential", 401}, {"read-only", reader, 403}, {"tenant-admin", tenantAdmin, 403}} {
		t.Run(tc.name, func(t *testing.T) {
			request(t, f.s, http.MethodGet, base, tc.credential, "", tc.status)
			request(t, f.s, http.MethodDelete, base+"/"+binding, tc.credential, "", tc.status)
		})
	}
	other := identityRecoveryOtherHuman(t, f)
	if _, err := f.s.control.Exec(`INSERT INTO oidc_identities(id,issuer,subject,principal_id,linked_email,created_at) VALUES(?,?,?,?,?,?)`, "other-human-identity", f.provider.host.URL, "other-subject", other, "unaffected@example.test", now()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"additional-identity-a", "additional-identity-z"} {
		if _, err := f.s.control.Exec(`INSERT INTO oidc_identities(id,issuer,subject,principal_id,linked_email,created_at) VALUES(?,?,?,?,?,?)`, id, "https://"+id+".example", id, f.humanID, "operator@example.test", now()); err != nil {
			t.Fatal(err)
		}
	}
	var seen []string
	cursor := ""
	for range 4 {
		page := request(t, f.s, http.MethodGet, base+"?limit=1&cursor="+url.QueryEscape(cursor), operator, "", http.StatusOK)
		identities := page["identities"].([]any)
		if len(identities) != 1 {
			t.Fatalf("pagination returned %d bindings, want one", len(identities))
		}
		identity := identities[0].(map[string]any)
		for field := range identity {
			if !slices.Contains([]string{"id", "issuer", "subject", "linked_email", "created_at"}, field) {
				t.Fatalf("unexpected identity metadata field %q", field)
			}
		}
		id := identity["id"].(string)
		if id == "other-human-identity" || slices.Contains(seen, id) || identity["linked_email"] != "operator@example.test" {
			t.Fatal("identity listing leaked another human or repeated a page")
		}
		seen = append(seen, id)
		next, _ := page["next_cursor"].(string)
		if next == "" {
			break
		}
		if next != id || next <= cursor {
			t.Fatal("identity cursor did not advance over the last binding")
		}
		cursor = next
	}
	if len(seen) != 3 {
		t.Fatalf("pagination lost bindings: %v", seen)
	}
	request(t, f.s, http.MethodGet, "/api/v1/humans/unknown-human/identities", operator, "", http.StatusNotFound)
	request(t, f.s, http.MethodDelete, "/api/v1/humans/unknown-human/identities/"+binding, operator, "", http.StatusNotFound)
	version := identityRecoveryVersion(t, f)
	request(t, f.s, http.MethodDelete, base+"/other-human-identity", operator, "", http.StatusNoContent)
	if identityRecoveryVersion(t, f) != version || len(identityRecoveryList(t, f, operator, other)) != 1 {
		t.Fatal("mismatched human/binding IDs changed an account")
	}
}

func TestOIDCIdentityManagementMCPUsesOperatorPolicy(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	f.login(t)
	operatorAPI := accountTestToken(t, f.s)
	binding := identityRecoveryBinding(t, f, operatorAPI)
	operatorMCP := oauthProtocolRequest(t, f.s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + "/mcp"}}, 200)["access_token"].(string)
	ctx, operator := mcpTestSession(t, f.host.URL, operatorMCP, "2025-11-25")
	page := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "list-human-identities", Path: map[string]string{"id": f.humanID}}, 200)
	if len(page["identities"].([]any)) != 1 {
		t.Fatal("MCP did not return the HTTP identity listing")
	}
	reader := f.issueToken(t, "project-a")
	readCtx, readClient := mcpTestSession(t, f.host.URL, reader, "2025-11-25")
	mcpTestCall(t, readCtx, readClient, mcpCallInput{OperationID: "list-human-identities", Path: map[string]string{"id": f.humanID}}, 403)
	mcpTestCall(t, readCtx, readClient, mcpCallInput{OperationID: "unlink-human-identity", Path: map[string]string{"id": f.humanID, "identity_id": binding}}, 403)
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "unlink-human-identity", Path: map[string]string{"id": f.humanID, "identity_id": binding}}, 204)
	if len(identityRecoveryList(t, f, operatorAPI, f.humanID)) != 0 {
		t.Fatal("MCP unlink did not remove the HTTP-visible binding")
	}
}

func TestOIDCIdentityRecoveryRevokesGrantsCodesAndPendingConsent(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	original := identityRecoveryIssue(t, f, "project-a")
	binding := identityRecoveryBinding(t, f, operator)
	otherID := identityRecoveryOtherHuman(t, f)
	f.provider.mu.Lock()
	f.provider.email, f.provider.subject = "unaffected@example.test", "unaffected-subject"
	f.provider.mu.Unlock()
	unaffected := identityRecoveryIssue(t, f, "project-b")
	f.provider.mu.Lock()
	f.provider.email, f.provider.subject = "operator@example.test", "operator-subject"
	f.provider.mu.Unlock()
	unredeemed := identityRecoveryCode(t, f, "project-a")
	pendingCSRF := f.login(t)
	version := identityRecoveryVersion(t, f)
	path := "/api/v1/humans/" + f.humanID + "/identities/" + binding
	request(t, f.s, http.MethodDelete, path, operator, "", http.StatusNoContent)
	if identityRecoveryVersion(t, f) != version+1 {
		t.Fatal("unlink did not advance the human's browser-proof version exactly once")
	}
	if _, err := f.s.authenticateResource(t.Context(), original.access, f.resource); err == nil {
		t.Fatal("issued access token survived identity recovery")
	}
	identityRecoveryExchange(t, f, url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {original.refresh}, "resource": {f.resource}}, http.StatusBadRequest)
	identityRecoveryExchange(t, f, unredeemed, http.StatusBadRequest)
	f.submit(t, url.Values{"csrf": {pendingCSRF}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, http.StatusForbidden)
	if _, err := f.s.authenticateResource(t.Context(), unaffected.access, f.resource); err != nil || len(identityRecoveryList(t, f, operator, otherID)) != 1 {
		t.Fatal("recovery affected another human", err)
	}
	fresh := identityRecoveryIssue(t, f, "project-a")
	freshBinding := identityRecoveryBinding(t, f, operator)
	if freshBinding == binding {
		t.Fatal("fresh sign-in reused a deleted binding ID")
	}
	request(t, f.s, http.MethodDelete, path, operator, "", http.StatusNoContent)
	if identityRecoveryVersion(t, f) != version+1 || identityRecoveryBinding(t, f, operator) != freshBinding {
		t.Fatal("retrying an old recovery invalidated the fresh sign-in")
	}
	if _, err := f.s.authenticateResource(t.Context(), fresh.access, f.resource); err != nil {
		t.Fatal("old binding retry revoked the fresh OAuth grant", err)
	}
	identityRecoveryExchange(t, f, url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {fresh.refresh}, "resource": {f.resource}}, http.StatusOK)
}

func TestOIDCIdentityRecoveryRollbackPreservesBindingAndGrants(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	tokens := identityRecoveryIssue(t, f, "project-a")
	binding := identityRecoveryBinding(t, f, operator)
	code := identityRecoveryCode(t, f, "project-a")
	version := identityRecoveryVersion(t, f)
	if _, err := f.s.control.Exec(`CREATE TRIGGER fail_identity_recovery BEFORE UPDATE OF login_version ON email_humans BEGIN SELECT RAISE(ABORT,'fixture version write failed'); END`); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/humans/" + f.humanID + "/identities/" + binding
	request(t, f.s, http.MethodDelete, path, operator, "", http.StatusServiceUnavailable)
	if identityRecoveryBinding(t, f, operator) != binding || identityRecoveryVersion(t, f) != version {
		t.Fatal("failed recovery partially removed the identity or advanced its version")
	}
	if _, err := f.s.authenticateResource(t.Context(), tokens.access, f.resource); err != nil {
		t.Fatal("failed recovery partially revoked an OAuth grant", err)
	}
	identityRecoveryExchange(t, f, code, http.StatusOK)
	if _, err := f.s.control.Exec(`DROP TRIGGER fail_identity_recovery`); err != nil {
		t.Fatal(err)
	}
	request(t, f.s, http.MethodDelete, path, operator, "", http.StatusNoContent)
	if _, err := f.s.authenticateResource(t.Context(), tokens.access, f.resource); err == nil {
		t.Fatal("successful retry did not revoke the original OAuth grant")
	}
}

func TestOIDCIdentityRecoveryPreservesTenantMembershipsAndTeamCredentials(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	var tenants, projects []string
	for _, name := range []string{"First team", "Second team"} {
		tenant := request(t, f.s, http.MethodPost, "/api/v1/tenants", operator, tenantOAuthJSON(t, map[string]any{"name": name}), 201)["tenant"].(map[string]any)["id"].(string)
		project := request(t, f.s, http.MethodPost, "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": "Product", "tenant_id": tenant}), 201)["project"].(map[string]any)["id"].(string)
		member := request(t, f.s, http.MethodPost, "/api/v1/tenants/"+tenant+"/members", operator, `{"email":"operator@example.test","role":"member","project_scope":"all","project_role":"reader"}`, 201)
		if member["human_id"] != f.humanID {
			t.Fatal("tenant invitation created a different human")
		}
		tenants, projects = append(tenants, tenant), append(projects, project)
	}
	base := "/api/v1/tenants/" + tenants[0] + "/service-accounts"
	machine := request(t, f.s, http.MethodPost, base, operator, tenantOAuthJSON(t, map[string]any{"name": "Team reporting", "permissions": []string{"events:read"}, "projects": []string{projects[0]}}), 201)
	machineID := machine["client_id"].(string)
	machineToken := oauthProtocolRequest(t, f.s, machineID, machine["client_secret"].(string), url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + "/api/v1"}}, 200)["access_token"].(string)
	serviceKey := request(t, f.s, http.MethodPost, base+"/"+machineID+"/api-keys", operator, tenantOAuthJSON(t, map[string]any{"name": "Independent team key", "audience": "api", "permissions": []string{"events:read"}, "projects": []string{projects[0]}}), 201)["token"].(string)
	first := identityRecoveryIssue(t, f, projects[0])
	second := identityRecoveryIssue(t, f, projects[1])
	binding := identityRecoveryBinding(t, f, operator)
	request(t, f.s, http.MethodDelete, "/api/v1/humans/"+f.humanID+"/identities/"+binding, operator, "", http.StatusNoContent)
	for _, credential := range []string{first.access, second.access} {
		if _, err := f.s.authenticateResource(t.Context(), credential, f.resource); err == nil {
			t.Fatal("a tenant's interactive connection survived global identity recovery")
		}
	}
	for _, credential := range []string{machineToken, serviceKey} {
		principal, err := f.s.authenticateResource(t.Context(), credential, f.host.URL+"/api/v1")
		if err != nil || !auth.Authorize(principal, "events:read", projects[0]) || auth.Authorize(principal, "events:read", projects[1]) {
			t.Fatal("identity recovery changed independent team credentials", err)
		}
	}
	for _, tenant := range tenants {
		members := request(t, f.s, http.MethodGet, "/api/v1/tenants/"+tenant+"/members", operator, "", 200)["members"].([]any)
		if len(members) != 1 || members[0].(map[string]any)["human_id"] != f.humanID || members[0].(map[string]any)["revoked_at"] != nil {
			t.Fatal("identity recovery removed a tenant membership")
		}
	}
	current, err := loadEmailHuman(t.Context(), f.s.control, f.humanID)
	if err != nil || !auth.Authorize(current, "events:read", projects[0]) || !auth.Authorize(current, "events:read", projects[1]) {
		t.Fatal("identity recovery changed the human's standing project permissions", err)
	}
	var revoked *string
	if err := f.s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=?`, f.humanID).Scan(&revoked); err != nil || revoked != nil {
		t.Fatal("identity recovery disabled the durable human", err)
	}
}
