package porcupine

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"testing"
)

// A deployment operator's personal privileges and an OAuth tool's authorization
// are independent. A general analytics tool never acquires a wildcard grant
// from the operator's email login.
func TestEmailOAuthExplicitScopeCeilings(t *testing.T) {
	j := newEmailJourney(t)
	s := j.service
	admin := oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	projects := []string{}
	for _, name := range []string{"test-a", "test-b"} {
		body, _ := json.Marshal(map[string]string{"name": name})
		result := request(t, s, "POST", "/api/v1/projects", admin, string(body), 201)
		projects = append(projects, result["project"].(map[string]any)["id"].(string))
	}
	public := j.client("porcupine-local", localOAuthCallback, []string{"events:read", "management:read"})
	// The intersection of two wildcard project assignments still offers explicit project choices.
	token := j.browserLogin(t, public, "operator@example.test", "/api/v1", projects[0], "*", 200)
	request(t, s, "GET", "/api/v1/projects/"+projects[0]+"/schema", token.AccessToken, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+projects[1]+"/schema", token.AccessToken, "", 403)
	request(t, s, "GET", "/api/v1/humans", token.AccessToken, "", 403)

	// The admin registration requires scope=instance:admin explicitly. Analytics and
	// operator registrations cannot be substituted for one another.
	for _, tc := range []struct {
		client string
		scope  string
	}{
		{"porcupine-local", "instance:admin"}, {"porcupine-local-admin", ""}, {"porcupine-local-admin", "events:read"},
	} {
		app := j.client(tc.client, localOAuthCallback, nil)
		q := url.Values{"response_type": {"code"}, "client_id": {app.ClientID}, "redirect_uri": {app.RedirectURL}, "code_challenge_method": {"S256"}, "code_challenge": {"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}
		if tc.scope != "" {
			q.Set("scope", tc.scope)
		}
		r, err := http.Get(j.issuer + "/oauth/authorize?" + q.Encode())
		if err != nil {
			t.Fatal(err)
		}
		var oauthError struct {
			Error string `json:"error"`
		}
		err = json.NewDecoder(r.Body).Decode(&oauthError)
		r.Body.Close()
		if err != nil || r.StatusCode != 400 || oauthError.Error != "invalid_scope" {
			t.Fatalf("unexpected elevated/default authorization: client %s, scope %q, HTTP %d", tc.client, tc.scope, r.StatusCode)
		}
	}
}

func TestEmailProjectManagementGrantCannotMintIndependentClient(t *testing.T) {
	j := newEmailJourney(t)
	s := j.service
	admin := oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	project := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"Demo"}`, 201)["project"].(map[string]any)["id"].(string)
	scopes := []string{"management:write", "events:read"}
	invite, _ := json.Marshal(map[string]any{"email": "tester@example.test", "grants": []map[string]any{{"project_id": project, "permissions": scopes}}})
	request(t, s, "POST", "/api/v1/humans", admin, string(invite), 201)
	app := j.register(t, admin, "test-tool", scopes, []string{project})
	token := j.browserLogin(t, app, "tester@example.test", "/api/v1", project, "", 200)
	// The caller would otherwise have every permission and project in this body.
	clientBody, _ := json.Marshal(map[string]any{"name": "independent", "permissions": []string{"events:read"}, "projects": []string{project}})
	request(t, s, "POST", "/api/v1/oauth-clients", token.AccessToken, string(clientBody), 403)
	request(t, s, "PATCH", "/api/v1/projects/"+project, token.AccessToken, `{"name":"Member-managed project"}`, 200)
	other := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"Separate project"}`, 201)["project"].(map[string]any)["id"].(string)
	request(t, s, "PATCH", "/api/v1/projects/"+other, token.AccessToken, `{"name":"Out of scope"}`, 403)
	request(t, s, "DELETE", "/api/v1/projects/"+project, token.AccessToken, "", 403)
}

func TestInteractiveScopeMatrixAcrossSelectedProjects(t *testing.T) {
	human := actor{emailHuman: true, Permissions: []string{"events:read", "events:write"}, Projects: []string{"a", "b"}, emailProjectPermissions: map[string][]string{
		"a": {"events:read"}, "b": {"events:read", "events:write"},
	}}
	tool := actor{Permissions: []string{"events:read", "events:write"}, Projects: []string{"*"}}
	for _, tt := range []struct {
		projects, scopes []string
		allowed          bool
	}{
		{[]string{"a", "b"}, []string{"events:read"}, true},
		{[]string{"b"}, []string{"events:write", "events:read"}, true},
		{[]string{"a"}, []string{"events:write"}, false},
		{[]string{"a", "b"}, []string{"events:write", "events:read"}, false},
		{[]string{"*"}, []string{"events:read"}, false},
	} {
		if got := interactiveProjectsAllowed(human, tool, tt.scopes, tt.projects); got != tt.allowed {
			t.Errorf("projects=%v scopes=%v got %v want %v", tt.projects, tt.scopes, got, tt.allowed)
		}
	}
}

func TestEmailReadScopeDiscoveryForRestrictedClient(t *testing.T) {
	j := newEmailJourney(t)
	s := j.service
	metadata := request(t, s, "GET", "/.well-known/oauth-authorization-server", "", "", 200)
	if metadata["service_documentation"] != j.issuer+"/openapi.json" {
		t.Fatal("missing complete scope documentation")
	}
	admin := oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	project := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"Demo"}`, 201)["project"].(map[string]any)["id"].(string)
	read := []string{"events:read"}
	invite, _ := json.Marshal(map[string]any{"email": "reader@example.test", "grants": []map[string]any{{"project_id": project, "permissions": read}}})
	request(t, s, "POST", "/api/v1/humans", admin, string(invite), 201)
	app := j.register(t, admin, "reader", read, []string{project})
	resourceMeta := request(t, s, "GET", "/.well-known/oauth-protected-resource/api/v1", "", "", 200)
	app.Scopes = nil
	for _, scope := range resourceMeta["scopes_supported"].([]any) {
		app.Scopes = append(app.Scopes, scope.(string))
	}
	if !slices.Equal(app.Scopes, interactiveOAuthScopes) {
		t.Fatal("protected resource advertised elevated default scopes")
	}
	token := j.browserLogin(t, app, "reader@example.test", "/api/v1", project, "", 200)
	if token.Extra("scope") != "events:read" {
		t.Fatalf("unexpected granted scope: %v", token.Extra("scope"))
	}
}
