package porcupine

import (
	"net/http"
	"strings"
	"testing"
)

func TestServiceKeysAuthenticateByResourceAndProject(t *testing.T) {
	f := newOAuthIntegration(t)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	a, public := f.project(t, admin)
	b, _ := f.project(t, admin)
	rights := []string{"events:read", "events:write", "management:read", "management:write", "imports:write"}
	client := f.createClient(t, admin, rights, []string{a})
	base := "/api/v1/oauth-clients/" + client.ClientID + "/api-keys"
	create := func(audience string, permissions []string) AccessKeyCreated {
		t.Helper()
		var key AccessKeyCreated
		h := f.call(t, http.MethodPost, base, admin, map[string]any{"name": "daily reporting", "audience": audience, "projects": []string{a}, "permissions": permissions}, 201, &key)
		if key.Audience != audience || key.ServiceClientID != client.ClientID || !strings.HasPrefix(key.Token, serviceKeyPrefix) || h.Get("Cache-Control") != "no-store" {
			t.Fatal("service key metadata or secrecy contract")
		}
		var stored string
		if err := f.s.db.QueryRow("SELECT token_hash FROM access_keys WHERE id=?", key.ID).Scan(&stored); err != nil || stored != hash(key.Token) {
			t.Fatal("service key stored incorrectly")
		}
		return key
	}
	api := create("api", rights)
	mcp := create("mcp", []string{"events:read"})
	eventPath := "/api/v1/projects/" + a + "/events"
	f.call(t, "GET", eventPath, api.Token, nil, 200, nil)
	f.call(t, "GET", "/api/v1/projects/"+b+"/events", api.Token, nil, 403, nil)
	f.call(t, "GET", "/api/v1/projects/"+a+"/imports", api.Token, nil, 200, nil)
	f.call(t, "GET", eventPath, mcp.Token, nil, 401, nil)
	for _, wrong := range []string{adminToken, public, client.ClientSecret} {
		f.call(t, "GET", eventPath, wrong, nil, 401, nil)
	}
	if _, err := f.s.authenticateResource(f.ctx, api.Token, f.s.oauth.mcpResource); err == nil {
		t.Fatal("API service key accepted at MCP audience")
	}
	ctx, session := mcpTestSession(t, f.host.URL, mcp.Token, "2025-11-25")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": a}}, 200)
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": b}}, 403)
	// Public ingestion credentials never gain bearer access, and private service
	// secrets cannot enter analytics as SDK keys in a public payload.
	for _, key := range []string{api.Token, mcp.Token} {
		f.call(t, "POST", "/capture/", "", map[string]any{"api_key": key, "event": "click", "distinct_id": "x"}, 401, nil)
	}
	f.call(t, "POST", "/capture/", api.Token, map[string]any{"event": "click", "distinct_id": "x"}, 200, nil)
	f.call(t, "POST", base, api.Token, map[string]any{"name": "child", "audience": "api", "projects": []string{a}, "permissions": []string{"events:read"}}, 403, nil)
	f.call(t, "POST", "/api/v1/access-keys", api.Token, map[string]any{"name": "child", "projects": []string{a}, "permissions": []string{"events:write"}}, 403, nil)
	for _, excess := range []map[string]any{
		{"name": "other project", "audience": "api", "projects": []string{b}, "permissions": []string{"events:read"}},
		{"name": "higher permission", "audience": "mcp", "projects": []string{a}, "permissions": []string{"identity:write"}},
	} {
		f.call(t, "POST", base, admin, excess, 403, nil)
	}
	var list AccessKeyList
	f.call(t, "GET", "/api/v1/access-keys", admin, nil, 200, &list)
	found := false
	for _, item := range list.AccessKeys {
		if item.ID == api.ID {
			found = item.Audience == "api" && item.ServiceClientID == client.ClientID
		}
	}
	if !found {
		t.Fatal("service key missing from management discovery")
	}

	f.call(t, "DELETE", "/api/v1/access-keys/"+api.ID, admin, nil, 204, nil)
	f.call(t, "GET", eventPath, api.Token, nil, 401, nil)
	if _, err := f.s.exportActor(f.ctx, api.ID); err == nil {
		t.Fatal("revoked key could still own export work")
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": a}}, 200)
	f.call(t, "DELETE", "/api/v1/oauth-clients/"+client.ClientID, admin, nil, 204, nil)
	if _, err := f.s.authenticateResource(f.ctx, mcp.Token, f.s.oauth.mcpResource); err == nil {
		t.Fatal("revoked account still accepts service key")
	}
	if _, err := f.s.exportActor(f.ctx, mcp.ID); err == nil {
		t.Fatal("revoked parent left background work authorized")
	}
}

func TestServiceKeyServerOnlyAndParentChange(t *testing.T) {
	f := newOAuthIntegration(t)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	var project ProjectCreated
	f.call(t, "POST", "/api/v1/projects", admin, map[string]string{"name": "private", "ingestion_mode": "server_only"}, 201, &project)
	id := project.Project.ID
	parent := f.createClient(t, admin, []string{"events:read", "events:write"}, []string{id})
	body := map[string]any{"name": "backend", "audience": "api", "permissions": []string{"events:write"}, "projects": []string{id}}
	var private AccessKeyCreated
	f.call(t, "POST", "/api/v1/oauth-clients/"+parent.ClientID+"/api-keys", admin, body, 201, &private)
	f.call(t, "POST", "/capture/", private.Token, map[string]any{"event": "server", "distinct_id": "person"}, 200, nil)
	f.call(t, "POST", "/capture/", "", map[string]any{"api_key": private.Token, "event": "server", "distinct_id": "person"}, 401, nil)
	if _, err := f.s.db.Exec(`UPDATE access_keys SET permissions='["events:read"]' WHERE id=?`, parent.ClientID); err != nil {
		t.Fatal(err)
	}
	f.call(t, "POST", "/capture/", private.Token, map[string]any{"event": "server", "distinct_id": "person"}, 401, nil)
}
