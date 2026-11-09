package porcupine

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The grant, tokens and project filter must hold at the public MCP boundary,
// including after the client has already opened a session.
func TestMCPInteractiveProjectIsolationAndRevocation(t *testing.T) {
	f := newOIDCFixture(t)
	for _, id := range []string{"project-a", "project-b"} {
		if _, err := f.s.db.Exec("INSERT INTO projects(id,name,created_at) VALUES(?,?,?)", id, id, now()); err != nil {
			t.Fatal(err)
		}
	}
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	bearer := tokens["access_token"].(string)
	ctx, session := mcpTestSession(t, f.host.URL, bearer, "2025-11-25")
	for projectID, want := range map[string]int{"project-a": 200, "project-b": 403} {
		mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": projectID}}, want)
	}
	request(t, f.s, "GET", "/api/v1/projects/project-a/schema", bearer, "", 401)
	admin := oauthProtocolRequest(t, f.s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + "/api/v1"}}, 200)["access_token"].(string)
	grants := request(t, f.s, "GET", "/api/v1/oauth-grants", admin, "", 200)["grants"].([]any)
	if len(grants) != 1 {
		t.Fatalf("expected one human grant: %d", len(grants))
	}
	id := grants[0].(map[string]any)["id"].(string)
	request(t, f.s, "DELETE", "/api/v1/oauth-grants/"+id, admin, "", 204)
	payload := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"porcupine_call","arguments":{"operation_id":"get-event-schema","path":{"id":"project-a"}}}}`
	req, err := http.NewRequestWithContext(ctx, "POST", f.host.URL+"/mcp", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+bearer)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set("MCP-Protocol-Version", "2025-11-25")
	res, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 401 {
		t.Fatalf("revoked MCP request status=%d", res.StatusCode)
	}
	interactiveEndpoint(t, f.s, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {tokens["refresh_token"].(string)}}, "", "", 400)
}
