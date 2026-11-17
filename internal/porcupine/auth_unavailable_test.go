package porcupine

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func controlJournalPaths(t *testing.T, s *Service) (string, string) {
	t.Helper()
	var seq int
	var name, journal, control string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &journal); err != nil {
		t.Fatal(err)
	}
	if err := s.control.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &control); err != nil {
		t.Fatal(err)
	}
	return journal, control
}

func assertUnavailableResponse(t *testing.T, w *httptest.ResponseRecorder, secrets ...string) {
	t.Helper()
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || strings.Contains(w.Header().Get("WWW-Authenticate"), "invalid_token") {
		t.Fatalf("dependency failure became terminal authentication failure: %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if json.Unmarshal(w.Body.Bytes(), &body) != nil || body["code"] != "authentication_unavailable" {
		t.Fatal("missing safe authentication retry category")
	}
	for _, private := range append(secrets, "SELECT ", "token_hash", "sql:", "database is closed") {
		if private != "" && strings.Contains(w.Body.String(), private) {
			t.Fatal("authentication dependency error leaked private detail")
		}
	}
}

func TestAuthenticationControlOutageIsRetryableAcrossSDKInputs(t *testing.T) {
	t.Parallel()
	for _, protocol := range []string{"posthog", "posthog-batch", "native", "segment"} {
		t.Run(protocol, func(t *testing.T) {
			t.Parallel()
			c := Config{DBPath: filepath.Join(t.TempDir(), "journal.db"), BootstrapToken: adminToken}
			s, err := Open(c)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = s.Close() })
			_, key := project(t, s)
			path := "/capture/"
			headers := map[string]string{"Content-Type": "application/json"}
			body := fmt.Sprintf(`{"api_key":%q,"uuid":"11111111-1111-4111-8111-111111111111","event":"retry","distinct_id":"person"}`, key)
			switch protocol {
			case "posthog-batch":
				path = "/batch/"
				body = fmt.Sprintf(`{"api_key":%q,"batch":[{"uuid":"11111111-1111-4111-8111-111111111111","event":"retry","distinct_id":"person"}]}`, key)
			case "native":
				path = "/ingest/v1/events"
				headers["X-Porcupine-Key"] = key
				body = `[{"type":"track","message_id":"retry","actor_id":"person","name":"retry"}]`
			case "segment":
				path = "/v1/track"
				headers = segmentHeaders(key)
				body = `{"type":"track","messageId":"retry","userId":"person","event":"retry"}`
			}
			if err := s.control.Close(); err != nil {
				t.Fatal(err)
			}
			unavailable := transportRequest(t, s, "POST", path, []byte(body), headers, 503)
			assertUnavailableResponse(t, unavailable, key, c.DBPath)
			if eventCount(t, s) != 0 {
				t.Fatal("control outage accepted events")
			}
			var receipts int
			if err := s.db.QueryRow(`SELECT count(*) FROM event_receipts`).Scan(&receipts); err != nil || receipts != 0 {
				t.Fatal("pre-auth failure wrote retry receipts", err)
			}
			_ = s.Close()
			c.BootstrapToken = ""
			s, err = Open(c)
			if err != nil {
				t.Fatal(err)
			}
			transportRequest(t, s, "POST", path, []byte(body), headers, 200)
			transportRequest(t, s, "POST", path, []byte(body), headers, 200)
			if eventCount(t, s) != 1 {
				t.Fatal("same SDK batch did not recover exactly once")
			}
		})
	}
}

func TestNativeMachineAuthenticationControlOutageIsRetryable(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	operator := f.operator(t)
	p := request(t, s, "POST", "/api/v1/projects", operator, `{"name":"native machine"}`, 201)
	id := p["project"].(map[string]any)["id"].(string)
	account := request(t, s, "POST", "/api/v1/oauth-clients", operator, externalBody(t, map[string]any{"name": "backend", "permissions": []string{"events:read", "events:write"}, "projects": []string{id}}), 201)
	key := request(t, s, "POST", "/api/v1/oauth-clients/"+account["client_id"].(string)+"/api-keys", operator, externalBody(t, map[string]any{"name": "backend", "audience": "api", "permissions": []string{"events:read", "events:write"}, "projects": []string{id}}), 201)["token"].(string)
	mcpKey := request(t, s, "POST", "/api/v1/oauth-clients/"+account["client_id"].(string)+"/api-keys", operator, externalBody(t, map[string]any{"name": "agent", "audience": "mcp", "permissions": []string{"events:read"}, "projects": []string{id}}), 201)["token"].(string)
	journal, control := controlJournalPaths(t, s)
	config := Config{DBPath: journal, ControlDBPath: control, Clock: s.clock, Auth: &AuthConfig{Mode: "external", PublicURL: s.resources.publicURL, External: s.resources.external.Config()}}
	request(t, s, "GET", "/api/v1/projects/"+id+"/events", key[:len(key)-1]+"!", "", 401)
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	for _, input := range []struct{ method, path, token, body string }{
		{"GET", "/api/v1/projects/" + id + "/events", key, ""},
		{"POST", "/mcp", mcpKey, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`},
		{"POST", "/capture/", key, `{"uuid":"11111111-1111-4111-8111-111111111111","event":"retry","distinct_id":"person"}`},
	} {
		r := httptest.NewRequest(input.method, input.path, strings.NewReader(input.body))
		r.Header.Set("Authorization", "Bearer "+input.token)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		assertUnavailableResponse(t, w, key, mcpKey, control)
	}
	if _, err := s.authenticateServiceKey(t.Context(), key, s.resources.apiResource); !authenticationUnavailable(err) {
		t.Fatal("native storage failure lost its retry category", err)
	}
	if eventCount(t, s) != 0 {
		t.Fatal("unavailable native auth accepted an event")
	}
	_ = s.Close()
	var err error
	f.s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	s = f.s
	body := `{"uuid":"11111111-1111-4111-8111-111111111111","event":"retry","distinct_id":"person"}`
	request(t, s, "POST", "/capture/", key, body, 200)
	request(t, s, "POST", "/capture/", key, body, 200)
	request(t, s, "GET", "/api/v1/projects/"+id+"/events", key, "", 200)
	if eventCount(t, s) != 1 {
		t.Fatal("native key batch did not recover exactly once")
	}
	a, err := s.authenticateServiceKey(t.Context(), key, s.resources.apiResource)
	if err != nil || !a.ServiceKey {
		t.Fatal("native key lost authority after restart", err)
	}
}

func TestOAuthAuthenticationControlOutageIsRetryable(t *testing.T) {
	t.Parallel()
	s, clock := oauthProtocolService(t)
	id, _ := project(t, s)
	client, secret := oauthProtocolClient(t, s, []string{"events:read", "events:write"}, []string{id})
	apiToken := oauthProtocolRequest(t, s, client, secret, oauthGrantForm(), 200)["access_token"].(string)
	form := oauthGrantForm()
	form.Set("resource", s.resources.mcpResource)
	mcpToken := oauthProtocolRequest(t, s, client, secret, form, 200)["access_token"].(string)
	journal, control := controlJournalPaths(t, s)
	config := Config{DBPath: journal, ControlDBPath: control, Clock: s.clock, OAuth: &s.oauth.config}
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	body := `{"uuid":"11111111-1111-4111-8111-111111111111","event":"retry","distinct_id":"person"}`
	for _, input := range []struct{ method, path, token, body string }{
		{"GET", "/api/v1/projects/" + id + "/events", apiToken, ""},
		{"POST", "/mcp", mcpToken, `{"jsonrpc":"2.0","id":1,"method":"tools/list"}`},
		{"POST", "/capture/", apiToken, body},
	} {
		r := httptest.NewRequest(input.method, input.path, strings.NewReader(input.body))
		r.Header.Set("Authorization", "Bearer "+input.token)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json, text/event-stream")
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		assertUnavailableResponse(t, w, apiToken, mcpToken, control)
	}
	if eventCount(t, s) != 0 {
		t.Fatal("unavailable OAuth authentication accepted events")
	}
	_ = s.Close()
	var err error
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	request(t, s, "POST", "/capture/", apiToken, body, 200)
	request(t, s, "POST", "/capture/", apiToken, body, 200)
	if eventCount(t, s) != 1 {
		t.Fatal("OAuth-authenticated retry did not recover exactly once")
	}
	clock.Add(900000)
	request(t, s, "GET", "/api/v1/projects/"+id+"/events", apiToken, "", 401)
}
