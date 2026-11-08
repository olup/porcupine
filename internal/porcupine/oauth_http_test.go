package porcupine

import (
	"net/http/httptest"
	"testing"
)

func TestOAuthIssuerAndBearerBoundaries(t *testing.T) {
	for _, issuer := range []string{"http://public.example.test", "https://user:secret@example.test", "https://example.test/path", "https://example.test?query=1", "https://example.test#fragment", "https://example.test//"} {
		if _, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer}); err == nil {
			t.Fatalf("accepted unsafe issuer %s", issuer)
		}
	}
	cfg, err := normalizeOAuthConfig(OAuthConfig{Issuer: "https://analytics.example.test/"})
	if err != nil || cfg.issuer != "https://analytics.example.test" || cfg.config.AccessTokenLifetimeSeconds != 900 || cfg.config.AllowLegacyKeys {
		t.Fatalf("config %+v %v", cfg, err)
	}
	r := httptest.NewRequest("GET", "http://localhost/api/v1/projects", nil)
	r.Header.Set("Authorization", "bearer token")
	if token, err := bearerCredential(r); err != nil || token != "token" {
		t.Fatal("Bearer scheme should be case insensitive")
	}
	r.Header.Add("Authorization", "Bearer another")
	if _, err := bearerCredential(r); err == nil {
		t.Fatal("accepted duplicate credentials")
	}
}

func TestOAuthOpenAPIAndPublicKeyCreation(t *testing.T) {
	f := newOAuthIntegration(t)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	project, _ := f.project(t, admin)
	f.call(t, "POST", "/api/v1/access-keys", admin, map[string]any{"name": "wrong service key", "permissions": []string{"events:read"}, "projects": []string{project}}, 400, nil)
	f.call(t, "POST", "/api/v1/access-keys", admin, map[string]any{"name": "browser ingestion", "permissions": []string{"events:write"}, "projects": []string{project}}, 201, nil)
	var schema map[string]any
	f.call(t, "GET", "/openapi.json", "", nil, 200, &schema)
	security := schema["components"].(map[string]any)["securitySchemes"].(map[string]any)
	flow := security["oauth2"].(map[string]any)["flows"].(map[string]any)["clientCredentials"].(map[string]any)
	if flow["tokenUrl"] != f.host.URL+"/oauth/token" {
		t.Fatal("OpenAPI does not describe pinned OAuth flow")
	}
	required := schema["paths"].(map[string]any)["/api/v1/projects"].(map[string]any)["post"].(map[string]any)["security"].([]any)[0].(map[string]any)["oauth2"].([]any)
	if len(required) != 2 {
		t.Fatal("OpenAPI project creation omitted a required scope")
	}
}
