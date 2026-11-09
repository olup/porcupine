package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestManagementKeyListingAndLifecycle(t *testing.T) {
	s := service(t, 20)
	projectA, _ := project(t, s)
	projectB, _ := project(t, s)
	_, reader := key(t, s, []string{"management:read"}, []string{projectA})
	visibleID, secret := key(t, s, []string{"events:read"}, []string{projectA})
	hiddenID, _ := key(t, s, []string{"events:read"}, []string{projectA, projectB})
	result := request(t, s, "GET", "/api/v1/access-keys", reader, "", 200)
	body, _ := json.Marshal(result)
	if strings.Contains(string(body), secret) || strings.Contains(string(body), hash(secret)) || strings.Contains(string(body), "token") || strings.Contains(string(body), hiddenID) || strings.Contains(string(body), projectB) {
		t.Fatalf("credential or cross-project scope leaked: %s", body)
	}
	if !strings.Contains(string(body), visibleID) {
		t.Fatal("key in scope missing")
	}
	request(t, s, "POST", "/api/v1/access-keys", reader, `{"name":"bad","permissions":["events:read"],"projects":["`+projectA+`"]}`, 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+visibleID, reader, "", 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+visibleID, adminToken, "", 204)
	request(t, s, "DELETE", "/api/v1/access-keys/"+visibleID, adminToken, "", 404)
	request(t, s, "GET", "/api/v1/projects/"+projectA+"/events", secret, "", 401)
	result = request(t, s, "GET", "/api/v1/access-keys", reader, "", 200)
	found := false
	for _, v := range result["access_keys"].([]any) {
		item := v.(map[string]any)
		if item["id"] == visibleID {
			found = item["revoked_at"] != nil
		}
	}
	if !found {
		t.Fatal("revocation metadata missing")
	}
	var stored string
	if err := s.db.QueryRow(`SELECT token_hash FROM access_keys WHERE id=?`, visibleID).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != hash(secret) {
		t.Fatal("created key was not stored as a hash")
	}
}

func TestManagementCannotEscalateAndPreventsAdminLockout(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	_, manager := key(t, s, []string{"management:write", "events:read"}, []string{"*"})
	request(t, s, "POST", "/api/v1/projects", manager, `{"name":"cannot mint ingestion"}`, 403)
	request(t, s, "POST", "/api/v1/access-keys", manager, `{"name":"bad","permissions":["events:write"],"projects":["`+a+`"]}`, 403)
	request(t, s, "POST", "/api/v1/access-keys", adminToken, `{"name":"bad","permissions":["unknown"],"projects":["*"]}`, 400)
	_, scoped := key(t, s, []string{"management:write", "events:read"}, []string{a})
	request(t, s, "POST", "/api/v1/access-keys", scoped, `{"name":"bad","permissions":["events:read"],"projects":["*"]}`, 403)
	request(t, s, "POST", "/api/v1/access-keys", scoped, `{"name":"reader","permissions":["events:read"],"projects":["`+a+`"]}`, 201)
	var bootstrapID string
	if err := s.db.QueryRow(`SELECT id FROM access_keys WHERE token_hash=?`, hash(adminToken)).Scan(&bootstrapID); err != nil {
		t.Fatal(err)
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+bootstrapID, adminToken, "", 409)
	newAdminID, newAdmin := key(t, s, []string{"instance:admin"}, []string{"*"})
	request(t, s, "DELETE", "/api/v1/access-keys/"+bootstrapID, newAdmin, "", 204)
	request(t, s, "GET", "/api/v1/access-keys", adminToken, "", 401)
	request(t, s, "DELETE", "/api/v1/access-keys/"+newAdminID, newAdmin, "", 409)
}

func TestManagementTypedOpenAPIAndValidation(t *testing.T) {
	s := service(t, 20)
	api := request(t, s, "GET", "/openapi.json", "", "", 200)
	paths := api["paths"].(map[string]any)
	operations := 0
	for path, value := range paths {
		if !strings.HasPrefix(path, "/api/v1/") {
			t.Fatalf("unexpected documented operation %s", path)
		}
		for _, operation := range value.(map[string]any) {
			op := operation.(map[string]any)
			if len(op["security"].([]any)) == 0 {
				t.Fatal("private operation lacks bearer security")
			}
			operations++
		}
	}
	if operations != 42 {
		t.Fatalf("got %d operations", operations)
	}
	components := api["components"].(map[string]any)
	security := components["securitySchemes"].(map[string]any)["bearerAuth"].(map[string]any)
	if security["type"] != "http" || security["scheme"] != "bearer" {
		t.Fatal("missing bearer scheme")
	}
	schemas := components["schemas"].(map[string]any)
	keyProps := schemas["AccessKey"].(map[string]any)["properties"].(map[string]any)
	for _, name := range []string{"token", "token_hash", "secret"} {
		if _, exists := keyProps[name]; exists {
			t.Fatalf("secret field in list schema: %s", name)
		}
	}
	projectSchema := schemas["ProjectCreateInputBody"].(map[string]any)
	name := projectSchema["properties"].(map[string]any)["name"].(map[string]any)
	if name["minLength"] != float64(1) || name["maxLength"] != float64(200) {
		t.Fatal("project name lacks validation schema")
	}
	props := schemas["Event"].(map[string]any)["properties"].(map[string]any)["properties"].(map[string]any)
	if props["type"] == "string" || props["contentEncoding"] == "base64" {
		t.Fatal("event properties documented as encoded string")
	}
	for _, path := range []string{"/docs", "/schemas/Project.json"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatalf("unexpected UI/schema route %s status %d", path, w.Code)
		}
	}
	request(t, s, "POST", "/api/v1/projects", "", `{"name":false}`, 401)
	invalid := request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":false}`, 422)
	if _, ok := invalid["error"]; !ok {
		t.Fatal("validation error lost the error envelope")
	}
	request(t, s, "POST", "/api/v1/projects", adminToken, `{`, 400)
	id, _ := project(t, s)
	request(t, s, "GET", "/api/v1/projects/"+id+"/events?limit=1001", adminToken, "", 422)
}

func TestManagementBodyBoundaries(t *testing.T) {
	s := service(t, 20)
	body := `{"name":"small"}` + strings.Repeat(" ", maxBodyBytes)
	request(t, s, "POST", "/api/v1/projects", adminToken, body, 413)
	for _, headers := range []map[string]string{
		{"Content-Type": "application/xml"},
		{"Content-Type": "application/json", "Content-Encoding": "gzip"},
	} {
		r := httptest.NewRequest("POST", "/api/v1/projects", strings.NewReader(`{"name":"small"}`))
		r.Header.Set("Authorization", "Bearer "+adminToken)
		for name, value := range headers {
			r.Header.Set(name, value)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		if w.Code != 415 {
			t.Fatalf("unsupported management transport returned %d: %s", w.Code, w.Body.String())
		}
	}
}
