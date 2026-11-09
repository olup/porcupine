package porcupine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func sourceCredentialBody(name, source string, permissions, projects []string) string {
	encoded, _ := json.Marshal(map[string]any{"name": name, "source_id": source, "permissions": permissions, "projects": projects})
	return string(encoded)
}
func TestCredentialSourceIDValidation(t *testing.T) {
	for _, value := range []string{"", "Backend.orders:v1", "0", "a" + strings.Repeat("-", 127)} {
		if !validSourceID(value) {
			t.Fatalf("rejected valid source %q", value)
		}
	}
	for _, value := range []string{" a", "a ", "_a", ":a", "a/b", "a\x00b", "é", strings.Repeat("x", 129)} {
		if validSourceID(value) {
			t.Fatalf("accepted invalid source %q", value)
		}
	}
	for _, test := range []struct {
		source                string
		permissions, projects []string
		allowed               bool
	}{
		{"", []string{"events:read"}, []string{"*"}, true},
		{"producer", []string{"events:write"}, []string{"project"}, true},
		{"producer", []string{"events:read"}, []string{"project"}, false},
		{"posthog", []string{"events:write"}, []string{"project"}, false},
		{"producer", []string{"instance:admin"}, []string{"*"}, false},
		{"producer", []string{"events:write"}, []string{"*"}, false},
		{"producer", []string{"events:write"}, []string{"project", "other"}, false},
	} {
		if err := validateCredentialSource(test.source, test.permissions, test.projects); (err == nil) != test.allowed {
			t.Fatalf("source binding grant validation mismatch: %+v %v", test, err)
		}
	}
}

func TestCredentialSourceKeysAndOAuthShareAuthenticatedBinding(t *testing.T) {
	s, _ := oauthProtocolService(t)
	projectID, _ := project(t, s)
	sourceID := "Orders.Backend:v1"
	body := sourceCredentialBody("native key", sourceID, []string{"events:write"}, []string{projectID})
	first := request(t, s, "POST", "/api/v1/access-keys", adminToken, body, 201)
	second := request(t, s, "POST", "/api/v1/access-keys", adminToken, body, 201)
	for _, created := range []map[string]any{first, second} {
		if created["source_id"] != sourceID {
			t.Fatal("missing source metadata", created)
		}
		a, err := s.authenticate(context.Background(), created["token"].(string))
		if err != nil || a.SourceID != sourceID {
			t.Fatal("key binding not authenticated", a, err)
		}
	}
	oauthBody := sourceCredentialBody("segment writer", sourceID, []string{"events:write"}, []string{projectID})
	client := request(t, s, "POST", "/api/v1/oauth-clients", adminToken, oauthBody, 201)
	if client["source_id"] != sourceID {
		t.Fatal("missing OAuth binding metadata", client)
	}
	loaded, err := loadOAuthClient(context.Background(), s.db, client["client_id"].(string))
	if err != nil || loaded.SourceID != sourceID {
		t.Fatal("OAuth client lost source binding", err)
	}
	issued := oauthProtocolRequest(t, s, client["client_id"].(string), client["client_secret"].(string), oauthGrantForm(), 200)
	tokenActor, err := s.authenticateOAuthToken(context.Background(), issued["access_token"].(string), s.oauth.apiResource)
	if err != nil || tokenActor.SourceID != sourceID || len(tokenActor.Projects) != 1 || tokenActor.Projects[0] != projectID {
		t.Fatal("OAuth access token lost bound source/project", tokenActor, err)
	}
	keys := request(t, s, "GET", "/api/v1/access-keys", adminToken, "", 200)
	clients := request(t, s, "GET", "/api/v1/oauth-clients", adminToken, "", 200)
	for _, listing := range []map[string]any{keys, clients} {
		encoded, _ := json.Marshal(listing)
		if !strings.Contains(string(encoded), sourceID) || strings.Contains(string(encoded), first["token"].(string)) || strings.Contains(string(encoded), client["client_secret"].(string)) {
			t.Fatal("binding listing missing or secret leak")
		}
	}
	blank := request(t, s, "POST", "/api/v1/access-keys", adminToken, sourceCredentialBody("default", "", []string{"events:write"}, []string{projectID}), 201)
	defaultActor, err := s.authenticate(context.Background(), blank["token"].(string))
	if err != nil || defaultActor.SourceID != "" {
		t.Fatal("default source changed", err)
	}
	request(t, s, "DELETE", "/api/v1/oauth-clients/"+client["client_id"].(string), adminToken, "", 204)
	if _, err = s.authenticateOAuthToken(context.Background(), issued["access_token"].(string), s.oauth.apiResource); err == nil {
		t.Fatal("revoked source credential still authenticates")
	}
}

func TestCredentialSourceGrantAndDelegationBoundaries(t *testing.T) {
	s, _ := oauthProtocolService(t)
	projectID, _ := project(t, s)
	other, _ := project(t, s)
	for _, endpoint := range []string{"/api/v1/access-keys", "/api/v1/oauth-clients"} {
		for _, grant := range []struct{ permissions, projects []string }{
			{[]string{"events:read"}, []string{projectID}},
			{[]string{"events:write"}, []string{"*"}},
			{[]string{"events:write"}, []string{projectID, other}},
		} {
			request(t, s, "POST", endpoint, adminToken, sourceCredentialBody("invalid", "producer", grant.permissions, grant.projects), 400)
		}
		request(t, s, "POST", endpoint, adminToken, sourceCredentialBody("invalid", "bad/source", []string{"events:write"}, []string{projectID}), 422)
		request(t, s, "POST", endpoint, adminToken, sourceCredentialBody("reserved", "posthog", []string{"events:write"}, []string{projectID}), 400)
	}
	parent := request(t, s, "POST", "/api/v1/access-keys", adminToken, sourceCredentialBody("bound manager", "producer", []string{"management:write", "events:write", "events:read"}, []string{projectID}), 201)
	parentToken := parent["token"].(string)
	for _, endpoint := range []string{"/api/v1/access-keys", "/api/v1/oauth-clients"} {
		request(t, s, "POST", endpoint, parentToken, sourceCredentialBody("cannot rebind", "other-producer", []string{"events:write"}, []string{projectID}), 403)
		request(t, s, "POST", endpoint, parentToken, sourceCredentialBody("cannot cross project", "producer", []string{"events:write"}, []string{other}), 403)
		inherited := request(t, s, "POST", endpoint, parentToken, sourceCredentialBody("inherits source", "", []string{"events:write"}, []string{projectID}), 201)
		if inherited["source_id"] != "producer" {
			t.Fatal("delegation escaped source binding", inherited)
		}
		readOnly := request(t, s, "POST", endpoint, parentToken, sourceCredentialBody("read only", "", []string{"events:read"}, []string{projectID}), 201)
		if readOnly["source_id"] != nil {
			t.Fatal("read-only credential received ingestion binding", readOnly)
		}
	}
}

func TestCredentialSourceMigrationPreservesExistingCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	projectID, _ := project(t, s)
	_, oldKey := key(t, s, []string{"events:write"}, []string{projectID})
	if _, err = s.db.Exec(`ALTER TABLE access_keys DROP COLUMN source_id`); err != nil {
		s.Close()
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, raw := range []string{adminToken, oldKey} {
		a, err := s.authenticate(context.Background(), raw)
		if err != nil || a.SourceID != "" {
			t.Fatal("migration changed legacy authentication", a, err)
		}
	}
	if err = s.initializeOAuthClients(context.Background()); err != nil {
		t.Fatal("migration not idempotent", err)
	}
	var missing int
	if s.db.QueryRow(`SELECT count(*) FROM access_keys WHERE source_id IS NULL OR source_id<>''`).Scan(&missing) != nil || missing != 0 {
		t.Fatal("migration did not preserve blank source defaults")
	}
}
