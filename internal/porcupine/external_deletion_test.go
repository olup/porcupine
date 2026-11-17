package porcupine

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestExternalTenantDeletionPreservesOtherTenant(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	operator := f.operator(t)
	create := func(name string) (string, string, string) {
		tenant := request(t, s, "POST", "/api/v1/tenants", operator, fmt.Sprintf(`{"name":%q}`, name), 201)["tenant"].(map[string]any)["id"].(string)
		p := request(t, s, "POST", "/api/v1/projects", operator, fmt.Sprintf(`{"name":"project","tenant_id":%q}`, tenant), 201)
		return tenant, p["project"].(map[string]any)["id"].(string), p["ingest_token"].(string)
	}
	a, pa, ka := create("A")
	b, pb, kb := create("B")
	request(t, s, "POST", "/capture/", "", captureBody(ka, "A"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "B"), 200)
	reader := f.token(t, "person", a, "events:read", s.resources.apiResource, pa)
	readerB := f.token(t, "person", b, "events:read", s.resources.apiResource, pb)
	request(t, s, "GET", "/api/v1/projects/"+pa+"/events", reader, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+pb+"/events", readerB, "", 200)
	actorA, err := s.authenticateResource(t.Context(), reader, s.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	actorB, err := s.authenticateResource(t.Context(), readerB, s.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	operatorActor, err := s.authenticateResource(t.Context(), operator, s.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	client := request(t, s, "POST", "/api/v1/tenants/"+a+"/service-accounts", operator, externalBody(t, map[string]any{"name": "machine", "permissions": []string{"events:read"}, "projects": []string{pa}}), 201)
	key := request(t, s, "POST", "/api/v1/tenants/"+a+"/service-accounts/"+client["client_id"].(string)+"/api-keys", operator, externalBody(t, map[string]any{"name": "worker", "audience": "api", "permissions": []string{"events:read"}, "projects": []string{pa}}), 201)["token"].(string)
	global := request(t, s, "POST", "/api/v1/oauth-clients", operator, externalBody(t, map[string]any{"name": "shared machine", "permissions": []string{"events:read"}, "projects": []string{pa, pb}}), 201)["client_id"].(string)
	globalBKey := request(t, s, "POST", "/api/v1/oauth-clients/"+global+"/api-keys", operator, externalBody(t, map[string]any{"name": "B only", "audience": "api", "permissions": []string{"events:read"}, "projects": []string{pb}}), 201)["token"].(string)
	request(t, s, "DELETE", "/api/v1/tenants/"+a, operator, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")
	request(t, s, "GET", "/api/v1/projects/"+pa+"/events", reader, "", 401)
	request(t, s, "GET", "/api/v1/projects/"+pa+"/events", key, "", 401)
	request(t, s, "GET", "/api/v1/projects/"+pa+"/events", operator, "", 404)
	request(t, s, "GET", "/api/v1/projects/"+pb+"/events", operator, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+pb+"/events", globalBKey, "", 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "B-still-live"), 200)
	for _, check := range []struct {
		id    string
		count int
	}{{actorA.ID, 0}, {actorB.ID, 1}, {operatorActor.ID, 1}} {
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM access_keys WHERE id=$1`, check.id).Scan(&count); err != nil || count != check.count {
			t.Fatal("tenant deletion removed another tenant's audit anchor or retained A", count, check, err)
		}
	}
	var links int
	if err := s.db.QueryRow(`SELECT count(*) FROM journal_actor_tenants WHERE tenant_id=$1`, a).Scan(&links); err != nil || links != 0 {
		t.Fatal("deleted tenant retained subject links", links, err)
	}
	for _, table := range []string{"email_humans", "oauth_interactive_grants", "oauth_browser_sessions"} {
		// Browser sessions are PostgreSQL-only. No native human or consent exists in local mode.
		if table == "oauth_browser_sessions" {
			continue
		}
		var count int
		if err := s.control.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatal("external cleanup created builtin identity state", table, count, err)
		}
	}
	journal, control := controlJournalPaths(t, s)
	config := Config{DBPath: journal, ControlDBPath: control, Clock: s.clock, Auth: &AuthConfig{Mode: "external", PublicURL: s.resources.publicURL, External: s.resources.external.Config()}}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	f.s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	request(t, f.s, "GET", "/api/v1/projects/"+pb+"/events", readerB, "", 200)
	request(t, f.s, "GET", "/api/v1/projects/"+pb+"/events", globalBKey, "", 200)
	request(t, f.s, "GET", "/api/v1/projects/"+pa+"/events", reader, "", 401)
}

// Seed an inert consent row only to detect accidental execution of a builtin
// trigger. These rows are not created by external mode or used to authenticate.
func assertExternalRevocationTriggers(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, id := range []string{"parent", "native-child", "consent-child"} {
		if _, err := db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,$1,$1,'[]','[]','now')`, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO oauth_clients(client_id,secret_hash,grant_types,redirect_uris,token_endpoint_auth_method) VALUES('parent','unused','["client_credentials"]','[]','client_secret_basic')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES('native-child','parent','api')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms) VALUES('sentinel','consent-child','parent','parent','unused',1,2)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE access_keys SET revoked_at='revoked' WHERE id='parent'`); err != nil {
		t.Fatal(err)
	}
	var nativeRevoked, consentRevoked sql.NullString
	if err := db.QueryRow(`SELECT revoked_at FROM access_keys WHERE id='native-child'`).Scan(&nativeRevoked); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT revoked_at FROM access_keys WHERE id='consent-child'`).Scan(&consentRevoked); err != nil {
		t.Fatal(err)
	}
	if !nativeRevoked.Valid || consentRevoked.Valid {
		t.Fatalf("native revocation or builtin separation failed: native=%v consent=%v", nativeRevoked, consentRevoked)
	}
}

func TestExternalNativeRevocationSkipsBuiltinConsentTrigger(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	assertExternalRevocationTriggers(t, f.s.control)
}

func TestSharedExternalNativeRevocationSkipsBuiltinConsentTrigger(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	queries, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(t.Context(), db, "", queries, true); err != nil {
		t.Fatal(err)
	}
	assertExternalRevocationTriggers(t, db)
}
