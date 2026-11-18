package porcupine

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"testing"
	"time"
)

// Operator-provisioned credentials whose projects all belong to the deleted
// tenant must not leave secrets or names behind. Other tenants, cross-tenant
// and instance-level credentials keep working.
func TestTenantDeletionRemovesOperatorCredentialsScopedToTenant(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: "https://oauth.example", AllowLegacyKeys: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	a, b := tenantTestCreate(t, s, "A"), tenantTestCreate(t, s, "B")
	pa1, _ := tenantJourneyProject(t, s, a)
	pa2, _ := tenantJourneyProject(t, s, a)
	pb1, _ := tenantJourneyProject(t, s, b)
	pb2, _ := tenantJourneyProject(t, s, b)
	namedKey := func(name string, permissions, projects []string) (string, string) {
		created := request(t, s, "POST", "/api/v1/access-keys", adminToken, tenantTestBody(t, map[string]any{"name": name, "permissions": permissions, "projects": projects}), 201)
		return created["id"].(string), created["token"].(string)
	}
	type machine struct{ id, secret, token, keyID, key string }
	operatorClient := func(name string, projects []string) machine {
		created := request(t, s, "POST", "/api/v1/oauth-clients", adminToken, tenantTestBody(t, map[string]any{"name": name, "permissions": []string{"events:read", "management:read"}, "projects": projects, "grant_types": []string{"client_credentials"}}), 201)
		m := machine{id: created["client_id"].(string), secret: created["client_secret"].(string)}
		m.token = oauthProtocolRequest(t, s, m.id, m.secret, url.Values{"grant_type": {"client_credentials"}, "resource": {s.resources.apiResource}}, 200)["access_token"].(string)
		static := request(t, s, "POST", "/api/v1/oauth-clients/"+m.id+"/api-keys", adminToken, tenantTestBody(t, ServiceKeySpec{Name: name + " static", Audience: "api", Permissions: []string{"events:read"}, Projects: projects}), 201)
		m.keyID, m.key = static["id"].(string), static["token"].(string)
		return m
	}
	read := []string{"events:read", "management:read"}
	keyA, rawA := namedKey("A multi key", read, []string{pa1, pa2})
	_, rawB := namedKey("B multi key", read, []string{pb1, pb2})
	crossID, rawCross := namedKey("Cross tenant key", read, []string{pa1, pb1})
	_, rawInstance := namedKey("Instance reader", read, []string{"*"})
	clientA, clientB := operatorClient("A machine client", []string{pa1, pa2}), operatorClient("B machine client", []string{pb1, pb2})
	previousProject, _ := tenantJourneyProject(t, s, a)
	prunedID, _ := namedKey("A key for previously deleted project", read, []string{previousProject})
	const legacyID = "a-pruned-legacy"
	sharedDeletionPrincipal(t, s.control, legacyID, []string{previousProject})
	request(t, s, "DELETE", "/api/v1/projects/"+previousProject, adminToken, "", 204)
	for _, credential := range []string{rawA, rawInstance, clientA.token, clientA.key} {
		request(t, s, "GET", "/api/v1/projects/"+pa2+"/schema", credential, "", 200)
	}

	request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	waitTenantDeletion(t, s, a, "logically_deleted")

	for _, credential := range []string{rawA, clientA.token, clientA.key} {
		request(t, s, "GET", "/api/v1/projects/"+pa2+"/schema", credential, "", 401)
	}
	oauthProtocolRequest(t, s, clientA.id, clientA.secret, url.Values{"grant_type": {"client_credentials"}, "resource": {s.resources.apiResource}}, 401)
	for _, db := range []*sql.DB{s.db, s.control} {
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM access_keys WHERE id IN ($1,$2,$3,$4,$5) OR token_hash=$6 OR token_hash=$7 OR name IN ('A multi key','A machine client','A machine client static')`, keyA, clientA.id, clientA.keyID, prunedID, legacyID, hash(rawA), hash(clientA.key)).Scan(&count); err != nil || count != 0 {
			t.Fatal("tenant-scoped operator credential survived", count, err)
		}
	}
	for _, check := range []struct{ query, value string }{
		{`SELECT count(*) FROM oauth_clients WHERE client_id=$1`, clientA.id},
		{`SELECT count(*) FROM oauth_clients WHERE secret_hash=$1`, hash(clientA.secret)},
		{`SELECT count(*) FROM oauth_tokens WHERE client_id=$1`, clientA.id},
		{`SELECT count(*) FROM service_api_keys WHERE client_id=$1`, clientA.id},
		{`SELECT count(*) FROM tenant_credentials WHERE tenant_id=$1`, a},
	} {
		var count int
		if err := s.control.QueryRow(check.query, check.value).Scan(&count); err != nil || count != 0 {
			t.Fatal("tenant-scoped OAuth state survived", check.query, count, err)
		}
	}

	for _, credential := range []string{rawB, rawCross, rawInstance, clientB.token, clientB.key} {
		request(t, s, "GET", "/api/v1/projects/"+pb1+"/schema", credential, "", 200)
	}
	oauthProtocolRequest(t, s, clientB.id, clientB.secret, url.Values{"grant_type": {"client_credentials"}, "resource": {s.resources.apiResource}}, 200)
	var crossProjects string
	var crossRevoked sql.NullString
	if err := s.control.QueryRow(`SELECT projects,revoked_at FROM access_keys WHERE id=$1`, crossID).Scan(&crossProjects, &crossRevoked); err != nil || crossProjects != tenantTestBody(t, []string{pb1}) || crossRevoked.Valid {
		t.Fatal("cross-tenant key was not pruned to the surviving tenant", crossProjects, crossRevoked, err)
	}
	var owned int
	if err := s.control.QueryRow(`SELECT count(*) FROM tenant_credentials WHERE principal_id IN ($1,$2)`, crossID, clientB.id).Scan(&owned); err != nil || owned != 0 {
		t.Fatal("deletion recorded ownership outside the deleted tenant", owned, err)
	}
}

func TestSharedTenantDeletionRemovesOperatorCredentialsScopedToTenant(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	s.eventRules = &eventRules{clock: time.Now}
	ctx := t.Context()
	if _, err := s.control.Exec(`INSERT INTO tenants SELECT 'B','B',created_at,ingestion_allowed,ingestion_monthly_events,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes FROM tenants WHERE id='default';
INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES('b1','B','B1','now','mixed'),('b2','B','B2','now','mixed'),('old-project','default','Old','now','mixed')`); err != nil {
		t.Fatal(err)
	}
	for _, spec := range []struct {
		id       string
		projects []string
	}{
		{"a-pruned-legacy", []string{"old-project"}}, {"a-multi", []string{"removed", "kept"}}, {"a-client", []string{"removed", "kept"}}, {"a-static", []string{"kept"}},
		{"b-multi", []string{"b1", "b2"}}, {"b-client", []string{"b1", "b2"}}, {"cross", []string{"removed", "b1"}}, {"instance", []string{"*"}},
	} {
		sharedDeletionPrincipal(t, s.control, spec.id, spec.projects)
	}
	if _, err := s.control.Exec(`INSERT INTO oauth_clients(client_id,secret_hash,grant_types) VALUES('a-client','a-client-secret','["client_credentials"]'),('b-client','b-client-secret','["client_credentials"]');
INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES('a-static','a-client','api');
INSERT INTO oauth_tokens(token_hash,client_id,resource,scope,projects_json,issued_at_ms,expires_at_ms) VALUES('a-token','a-client','api','events:read','["removed","kept"]',0,1),('b-token','b-client','api','events:read','["b1","b2"]',0,1)`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(ctx, "old-project"); err != nil {
		t.Fatal(err)
	}
	if err := s.completeControlProjectDeletion(ctx, "old-project"); err != nil {
		t.Fatal(err)
	}
	if err := s.beginTenantDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	// Hosted project cleanup finishes each fenced project before control cleanup.
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{"removed", "kept"} {
		if err := s.completeProjectDeletionTx(ctx, tx, project, defaultTenantID); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := s.finishTenantControlDeletion(ctx, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	for _, check := range []string{
		`SELECT count(*) FROM access_keys WHERE id IN ('a-multi','a-client','a-static','a-pruned-legacy') OR token_hash IN ('a-multi','a-client','a-static','a-pruned-legacy')`,
		`SELECT count(*) FROM oauth_clients WHERE client_id='a-client' OR secret_hash='a-client-secret'`,
		`SELECT count(*) FROM oauth_tokens WHERE client_id='a-client'`,
		`SELECT count(*) FROM service_api_keys WHERE client_id='a-client'`,
		`SELECT count(*) FROM tenant_credentials WHERE tenant_id='default' OR principal_id IN ('b-multi','b-client','cross','instance')`,
	} {
		var count int
		if err := s.control.QueryRow(check).Scan(&count); err != nil || count != 0 {
			t.Fatal("tenant-scoped credential state survived", check, count, err)
		}
	}
	if _, err := s.loadCredentialActor(ctx, "a-multi"); err == nil {
		t.Fatal("deleted tenant key still authenticates")
	}
	if _, err := loadOAuthClient(ctx, s.control, "a-client"); err == nil {
		t.Fatal("deleted tenant OAuth client still authenticates")
	}
	for _, id := range []string{"b-multi", "b-client", "cross", "instance"} {
		if _, err := s.loadCredentialActor(ctx, id); err != nil {
			t.Fatal("surviving credential stopped authenticating", id, err)
		}
	}
	if client, err := loadOAuthClient(ctx, s.control, "b-client"); err != nil || len(client.Projects) != 2 {
		t.Fatal("other tenant OAuth client changed", client, err)
	}
	var cross, tokens string
	if err := s.control.QueryRow(`SELECT projects FROM access_keys WHERE id='cross'`).Scan(&cross); err != nil || cross != `["b1"]` {
		t.Fatal("cross-tenant key was not pruned to the surviving tenant", cross, err)
	}
	if err := s.control.QueryRow(`SELECT projects_json FROM oauth_tokens WHERE token_hash='b-token'`).Scan(&tokens); err != nil || tokens != `["b1","b2"]` {
		t.Fatal("other tenant token changed", tokens, err)
	}
}
