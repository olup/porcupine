package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func runProjectPurge(t *testing.T, s *Service, project string, commit bool) {
	t.Helper()
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := purgeProjectSQLite(t.Context(), tx, project); err != nil {
		t.Fatal(err)
	}
	if commit {
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProjectPurgeSQLiteRowsAndAccounting(t *testing.T) {
	s := service(t, 20)
	remove, aKey := project(t, s)
	keep, bKey := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(aKey, "stable"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(bKey, "stable"), 200)
	var adminID string
	if err := s.db.QueryRow("SELECT id FROM access_keys WHERE token_hash=?", hash(adminToken)).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{remove, keep} {
		for _, q := range []string{
			"INSERT INTO delivery_quarantine SELECT id,321,'now' FROM events WHERE project_id=?1",
			"INSERT INTO identity_profiles VALUES(?1,'user',1,'{}',1)",
			"INSERT INTO identity_links VALUES(?1,'anon','user',1)",
			"INSERT OR IGNORE INTO event_receipts(project_id,uuid) VALUES(?1,'stable')",
			"INSERT OR IGNORE INTO receipt_provenance(project_id,uuid,source_uuid) VALUES(?1,'stable','source')",
			"INSERT INTO receipt_lookup_leases VALUES('lease',?1,'stable',42)",
			"INSERT INTO mutation_audit(mutation_id,project_id,uuid,actor_id,operation,idempotency_key_hash,request_hash,expected_revision,state,created_at) VALUES(?1,?1,'stable',?2,'edit','key','hash',1,'queued','now')",
			"INSERT INTO mutation_outbox SELECT seq,project_id,uuid,'{}' FROM mutation_audit WHERE project_id=?1",
			"INSERT INTO pipelines VALUES(?1,?1,?2,'webhook','hook','paused','{}',X'00',1)",
			"INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,state,next_attempt_ms,created_at_ms) VALUES(?1,?1,?1,'stable','{}',2,'queued',0,1)",
			"INSERT INTO webhook_members VALUES(?1,?1,'stable',?1)",
			"INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at) VALUES(?1,?1,?2,'SELECT 1','{}','csv','i','r','completed',1,1,'now')",
			"INSERT INTO export_files(job_id,attempt_id,file_index,object_key) VALUES(?1,'attempt',0,'result.csv')",
			"INSERT INTO import_uploads(id,project_id,actor_id,idempotency_hash,request_hash,state,created_at,updated_at) VALUES(?1,?1,?2,'i','r','uploading','now','now')",
			"INSERT INTO import_upload_files(id,job_id,ordinal,path,kind,size_bytes,sha256,hash_state) VALUES(?1,?1,0,'input','events',2,'hash',X'00')",
			"INSERT INTO import_upload_samples VALUES(?1,?1,1,'invalid')",
			"INSERT INTO metadata VALUES('last_delivery_success:' || ?1,'now')",
		} {
			args := []any{id}
			if strings.Contains(q, "?2") {
				args = append(args, adminID)
			}
			if _, err := s.db.Exec(q, args...); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
	}
	runProjectPurge(t, s, remove, false)
	if eventCount(t, s) != 2 {
		t.Fatal("partial purge after rollback")
	}
	runProjectPurge(t, s, remove, true)
	for _, table := range []string{"projects", "events", "event_receipts", "receipt_provenance", "receipt_lookup_leases", "delivery_quarantine", "identity_profiles", "identity_links", "mutation_audit", "mutation_outbox", "export_jobs", "export_files", "import_uploads", "import_upload_files", "import_upload_samples", "pipelines", "webhook_deliveries", "webhook_members"} {
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s count = %d err %v", table, n, err)
		}
	}
	for _, q := range []string{
		"SELECT pending_count FROM queue_usage",
		"SELECT count FROM webhook_queue_usage WHERE scope='instance'",
		"SELECT count(*) FROM webhook_queue_usage WHERE scope='project:' || ?",
		"SELECT count(*) FROM metadata WHERE key='last_delivery_success:' || ?",
		"SELECT count(*) FROM events WHERE project_id=?",
	} {
		var args []any
		if strings.Contains(q, "?") {
			args = []any{keep}
		}
		var n int
		if err := s.db.QueryRow(q, args...).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s = %d: %v", q, n, err)
		}
	}
	var violations int
	if err := s.db.QueryRow("SELECT count(*) FROM pragma_foreign_key_check").Scan(&violations); err != nil || violations != 0 {
		t.Fatal(violations, err)
	}
}

func TestProjectPurgeCredentialScopes(t *testing.T) {
	s := service(t, 20)
	remove, _ := project(t, s)
	keep, _ := project(t, s)
	one, oneToken := key(t, s, []string{"events:read"}, []string{remove})
	_, bothToken := key(t, s, []string{"events:read"}, []string{remove, keep})
	_, manager := key(t, s, []string{"management:read"}, []string{keep})
	h := EmailHuman{ID: newID(), Email: "dev@example.org", CreatedAt: now(), Projects: []string{remove, keep}, Permissions: []string{"events:edit", "events:read"}, Grants: []HumanProjectGrant{{remove, []string{"events:edit"}}, {keep, []string{"events:read"}}}}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := insertEmailHuman(t.Context(), tx, h); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	runProjectPurge(t, s, remove, true)
	request(t, s, "GET", "/api/v1/projects/"+keep+"/events", oneToken, "", 401)
	request(t, s, "GET", "/api/v1/projects/"+keep+"/events", bothToken, "", 200)
	result := request(t, s, "GET", "/api/v1/access-keys", manager, "", 200)
	for _, v := range result["access_keys"].([]any) {
		if v.(map[string]any)["id"] == one {
			t.Fatal("unrelated project manager saw a deleted project's credential")
		}
	}
	record, err := loadEmailHuman(t.Context(), s.db, h.ID)
	if err != nil || !slices.Equal(record.Projects, []string{keep}) || !slices.Equal(record.Permissions, []string{"events:read"}) || len(record.emailProjectPermissions) != 1 {
		t.Fatal(record, err)
	}
	runProjectPurge(t, s, keep, true)
	var grants string
	var revoked sql.NullString
	if err := s.db.QueryRow("SELECT project_grants,revoked_at FROM email_humans JOIN access_keys ON principal_id=id WHERE id=?", h.ID).Scan(&grants, &revoked); err != nil {
		t.Fatal(err)
	}
	var remaining []HumanProjectGrant
	if json.Unmarshal([]byte(grants), &remaining) != nil || remaining == nil || len(remaining) != 0 || revoked.Valid {
		t.Fatal("human should remain provisionable with zero grants")
	}
}

func TestProjectPurgeMachineGrants(t *testing.T) {
	f := newOAuthIntegration(t)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource).AccessToken
	remove, _ := f.project(t, admin)
	keep, _ := f.project(t, admin)
	rights := []string{"events:read"}
	multi := f.createClient(t, admin, rights, []string{remove, keep})
	single := f.createClient(t, admin, rights, []string{remove})
	unaffected := f.createClient(t, admin, rights, []string{keep})
	serviceKey := func(client string) string {
		t.Helper()
		var key AccessKeyCreated
		f.call(t, "POST", "/api/v1/oauth-clients/"+client+"/api-keys", admin, map[string]any{
			"name": "reader", "audience": "api", "permissions": rights, "projects": []string{keep},
		}, 201, &key)
		return key.Token
	}
	multiKey := serviceKey(multi.ClientID)
	keepKey := serviceKey(unaffected.ClientID)
	multiToken := f.token(t, multi.ClientID, multi.ClientSecret, f.s.oauth.apiResource).AccessToken
	singleToken := f.token(t, single.ClientID, single.ClientSecret, f.s.oauth.apiResource).AccessToken
	runProjectPurge(t, f.s, remove, true)
	path := "/api/v1/projects/" + keep + "/events"
	f.call(t, "GET", path, multiKey, nil, 401, nil)
	f.call(t, "GET", path, keepKey, nil, 200, nil)
	f.call(t, "GET", path, singleToken, nil, 401, nil)
	f.call(t, "GET", path, multiToken, nil, 200, nil)
	actor, err := f.s.authenticateResource(t.Context(), multiToken, f.s.oauth.apiResource)
	if err != nil || !slices.Equal(actor.Projects, []string{keep}) {
		t.Fatal(actor, err)
	}
}

func TestProjectPurgeOIDCBindingsAfterRestart(t *testing.T) {
	s := service(t, 20)
	removed, _ := project(t, s)
	keep, _ := project(t, s)
	seeder := &Service{db: s.db, oauth: &oauthRuntime{oidc: &interactiveOIDC{issuer: "https://identity.example", bindings: map[string]actor{"member": {Permissions: []string{"events:read"}, Projects: []string{removed, keep}}}}}}
	if err := seeder.initializeOIDCHumans(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO project_deletions VALUES(?,1)", removed); err != nil {
		t.Fatal(err)
	}
	runProjectPurge(t, s, removed, true)
	if err := seeder.initializeOIDCHumans(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := seeder.oidcHuman(t.Context(), "member")
	if err != nil || !slices.Equal(got.Projects, []string{keep}) {
		t.Fatal("startup reapplied deleted project", got, err)
	}
}
