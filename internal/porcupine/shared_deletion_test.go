package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"testing"
)

func sharedDeletionFixture(t *testing.T) *Service {
	t.Helper()
	db, _ := sharedControlFixture(t)
	queries, _ := normalizeInteractiveQueries(nil)
	if err := initializeSharedControl(t.Context(), db, adminToken, queries); err != nil {
		t.Fatal(err)
	}
	s := &Service{control: db, shared: &sharedRuntime{config: SharedConfig{WorkerID: "deletion-test"}}, interactiveConfig: queries}
	if _, err := db.Exec(`INSERT INTO shared_workers(worker_id,volume_id,route_url,registration_token) VALUES('owner-a','volume-a','http://a.invalid','token-a'),('owner-b','volume-b','http://b.invalid','token-b');
 INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES('removed','default','Removed','now','mixed'),('kept','default','Kept','now','mixed');
 INSERT INTO shared_project_owners(project_id,worker_id) VALUES('removed','owner-a'),('kept','owner-b')`); err != nil {
		t.Fatal(err)
	}
	return s
}

func sharedDeletionPrincipal(t *testing.T, db *sql.DB, id string, projects []string) {
	t.Helper()
	raw, err := json.Marshal(projects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,$1,$1,'["events:read"]',$2,'now')`, id, string(raw)); err != nil {
		t.Fatal(err)
	}
}

func TestSharedProjectDeletionGrantsOwnershipAndUsage(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	ctx := t.Context()
	for _, spec := range []struct {
		id       string
		projects []string
	}{
		{"single", []string{"removed"}}, {"multi", []string{"removed", "kept"}}, {"unaffected", []string{"kept"}}, {"global", []string{"*"}}, {"oidc", []string{"removed"}}, {"already-revoked", []string{"removed"}}, {"service-key", []string{"kept"}},
	} {
		sharedDeletionPrincipal(t, s.control, spec.id, spec.projects)
	}
	if _, err := s.control.Exec(`INSERT INTO oauth_clients(client_id,secret_hash) VALUES('multi','hashed');
 INSERT INTO service_api_keys(principal_id,client_id,audience) VALUES('service-key','multi','api');
 INSERT INTO email_humans(principal_id,email,project_grants) VALUES('oidc','oidc@example.test','[{"project_id":"removed","permissions":["events:read"]}]');
 INSERT INTO oidc_identities(id,issuer,subject,principal_id,linked_email,created_at) VALUES('member-link','https://identity.test','member','oidc','oidc@example.test','now');
 UPDATE access_keys SET revoked_at='original-revocation' WHERE id='already-revoked';
 INSERT INTO oauth_tokens(token_hash,client_id,resource,scope,projects_json,issued_at_ms,expires_at_ms) VALUES('single-token','single','api','events:read','["removed"]',0,1),('multi-token','multi','api','events:read','["removed","kept"]',0,1);
 INSERT INTO tenant_usage(tenant_id,period_start,accepted_events) VALUES('default','2026-11-01T00:00:00Z',7)`); err != nil {
		t.Fatal(err)
	}
	human := EmailHuman{ID: "human", Email: "member@example.test", CreatedAt: "now", Projects: []string{"removed", "kept"}, Permissions: []string{"events:edit", "events:read"}, Grants: []HumanProjectGrant{{ProjectID: "removed", Permissions: []string{"events:edit"}}, {ProjectID: "kept", Permissions: []string{"events:read"}}}}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertEmailHuman(ctx, tx, human); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	sharedDeletionPrincipal(t, s.control, "consent", []string{"removed", "kept"})
	if _, err := s.control.Exec(`INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms) VALUES('consent','consent','human','multi','api',0,1);
INSERT INTO oauth_refresh_tokens(token_hash,grant_id,family_id,client_id,scope,projects_json,expires_at_ms) VALUES('single-refresh','consent','family','multi','events:read','["removed"]',1),('multi-refresh','consent','family','multi','events:read','["removed","kept"]',1)`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	var owners int
	if err := s.control.QueryRow(`SELECT count(*) FROM shared_project_owners WHERE project_id='removed'`).Scan(&owners); err != nil || owners != 1 {
		t.Fatal("deletion fence lost durable owner before cleanup", owners, err)
	}
	current, err := loadEmailHuman(ctx, s.control, human.ID)
	if err != nil || !slices.Equal(current.Projects, []string{"kept"}) || !slices.Equal(current.Permissions, []string{"events:read"}) {
		t.Fatal("human scopes were not pruned", current, err)
	}
	for _, id := range []string{"single", "already-revoked", "service-key"} {
		var revoked sql.NullString
		if err := s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=$1`, id).Scan(&revoked); err != nil || !revoked.Valid {
			t.Fatal("exhausted or dependent machine stayed active", id, err)
		}
	}
	var original string
	if err := s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id='already-revoked'`).Scan(&original); err != nil || original != "original-revocation" {
		t.Fatal("existing revocation changed", original, err)
	}
	var oidcRevoked sql.NullString
	if err := s.control.QueryRow(`SELECT revoked_at FROM access_keys WHERE id='oidc'`).Scan(&oidcRevoked); err != nil || oidcRevoked.Valid {
		t.Fatal("exhausted OIDC human should remain provisionable", err)
	}
	var multiProjects string
	if err := s.control.QueryRow(`SELECT projects_json FROM oauth_tokens WHERE token_hash='multi-token'`).Scan(&multiProjects); err != nil || multiProjects != `["kept"]` {
		t.Fatal("multi-project token lost surviving scope", multiProjects, err)
	}
	var singleTokens int
	if err := s.control.QueryRow(`SELECT count(*) FROM oauth_tokens WHERE token_hash='single-token'`).Scan(&singleTokens); err != nil || singleTokens != 0 {
		t.Fatal("exhausted token retained", singleTokens, err)
	}
	if err := s.control.QueryRow(`SELECT projects_json FROM oauth_refresh_tokens WHERE token_hash='multi-refresh'`).Scan(&multiProjects); err != nil || multiProjects != `["kept"]` {
		t.Fatal("multi-project refresh token lost surviving scope", multiProjects, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM oauth_refresh_tokens WHERE token_hash='single-refresh'`).Scan(&singleTokens); err != nil || singleTokens != 0 {
		t.Fatal("exhausted refresh token retained", singleTokens, err)
	}
	if err := s.beginControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal("begin not idempotent", err)
	}
	if err := s.completeControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	if err := s.completeControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal("completion not idempotent", err)
	}
	if err := s.beginControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal("completed deletion cannot be retried", err)
	}
	var completed bool
	if err := s.control.QueryRow(`SELECT completed FROM project_deletions WHERE project_id='removed'`).Scan(&completed); err != nil || !completed {
		t.Fatal("permanent tombstone missing", err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM shared_project_owners`).Scan(&owners); err != nil || owners != 1 {
		t.Fatal("owner cleanup changed unrelated project", owners, err)
	}
	var projects, usage int
	if err := s.control.QueryRow(`SELECT count(*) FROM projects`).Scan(&projects); err != nil || projects != 1 {
		t.Fatal("project cleanup changed unrelated project", projects, err)
	}
	if err := s.control.QueryRow(`SELECT accepted_events FROM tenant_usage WHERE tenant_id='default'`).Scan(&usage); err != nil || usage != 7 {
		t.Fatal("deletion refunded tenant usage", usage, err)
	}
	var global string
	if err := s.control.QueryRow(`SELECT projects FROM access_keys WHERE id='global'`).Scan(&global); err != nil || global != `["*"]` {
		t.Fatal("explicit global grant changed", global, err)
	}
	if err := s.beginControlProjectDeletion(ctx, "kept"); err != nil {
		t.Fatal(err)
	}
	var humanRevoked sql.NullString
	var rawGrants string
	if err := s.control.QueryRow(`SELECT k.revoked_at,h.project_grants FROM access_keys k JOIN email_humans h ON h.principal_id=k.id WHERE k.id='human'`).Scan(&humanRevoked, &rawGrants); err != nil || humanRevoked.Valid || rawGrants != "[]" {
		t.Fatal("human with zero grants should remain provisionable", rawGrants, err)
	}
}

func TestProjectPurgeLocalRecordsLeavesGrantAuthorityAlone(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	removed, ingest := project(t, s)
	kept, _ := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "queued"), 200)
	id, _ := key(t, s, []string{"events:read"}, []string{removed})
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := purgeProjectLocalRecords(t.Context(), tx, removed); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var revoked sql.NullString
	var projects string
	if err := s.control.QueryRow(`SELECT projects,revoked_at FROM access_keys WHERE id=$1`, id).Scan(&projects, &revoked); err != nil || revoked.Valid {
		t.Fatal("local payload purge changed credential authority", err)
	}
	var scopes []string
	if err := json.Unmarshal([]byte(projects), &scopes); err != nil || !slices.Equal(scopes, []string{removed}) {
		t.Fatal("local payload purge pruned shared-scope mirror", projects, err)
	}
	var retained string
	if err := s.db.QueryRow(`SELECT id FROM projects`).Scan(&retained); err != nil || retained != kept {
		t.Fatal("local payload purge touched unrelated project", retained, err)
	}
	if eventCount(t, s) != 0 {
		t.Fatal("local payload purge retained accepted payload")
	}
}

func TestSharedProjectDeletionFencesAdmissionAndRequiresStaging(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	ctx := t.Context()
	if err := s.completeControlProjectDeletion(ctx, "kept"); err == nil {
		t.Fatal("unstaged project was deleted")
	}
	if err := s.beginControlProjectDeletion(ctx, "missing"); err == nil {
		t.Fatal("unknown project deletion succeeded")
	}
	_, release, err := s.acquireSharedQueryPermit(ctx, "removed", defaultTenantID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(ctx, "removed"); err != nil {
		release.release()
		t.Fatal(err)
	}
	if _, unexpectedRelease, err := s.acquireSharedQueryPermit(ctx, "removed", defaultTenantID); err == nil {
		unexpectedRelease.release()
		release.release()
		t.Fatal("new query crossed durable deletion fence")
	}
	release.release()
	if err := s.waitSharedProjectQueries(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	if err := s.completeControlProjectDeletion(ctx, "removed"); err != nil {
		t.Fatal(err)
	}
	if _, release, err := s.acquireSharedQueryPermit(ctx, "kept", defaultTenantID); err != nil {
		t.Fatal("deletion blocked unrelated project", err)
	} else {
		release.release()
	}
}

func TestSharedProjectDeletionRollsBackFenceOnGrantFailure(t *testing.T) {
	t.Parallel()
	s := sharedDeletionFixture(t)
	sharedDeletionPrincipal(t, s.control, "corrupt", []string{"removed"})
	if _, err := s.control.Exec(`UPDATE access_keys SET projects='invalid-json' WHERE id='corrupt'`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(t.Context(), "removed"); err == nil {
		t.Fatal("corrupt grants were silently ignored")
	}
	var fence bool
	if err := s.control.QueryRow(`SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id='removed')`).Scan(&fence); err != nil || fence {
		t.Fatal("failed grant cleanup committed deletion fence", fence, err)
	}
	if _, err := s.control.Exec(`UPDATE access_keys SET projects='["removed"]' WHERE id='corrupt'`); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(context.Background(), "removed"); err != nil {
		t.Fatal("failed deletion could not resume", err)
	}
	err := s.control.QueryRow(`SELECT id FROM projects WHERE id='removed'`).Scan(new(string))
	if errors.Is(err, sql.ErrNoRows) || err != nil {
		t.Fatal("begin removed project before owner cleanup", err)
	}
}
