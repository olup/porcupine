package porcupine

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/auth"
)

func TestExternalRestoreModeAndPrivateRequest(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, request, destination, want string
		configured                       bool
		invalid                          bool
	}{
		{name: "missing offline mode", invalid: true},
		{name: "missing mode with unset destination", configured: true, invalid: true},
		{name: "offline builtin", request: "builtin", want: "builtin"},
		{name: "offline external", request: "external", want: "external"},
		{name: "request with unset destination", request: "external", configured: true, want: "external"},
		{name: "configured external", destination: "external", configured: true, want: "external"},
		{name: "configured builtin", destination: "builtin", configured: true, want: "builtin"},
		{name: "request differs from builtin", request: "external", destination: "builtin", configured: true, invalid: true},
		{name: "request differs from external", request: "builtin", destination: "external", configured: true, invalid: true},
		{name: "unknown request", request: "other", invalid: true},
		{name: "unknown destination", destination: "other", configured: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var destination *AuthConfig
			if tc.configured {
				destination = &AuthConfig{Mode: tc.destination}
			}
			mode, err := nativeRestoreAuthMode(NativeRestoreRequest{AuthMode: tc.request}, destination)
			if tc.invalid && err == nil || !tc.invalid && (err != nil || mode != tc.want) {
				t.Fatalf("unexpected mode %q: %v", mode, err)
			}
		})
	}
	root := t.TempDir()
	r := NativeRestoreRequest{BackupID: "pc_backup_external", AuthMode: "external", TargetDBPath: filepath.Join(root, "target", "journal.db"), TargetDuckLakeDirectory: filepath.Join(root, "target", "lake"), ResetCredentialsAcknowledged: true}
	if err := validateNativeRestoreRequest(r); err != nil {
		t.Fatalf("external request required a builtin recovery credential: %v", err)
	}
	r.BootstrapToken = strings.Repeat("private-bootstrap", 3)
	if err := validateNativeRestoreRequest(r); err == nil {
		t.Fatal("external request accepted a builtin recovery credential")
	}
	r.BootstrapToken = ""
	if _, err := RestoreNativeBackup(t.Context(), NativeRestoreConfig{Request: r, Auth: &AuthConfig{Mode: "builtin"}}); err == nil || !strings.Contains(err.Error(), "auth_mode differs") {
		t.Fatalf("destination mismatch was not rejected first: %v", err)
	}
}

func TestExternalRestoreRejectsArchivedModeBeforeReset(t *testing.T) {
	t.Parallel()
	builtin := service(t, 10)
	var version int
	if err := builtin.control.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if _, err := resetNativeRestoreCredentials(t.Context(), builtin.db, builtin.control, "", "external"); err == nil {
		t.Fatal("builtin archive adopted by external destination")
	}
	var current, active int
	if err := builtin.control.QueryRow(`PRAGMA user_version`).Scan(&current); err != nil || current != version {
		t.Fatal("mismatch changed the archived schema", err)
	}
	if err := builtin.control.QueryRow(`SELECT count(*) FROM access_keys WHERE revoked_at IS NULL`).Scan(&active); err != nil || active != 1 {
		t.Fatal("mismatch reset archived credentials", active, err)
	}
	f := newExternalAuthFixture(t)
	if _, err := resetNativeRestoreCredentials(t.Context(), f.s.db, f.s.control, strings.Repeat("fresh-private-token", 2), "builtin"); err == nil {
		t.Fatal("external archive adopted by builtin destination")
	}
}

func TestExternalRestoreRevokesNativeKeysAndPreservesArtifactHolds(t *testing.T) {
	t.Parallel()
	f := newExternalAuthFixture(t)
	s := f.s
	operator := f.operator(t)
	created := request(t, s, "POST", "/api/v1/projects", operator, `{"name":"Restored project"}`, 201)
	project := created["project"].(map[string]any)["id"].(string)
	oldIngestion := created["ingest_token"].(string)
	account := request(t, s, "POST", "/api/v1/tenants/"+defaultTenantID+"/service-accounts", operator, externalBody(t, map[string]any{"name": "Team", "permissions": []string{"events:read"}, "projects": []string{project}}), 201)
	key := request(t, s, "POST", "/api/v1/tenants/"+defaultTenantID+"/service-accounts/"+account["client_id"].(string)+"/api-keys", operator, externalBody(t, map[string]any{"name": "Reader", "audience": "api", "permissions": []string{"events:read"}, "projects": []string{project}}), 201)
	actor := key["id"].(string)
	if err := s.ensureProjectLocal(t.Context(), project, auth.Principal{ID: actor}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES('source-pipeline',?,?,'webhook','Source pipeline','active','{}',X'0102',1);
INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at,attempt_id) VALUES('source-export',?,?,'SELECT 1','{}','jsonl','export','export','running',0,0,'now','attempt');
INSERT INTO export_files(job_id,attempt_id,file_index,object_key,upload_id,storage_id) VALUES('source-export','attempt',0,'source/export.jsonl.gz','source-upload','source-storage');
INSERT INTO export_external_authority(job_id,tenant_id,issuer,expires_at_ms) VALUES('source-export',?,?,1);
CREATE TRIGGER fail_external_reset BEFORE DELETE ON export_files BEGIN SELECT RAISE(ABORT,'interrupted external reset'); END`, project, actor, project, actor, defaultTenantID, f.issuer); err != nil {
		t.Fatal(err)
	}
	if _, err := resetNativeRestoreCredentials(t.Context(), s.db, s.control, "", "external"); err == nil {
		t.Fatal("fixture failed to interrupt journal reset")
	}
	var holds int
	if err := s.control.QueryRow(`SELECT count(*) FROM restored_artifact_holds WHERE project_id=?`, project).Scan(&holds); err != nil || holds != 1 {
		t.Fatal("external reset lost the source artifact before journal reset", holds, err)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_external_reset`); err != nil {
		t.Fatal(err)
	}
	result, err := resetNativeRestoreCredentials(t.Context(), s.db, s.control, "", "external")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(result)
	if err != nil || result.RecoveryAdminID != "" || strings.Contains(string(encoded), "recovery_admin_id") || strings.Contains(string(encoded), "bootstrap_token") {
		t.Fatalf("external reset produced builtin recovery authority: %s %v", encoded, err)
	}
	if len(result.IngestionKeys) != 1 || result.IngestionKeys[0].ProjectID != project {
		t.Fatal("external reset did not rotate ingestion access", result)
	}
	request(t, s, "GET", "/api/v1/projects", operator, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+project+"/events", key["token"].(string), "", 401)
	request(t, s, "POST", "/capture/", "", captureBody(oldIngestion, "old key"), 401)
	request(t, s, "POST", "/capture/", "", captureBody(result.IngestionKeys[0].Token, "rotated key"), 200)
	for _, table := range []string{"email_humans", "oidc_identities", "tenant_memberships", "oauth_grant_tenants", "oauth_authorization_requests", "oauth_tokens", "oauth_refresh_tokens", "oauth_interactive_grants", "oauth_clients", "service_api_keys"} {
		var count int
		if err := s.control.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("external reset retained or created auth state in %s: %d %v", table, count, err)
		}
	}
	var admin, metadata int
	if err := s.control.QueryRow(`SELECT count(*) FROM access_keys WHERE revoked_at IS NULL AND permissions='["instance:admin"]'`).Scan(&admin); err != nil || admin != 0 {
		t.Fatal("external reset minted local administrator", admin, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&metadata); err != nil || metadata != 0 {
		t.Fatal("external reset initialized builtin bootstrap metadata", metadata, err)
	}
	var paused, canceled bool
	if err := s.db.QueryRow(`SELECT state='paused' AND length(secret)=0 FROM pipelines WHERE id='source-pipeline'`).Scan(&paused); err != nil || !paused {
		t.Fatal("external reset left pipeline executable", err)
	}
	if err := s.db.QueryRow(`SELECT state='canceled' AND cancel_requested=1 AND attempt_id='' FROM export_jobs WHERE id='source-export'`).Scan(&canceled); err != nil || !canceled {
		t.Fatal("external reset left export executable", err)
	}
	var grants int
	if err := s.db.QueryRow(`SELECT count(*) FROM export_external_authority`).Scan(&grants); err != nil || grants != 0 {
		t.Fatal("external restore retained delegated export authority", grants, err)
	}
	if err := s.control.QueryRow(`SELECT count(*) FROM restored_artifact_holds WHERE project_id=?`, project).Scan(&holds); err != nil || holds != 1 {
		t.Fatal("external reset retry duplicated or discarded artifact holds", holds, err)
	}
}
