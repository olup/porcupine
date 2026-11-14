package porcupine

import (
	"context"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeRestoreRequiresFreshTargetsAndPrivateReset(t *testing.T) {
	root := t.TempDir()
	valid := NativeRestoreRequest{BackupID: "pc_backup_test", TargetDBPath: filepath.Join(root, "target", "queue.db"), TargetDuckLakeDirectory: filepath.Join(root, "target", "lake"), ResetCredentialsAcknowledged: true, BootstrapToken: strings.Repeat("fresh-private-bootstrap", 2)}
	if err := validateNativeRestoreRequest(valid); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*NativeRestoreRequest){
		func(r *NativeRestoreRequest) { r.ResetCredentialsAcknowledged = false },
		func(r *NativeRestoreRequest) { r.BootstrapToken = "short" },
		func(r *NativeRestoreRequest) { r.TargetDBPath = "relative.db" },
		func(r *NativeRestoreRequest) { r.LocalBundlePath = filepath.Join(root, "target") },
		func(r *NativeRestoreRequest) { r.TargetDBPath = filepath.Join(r.TargetDuckLakeDirectory, "queue.db") },
	} {
		r := valid
		change(&r)
		if err := validateNativeRestoreRequest(r); err == nil {
			t.Fatal("unsafe restore request accepted")
		}
	}
	if err := os.MkdirAll(valid.TargetDuckLakeDirectory, 0700); err != nil {
		t.Fatal(err)
	}
	if err := validateNativeRestoreRequest(valid); err == nil {
		t.Fatal("existing target accepted")
	}
	if strings.Contains(valid.String(), valid.BootstrapToken) {
		t.Fatal("request formatting exposed private credential")
	}
}

func TestNativeRestoreCredentialResetKeepsAnalyticsAndRevokesHumans(t *testing.T) {
	s := service(t, 100)
	projectID, ingest := project(t, s)
	readerID, oldReader := key(t, s, []string{"events:read"}, []string{projectID})
	ctx := context.Background()
	if _, err := s.db.Exec(`INSERT INTO email_humans(principal_id,email,project_grants,notice_sent) VALUES(?,'revoked@example.test','[]',0);UPDATE access_keys SET revoked_at=? WHERE id=?`, readerID, now(), readerID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO identity_profiles(project_id,identified_id,is_identified,version,traits) VALUES(?,'person',1,7,'{"plan":"team"}')`, projectID); err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{adminToken, ingest, oldReader} {
		if _, err := resetNativeRestoreCredentials(ctx, s.db, old); err == nil {
			t.Fatal("archived access or ingestion credential reused as recovery root")
		}
	}
	if _, err := s.db.Exec(`INSERT INTO metadata(key,value) VALUES('native_backup_writer_id','source-writer')`); err != nil {
		t.Fatal(err)
	}
	fresh := strings.Repeat("fresh-recovery", 3)
	result, err := resetNativeRestoreCredentials(ctx, s.db, fresh)
	if err != nil {
		t.Fatal(err)
	}
	request(t, s, "GET", "/api/v1/projects", adminToken, "", 401)
	request(t, s, "GET", "/api/v1/projects", oldReader, "", 401)
	request(t, s, "GET", "/api/v1/projects", fresh, "", 200)
	nativeRequest(t, s, ingest, `{"type":"track","name":"old-key","actor_id":"person"}`, 401)
	if len(result.IngestionKeys) != 1 || result.IngestionKeys[0].ProjectID != projectID {
		t.Fatalf("missing rotated ingestion credential: %+v", result)
	}
	nativeRequest(t, s, result.IngestionKeys[0].Token, `{"type":"track","name":"new-key","actor_id":"person"}`, 200)
	for _, table := range []string{"email_humans", "oidc_humans", "oauth_clients", "oauth_tokens", "oauth_authorization_codes", "oauth_refresh_tokens", "oauth_interactive_grants", "service_api_keys", "webhook_deliveries", "export_files"} {
		var count int
		if err = s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("old credential/output table %s retained rows: %d %v", table, count, err)
		}
	}
	var writerCount int
	if err = s.db.QueryRow(`SELECT count(*) FROM metadata WHERE key='native_backup_writer_id'`).Scan(&writerCount); err != nil || writerCount != 0 {
		t.Fatal("restored instance retained backup writer ownership", err)
	}
	var alias string
	if err = s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&alias); err != nil || alias != result.RecoveryAdminID {
		t.Fatal("fresh admin not pinned", err)
	}
	var retained string
	if err = s.db.QueryRow(`SELECT traits FROM identity_profiles WHERE project_id=? AND identified_id='person'`, projectID).Scan(&retained); err != nil || retained != `{"plan":"team"}` {
		t.Fatal("analytical identity changed", err)
	}
	var revoked bool
	if err = s.db.QueryRow(`SELECT revoked_at IS NOT NULL FROM access_keys WHERE id=?`, readerID).Scan(&revoked); err != nil || !revoked {
		t.Fatal("audit principal not retained revoked", err)
	}
}

func TestNativeRestoreRejectsSQLiteSchemaChanges(t *testing.T) {
	s := service(t, 100)
	if err := verifyRestoreSQLiteSchema(context.Background(), s.db); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("ALTER TABLE projects ADD COLUMN future_column TEXT"); err != nil {
		t.Fatal(err)
	}
	if err := verifyRestoreSQLiteSchema(context.Background(), s.db); err == nil {
		t.Fatal("future schema accepted")
	}
}

func TestNativeRestoreOAuthCredentialsAndRevokedHumanOTPStayInvalid(t *testing.T) {
	j := newEmailJourney(t)
	s := j.service
	oldAdmin := oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	created := request(t, s, "POST", "/api/v1/projects", oldAdmin, `{"name":"recovery"}`, 201)
	projectID := created["project"].(map[string]any)["id"].(string)
	app := j.register(t, oldAdmin, "old-application", []string{"events:read"}, []string{projectID})
	oldAccess := j.browserLogin(t, app, "operator@example.test", "/api/v1", projectID, "", 200)
	revoked := request(t, s, "POST", "/api/v1/humans", oldAdmin, `{"email":"revoked@example.test","projects":["`+projectID+`"],"permissions":["events:read"]}`, 201)
	<-j.notices
	request(t, s, "DELETE", "/api/v1/humans/"+revoked["id"].(string), oldAdmin, "", 204)
	_, clientSecret := oauthProtocolClient(t, s, []string{"events:read"}, []string{projectID})
	var grantID string
	if err := s.db.QueryRow(`SELECT grant_id FROM oauth_interactive_access WHERE token_hash=?`, hash(oldAccess.AccessToken)).Scan(&grantID); err != nil {
		t.Fatal(err)
	}
	oldCode := strings.Repeat("used-source-authorization-code", 2)
	if _, err := s.db.Exec(`INSERT INTO oauth_authorization_codes(code_hash,grant_id,client_id,redirect_uri,challenge,expires_at_ms,used_at_ms) VALUES(?,?,?,'http://127.0.0.1/callback','challenge',1,1)`, hash(oldCode), grantID, app.ClientID); err != nil {
		t.Fatal(err)
	}
	for _, old := range []string{clientSecret, oldAdmin, oldAccess.AccessToken, oldAccess.RefreshToken, oldCode} {
		if _, err := resetNativeRestoreCredentials(context.Background(), s.db, old); err == nil {
			t.Fatal("archived OAuth credential reused as recovery root")
		}
	}
	fresh := strings.Repeat("fresh-offline-admin", 3)
	if _, err := resetNativeRestoreCredentials(context.Background(), s.db, fresh); err != nil {
		t.Fatal(err)
	}
	request(t, s, "GET", "/api/v1/projects", oldAdmin, "", 401)
	request(t, s, "POST", "/api/v1/projects/"+projectID+"/query", oldAccess.AccessToken, `{"sql":"SELECT count(*) FROM events"}`, 401)
	oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 401)
	j.checkRefresh(t, app, oldAccess, "/api/v1", 401)
	recovery := oauthProtocolRequest(t, s, "bootstrap", fresh, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	request(t, s, "POST", "/api/v1/humans", recovery, `{"email":"fresh@example.test","projects":["`+projectID+`"],"permissions":["events:read"]}`, 201)
	if recipient := <-j.notices; recipient != "fresh@example.test" {
		t.Fatal("fresh admin could not invite a fresh human")
	}
	start := httptest.NewRecorder()
	s.startEmailLogin(start, oauthAuthorizationRequest{ID: "recovery-otp-check", ClientName: "Fresh recovery tool"})
	csrf := csrfFromJourneyForm(t, start.Body.String())
	cookie := start.Result().Cookies()[0]
	form := url.Values{"email": {"revoked@example.test"}, "csrf": {csrf}}
	requestOTP := httptest.NewRequest("POST", j.issuer+"/oauth/email/request", strings.NewReader(form.Encode()))
	requestOTP.AddCookie(cookie)
	requestOTP.Header.Set("Origin", j.issuer)
	requestOTP.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, requestOTP)
	if response.Code != 200 {
		t.Fatal("revoked-human OTP response leaked account existence", response.Code)
	}
	if id, err := s.lookupActiveEmailID(context.Background(), "revoked@example.test"); err == nil || id != "" {
		t.Fatal("archived revoked human regained OTP eligibility")
	}
	select {
	case mail := <-j.mail:
		t.Fatal("restored source identity received an OTP", mail.to)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNativeRestoreCancelsOutboundWorkAndSourceObjectReferences(t *testing.T) {
	s := service(t, 100)
	projectID, _ := project(t, s)
	var actorID string
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&actorID); err != nil {
		t.Fatal(err)
	}
	_, err := s.db.Exec(`INSERT INTO pipelines(id,project_id,actor_id,kind,name,state,config,secret,created_at_ms) VALUES('old-pipeline',?,?,'webhook','Old delivery','active','{}',X'0102',1);
 INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,state,next_attempt_ms,created_at_ms) VALUES('old-delivery','old-pipeline',?,'old-event','body',4,'queued',0,1);
 INSERT INTO pipeline_s3_runs(id,pipeline_id,window_start_ms,window_end_ms,state,created_at_ms,manifest) VALUES('old-run','old-pipeline',0,1,'completed',1,'source/manifest.json');
 INSERT INTO pipeline_s3_files(run_id,object_key,upload_id) VALUES('old-run','source/pipeline.parquet','old-upload');
 INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,event_watermark,mutation_watermark,created_at,attempt_id,query_id,cleanup_pending,file_count) VALUES('old-export',?,?,'SELECT 1','{}','parquet','old-idempotency','old-request','running',1,0,'now','old-attempt','old-query',1,1);
 INSERT INTO export_files(job_id,attempt_id,file_index,object_key,upload_id) VALUES('old-export','old-attempt',0,'source/export.parquet','old-upload');
 CREATE TABLE project_deletion_files(project_id TEXT NOT NULL,object_key TEXT NOT NULL,upload_id TEXT NOT NULL,PRIMARY KEY(project_id,object_key,upload_id)) WITHOUT ROWID;
 INSERT INTO project_deletion_files(project_id,object_key,upload_id) VALUES(?,'source/deleted-project.parquet','old-upload');`, projectID, actorID, projectID, projectID, actorID, projectID)
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyRestoreSQLiteSchema(context.Background(), s.db); err != nil {
		t.Fatal("same-release lazy deletion table was rejected", err)
	}
	if _, err = resetNativeRestoreCredentials(context.Background(), s.db, strings.Repeat("fresh-offline-admin", 3)); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"webhook_deliveries", "webhook_members", "pipeline_s3_runs", "pipeline_s3_files", "export_files", "project_deletion_files"} {
		var count int
		if err = s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("source outbound references survived in %s: %d %v", table, count, err)
		}
	}
	var paused, cleared bool
	if err = s.db.QueryRow(`SELECT state='paused',length(secret)=0 FROM pipelines WHERE id='old-pipeline'`).Scan(&paused, &cleared); err != nil || !paused || !cleared {
		t.Fatal("restored pipeline was not paused with erased credentials", err)
	}
	var cancelled bool
	if err = s.db.QueryRow(`SELECT state='canceled' AND cancel_requested=1 AND attempt_id='' AND query_id='' AND cleanup_pending=0 AND file_count=0 FROM export_jobs WHERE id='old-export'`).Scan(&cancelled); err != nil || !cancelled {
		t.Fatal("restored export could restart or delete source output", err)
	}
}
