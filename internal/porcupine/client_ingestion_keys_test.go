package porcupine

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/olup/porcupine/internal/query"
)

func TestClientIngestionKeyLifecycle(t *testing.T) {
	t.Parallel()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: dbPath, BootstrapToken: adminToken, QueueMaxEvents: 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	id, browser := project(t, s)
	other, otherBrowser := project(t, s)
	path := "/api/v1/projects/" + id + "/client-ingestion-key"
	otherPath := "/api/v1/projects/" + other + "/client-ingestion-key"
	_, reader := key(t, s, []string{"management:read"}, []string{id})
	_, writer := key(t, s, []string{"events:write"}, []string{id})
	if got := request(t, s, "GET", path, reader, "", 200)["key"]; got != browser || browser == "" || browser == otherBrowser {
		t.Fatal("project creation and retrieval did not return an isolated browser key")
	}
	request(t, s, "GET", otherPath, reader, "", 403)
	request(t, s, "GET", path, writer, "", 403)
	request(t, s, "POST", path, reader, "", 403)
	request(t, s, "GET", path, browser, "", 401)
	request(t, s, "GET", "/api/v1/projects/"+id+"/events", browser, "", 401)
	request(t, s, "POST", "/api/v1/projects/"+id+"/query", browser, `{"sql":"SELECT * FROM events"}`, 401)
	request(t, s, "POST", "/mcp", browser, `{}`, 401)

	nativeRequest(t, s, browser, `{"type":"track","name":"native browser","actor_id":"visitor"}`, 200)
	request(t, s, "POST", "/capture/", browser, captureBody(otherBrowser, "wrong-selector"), 401)
	request(t, s, "POST", "/capture/", "", tenantTestBody(t, map[string]string{
		"api_key": browser, "project_id": other, "event": "browser event", "distinct_id": "visitor", "uuid": "browser-scope",
	}), 200)
	var capturedProject string
	if err := s.db.QueryRow(`SELECT project_id FROM events WHERE uuid='browser-scope'`).Scan(&capturedProject); err != nil || capturedProject != id {
		t.Fatal("client ingestion key allowed a different project", err)
	}

	var keyID string
	list := request(t, s, "GET", "/api/v1/access-keys", adminToken, "", 200)
	encoded, _ := json.Marshal(list)
	if strings.Contains(string(encoded), browser) || strings.Contains(string(encoded), hash(browser)) {
		t.Fatal("generic key discovery exposed a credential")
	}
	for _, item := range list["access_keys"].([]any) {
		key := item.(map[string]any)
		projects := key["projects"].([]any)
		if key["kind"] == clientIngestionKeyKind && len(projects) == 1 && projects[0] == id {
			keyID = key["id"].(string)
		}
	}
	if keyID == "" {
		t.Fatal("browser key is missing from lifecycle metadata")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(Config{DBPath: dbPath, QueueMaxEvents: 20})
	if err != nil {
		t.Fatal(err)
	}
	s = reopened
	if got := request(t, s, "GET", path, adminToken, "", 200)["key"]; got != browser {
		t.Fatal("browser key changed after restart")
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+keyID, adminToken, "", 204)
	var revokedPublicValue sql.NullString
	if err := s.control.QueryRow(`SELECT client_ingestion_token FROM access_keys WHERE id=$1`, keyID).Scan(&revokedPublicValue); err != nil || revokedPublicValue.Valid {
		t.Fatal("revocation retained a retrievable browser key", err)
	}
	request(t, s, "GET", path, adminToken, "", 404)
	request(t, s, "POST", "/capture/", "", captureBody(browser, "revoked-browser-key"), 401)
	replacement := request(t, s, "POST", path, adminToken, "", 200)["key"].(string)
	if replacement == browser || replacement == otherBrowser || replacement == "" {
		t.Fatal("revoked browser key was reused")
	}
	if got := request(t, s, "POST", path, adminToken, "", 200)["key"]; got != replacement {
		t.Fatal("ensure did not return the same active browser key")
	}
	if got := request(t, s, "GET", otherPath, adminToken, "", 200)["key"]; got != otherBrowser {
		t.Fatal("replacement affected the other project's key")
	}
	request(t, s, "POST", "/capture/", "", captureBody(replacement, "replacement-browser-key"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(writer, "ordinary-write-key"), 200)
}

const (
	legacyClientFixtureProject = "legacy-client-ingestion-project"
	legacyClientFixtureKey     = "test-legacy-ingestion-key"
)

func seedLegacyClientIngestion(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, statement := range []string{
		`INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes)
VALUES('default','Default','2026-01-01',1,1000,100,1024)`,
		`INSERT INTO projects(id,tenant_id,name,created_at) VALUES('legacy-client-ingestion-project','default','Legacy website','2026-01-01')`,
	} {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
VALUES('legacy-write-key','Legacy writer',$1,'["events:write"]','["legacy-client-ingestion-project"]','2026-01-01')`, hash(legacyClientFixtureKey)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tenant_credentials(principal_id,tenant_id) VALUES('legacy-write-key','default')`); err != nil {
		t.Fatal(err)
	}
}

func verifyLegacyClientIngestion(t *testing.T, s *Service) {
	t.Helper()
	var kind string
	var public sql.NullString
	var legacyHash string
	if err := s.control.QueryRow(`SELECT kind,client_ingestion_token,token_hash FROM access_keys WHERE id='legacy-write-key'`).Scan(&kind, &public, &legacyHash); err != nil || kind != "access" || public.Valid || legacyHash != hash(legacyClientFixtureKey) {
		t.Fatal("migration changed a legacy writer or made its secret recoverable", err)
	}
	actor, err := s.authenticateIngestionKey(t.Context(), legacyClientFixtureKey)
	if err != nil || !actor.Can("events:write") || len(actor.Projects) != 1 || actor.Projects[0] != legacyClientFixtureProject {
		t.Fatal("legacy writer cannot authenticate for its existing project", err)
	}
}

func TestClientIngestionKeyFromSQLiteControlUpgrade(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events.db")
	control, err := openLocalSQLite(localControlPath(path))
	if err != nil {
		t.Fatal(err)
	}
	old := localControlSchema
	old.steps = old.steps[:8]
	if err := migrateSQLite(t.Context(), control, old); err != nil {
		control.Close()
		t.Fatal(err)
	}
	seedLegacyClientIngestion(t, control)
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	keyPath := "/api/v1/projects/" + legacyClientFixtureProject + "/client-ingestion-key"
	verifyLegacyClientIngestion(t, s)
	request(t, s, "GET", keyPath, adminToken, "", 404)
	request(t, s, "POST", "/capture/", "", captureBody(legacyClientFixtureKey, "before-ensure"), 200)
	browser := request(t, s, "POST", keyPath, adminToken, "", 200)["key"].(string)
	request(t, s, "POST", "/capture/", "", captureBody(browser, "new-client"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(legacyClientFixtureKey, "after-ensure"), 200)
	verifyLegacyClientIngestion(t, s)
}

func TestSharedClientIngestionKeyUpgradeAndConcurrentEnsure(t *testing.T) {
	t.Parallel()
	control, dsn := sharedControlFixture(t)
	for _, step := range sharedControlSteps[:24] {
		if _, err := control.Exec(step); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := control.Exec(`INSERT INTO metadata(key,value) VALUES('schema_version','24')`); err != nil {
		t.Fatal(err)
	}
	seedLegacyClientIngestion(t, control)
	limits, _ := query.Normalize(nil)
	if err := initializeSharedControl(t.Context(), control, adminToken, limits, false); err != nil {
		t.Fatal(err)
	}
	second, err := openSharedControl(t.Context(), SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	firstServer := &Service{control: control, shared: &sharedRuntime{}}
	secondServer := &Service{control: second, shared: &sharedRuntime{}}
	verifyLegacyClientIngestion(t, firstServer)
	path := "/api/v1/projects/" + legacyClientFixtureProject + "/client-ingestion-key"
	request(t, firstServer, "GET", path, adminToken, "", 404)
	servers := []http.Handler{firstServer.Handler(), secondServer.Handler()}
	start := make(chan struct{})
	type result struct {
		key string
		err error
	}
	results := make(chan result, 8)
	var group sync.WaitGroup
	for i := range cap(results) {
		group.Go(func() {
			<-start
			req := httptest.NewRequest("POST", path, nil)
			req.Header.Set("Authorization", "Bearer "+adminToken)
			rec := httptest.NewRecorder()
			servers[i%len(servers)].ServeHTTP(rec, req)
			if rec.Code != 200 {
				results <- result{err: fmt.Errorf("ensure returned HTTP %d: %s", rec.Code, rec.Body.String())}
				return
			}
			var key struct{ Key string }
			if err := json.Unmarshal(rec.Body.Bytes(), &key); err != nil {
				results <- result{err: err}
				return
			}
			results <- result{key: key.Key}
		})
	}
	close(start)
	group.Wait()
	close(results)
	active := request(t, secondServer, "GET", path, adminToken, "", 200)["key"].(string)
	for result := range results {
		if result.err != nil || result.key == "" || result.key != active {
			t.Fatalf("concurrent ensures did not return the single active key: %v", result.err)
		}
	}
	verifyLegacyClientIngestion(t, secondServer)
}
