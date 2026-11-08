package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestRestoreDrillFixture is driven in two separate processes by restore_drill.py.
// Each process closes SQLite before the script stops and copies both storage services.
func TestRestoreDrillFixture(t *testing.T) {
	if os.Getenv("PORCUPINE_RESTORE_DRILL") != "1" {
		t.Skip("run python3 scripts/restore_drill.py for an isolated stopped-copy restore")
	}
	phase := os.Getenv("PORCUPINE_RESTORE_PHASE")
	if phase != "seed" && phase != "verify" {
		t.Fatal("restore drill phase must be seed or verify")
	}
	root := os.Getenv("PORCUPINE_RESTORE_ROOT")
	dbPath, manifestPath := os.Getenv("PORCUPINE_RESTORE_DB"), os.Getenv("PORCUPINE_RESTORE_MANIFEST")
	for _, path := range []string{dbPath, manifestPath} {
		relative, err := filepath.Rel(root, path)
		if !filepath.IsAbs(root) || !filepath.IsAbs(path) || err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatal("restore fixture paths must be inside the drill's private temporary directory")
		}
	}
	if _, err := os.Stat(dbPath); phase == "seed" && !os.IsNotExist(err) || phase == "verify" && err != nil {
		t.Fatal("unexpected fixture SQLite state")
	}
	config := ClickHouseConfig{URL: os.Getenv("PORCUPINE_TEST_CLICKHOUSE_URL"), Database: "porcupine_restore_drill",
		User: os.Getenv("PORCUPINE_TEST_CLICKHOUSE_USER"), Password: os.Getenv("PORCUPINE_TEST_CLICKHOUSE_PASSWORD"),
		StoragePolicy: "hot_cold", HotStorageDays: 30, ColdVolume: "cold"}
	if config.URL == "" || config.User == "" || config.Password == "" {
		t.Fatal("restore drill requires its isolated ClickHouse configuration")
	}
	upstream, err := url.Parse(config.URL)
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	reverse := httputil.NewSingleHostReverseProxy(upstream)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-gate:
			reverse.ServeHTTP(w, r)
		case <-r.Context().Done():
		}
	}))
	defer proxy.Close()
	config.URL = proxy.URL
	bootstrap := adminToken
	if phase == "verify" {
		bootstrap = "restored-config-must-not-reset-bootstrap"
	}
	s, err := Open(Config{DBPath: dbPath, BootstrapToken: bootstrap, QueueMaxEvents: 100,
		ClickHouse: &config, OAuth: &OAuthConfig{Issuer: "http://localhost", AccessTokenLifetimeSeconds: 3600}})
	if err != nil {
		t.Fatal("open restore fixture", err)
	}
	// The fixture controls flushes so the coordinated checkpoint includes pending work.
	s.deliveryCancel()
	<-s.deliveryDone
	close(gate)
	defer func() {
		if err := s.Close(); err != nil {
			t.Error("close restore fixture", err)
		}
	}()
	if phase == "seed" {
		restoreDrillSeed(t, s, manifestPath)
	} else {
		restoreDrillVerify(t, s, manifestPath)
	}
}

type restoreDrillManifest struct {
	Project, OtherProject, Ingest, Admin, Reader, MCPReader string
	RevokedClient, RevokedSecret, RevokedAccess             string
	PendingEdit, PendingDelete                              string
	Fingerprint, DailyKeys                                  string
	Counts                                                  map[string]int
	Before                                                  []map[string]any
}

func restoreDrillToken(t *testing.T, s *Service, id, secret, resource string, status int) string {
	t.Helper()
	form := oauthGrantForm()
	form.Set("resource", resource)
	response := oauthProtocolRequest(t, s, id, secret, form, status)
	if status != 200 {
		return ""
	}
	return response["access_token"].(string)
}

func restoreDrillRequest(t *testing.T, s *Service, method, path, bearer string, body any, status int) map[string]any {
	t.Helper()
	encoded := ""
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		encoded = string(data)
	}
	return request(t, s, method, path, bearer, encoded, status)
}

func restoreDrillQuery(t *testing.T, s *Service, manifest restoreDrillManifest) []map[string]any {
	t.Helper()
	response := restoreDrillRequest(t, s, "POST", "/api/v1/projects/"+manifest.Project+"/query", manifest.Reader,
		map[string]string{"sql": "SELECT uuid,event,distinct_id,properties,timestamp,received_at,revision FROM events ORDER BY uuid"}, 200)
	raw, _ := json.Marshal(response["result"].(map[string]any)["data"])
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func restoreDrillFingerprint(t *testing.T, s *Service) string {
	t.Helper()
	sql := "SELECT " + projectionColumns + " FROM (SELECT " + projectionColumns + " FROM " + s.ch.config.Database + "." + eventTable + " ORDER BY revision DESC LIMIT 1 BY project_id,uuid) ORDER BY project_id,uuid FORMAT JSONEachRow"
	data, err := s.ch.admin(context.Background(), sql)
	if err != nil {
		t.Fatal("read checkpoint projections", err)
	}
	return hash(string(data))
}

func restoreDrillDailyKeys(t *testing.T, s *Service) string {
	t.Helper()
	var keys string
	if err := s.db.QueryRow(`SELECT coalesce(group_concat(day||':'||hex(secret),'|'),'') FROM (SELECT day,secret FROM cookieless_keys ORDER BY day)`).Scan(&keys); err != nil {
		t.Fatal(err)
	}
	return hash(keys)
}

func restoreDrillCounts(t *testing.T, s *Service) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"projects", "access_keys", "oauth_clients", "oauth_tokens", "events", "event_receipts", "event_heads", "mutation_audit", "mutation_outbox"} {
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}

func restoreDrillCold(t *testing.T, s *Service) {
	t.Helper()
	data, err := s.ch.admin(context.Background(), "SELECT count() FROM system.parts WHERE database='"+s.ch.config.Database+"' AND table='"+eventTable+"' AND active AND disk_name='s3'")
	if err != nil || strings.TrimSpace(string(data)) == "0" || len(data) == 0 {
		t.Fatal("checkpoint has no active native S3 part", err)
	}
}

func restoreDrillSeed(t *testing.T, s *Service, manifestPath string) {
	t.Helper()
	ctx := context.Background()
	m := restoreDrillManifest{Admin: restoreDrillToken(t, s, "bootstrap", adminToken, s.oauth.apiResource, 200)}
	for index := range 2 {
		created := restoreDrillRequest(t, s, "POST", "/api/v1/projects", m.Admin, map[string]string{"name": "restore fixture"}, 201)
		id := created["project"].(map[string]any)["id"].(string)
		if index == 0 {
			m.Project, m.Ingest = id, created["ingest_token"].(string)
		} else {
			m.OtherProject = id
			request(t, s, "POST", "/capture/", "", captureBody(created["ingest_token"].(string), "other-project-private"), 200)
		}
	}
	client := func(name string) map[string]any {
		return restoreDrillRequest(t, s, "POST", "/api/v1/oauth-clients", m.Admin, map[string]any{
			"name": name, "permissions": []string{"events:read", "events:edit", "events:delete"}, "projects": []string{m.Project}}, 201)
	}
	active, revoked := client("restored active client"), client("restored revoked client")
	m.Reader = restoreDrillToken(t, s, active["client_id"].(string), active["client_secret"].(string), s.oauth.apiResource, 200)
	m.MCPReader = restoreDrillToken(t, s, active["client_id"].(string), active["client_secret"].(string), s.oauth.mcpResource, 200)
	m.RevokedClient, m.RevokedSecret = revoked["client_id"].(string), revoked["client_secret"].(string)
	m.RevokedAccess = restoreDrillToken(t, s, m.RevokedClient, m.RevokedSecret, s.oauth.apiResource, 200)
	request(t, s, "DELETE", "/api/v1/oauth-clients/"+m.RevokedClient, m.Admin, "", 204)
	for _, uuid := range []string{"delivered", "corrected", "deleted", "pending-correction", "pending-deletion"} {
		body := map[string]any{"api_key": m.Ingest, "uuid": uuid, "event": uuid, "distinct_id": "synthetic-person", "properties": map[string]any{"unicode": "café 東京 🦔", "amount": 1}}
		if uuid == "delivered" {
			body["distinct_id"] = cookielessSentinel
		}
		restoreDrillRequest(t, s, "POST", "/capture/", "", body, 200)
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	// Keep one separately inserted part cold, with the same native disk metadata
	// and remote objects that a stopped restore must preserve together.
	old := time.Now().UTC().Add(-45 * 24 * time.Hour)
	s.clock = func() time.Time { return old }
	request(t, s, "POST", "/capture/", "", captureBody(m.Ingest, "cold"), 200)
	s.clock = time.Now
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	restoreDrillCold(t, s)
	edit := func(uuid, name, key string) string {
		result := restoreDrillRequest(t, s, "PATCH", "/api/v1/projects/"+m.Project+"/events/"+uuid, m.Reader, map[string]any{
			"idempotency_key": key, "expected_revision": "0", "snapshot": map[string]any{
				"event": name, "distinct_id": "synthetic-person", "properties": map[string]any{"amount": 42}, "timestamp": time.Now().UTC().Format(time.RFC3339Nano)}}, 202)
		return result["mutation_id"].(string)
	}
	remove := func(uuid, key string) string {
		result := restoreDrillRequest(t, s, "DELETE", "/api/v1/projects/"+m.Project+"/events/"+uuid, m.Reader,
			map[string]string{"idempotency_key": key, "expected_revision": "0"}, 202)
		return result["mutation_id"].(string)
	}
	edit("corrected", "corrected-v2", "applied-correction")
	remove("deleted", "applied-deletion")
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	m.PendingEdit = edit("pending-correction", "pending-correction-v2", "pending-correction")
	m.PendingDelete = remove("pending-deletion", "pending-deletion")
	request(t, s, "POST", "/capture/", "", captureBody(m.Ingest, "pending"), 200)
	m.Before = restoreDrillQuery(t, s, m)
	if len(m.Before) != 5 {
		t.Fatal("unexpected visible checkpoint event count")
	}
	m.Counts = restoreDrillCounts(t, s)
	if m.Counts["events"] != 1 || m.Counts["mutation_outbox"] != 2 {
		t.Fatal("checkpoint must contain pending original, correction, and deletion")
	}
	m.Fingerprint, m.DailyKeys = restoreDrillFingerprint(t, s), restoreDrillDailyKeys(t, s)
	encoded, err := json.Marshal(m)
	if err != nil || os.WriteFile(manifestPath, encoded, 0600) != nil {
		t.Fatal("cannot write private fixture manifest")
	}
	t.Log("seeded checkpoint: 1 pending original, 2 pending mutations, 5 visible project events, native cold parts, revoked OAuth client")
}

func restoreDrillVerify(t *testing.T, s *Service, manifestPath string) {
	t.Helper()
	encoded, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var m restoreDrillManifest
	if json.Unmarshal(encoded, &m) != nil {
		t.Fatal("invalid private fixture manifest")
	}
	if !reflect.DeepEqual(restoreDrillCounts(t, s), m.Counts) || restoreDrillDailyKeys(t, s) != m.DailyKeys {
		t.Fatal("restored SQLite state differs from the stopped checkpoint")
	}
	if restoreDrillFingerprint(t, s) != m.Fingerprint || !reflect.DeepEqual(restoreDrillQuery(t, s, m), m.Before) {
		t.Fatal("restored ClickHouse projections differ from the stopped checkpoint")
	}
	restoreDrillCold(t, s)
	request(t, s, "GET", "/api/v1/projects/"+m.Project+"/events", m.RevokedAccess, "", 401)
	restoreDrillToken(t, s, m.RevokedClient, m.RevokedSecret, s.oauth.apiResource, 401)
	restoreDrillToken(t, s, "bootstrap", "restored-config-must-not-reset-bootstrap", s.oauth.apiResource, 401)
	request(t, s, "GET", "/api/v1/projects/"+m.OtherProject+"/events", m.Reader, "", 403)
	for _, uuid := range []string{"deleted", "pending-deletion"} {
		request(t, s, "GET", "/api/v1/projects/"+m.Project+"/events/"+uuid, m.Reader, "", 410)
	}
	pending := request(t, s, "GET", "/api/v1/projects/"+m.Project+"/events/pending-correction", m.Reader, "", 200)
	if pending["pending"] != true || pending["event"].(map[string]any)["event"] != "pending-correction-v2" {
		t.Fatal("pending correction did not survive restore")
	}
	if err := s.flushDelivery(context.Background()); err != nil {
		t.Fatal("resume delivery after restore", err)
	}
	for _, id := range []string{m.PendingEdit, m.PendingDelete} {
		status := request(t, s, "GET", "/api/v1/projects/"+m.Project+"/event-mutations/"+id, m.Reader, "", 200)
		if status["state"] != "applied" {
			t.Fatal("restored pending mutation was not acknowledged")
		}
	}
	rows := restoreDrillQuery(t, s, m)
	expected := map[string]string{"delivered": "delivered", "corrected": "corrected-v2", "cold": "purchase", "pending-correction": "pending-correction-v2", "pending": "purchase"}
	if len(rows) != len(expected) {
		t.Fatal("restored delivery changed the logical event count")
	}
	for _, row := range rows {
		if expected[row["uuid"].(string)] != row["event"] {
			t.Fatal("restored delivery lost a correction, resurrected a deletion, or crossed project scope")
		}
	}
	for _, uuid := range []string{"delivered", "corrected", "deleted", "cold", "pending-correction", "pending-deletion", "pending"} {
		response := request(t, s, "POST", "/capture/", "", captureBody(m.Ingest, uuid), 200)
		if response["accepted"] != float64(0) || response["duplicates"] != float64(1) {
			t.Fatal("restored receipt allowed a duplicate or resurrection")
		}
	}
	if eventCount(t, s) != 0 || mutationCount(t, s, "mutation_outbox") != 0 {
		t.Fatal("restored queues did not drain")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, m.MCPReader, "")
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": m.Project}, Body: map[string]any{"sql": "SELECT count() AS total FROM events"}}, 200)
	if result["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "5" {
		t.Fatal("restored MCP query disagrees with the native API")
	}
	t.Log("restored checkpoint verified: exact logical projections and SQLite counts, cold reads, OAuth persistence/revocation, project isolation, pending replay, mutation acknowledgments, receipts, and MCP")
}
