package porcupine

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func databaseFile(t *testing.T, db *sql.DB) string {
	t.Helper()
	var sequence int
	var name, file string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &name, &file); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestLocalControlIsSeparateFromDurableJournal(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "journal.sqlite")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.db == s.control || databaseFile(t, s.db) == databaseFile(t, s.control) {
		t.Fatal("control and journal share a database")
	}
	for _, file := range []string{path, localControlPath(path)} {
		info, err := os.Stat(file)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("database is missing or not private: %v", err)
		}
	}
	var count int
	if err := s.control.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('events','event_receipts','identity_profiles','import_uploads','export_jobs','pipelines')`).Scan(&count); err != nil || count != 0 {
		t.Fatal("payload or job state leaked into control", count, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE name IN ('oauth_clients','oauth_tokens','email_humans','oauth_interactive_grants')`).Scan(&count); err != nil || count != 0 {
		t.Fatal("authentication state leaked into journal", count, err)
	}
	projectID, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "first"), 200)
	usage := request(t, s, "GET", "/api/v1/tenants/default/usage", adminToken, "", 200)
	if usage["accepted_events"] != "1" {
		t.Fatalf("journal usage not published: %v", usage)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM access_keys WHERE token_hash=$1 OR permissions!='[]' OR projects!='[]'`, hash(adminToken)).Scan(&count); err != nil || count != 0 {
		t.Fatal("journal actor anchors retained authority", count, err)
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "second"), 200)
	if _, err := s.control.Exec(`UPDATE tenant_usage SET accepted_events=0`); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		usage = request(t, s, "GET", "/api/v1/tenants/default/usage", adminToken, "", 200)
		if usage["accepted_events"] != "2" {
			t.Fatalf("high-water replay lost or doubled consumption: %v", usage)
		}
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "first"), 200)
	if eventCount(t, s) != 2 {
		t.Fatal("retry created a duplicate")
	}
	request(t, s, "DELETE", "/api/v1/projects/"+projectID, adminToken, "", 204)
	usage = request(t, s, "GET", "/api/v1/tenants/default/usage", adminToken, "", 200)
	if usage["accepted_events"] != "2" {
		t.Fatal("project deletion refunded durable usage")
	}
}

func TestLocalControlAndJournalPairCannotBeReplacedIndependently(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	configs := []Config{
		{DBPath: filepath.Join(root, "one.sqlite"), ControlDBPath: filepath.Join(root, "one-control.sqlite"), BootstrapToken: adminToken},
		{DBPath: filepath.Join(root, "two.sqlite"), ControlDBPath: filepath.Join(root, "two-control.sqlite"), BootstrapToken: adminToken},
	}
	for _, cfg := range configs {
		s, err := Open(cfg)
		if err != nil {
			t.Fatal(err)
		}
		project(t, s)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	wrong := configs[0]
	wrong.ControlDBPath = configs[1].ControlDBPath
	if s, err := Open(wrong); err == nil {
		s.Close()
		t.Fatal("foreign control database accepted")
	}
	wrong = configs[0]
	wrong.DBPath = filepath.Join(root, "empty-journal.sqlite")
	if s, err := Open(wrong); err == nil {
		s.Close()
		t.Fatal("empty replacement journal accepted")
	}
	for _, cfg := range configs {
		cfg.BootstrapToken = ""
		s, err := Open(cfg)
		if err != nil {
			t.Fatal("failed open damaged original pair", err)
		}
		projects := request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)["projects"].([]any)
		if len(projects) != 1 {
			t.Fatal("original control data changed")
		}
		s.Close()
	}
}

func TestLocalControlDatabasePathBoundaries(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	journal := filepath.Join(root, "journal.sqlite")
	for _, control := range []string{journal, journal + "-wal", journal + "-shm", ":memory:"} {
		if s, err := Open(Config{DBPath: journal, ControlDBPath: control, BootstrapToken: adminToken}); err == nil {
			s.Close()
			t.Fatal("overlapping or mixed storage accepted", control)
		}
	}
	alias := filepath.Join(root, "alias.sqlite")
	if err := os.Symlink(journal, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(Config{DBPath: journal, ControlDBPath: alias, BootstrapToken: adminToken}); err == nil {
		s.Close()
		t.Fatal("dangling symlink alias accepted")
	}
	if err := os.Remove(alias); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(journal, alias); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(Config{DBPath: journal, ControlDBPath: alias, BootstrapToken: adminToken}); err == nil {
		s.Close()
		t.Fatal("hard-link alias accepted")
	}
	s, err := Open(Config{DBPath: ":memory:", BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.db == s.control {
		t.Fatal("in-memory control is not separate")
	}
	project(t, s)
}

func TestLocalControlFailureBlocksAcceptanceAndReadiness(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	projectID, _ := project(t, s)
	if err := s.control.Close(); err != nil {
		t.Fatal(err)
	}
	batch := preparedLiveBatch{projectID: projectID, events: []Event{{ProjectID: projectID, UUID: "not-accepted", Event: "hello", DistinctID: "actor", Properties: []byte(`{}`), Timestamp: now(), ReceivedAt: now()}}}
	if _, err := s.acceptance.Accept(context.Background(), batch, ingestionPeer{}, false); err == nil {
		t.Fatal("accepted an event without authoritative control")
	}
	if eventCount(t, s) != 0 {
		t.Fatal("control failure left accepted data")
	}
	request(t, s, "GET", "/readyz", "", "", 503)
	status, err := s.getInstanceStatus(instanceStatusReaderContext(context.Background()), &struct{}{})
	if err != nil || status.Body.Status != "degraded" || status.Body.Checks.SQLite != dependencyUnavailable {
		t.Fatal("control failure not reported by status", err)
	}
}

func TestCombinedControlJournalLayoutRequiresFreshDirectory(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "old.sqlite")
	db, err := openLocalSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL); INSERT INTO metadata VALUES('initialized','1')`)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err == nil {
		s.Close()
		t.Fatal("old combined layout silently reused")
	}
	if !strings.Contains(err.Error(), "fresh data directory") {
		t.Fatal("layout failure lacks recovery guidance", err)
	}
}
