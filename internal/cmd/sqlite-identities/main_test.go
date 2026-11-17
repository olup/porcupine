package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotIsScopedAndReadOnly(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "identity with spaces.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE identity_profiles(project_id TEXT, traits TEXT); CREATE TABLE identity_links(project_id TEXT, anonymous_id TEXT); CREATE TABLE events(id INTEGER, project_id TEXT, uuid TEXT); CREATE TABLE event_receipts(event_id INTEGER, project_id TEXT, uuid TEXT); INSERT INTO identity_profiles VALUES('one','{"plan":"pro"}'),('two','{}'); INSERT INTO identity_links VALUES('one','anon'); INSERT INTO events VALUES(2,'one','queued'); INSERT INTO event_receipts VALUES(1,'one','delivered'),(3,'two','other')`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	result, err := snapshot(path, "one", []control{{UUID: "queued", Event: "$identify"}, {UUID: "delivered", Event: "$set"}, {UUID: "other"}})
	if err != nil {
		t.Fatal(err)
	}
	profiles := result["identity_profiles"].([]map[string]any)
	if len(profiles) != 1 || profiles[0]["traits"].(map[string]any)["plan"] != "pro" {
		t.Fatal(profiles)
	}
	controls := result["control_events"].([]control)
	if len(controls) != 2 || controls[0].ID != 1 || controls[1].ID != 2 {
		t.Fatal(controls)
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	if _, err := snapshot(missing, "one", nil); err == nil {
		t.Fatal("created missing database")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("read probe created file")
	}
}
