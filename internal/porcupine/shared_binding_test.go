package porcupine

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestSharedWorkerJournalCannotBecomeStandalone(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO metadata(key,value) VALUES('shared_worker_id','worker-one')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err == nil {
		s.Close()
		t.Fatal("shared journal opened without its control authority")
	}
	if !strings.Contains(err.Error(), "shared control configuration") {
		t.Fatal(err)
	}
}

func TestSharedWorkerControlIdentityIsPinned(t *testing.T) {
	local, control := service(t, 20), service(t, 20)
	if _, err := control.db.Exec(`INSERT INTO metadata(key,value) VALUES('shared_source_id','cluster-one')`); err != nil {
		t.Fatal(err)
	}
	bound := &Service{db: local.db, control: control.db}
	if err := bound.bindSharedControl(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := bound.bindSharedControl(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := control.db.Exec(`UPDATE metadata SET value='cluster-two' WHERE key='shared_source_id'`); err != nil {
		t.Fatal(err)
	}
	if err := bound.bindSharedControl(context.Background()); err == nil {
		t.Fatal("journal rebound to unrelated tenant and quota authority")
	}
}

func TestSharedWorkerDoesNotMigrateLocalProjects(t *testing.T) {
	local, control := service(t, 20), service(t, 20)
	project(t, local)
	if _, err := control.db.Exec(`INSERT INTO metadata(key,value) VALUES('shared_source_id','cluster-one')`); err != nil {
		t.Fatal(err)
	}
	if err := (&Service{db: local.db, control: control.db}).bindSharedControl(context.Background()); err == nil {
		t.Fatal("local projects silently became shared projects")
	}
}
