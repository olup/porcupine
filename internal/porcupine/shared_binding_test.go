package porcupine

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestOwnerRoutedJournalCannotBecomeStandalone(t *testing.T) {
	t.Parallel()
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
		t.Fatal("owner-routed journal opened in standalone")
	}
	if !strings.Contains(err.Error(), "unsupported owner-routed deployment") {
		t.Fatal(err)
	}
}
