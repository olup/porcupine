//go:build ducklake

package porcupine

import (
	"context"
	"path/filepath"
	"testing"
)

func TestDuckLakeWrongBackendDoesNotPoisonReopen(t *testing.T) {
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "queue.db"), BootstrapToken: adminToken,
		DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "lake"), MemoryLimitMB: 128, Threads: 1}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	source, err := s.lake.SourceID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := config
	wrong.DuckLake = nil
	wrong.ClickHouse = &ClickHouseConfig{URL: "http://127.0.0.1:1", Database: "unused", User: "unused", Password: "unused"}
	bad, err := Open(wrong)
	if bad != nil {
		bad.Close()
		t.Fatal("wrong backend accepted")
	}
	if err == nil {
		t.Fatal("wrong backend not rejected")
	}
	s, err = Open(config)
	if err != nil {
		t.Fatalf("wrong configuration poisoned valid archive: %v", err)
	}
	defer s.Close()
	actual, err := s.lake.SourceID(context.Background())
	if err != nil || actual != source {
		t.Fatalf("source changed: %v", err)
	}
	var poisoned int
	if err := s.db.QueryRow("SELECT count(*) FROM metadata WHERE key='query_master_key'").Scan(&poisoned); err != nil || poisoned != 0 {
		t.Fatalf("wrong backend persisted metadata: count=%d err=%v", poisoned, err)
	}
}
