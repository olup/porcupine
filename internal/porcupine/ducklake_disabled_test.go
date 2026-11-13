//go:build !ducklake

package porcupine

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDuckLakeDisabledBuildFailsExplicitly(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Config{DBPath: filepath.Join(dir, "queue.db"), DuckLake: &DuckLakeConfig{Directory: filepath.Join(dir, "lake")}})
	if s != nil {
		s.Close()
		t.Fatal("non-native build accepted DuckLake configuration")
	}
	if err == nil || !strings.Contains(err.Error(), "ducklake build tag") {
		t.Fatalf("missing build instruction: %v", err)
	}
}
