package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostedRecoveryFloorFile(t *testing.T) {
	t.Parallel()
	var file FileConfig
	config := fileSettings(file)
	if source, err := config.hostedRecoveryFloor(); source != nil || err != nil {
		t.Fatalf("unset file: %v", err)
	}
	file.Profile = "hosted"
	file.DuckLake.RestoreFloorFile = "relative"
	if _, err := fileSettings(file).hostedRecoveryFloor(); err == nil {
		t.Fatal("relative floor path was accepted")
	}
	path := filepath.Join(t.TempDir(), "restore-floor")
	file.DuckLake.RestoreFloorFile = path
	read, err := fileSettings(file).hostedRecoveryFloor()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(t.Context()); err == nil {
		t.Fatal("missing floor was accepted")
	}
	for _, text := range []string{"", "2026-12-01", strings.Repeat("0", 129)} {
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := read(t.Context()); err == nil {
			t.Fatalf("malformed floor was accepted: %q", text)
		}
	}
	want := time.Date(2026, time.October, 17, 21, 0, 0, 0, time.UTC)
	if err := os.WriteFile(path, []byte(want.Format(time.RFC3339Nano)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if floor, err := read(t.Context()); err != nil || !floor.Equal(want) {
		t.Fatalf("refreshed file returned %v: %v", floor, err)
	}
}
