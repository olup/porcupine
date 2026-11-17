package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactSelection(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "published server")
	if err := os.WriteFile(path, []byte("artifact"), 0755); err != nil {
		t.Fatal(err)
	}
	absolute, err := artifact(path)
	if err != nil || absolute != path {
		t.Fatalf("artifact %q, %v", absolute, err)
	}
	if _, err := artifact(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("missing prebuilt artifact accepted")
	}
	if _, err := artifact(dir); err == nil {
		t.Fatal("directory accepted as executable")
	}
}
func TestServerEnvironmentHasNoInheritedServiceSettings(t *testing.T) {
	// Setenv requires a serial test; the fixture must never inherit real service credentials.
	t.Setenv("PORCUPINE_API_KEY", "do-not-inherit")
	t.Setenv("PORCUPINE_DB", "real.db")
	t.Setenv("AWS_ACCESS_KEY_ID", "do-not-inherit")
	env := serverEnvironment("/isolated", "127.0.0.1:1234", "synthetic")
	settings := map[string]string{}
	for _, item := range env {
		key, value, _ := strings.Cut(item, "=")
		settings[key] = value
		if strings.Contains(item, "do-not-inherit") {
			t.Fatal("inherited credential")
		}
	}
	if settings["PORCUPINE_DB"] != filepath.Join("/isolated", "test.db") {
		t.Fatal("wrong private SQLite path")
	}
	if settings["PORCUPINE_BOOTSTRAP_TOKEN"] != "synthetic" || settings["PORCUPINE_OAUTH_ISSUER"] != "http://127.0.0.1:1234" {
		t.Fatal("wrong fixture credentials")
	}
}
