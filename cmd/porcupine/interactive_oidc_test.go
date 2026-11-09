package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOIDCOperatorConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_OAUTH_ISSUER", "PORCUPINE_OIDC_ISSUER", "PORCUPINE_OIDC_CLIENT_ID", "PORCUPINE_OIDC_CLIENT_SECRET_FILE", "PORCUPINE_OIDC_BINDINGS_FILE"} {
		t.Setenv(name, "")
	}
	if config, err := oidcConfiguration(); err != nil || config != nil {
		t.Fatal("absent OIDC should remain disabled")
	}
	t.Setenv("PORCUPINE_OIDC_CLIENT_ID", "client")
	if _, err := oidcConfiguration(); err == nil {
		t.Fatal("partial OIDC configuration accepted")
	}
	dir := t.TempDir()
	secret := filepath.Join(dir, "secret")
	bindings := filepath.Join(dir, "bindings.json")
	if err := os.WriteFile(secret, []byte("test-upstream-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bindings, []byte(`[{"subject":"subject","permissions":["events:read"],"projects":["a"]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example")
	t.Setenv("PORCUPINE_OIDC_ISSUER", "https://login.example")
	t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET_FILE", secret)
	t.Setenv("PORCUPINE_OIDC_BINDINGS_FILE", bindings)
	config, err := oidcConfiguration()
	if err != nil || config.ClientSecret != "test-upstream-secret" || len(config.Bindings) != 1 {
		t.Fatalf("valid operator files failed: %v", err)
	}
	if err := os.WriteFile(bindings, []byte(`[{"subject":"subject","permissions":["events:read"],"projects":["a"],"ambient_group":"admins"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := oidcConfiguration(); err == nil {
		t.Fatal("unknown OIDC privilege mapping accepted")
	}
}
