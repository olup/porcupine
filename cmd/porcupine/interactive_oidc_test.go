package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOIDCOperatorConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_OAUTH_ISSUER", "PORCUPINE_OIDC_ISSUER", "PORCUPINE_OIDC_CLIENT_ID", "PORCUPINE_OIDC_CLIENT_SECRET", "PORCUPINE_OIDC_CLIENT_SECRET_FILE", "PORCUPINE_OIDC_BINDINGS_FILE"} {
		t.Setenv(name, "")
	}
	if config, err := oidcConfiguration(); err != nil || config != nil {
		t.Fatal("absent OIDC should remain disabled")
	}
	t.Setenv("PORCUPINE_OIDC_CLIENT_ID", "client")
	if _, err := oidcConfiguration(); err == nil {
		t.Fatal("partial OIDC configuration accepted")
	}
	secret := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secret, []byte("test-upstream-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example")
	t.Setenv("PORCUPINE_OIDC_ISSUER", "https://accounts.google.com")
	t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET_FILE", secret)
	config, err := oidcConfiguration()
	if err != nil || config.ClientSecret != "test-upstream-secret" || config.Issuer != "https://accounts.google.com" {
		t.Fatalf("valid operator configuration failed: %v", err)
	}
	t.Setenv("PORCUPINE_OIDC_BINDINGS_FILE", "/not-read/retired-bindings.json")
	if _, err := oidcConfiguration(); err == nil {
		t.Fatal("retired static authority bindings accepted")
	}
	t.Setenv("PORCUPINE_OIDC_BINDINGS_FILE", "")
	t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET", "direct-upstream-secret")
	if _, err := oidcConfiguration(); err == nil {
		t.Fatal("ambiguous secret sources accepted")
	}
	t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET_FILE", "")
	config, err = oidcConfiguration()
	if err != nil || config.ClientSecret != "direct-upstream-secret" {
		t.Fatalf("direct secret configuration failed: %v", err)
	}
	for _, invalid := range []string{"secret\ninside", "secret\tinside"} {
		t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET", invalid)
		_, err := oidcConfiguration()
		if err == nil || strings.Contains(err.Error(), invalid) {
			t.Fatal("invalid secret accepted or reflected")
		}
	}
}

func TestOIDCAdministratorWithoutSMTP(t *testing.T) {
	for _, name := range []string{"PORCUPINE_SMTP_HOST", "PORCUPINE_SMTP_PORT", "PORCUPINE_SMTP_TLS", "PORCUPINE_SMTP_FROM", "PORCUPINE_SMTP_USERNAME", "PORCUPINE_SMTP_PASSWORD_FILE", "PORCUPINE_OIDC_BINDINGS_FILE", "PORCUPINE_OIDC_CLIENT_SECRET_FILE"} {
		t.Setenv(name, "")
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example.test")
	t.Setenv("PORCUPINE_OIDC_ISSUER", "https://accounts.google.com")
	t.Setenv("PORCUPINE_OIDC_CLIENT_ID", "configured-client")
	t.Setenv("PORCUPINE_OIDC_CLIENT_SECRET", "configured-secret")
	t.Setenv("PORCUPINE_ADMIN_EMAIL", "operator@example.test")
	config, err := oauthConfiguration()
	if err != nil || config.OIDC == nil || config.Email == nil || config.Email.AdminEmail != "operator@example.test" || config.Email.SendCode != nil || config.Email.SendInvitation != nil {
		t.Fatalf("OIDC-only administrator setup failed: %v", err)
	}
	t.Setenv("PORCUPINE_SMTP_PORT", "587")
	if _, err := oauthConfiguration(); err == nil {
		t.Fatal("partial SMTP configuration was silently disabled")
	}
}
