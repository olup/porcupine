package main

import "testing"

func TestOAuthOperatorConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_OAUTH_ISSUER", "PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP", "PORCUPINE_OIDC_ISSUER", "PORCUPINE_OIDC_CLIENT_ID", "PORCUPINE_OIDC_CLIENT_SECRET_FILE", "PORCUPINE_OIDC_BINDINGS_FILE"} {
		t.Setenv(name, "")
	}
	config, err := (settings{}).oauthConfiguration()
	if err != nil || config != nil {
		t.Fatalf("expected OAuth to be disabled with no settings: %+v %v", config, err)
	}
	t.Setenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "600")
	if _, err := (settings{}).oauthConfiguration(); err == nil {
		t.Fatal("accepted a token lifetime without an issuer")
	}
	const issuer = "https://analytics.example.test"
	t.Setenv("PORCUPINE_OAUTH_ISSUER", issuer)
	config, err = (settings{}).oauthConfiguration()
	if err != nil || config == nil || config.Issuer != issuer || config.AccessTokenLifetimeSeconds != 600 {
		t.Fatalf("expected configured OAuth issuer and token lifetime: %+v %v", config, err)
	}
	t.Setenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "3601")
	if _, err := (settings{}).oauthConfiguration(); err == nil {
		t.Fatal("accepted excessive access token lifetime")
	}
}

func TestRestoreIdentityBootstrapRequiresExplicitValidOAuthSetting(t *testing.T) {
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example.test")
	t.Setenv("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP", "true")
	config, err := (settings{}).oauthConfiguration()
	if err != nil || !config.AllowRestoredIdentityBootstrap {
		t.Fatalf("explicit restore identity bootstrap rejected: %v", err)
	}
	t.Setenv("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP", "maybe")
	if _, err := (settings{}).oauthConfiguration(); err == nil {
		t.Fatal("invalid identity bootstrap opt-in accepted")
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "")
	t.Setenv("PORCUPINE_RESTORE_IDENTITY_BOOTSTRAP", "true")
	if _, err := (settings{}).oauthConfiguration(); err == nil {
		t.Fatal("identity bootstrap setting ignored without OAuth")
	}
}
