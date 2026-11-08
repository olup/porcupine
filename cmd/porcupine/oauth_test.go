package main

import "testing"

func TestOAuthOperatorConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_OAUTH_ISSUER", "PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS"} {
		t.Setenv(name, "")
	}
	config, err := oauthConfiguration()
	if err != nil || config != nil {
		t.Fatal("legacy/offline configuration changed")
	}
	t.Setenv("PORCUPINE_OAUTH_ALLOW_LEGACY_KEYS", "false")
	if _, err := oauthConfiguration(); err == nil {
		t.Fatal("ignored OAuth mode without pinned issuer")
	}
	t.Setenv("PORCUPINE_OAUTH_ISSUER", "https://analytics.example.test")
	config, err = oauthConfiguration()
	if err != nil || config.AllowLegacyKeys {
		t.Fatalf("OAuth must reject legacy keys by default: %+v %v", config, err)
	}
	t.Setenv("PORCUPINE_OAUTH_TOKEN_LIFETIME_SECONDS", "3601")
	if _, err := oauthConfiguration(); err == nil {
		t.Fatal("accepted excessive access token lifetime")
	}
}
