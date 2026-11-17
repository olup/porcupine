package main

import (
	"github.com/olup/porcupine/internal/auth"
	"strings"
	"testing"
)

func TestExternalAuthConfiguration(t *testing.T) {
	t.Parallel()
	settings, err := readSettings(strings.NewReader(`{"auth":{"mode":"external","public_url":"https://data.test","external":{"issuer":"https://identity.test/realm","jwks_url":"https://identity.test/keys","algorithms":["RS256"],"clock_skew":"10s","max_token_lifetime":"5m"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	config, err := settings.authConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	normalized, err := auth.NormalizeExternalConfig(config.External)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.APIAudience != "https://data.test/api/v1" || normalized.MCPAudience != "https://data.test/mcp" || normalized.MaxTokenLifetime.String() != "5m0s" {
		t.Fatal("external defaults or durations changed", normalized)
	}
	if builtin, err := settings.oauthConfiguration(); err != nil || builtin != nil {
		t.Fatal("external config initialized builtin OAuth", err)
	}
	for _, field := range []string{"PORCUPINE_BOOTSTRAP_TOKEN", "PORCUPINE_OAUTH_ISSUER", "PORCUPINE_ADMIN_EMAIL", "PORCUPINE_SMTP_HOST", "PORCUPINE_OIDC_ISSUER"} {
		settings[field] = "forbidden"
		if _, err := settings.oauthConfiguration(); err == nil {
			t.Fatal("builtin setting accepted", field)
		}
		delete(settings, field)
	}
	settings["PORCUPINE_AUTH_MODE"] = "builtin"
	if _, err := settings.authConfiguration(); err == nil {
		t.Fatal("external configuration silently ignored in builtin mode")
	}
	settings["PORCUPINE_AUTH_MODE"] = "other"
	if _, err := settings.authConfiguration(); err == nil {
		t.Fatal("unknown auth mode accepted")
	}
}
