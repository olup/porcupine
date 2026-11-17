package porcupine

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestOAuthConfigValidationDoesNotContactProvider(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected discovery", 500)
	}))
	defer provider.Close()
	config := OAuthConfig{Issuer: "https://analytics.example.test", OIDC: &OIDCConfig{Issuer: provider.URL, ClientID: "client", ClientSecret: "secret"}}
	if err := ValidateOAuthConfig(config); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatal("offline validation contacted identity provider")
	}
	if _, err := normalizeOAuthConfig(config); err == nil {
		t.Fatal("runtime accepted failed provider discovery")
	}
	if requests.Load() != 1 {
		t.Fatalf("runtime discovery requests = %d", requests.Load())
	}
}

func TestOAuthConfigValidationSharesRuntimeRules(t *testing.T) {
	t.Parallel()
	for _, cfg := range []OAuthConfig{
		{Issuer: "http://analytics.example.test"},
		{Issuer: "https://analytics.example.test", AccessTokenLifetimeSeconds: 30},
		{Issuer: "https://analytics.example.test", Email: &EmailConfig{}},
		{Issuer: "https://analytics.example.test", OIDC: &OIDCConfig{Issuer: "https://identity.example.test", ClientID: "client"}},
	} {
		if err := ValidateOAuthConfig(cfg); err == nil {
			t.Fatal("offline validation accepted invalid settings")
		}
		if _, err := normalizeOAuthConfig(cfg); err == nil {
			t.Fatal("runtime accepted invalid settings")
		}
	}
}
