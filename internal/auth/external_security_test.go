package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

func externalSecurityConfig(address string) ExternalConfig {
	return ExternalConfig{Issuer: address + "/realm", JWKSURL: address + "/keys", APIAudience: "https://data.test/api/v1", MCPAudience: "https://data.test/mcp", Algorithms: []string{"EdDSA"}, TokenTypes: []string{"at+jwt"}}
}

func externalSecurityClaims(c ExternalConfig, now time.Time) jwt.MapClaims {
	return jwt.MapClaims{"iss": c.Issuer, "sub": "person", "aud": c.APIAudience, "iat": now.Unix(), "exp": now.Add(5 * time.Minute).Unix(), "scope": "events:read", "porcupine": map[string]any{"tenant_id": "tenant", "project_scope": "selected", "projects": []string{"project"}}}
}

func externalSecurityToken(t *testing.T, key ed25519.PrivateKey, kid string, claims jwt.MapClaims, changeHeader func(map[string]any)) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims)
	token.Header["kid"] = kid
	token.Header["typ"] = "at+jwt"
	if changeHeader != nil {
		changeHeader(token.Header)
	}
	raw, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestExternalExactAuthorizationFields(t *testing.T) {
	t.Parallel()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}}})
	}))
	defer server.Close()
	config := externalSecurityConfig(server.URL)
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1800000000, 0)
	if _, err := verifier.Verify(context.Background(), externalSecurityToken(t, private, "one", externalSecurityClaims(config, now), nil), config.APIAudience, now); err != nil {
		t.Fatalf("valid fixture rejected: %v", err)
	}
	cases := []struct {
		name   string
		header func(map[string]any)
		claims func(jwt.MapClaims)
	}{
		{name: "ALG", header: func(h map[string]any) { h["ALG"] = h["alg"]; delete(h, "alg") }},
		{name: "Typ", header: func(h map[string]any) { h["Typ"] = h["typ"]; delete(h, "typ") }},
		{name: "KID", header: func(h map[string]any) { h["KID"] = h["kid"]; delete(h, "kid") }},
		{name: "mixed JOSE casing", header: func(h map[string]any) { h["ALG"] = "EdDSA" }},
		{name: "null alg", header: func(h map[string]any) { h["alg"] = nil }},
		{name: "null typ", header: func(h map[string]any) { h["typ"] = nil }},
		{name: "null kid", header: func(h map[string]any) { h["kid"] = nil }},
		{name: "namespace casing", claims: func(c jwt.MapClaims) { c["Porcupine"] = c["porcupine"]; delete(c, "porcupine") }},
		{name: "Tenant_id", claims: func(c jwt.MapClaims) {
			a := c["porcupine"].(map[string]any)
			a["Tenant_id"] = a["tenant_id"]
			delete(a, "tenant_id")
		}},
		{name: "Project_Scope", claims: func(c jwt.MapClaims) {
			a := c["porcupine"].(map[string]any)
			a["Project_Scope"] = a["project_scope"]
			delete(a, "project_scope")
		}},
		{name: "Projects", claims: func(c jwt.MapClaims) {
			a := c["porcupine"].(map[string]any)
			a["Projects"] = a["projects"]
			delete(a, "projects")
		}},
		{name: "Operator", claims: func(c jwt.MapClaims) {
			c["scope"] = "instance:admin"
			c["porcupine"] = map[string]any{"Operator": true}
		}},
		{name: "null namespace", claims: func(c jwt.MapClaims) { c["porcupine"] = nil }},
		{name: "null tenant", claims: func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["tenant_id"] = nil }},
		{name: "null project scope", claims: func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["project_scope"] = nil }},
		{name: "null operator", claims: func(c jwt.MapClaims) { c["scope"] = "instance:admin"; c["porcupine"] = map[string]any{"operator": nil} }},
		{name: "false operator", claims: func(c jwt.MapClaims) {
			c["scope"] = "instance:admin"
			c["porcupine"] = map[string]any{"operator": false}
		}},
		{name: "mixed false operator", claims: func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["operator"] = false }},
		{name: "mixed true operator", claims: func(c jwt.MapClaims) {
			c["scope"] = "instance:admin"
			c["porcupine"].(map[string]any)["operator"] = true
		}},
		{name: "operator null tenant", claims: func(c jwt.MapClaims) {
			c["scope"] = "instance:admin"
			c["porcupine"] = map[string]any{"operator": true, "tenant_id": nil}
		}},
		{name: "operator empty projects", claims: func(c jwt.MapClaims) {
			c["scope"] = "instance:admin"
			c["porcupine"] = map[string]any{"operator": true, "projects": []string{}}
		}},
		{name: "null projects", claims: func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["projects"] = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := externalSecurityClaims(config, now)
			if tc.claims != nil {
				tc.claims(claims)
			}
			if _, err := verifier.Verify(context.Background(), externalSecurityToken(t, private, "one", claims, tc.header), config.APIAudience, now); err == nil {
				t.Fatal("malformed authority accepted")
			}
		})
	}
}

func TestExternalJWKUseAndCurveBoundaries(t *testing.T) {
	t.Parallel()
	public, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ec, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		key  jose.JSONWebKey
		ops  any
		ok   bool
	}{
		{"verify", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, []string{"verify"}, true},
		{"sign", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, []string{"sign"}, false},
		{"encrypt", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, []string{"encrypt"}, false},
		{"verify and sign", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, []string{"verify", "sign"}, false},
		{"empty operations", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, []string{}, false},
		{"null operations", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, nil, false},
		{"string operations", jose.JSONWebKey{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}, "verify", false},
		{"ES256 with P256", jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "one", Algorithm: "ES256", Use: "sig"}, []string{"verify"}, true},
		{"ES384 with P256", jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "one", Algorithm: "ES384", Use: "sig"}, []string{"verify"}, false},
		{"ES512 with P256", jose.JSONWebKey{Key: &ec.PublicKey, KeyID: "one", Algorithm: "ES512", Use: "sig"}, []string{"verify"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(tc.key)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			json.Unmarshal(encoded, &fields)
			fields["key_ops"] = tc.ops
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				json.NewEncoder(w).Encode(map[string]any{"keys": []any{fields}})
			}))
			defer server.Close()
			config := externalSecurityConfig(server.URL)
			config.Algorithms = []string{"EdDSA", "ES256", "ES384", "ES512"}
			verifier, err := NewExternalVerifier(config)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := verifier.fetchKeys(context.Background())
			if tc.ok && (err != nil || len(keys) != 1) || !tc.ok && err == nil {
				t.Fatalf("unexpected key acceptance: %d %v", len(keys), err)
			}
		})
	}
}

func TestExternalVerifierConfigurationIsImmutable(t *testing.T) {
	t.Parallel()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: public, KeyID: "one", Algorithm: "EdDSA", Use: "sig"}}})
	}))
	defer server.Close()
	config := externalSecurityConfig(server.URL)
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	config.Algorithms[0], config.TokenTypes[0] = "none", "JWT"
	copy := verifier.Config()
	copy.Algorithms[0], copy.TokenTypes[0] = "none", "JWT"
	now := time.Unix(1800000000, 0)
	claims := externalSecurityClaims(config, now)
	if _, err := verifier.Verify(context.Background(), externalSecurityToken(t, private, "one", claims, nil), config.APIAudience, now); err != nil {
		t.Fatalf("caller mutated verifier configuration: %v", err)
	}
	if _, err := verifier.Verify(context.Background(), externalSecurityToken(t, private, "one", claims, func(h map[string]any) { h["typ"] = "JWT" }), config.APIAudience, now); err == nil {
		t.Fatal("caller widened accepted token types")
	}
}

func TestExternalJWKSRefreshDoesNotBlockCachedKeysOrInheritCancellation(t *testing.T) {
	t.Parallel()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := requests.Add(1)
		if request > 1 {
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				t.Error("shared refresh inherited a canceled caller")
				return
			}
		}
		keys := []jose.JSONWebKey{{Key: public, KeyID: "cached", Algorithm: "EdDSA", Use: "sig"}}
		if request > 1 {
			keys = append(keys, jose.JSONWebKey{Key: public, KeyID: "rotated", Algorithm: "EdDSA", Use: "sig"})
		}
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: keys})
	}))
	defer server.Close()
	config := externalSecurityConfig(server.URL)
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1800000000, 0)
	claims := externalSecurityClaims(config, now)
	cached := externalSecurityToken(t, private, "cached", claims, nil)
	if _, err := verifier.Verify(context.Background(), cached, config.APIAudience, now); err != nil {
		t.Fatal(err)
	}
	rotated := externalSecurityToken(t, private, "rotated", claims, nil)
	wait, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := verifier.Verify(wait, rotated, config.APIAudience, now.Add(11*time.Second))
		first <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		cancel()
		close(release)
		t.Fatal("unknown key did not trigger refresh")
	}
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		close(release)
		t.Fatalf("canceled caller did not leave shared refresh: %v", err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if _, err := verifier.Verify(ctx, cached, config.APIAudience, now.Add(11*time.Second)); err != nil {
		close(release)
		t.Fatalf("valid cached key blocked behind refresh: %v", err)
	}
	second := make(chan error, 1)
	go func() {
		_, err := verifier.Verify(ctx, rotated, config.APIAudience, now.Add(11*time.Second))
		second <- err
	}()
	close(release)
	if err := <-second; err != nil {
		t.Fatalf("another caller could not reuse shared refresh: %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("refresh was duplicated: %d requests", requests.Load())
	}
}
