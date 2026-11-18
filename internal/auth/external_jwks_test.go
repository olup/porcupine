package auth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

// jwksEntry renders one public key as the JSON object a provider publishes,
// then applies provider-specific changes such as removing alg.
func jwksEntry(t *testing.T, key jose.JSONWebKey, change func(map[string]any)) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(key)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if change != nil {
		change(fields)
	}
	return fields
}

func jwksServer(t *testing.T, keys func() []any) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		set := keys()
		if set == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": set})
	}))
	t.Cleanup(server.Close)
	return server
}

func signedExternalToken(t *testing.T, method jwt.SigningMethod, key crypto.Signer, kid string, config ExternalConfig, now time.Time) string {
	t.Helper()
	token := jwt.NewWithClaims(method, externalSecurityClaims(config, now))
	token.Header["kid"] = kid
	token.Header["typ"] = "at+jwt"
	raw, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestExternalJWKSSkipsKeysThatCannotVerifyTokens(t *testing.T) {
	t.Parallel()
	signing, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encryption, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	x25519 := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	server := jwksServer(t, func() []any {
		return []any{
			// Keycloak publishes its realm encryption key next to the signing key.
			jwksEntry(t, jose.JSONWebKey{Key: &encryption.PublicKey, KeyID: "enc", Algorithm: "RSA-OAEP", Use: "enc"}, nil),
			// Key types and curves Porcupine cannot verify with are not a reason to distrust the set.
			map[string]any{"kty": "OKP", "crv": "X25519", "kid": "agreement", "x": x25519},
			map[string]any{"kty": "EC", "crv": "secp256k1", "kid": "bitcoin", "x": x25519, "y": x25519},
			map[string]any{"kty": "unknown", "kid": "future"},
			jwksEntry(t, jose.JSONWebKey{Key: &signing.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, nil),
		}
	})
	config := externalSecurityConfig(server.URL)
	config.Algorithms = []string{"RS256", "ES256", "EdDSA"}
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1800000000, 0)
	if _, err := verifier.Verify(context.Background(), signedExternalToken(t, jwt.SigningMethodRS256, signing, "sig", config, now), config.APIAudience, now); err != nil {
		t.Fatalf("signing key rejected next to an encryption key: %v", err)
	}
	if _, err := verifier.Verify(context.Background(), signedExternalToken(t, jwt.SigningMethodRS256, encryption, "enc", config, now), config.APIAudience, now); err == nil {
		t.Fatal("encryption key accepted for token signatures")
	}
}

func TestExternalJWKSRejectsAmbiguousSigningKeys(t *testing.T) {
	t.Parallel()
	one, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	two, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		keys []any
		ok   bool
	}{
		{"duplicate signing kid", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "same", Algorithm: "RS256", Use: "sig"}, nil),
			jwksEntry(t, jose.JSONWebKey{Key: &two.PublicKey, KeyID: "same", Algorithm: "RS256", Use: "sig"}, nil),
		}, false},
		{"encryption key shares a signing kid", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "same", Algorithm: "RS256", Use: "sig"}, nil),
			jwksEntry(t, jose.JSONWebKey{Key: &two.PublicKey, KeyID: "same", Algorithm: "RSA-OAEP", Use: "enc"}, nil),
		}, true},
		{"published private key", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, nil),
			jwksEntry(t, jose.JSONWebKey{Key: two, KeyID: "enc", Algorithm: "RSA-OAEP", Use: "enc"}, nil),
		}, false},
		{"signing key with encryption operations", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, func(f map[string]any) { f["key_ops"] = []string{"encrypt"} }),
		}, false},
		{"encryption operations without use", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, nil),
			jwksEntry(t, jose.JSONWebKey{Key: &two.PublicKey, KeyID: "wrap", Algorithm: "RSA-OAEP"}, func(f map[string]any) { f["key_ops"] = []string{"wrapKey"} }),
		}, true},
		{"undersized usable key", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, func(f map[string]any) { f["n"] = "AQAB" }),
		}, false},
		{"non-object entry", []any{
			jwksEntry(t, jose.JSONWebKey{Key: &one.PublicKey, KeyID: "sig", Algorithm: "RS256", Use: "sig"}, nil),
			"key",
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := jwksServer(t, func() []any { return tc.keys })
			config := externalSecurityConfig(server.URL)
			config.Algorithms = []string{"RS256"}
			verifier, err := NewExternalVerifier(config)
			if err != nil {
				t.Fatal(err)
			}
			keys, err := verifier.fetchKeys(context.Background())
			if tc.ok && (err != nil || len(keys) != 1) || !tc.ok && err == nil {
				t.Fatalf("unexpected key set acceptance: %d %v", len(keys), err)
			}
		})
	}
}

func TestExternalJWKSInfersMissingAlgorithmOnlyWhenUnambiguous(t *testing.T) {
	t.Parallel()
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	ecKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, edKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		algorithms []string
		method     jwt.SigningMethod
		signer     crypto.Signer
		ok         bool
	}{
		// Entra ID publishes RSA signing keys without alg.
		{"RSA with one RSA algorithm", []string{"RS256", "ES256"}, jwt.SigningMethodRS256, rsaKey, true},
		{"RSA with two RSA algorithms", []string{"RS256", "RS384"}, jwt.SigningMethodRS256, rsaKey, false},
		{"RSA without an RSA algorithm", []string{"ES256"}, jwt.SigningMethodRS256, rsaKey, false},
		{"P-384 curve", []string{"RS256", "ES384"}, jwt.SigningMethodES384, ecKey, true},
		{"P-384 curve without ES384", []string{"ES256", "ES512"}, jwt.SigningMethodES384, ecKey, false},
		{"Ed25519", []string{"RS256", "EdDSA"}, jwt.SigningMethodEdDSA, edKey, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			server := jwksServer(t, func() []any {
				return []any{jwksEntry(t, jose.JSONWebKey{Key: tc.signer.Public(), KeyID: "no-alg", Use: "sig"}, func(f map[string]any) { delete(f, "alg") })}
			})
			config := externalSecurityConfig(server.URL)
			config.Algorithms = tc.algorithms
			verifier, err := NewExternalVerifier(config)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Unix(1800000000, 0)
			raw := signedExternalToken(t, tc.method, tc.signer, "no-alg", config, now)
			_, err = verifier.Verify(context.Background(), raw, config.APIAudience, now)
			if tc.ok && err != nil {
				t.Fatalf("key without alg rejected: %v", err)
			}
			if !tc.ok && (err == nil || errors.Is(err, ErrExternalKeysUnavailable)) {
				t.Fatalf("ambiguous key without alg was not skipped: %v", err)
			}
		})
	}
}

func TestExternalJWKSStaleGraceCoversOnlyFetchFailures(t *testing.T) {
	t.Parallel()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	broken, kid := false, "old"
	server := jwksServer(t, func() []any {
		mu.Lock()
		defer mu.Unlock()
		if broken {
			return nil
		}
		return []any{jwksEntry(t, jose.JSONWebKey{Key: private.Public(), KeyID: kid, Algorithm: "EdDSA", Use: "sig"}, nil)}
	})
	set := func(down bool, id string) {
		mu.Lock()
		broken, kid = down, id
		mu.Unlock()
	}
	config := externalSecurityConfig(server.URL)
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1800000000, 0)
	verify := func(id string, now time.Time) error {
		_, err := verifier.Verify(context.Background(), signedExternalToken(t, jwt.SigningMethodEdDSA, private, id, config, now), config.APIAudience, now)
		return err
	}
	if err := verify("old", start); err != nil {
		t.Fatal(err)
	}
	set(true, "old")
	expired := start.Add(6 * time.Minute)
	if err := verify("old", expired); err != nil {
		t.Fatalf("cached key not served during the stale grace: %v", err)
	}
	if verifier.SigningKeysAvailable(expired) {
		t.Fatal("stale grace not reported as degraded")
	}
	if err := verify("old", expired.Add(30*time.Minute)); err != nil {
		t.Fatalf("cached key not served later in the stale grace: %v", err)
	}
	if err := verify("unknown", expired.Add(31*time.Minute)); !errors.Is(err, ErrExternalKeysUnavailable) {
		t.Fatalf("unknown key during an outage was not unavailable: %v", err)
	}
	beyond := start.Add(5*time.Minute + externalKeyStaleGrace + time.Second)
	if err := verify("old", beyond); !errors.Is(err, ErrExternalKeysUnavailable) {
		t.Fatalf("stale key accepted after the grace: %v", err)
	}

	// A successful fetch that drops the key revokes it at once, even inside a grace window.
	rotated, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	verifier = rotated
	set(false, "old")
	if err := verify("old", start); err != nil {
		t.Fatal(err)
	}
	set(true, "old")
	if err := verify("old", expired); err != nil {
		t.Fatalf("cached key not served during the stale grace: %v", err)
	}
	set(false, "new")
	recovered := expired.Add(11 * time.Second)
	if err := verify("old", recovered); err == nil || errors.Is(err, ErrExternalKeysUnavailable) {
		t.Fatalf("key removed by a successful fetch still accepted: %v", err)
	}
	if err := verify("new", recovered); err != nil {
		t.Fatalf("rotated key rejected: %v", err)
	}
}
