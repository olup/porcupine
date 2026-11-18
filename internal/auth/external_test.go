package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
)

func TestExternalAccessTokenBoundaries(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var broken atomic.Bool
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() {
			w.WriteHeader(503)
			return
		}
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: "one", Algorithm: "RS256", Use: "sig"}}})
	}))
	defer jwks.Close()
	clock := time.Unix(1800000000, 0)
	config := ExternalConfig{Issuer: jwks.URL + "/realm", JWKSURL: jwks.URL + "/keys", APIAudience: "https://data.example/api/v1", MCPAudience: "https://data.example/mcp"}
	verifier, err := NewExternalVerifier(config)
	if err != nil {
		t.Fatal(err)
	}
	claims := func() jwt.MapClaims {
		return jwt.MapClaims{"iss": config.Issuer, "sub": "person1", "aud": config.APIAudience, "iat": clock.Unix(), "exp": clock.Add(5 * time.Minute).Unix(), "scope": "management:read events:read", "porcupine": map[string]any{"tenant_id": "a", "project_scope": "selected", "projects": []string{"p"}}}
	}
	sign := func(c jwt.MapClaims, typ string) string {
		t.Helper()
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
		token.Header["kid"] = "one"
		token.Header["typ"] = typ
		raw, e := token.SignedString(key)
		if e != nil {
			t.Fatal(e)
		}
		return raw
	}
	raw := sign(claims(), "at+jwt")
	grant, err := verifier.Verify(context.Background(), raw, config.APIAudience, clock)
	if err != nil {
		t.Fatal(err)
	}
	if grant.Tenant() != "a" || grant.Operator() || !grant.Valid(clock) || grant.Valid(clock.Add(5*time.Minute)) {
		t.Fatalf("unexpected grant: %+v", grant)
	}
	projects := grant.Projects()
	projects[0] = "evil"
	if grant.Projects()[0] != "p" {
		t.Fatal("mutable grant")
	}
	actor := grant.Principal()
	if actor.CanProvisionCredentials() || actor.Admin() {
		t.Fatal("project token provisions permanent credentials")
	}
	cases := []struct {
		name   string
		change func(jwt.MapClaims)
		typ    string
		aud    string
	}{
		{"wrong issuer", func(c jwt.MapClaims) { c["iss"] = "https://evil.example" }, "at+jwt", config.APIAudience},
		{"MCP audience", func(c jwt.MapClaims) {}, "at+jwt", config.MCPAudience},
		{"ID token", func(c jwt.MapClaims) {}, "JWT", config.APIAudience},
		{"dual audience", func(c jwt.MapClaims) { c["aud"] = []string{config.APIAudience, config.MCPAudience} }, "at+jwt", config.APIAudience},
		{"expired", func(c jwt.MapClaims) { c["exp"] = clock.Unix() }, "at+jwt", config.APIAudience},
		{"no iat", func(c jwt.MapClaims) { delete(c, "iat") }, "at+jwt", config.APIAudience},
		{"fractional expiry", func(c jwt.MapClaims) { c["exp"] = float64(clock.Unix()) + 0.5 }, "at+jwt", config.APIAudience},
		{"long lifetime", func(c jwt.MapClaims) { c["exp"] = clock.Add(time.Hour).Unix() }, "at+jwt", config.APIAudience},
		{"empty validity window", func(c jwt.MapClaims) {
			c["exp"] = clock.Add(5 * time.Second).Unix()
			c["nbf"] = clock.Add(6 * time.Second).Unix()
		}, "at+jwt", config.APIAudience},
		{"sender constrained token", func(c jwt.MapClaims) { c["cnf"] = map[string]string{"jkt": "unsupported"} }, "at+jwt", config.APIAudience},
		{"scope control whitespace", func(c jwt.MapClaims) { c["scope"] = "events:read\nmanagement:read" }, "at+jwt", config.APIAudience},
		{"unknown scope", func(c jwt.MapClaims) { c["scope"] = "events:read super:admin" }, "at+jwt", config.APIAudience},
		{"unknown authorization claim", func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["superuser"] = true }, "at+jwt", config.APIAudience},
		{"tenant escalation", func(c jwt.MapClaims) { c["scope"] = "instance:admin" }, "at+jwt", config.APIAudience},
		{"wildcard project", func(c jwt.MapClaims) { c["porcupine"].(map[string]any)["projects"] = []string{"*"} }, "at+jwt", config.APIAudience},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := claims()
			tc.change(c)
			if _, err := verifier.Verify(context.Background(), sign(c, tc.typ), tc.aud, clock); err == nil {
				t.Fatal("invalid token accepted")
			}
		})
	}
	broken.Store(true)
	unknown := jwt.NewWithClaims(jwt.SigningMethodRS256, claims())
	unknown.Header["kid"] = "rotated"
	unknown.Header["typ"] = "at+jwt"
	unknownRaw, err := unknown.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(context.Background(), unknownRaw, config.APIAudience, clock.Add(time.Minute)); err != ErrExternalKeysUnavailable {
		t.Fatalf("missing rotated key outage classified as invalid token: %v", err)
	}
	if _, err := verifier.Verify(context.Background(), raw, config.APIAudience, clock.Add(time.Minute)); err != nil {
		t.Fatalf("cached known key unavailable: %v", err)
	}
	if verifier.SigningKeysAvailable(clock.Add(time.Minute)) {
		t.Fatal("failed refresh not reflected in component health")
	}
	afterGrace := clock.Add(5*time.Minute + externalKeyStaleGrace + time.Second)
	fresh := claims()
	fresh["iat"] = afterGrace.Unix()
	fresh["exp"] = afterGrace.Add(5 * time.Minute).Unix()
	if _, err := verifier.Verify(context.Background(), sign(fresh, "at+jwt"), config.APIAudience, afterGrace); err != ErrExternalKeysUnavailable {
		t.Fatalf("stale key accepted after the grace: %v", err)
	}
}

func TestExternalKeyRotation(t *testing.T) {
	t.Parallel()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var kid atomic.Value
	kid.Store("old")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: kid.Load().(string), Algorithm: "RS256", Use: "sig"}}})
	}))
	defer server.Close()
	v, err := NewExternalVerifier(ExternalConfig{Issuer: server.URL, JWKSURL: server.URL, APIAudience: "https://data.test/api/v1", MCPAudience: "https://data.test/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Unix(1800000000, 0)
	token := func(id string, now time.Time) string {
		c := jwt.MapClaims{"iss": server.URL, "sub": "operator", "aud": "https://data.test/api/v1", "iat": now.Unix(), "exp": now.Add(time.Minute).Unix(), "scope": "instance:admin", "porcupine": map[string]bool{"operator": true}}
		j := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
		j.Header["typ"] = "at+jwt"
		j.Header["kid"] = id
		s, e := j.SignedString(key)
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	if _, err := v.Verify(context.Background(), token("old", start), v.config.APIAudience, start); err != nil {
		t.Fatal(err)
	}
	kid.Store("new")
	later := start.Add(11 * time.Second)
	if _, err := v.Verify(context.Background(), token("new", later), v.config.APIAudience, later); err != nil {
		t.Fatalf("new key not refreshed: %v", err)
	}
	if _, err := v.Verify(context.Background(), token("old", later), v.config.APIAudience, later); err == nil {
		t.Fatal("removed key accepted after refresh")
	}
}

func TestExternalStrictJSON(t *testing.T) {
	t.Parallel()
	for _, s := range []string{`{"a":1,"a":2}`, `{"a":{"b":1,"b":2}}`, `{} {}`, `[1,`} {
		if uniqueJSON([]byte(s)) == nil {
			t.Fatalf("invalid JSON accepted: %s", s)
		}
	}
	for _, s := range []string{`{}`, `{"a":[1,{"b":true}]}`} {
		if err := uniqueJSON([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	c := ExternalConfig{Issuer: "https://identity.test/realm", JWKSURL: "https://identity.test/keys", APIAudience: "https://data.test/api/v1", MCPAudience: "https://data.test/mcp"}
	for _, alg := range []string{"none", "HS256"} {
		c.Algorithms = []string{alg}
		if _, err := NewExternalVerifier(c); err == nil {
			t.Fatal("symmetric algorithm accepted")
		}
	}
	c.Algorithms = nil
	c.JWKSURL = "http://identity.test/keys"
	if _, err := NewExternalVerifier(c); err == nil {
		t.Fatal("insecure URL accepted")
	}
	request := httptest.NewRequest("GET", "/", nil)
	request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 16000))
	if _, err := BearerCredential(request); err != nil {
		t.Fatal(err)
	}
}
