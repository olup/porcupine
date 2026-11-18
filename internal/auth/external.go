package auth

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
)

// ExternalConfig describes one trusted access-token issuer. No token can select
// a provider, key URL, algorithm, audience, or authorization claim namespace.
type ExternalConfig struct {
	Issuer           string
	JWKSURL          string
	APIAudience      string
	MCPAudience      string
	Algorithms       []string
	TokenTypes       []string
	ClockSkew        time.Duration
	MaxTokenLifetime time.Duration
}

var ErrExternalKeysUnavailable = errors.New("external signing keys unavailable")

// ExternalGrant is verified authority, independent of local human accounts.
// Its fields are private so callers cannot manufacture authority from JSON.
type ExternalGrant struct {
	issuer, subject, tenant string
	permissions, projects   []string
	all, operator           bool
	expires                 time.Time
}

func (g *ExternalGrant) Tenant() string           { return g.tenant }
func (g *ExternalGrant) Issuer() string           { return g.issuer }
func (g *ExternalGrant) Expires() time.Time       { return g.expires }
func (g *ExternalGrant) AllProjects() bool        { return g.all }
func (g *ExternalGrant) Operator() bool           { return g.operator }
func (g *ExternalGrant) Projects() []string       { return slices.Clone(g.projects) }
func (g *ExternalGrant) Permissions() []string    { return slices.Clone(g.permissions) }
func (g *ExternalGrant) Valid(now time.Time) bool { return g != nil && now.Before(g.expires) }
func (g *ExternalGrant) Principal() Principal {
	h := sha256.Sum256([]byte(g.issuer + "\x00" + g.subject + "\x00" + g.tenant))
	p := Principal{ID: "external:" + hex.EncodeToString(h[:]), External: g, Interactive: true, Permissions: g.Permissions(), Projects: g.Projects()}
	if g.operator {
		p.Projects = []string{"*"}
	} else {
		p.TenantIDs = []string{g.tenant}
		if slices.Contains(g.permissions, TenantAdminScope) {
			p.TenantAdmins = []string{g.tenant}
		}
	}
	return p
}

type ExternalVerifier struct {
	config                  ExternalConfig
	client                  *http.Client
	mu                      sync.Mutex
	keys                    map[string]any
	validUntil, lastRefresh time.Time
	refreshing              chan struct{}
	lastRefreshFailed       bool
}

func NormalizeExternalConfig(c ExternalConfig) (ExternalConfig, error) {
	for _, raw := range []string{c.Issuer, c.JWKSURL} {
		if len(raw) > 4096 {
			return c, errors.New("external authentication URL exceeds 4096 bytes")
		}
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
			return c, errors.New("invalid external authentication URL")
		}
		ip := net.ParseIP(u.Hostname())
		loopback := u.Hostname() == "localhost" || ip != nil && ip.IsLoopback()
		if u.Scheme != "https" && !(u.Scheme == "http" && loopback) {
			return c, errors.New("external authentication URLs require HTTPS (HTTP is allowed on loopback only)")
		}
	}
	for _, audience := range []string{c.APIAudience, c.MCPAudience} {
		if audience == "" || len(audience) > 1024 || strings.ContainsAny(audience, " \t\r\n") {
			return c, errors.New("external audiences must be nonempty bounded identifiers without whitespace")
		}
	}
	if c.APIAudience == c.MCPAudience {
		return c, errors.New("external API and MCP audiences must differ")
	}
	if len(c.Algorithms) == 0 {
		c.Algorithms = []string{"RS256"}
	}
	for _, a := range c.Algorithms {
		if !slices.Contains([]string{"RS256", "RS384", "RS512", "ES256", "ES384", "ES512", "EdDSA"}, a) {
			return c, errors.New("external authentication requires explicitly allowed asymmetric algorithms")
		}
	}
	if len(c.TokenTypes) == 0 {
		c.TokenTypes = []string{"at+jwt"}
	}
	for _, typ := range c.TokenTypes {
		if typ == "" || len(typ) > 64 || strings.EqualFold(typ, "jwt") || strings.EqualFold(typ, "application/jwt") || strings.Contains(strings.ToLower(typ), "id_token") {
			return c, errors.New("external token types must identify access tokens explicitly")
		}
	}
	if c.ClockSkew == 0 {
		c.ClockSkew = 30 * time.Second
	}
	if c.ClockSkew < 0 || c.ClockSkew > time.Minute {
		return c, errors.New("external clock skew must be between zero and one minute")
	}
	if c.MaxTokenLifetime == 0 {
		c.MaxTokenLifetime = 15 * time.Minute
	}
	if c.MaxTokenLifetime < time.Minute || c.MaxTokenLifetime > time.Hour {
		return c, errors.New("external token lifetime must be one minute to one hour")
	}
	c.Algorithms = slices.Clone(c.Algorithms)
	c.TokenTypes = slices.Clone(c.TokenTypes)
	return c, nil
}
func NewExternalVerifier(c ExternalConfig) (*ExternalVerifier, error) {
	c, e := NormalizeExternalConfig(c)
	if e != nil {
		return nil, e
	}
	return &ExternalVerifier{config: c, client: &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (v *ExternalVerifier) Config() ExternalConfig {
	c := v.config
	c.Algorithms = slices.Clone(c.Algorithms)
	c.TokenTypes = slices.Clone(c.TokenTypes)
	return c
}

// SigningKeysAvailable reports cache health without fetching keys or exposing them.
func (v *ExternalVerifier) SigningKeysAvailable(now time.Time) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	return now.Before(v.validUntil) && len(v.keys) > 0 && !v.lastRefreshFailed
}

// Verify checks access-token provenance before interpreting authorization claims.
func (v *ExternalVerifier) Verify(ctx context.Context, raw, audience string, now time.Time) (*ExternalGrant, error) {
	if audience != v.config.APIAudience && audience != v.config.MCPAudience {
		return nil, errors.New("invalid resource audience")
	}
	parts := strings.Split(raw, ".")
	if len(raw) > 16<<10 || len(parts) != 3 {
		return nil, errors.New("invalid access token")
	}
	header, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return nil, errors.New("invalid access token header")
	}
	var h struct {
		Algorithm string `json:"alg"`
		Type      string `json:"typ"`
		KeyID     string `json:"kid"`
	}
	// RFC 9068 access tokens carry an explicit JOSE type (at+jwt by default)
	// and a kid. The header is limited to exactly alg, typ and kid, so a token
	// cannot carry jwk, jku, x5u, x5c, crit or other parameters that would
	// select keys or change processing.
	if exactFields(header, []string{"alg", "typ", "kid"}, []string{"alg", "typ", "kid"}) != nil || strictJSON(header, &h) != nil || !slices.Contains(v.config.Algorithms, h.Algorithm) || !slices.Contains(v.config.TokenTypes, h.Type) || h.KeyID == "" || len(h.KeyID) > 128 {
		return nil, errors.New("unsupported access token header")
	}
	payload, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return nil, errors.New("invalid access token claims")
	}
	var claims map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if uniqueJSON(payload) != nil || decoder.Decode(&claims) != nil || claims == nil {
		return nil, errors.New("invalid access token claims")
	}
	if _, bound := claims["cnf"]; bound {
		return nil, errors.New("sender-constrained access tokens require an unsupported proof mechanism")
	}
	for _, field := range []string{"iat", "exp", "nbf"} {
		value, exists := claims[field]
		if !exists && field == "nbf" {
			continue
		}
		number, ok := value.(json.Number)
		seconds, err := number.Int64()
		if !ok || err != nil || seconds <= 0 || seconds > 253402300799 {
			return nil, errors.New("integer access-token timestamps required")
		}
	}
	if notBefore, ok := claims["nbf"].(json.Number); ok {
		nbf, _ := notBefore.Int64()
		exp, _ := claims["exp"].(json.Number).Int64()
		if nbf >= exp {
			return nil, errors.New("access token validity window is empty")
		}
	}
	key, e := v.key(ctx, h.KeyID, h.Algorithm, now)
	if e != nil {
		return nil, e
	}
	// The header was checked above, so the compact JWS signs exactly the
	// decoded payload; the algorithm list is the token's own allowed alg.
	signed, e := jose.ParseSignedCompact(raw, []jose.SignatureAlgorithm{jose.SignatureAlgorithm(h.Algorithm)})
	if e != nil {
		return nil, errors.New("invalid access token")
	}
	verified, e := signed.Verify(key)
	if e != nil || !bytes.Equal(verified, payload) {
		return nil, errors.New("invalid access token")
	}
	if issuer, ok := claims["iss"].(string); !ok || issuer != v.config.Issuer {
		return nil, errors.New("invalid access token issuer")
	}
	// A token for both resources would erase the API/MCP boundary.
	var aud []string
	switch value := claims["aud"].(type) {
	case string:
		aud = []string{value}
	case []any:
		for _, item := range value {
			s, ok := item.(string)
			if !ok {
				return nil, errors.New("access token requires one exact resource audience")
			}
			aud = append(aud, s)
		}
	}
	if len(aud) != 1 || aud[0] != audience {
		return nil, errors.New("access token requires one exact resource audience")
	}
	exp := externalTime(claims["exp"])
	iat := externalTime(claims["iat"])
	if !exp.After(iat) || exp.Sub(iat) > v.config.MaxTokenLifetime || !now.Before(exp) || iat.After(now.Add(v.config.ClockSkew)) {
		return nil, errors.New("invalid access token lifetime")
	}
	if _, exists := claims["nbf"]; exists && externalTime(claims["nbf"]).After(now.Add(v.config.ClockSkew)) {
		return nil, errors.New("access token is not yet valid")
	}
	sub, ok := claims["sub"].(string)
	if !ok || sub == "" || len(sub) > 256 {
		return nil, errors.New("access token subject required")
	}
	scope, ok := claims["scope"].(string)
	if !ok || len(scope) > 2048 {
		return nil, errors.New("access token scopes required")
	}
	scopes := strings.Fields(scope)
	if len(scopes) == 0 || len(scopes) > 32 || strings.Join(scopes, " ") != scope {
		return nil, errors.New("invalid access token scopes")
	}
	for i, p := range scopes {
		if !slices.Contains(Permissions(), p) || slices.Contains(scopes[:i], p) {
			return nil, errors.New("unknown or duplicate access token scope")
		}
	}
	var authority struct {
		TenantID     string   `json:"tenant_id"`
		ProjectScope string   `json:"project_scope"`
		Projects     []string `json:"projects"`
		Operator     bool     `json:"operator"`
	}
	data, e := json.Marshal(claims["porcupine"])
	if e != nil || strictJSON(data, &authority) != nil {
		return nil, errors.New("invalid Porcupine authorization claims")
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return nil, errors.New("invalid Porcupine authorization claims")
	}
	if authority.Operator {
		if exactFields(data, []string{"operator"}, []string{"operator"}) != nil {
			return nil, errors.New("operator claims cannot mix tenant authority")
		}
	} else {
		required := []string{"tenant_id", "project_scope"}
		allowed := []string{"tenant_id", "project_scope"}
		if authority.ProjectScope == "selected" {
			required = append(required, "projects")
			allowed = append(allowed, "projects")
		}
		if exactFields(data, allowed, required) != nil {
			return nil, errors.New("invalid tenant authorization claims")
		}
		if authority.ProjectScope == "selected" && (len(fields["projects"]) == 0 || fields["projects"][0] != '[') {
			return nil, errors.New("project list required")
		}
	}
	g := &ExternalGrant{issuer: v.config.Issuer, subject: sub, tenant: authority.TenantID, permissions: scopes, projects: authority.Projects, all: authority.ProjectScope == "all", operator: authority.Operator, expires: exp}
	if g.operator {
		if g.tenant != "" || authority.ProjectScope != "" || len(g.projects) != 0 || !slices.Equal(scopes, []string{"instance:admin"}) {
			return nil, errors.New("operator claims cannot mix tenant authority")
		}
		return g, nil
	}
	if g.tenant == "" || len(g.tenant) > 128 || slices.Contains(scopes, "instance:admin") || (!g.all && authority.ProjectScope != "selected") || (g.all && len(g.projects) != 0) || (!g.all && (len(g.projects) == 0 || len(g.projects) > 64)) || (slices.Contains(scopes, TenantAdminScope) && !g.all) {
		return nil, errors.New("invalid tenant authorization claims")
	}
	for i, p := range g.projects {
		if p == "" || p == "*" || len(p) > 128 || slices.Contains(g.projects[:i], p) {
			return nil, errors.New("invalid project authorization claims")
		}
	}
	return g, nil
}

// externalKeyStaleGrace bounds how long the last successfully fetched key set
// keeps verifying tokens past its cache expiry while every refresh fails. It
// covers short provider outages only: a successful fetch replaces the set at
// once, so a key the provider removed stops working immediately.
const externalKeyStaleGrace = time.Hour

func (v *ExternalVerifier) key(ctx context.Context, kid, algorithm string, now time.Time) (any, error) {
	for {
		v.mu.Lock()
		k := v.keys[kid+"\x00"+algorithm]
		if k != nil && now.Before(v.validUntil) {
			v.mu.Unlock()
			return k, nil
		}
		// After a failed refresh, a key from the last good set stays usable
		// until the grace ends. It does not wait behind a pending refresh, so
		// a hanging provider adds little latency to cached keys.
		stale := k != nil && v.lastRefreshFailed && now.Before(v.validUntil.Add(externalKeyStaleGrace))
		if pending := v.refreshing; pending != nil {
			v.mu.Unlock()
			if stale {
				return k, nil
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending:
				continue
			}
		}
		if now.Sub(v.lastRefresh) < 10*time.Second {
			fresh := now.Before(v.validUntil)
			failed := v.lastRefreshFailed
			v.mu.Unlock()
			if stale {
				return k, nil
			}
			if fresh && !failed {
				return nil, errors.New("unknown access token signing key")
			}
			return nil, ErrExternalKeysUnavailable
		}
		v.lastRefresh = now
		started := make(chan struct{})
		v.refreshing = started
		v.mu.Unlock()
		// A canceled caller must not cancel a refresh shared by other requests.
		go v.refresh(now)
		// The caller that starts a refresh waits for its result, so a key
		// removed by a successful fetch is rejected rather than served stale.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-started:
		}
	}
}

func (v *ExternalVerifier) refresh(now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	keys, err := v.fetchKeys(ctx)
	v.mu.Lock()
	defer v.mu.Unlock()
	switch {
	case err == nil && v.lastRefreshFailed:
		slog.Info("Porcupine refreshed external signing keys after a failure")
	case err != nil && !v.lastRefreshFailed && len(v.keys) > 0:
		slog.Warn("Porcupine could not refresh external signing keys; cached keys remain usable until the stale grace ends", "stale_until", v.validUntil.Add(externalKeyStaleGrace).UTC())
	case err != nil && !v.lastRefreshFailed:
		slog.Warn("Porcupine could not fetch external signing keys")
	}
	v.lastRefreshFailed = err != nil
	if err == nil {
		v.keys = keys
		v.validUntil = now.Add(5 * time.Minute)
	}
	close(v.refreshing)
	v.refreshing = nil
}

func (v *ExternalVerifier) fetchKeys(ctx context.Context) (map[string]any, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, v.config.JWKSURL, nil)
	if e != nil {
		return nil, ErrExternalKeysUnavailable
	}
	response, e := v.client.Do(req)
	if e != nil {
		return nil, ErrExternalKeysUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, ErrExternalKeysUnavailable
	}
	data, e := io.ReadAll(io.LimitReader(response.Body, 256<<10+1))
	if e != nil || len(data) > 256<<10 {
		return nil, ErrExternalKeysUnavailable
	}
	var set struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if uniqueJSON(data) != nil || json.Unmarshal(data, &set) != nil || len(set.Keys) == 0 || len(set.Keys) > 32 {
		return nil, ErrExternalKeysUnavailable
	}
	// Providers publish keys that never verify access tokens, such as
	// Keycloak's encryption key or curves outside the supported algorithms.
	// Those are skipped. A malformation still invalidates the whole set when it
	// makes the document untrustworthy or a usable key ambiguous; each such
	// case below says why.
	keys := map[string]any{}
	seen := map[string]bool{}
	for _, entry := range set.Keys {
		var fields map[string]json.RawMessage
		if json.Unmarshal(entry, &fields) != nil || fields == nil {
			// A non-object entry means the document is not a JWK Set.
			return nil, ErrExternalKeysUnavailable
		}
		_, private := fields["d"]
		_, secret := fields["k"]
		if private || secret {
			// Published private or symmetric material means the provider
			// leaked a key, so no sibling key in that document is trusted.
			return nil, ErrExternalKeysUnavailable
		}
		// Exact member names: encoding/json would match struct fields
		// case-insensitively.
		member := map[string]string{}
		for _, name := range []string{"kty", "use", "alg", "crv", "kid"} {
			if raw, ok := fields[name]; ok {
				var value string
				if json.Unmarshal(raw, &value) != nil {
					// A parameter with the wrong JSON type is a broken document.
					return nil, ErrExternalKeysUnavailable
				}
				member[name] = value
			}
		}
		if use := member["use"]; use != "" && use != "sig" {
			continue
		}
		if operations, exists := fields["key_ops"]; exists {
			var ops []string
			if len(operations) == 0 || operations[0] != '[' || json.Unmarshal(operations, &ops) != nil {
				return nil, ErrExternalKeysUnavailable
			}
			if member["use"] == "" && !slices.Contains(ops, "verify") && !slices.Contains(ops, "sign") {
				continue // An encryption or key-wrapping key described only by key_ops.
			}
			// A signing key must allow verification only. Mixed or
			// contradictory declarations make its purpose ambiguous.
			if !slices.Equal(ops, []string{"verify"}) {
				return nil, ErrExternalKeysUnavailable
			}
		}
		candidates := externalKeyAlgorithms(member["kty"], member["crv"])
		algorithm := member["alg"]
		if algorithm != "" {
			if !slices.Contains(v.config.Algorithms, algorithm) {
				continue
			}
			// A key that names an allowed algorithm its material cannot
			// verify, such as ES384 on P-256, is a misconfigured signing key.
			if !slices.Contains(candidates, algorithm) {
				return nil, ErrExternalKeysUnavailable
			}
		} else {
			// Without alg, infer it only when exactly one configured
			// algorithm fits the key type and curve. Two RSA algorithms would
			// let one key verify under either, so such a key is skipped.
			var fits []string
			for _, candidate := range candidates {
				if slices.Contains(v.config.Algorithms, candidate) {
					fits = append(fits, candidate)
				}
			}
			if len(fits) != 1 {
				continue
			}
			algorithm = fits[0]
		}
		kid := member["kid"]
		if kid == "" || len(kid) > 128 {
			continue // Tokens must name a kid of at most 128 bytes, so this key is never selected.
		}
		if seen[kid] {
			// Two usable keys with one kid would make key selection ambiguous.
			return nil, ErrExternalKeysUnavailable
		}
		seen[kid] = true
		var k jose.JSONWebKey
		if k.UnmarshalJSON(entry) != nil || !k.IsPublic() || !k.Valid() {
			// A usable key with broken material is a provider fault to surface.
			return nil, ErrExternalKeysUnavailable
		}
		switch p := k.Key.(type) {
		case *rsa.PublicKey:
			if p.N.BitLen() < 2048 || p.N.BitLen() > 8192 || p.N.Bit(0) != 1 || p.E < 3 || p.E > 1<<31-1 || p.E%2 == 0 {
				return nil, ErrExternalKeysUnavailable
			}
		case *ecdsa.PublicKey:
			if map[string]int{"ES256": 256, "ES384": 384, "ES512": 521}[algorithm] != p.Curve.Params().BitSize {
				return nil, ErrExternalKeysUnavailable
			}
		case ed25519.PublicKey:
		default:
			return nil, ErrExternalKeysUnavailable
		}
		keys[kid+"\x00"+algorithm] = k.Key
	}
	return keys, nil
}

// externalKeyAlgorithms lists the supported signature algorithms a key's
// material can verify. Other key types and curves verify none of them.
func externalKeyAlgorithms(kty, crv string) []string {
	switch {
	case kty == "RSA":
		return []string{"RS256", "RS384", "RS512"}
	case kty == "EC" && crv == "P-256":
		return []string{"ES256"}
	case kty == "EC" && crv == "P-384":
		return []string{"ES384"}
	case kty == "EC" && crv == "P-521":
		return []string{"ES512"}
	case kty == "OKP" && crv == "Ed25519":
		return []string{"EdDSA"}
	}
	return nil
}

// externalTime reads a NumericDate already checked to be a positive integer.
func externalTime(value any) time.Time {
	number, _ := value.(json.Number)
	seconds, _ := number.Int64()
	return time.Unix(seconds, 0)
}

func exactFields(data []byte, allowed, required []string) error {
	var fields map[string]json.RawMessage
	if uniqueJSON(data) != nil || json.Unmarshal(data, &fields) != nil || fields == nil {
		return errors.New("JSON object required")
	}
	for k, value := range fields {
		if !slices.Contains(allowed, k) || bytes.Equal(value, []byte("null")) {
			return errors.New("unexpected or null authorization field")
		}
	}
	for _, k := range required {
		if _, ok := fields[k]; !ok {
			return errors.New("required authorization field missing")
		}
	}
	return nil
}

func strictJSON(data []byte, dst any) error {
	if e := uniqueJSON(data); e != nil {
		return e
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}
func uniqueJSON(data []byte) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var read func() error
	read = func() error {
		t, e := d.Token()
		if e != nil {
			return e
		}
		switch t {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return errors.New("duplicate JSON field")
				}
				seen[s] = true
				if e = read(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case json.Delim('['):
			for d.More() {
				if e = read(); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		}
		return nil
	}
	if e := read(); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return nil
}
