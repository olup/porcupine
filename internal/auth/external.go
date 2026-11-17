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
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/golang-jwt/jwt/v5"
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
}

func NormalizeExternalConfig(c ExternalConfig) (ExternalConfig, error) {
	for _, raw := range []string{c.Issuer, c.JWKSURL, c.APIAudience, c.MCPAudience} {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"))) {
			return c, errors.New("external authentication URLs require HTTPS (HTTP is allowed on loopback only)")
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
	return now.Before(v.validUntil) && len(v.keys) > 0
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
	if exactFields(header, []string{"alg", "typ", "kid"}, []string{"alg", "typ", "kid"}) != nil || strictJSON(header, &h) != nil || !slices.Contains(v.config.Algorithms, h.Algorithm) || !slices.Contains(v.config.TokenTypes, h.Type) || h.KeyID == "" || len(h.KeyID) > 128 {
		return nil, errors.New("unsupported access token header")
	}
	payload, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return nil, errors.New("invalid access token claims")
	}
	var claims jwt.MapClaims
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if uniqueJSON(payload) != nil || decoder.Decode(&claims) != nil {
		return nil, errors.New("invalid access token claims")
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
	key, e := v.key(ctx, h.KeyID, h.Algorithm, now)
	if e != nil {
		return nil, e
	}
	parser := jwt.NewParser(jwt.WithJSONNumber(), jwt.WithValidMethods(v.config.Algorithms), jwt.WithIssuer(v.config.Issuer), jwt.WithAudience(audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(v.config.ClockSkew), jwt.WithTimeFunc(func() time.Time { return now }))
	token, e := parser.ParseWithClaims(raw, claims, func(*jwt.Token) (any, error) { return key, nil })
	if e != nil || !token.Valid {
		return nil, errors.New("invalid access token")
	}
	// A token for both resources would erase the API/MCP boundary.
	aud, e := claims.GetAudience()
	if e != nil || len(aud) != 1 || aud[0] != audience {
		return nil, errors.New("access token requires one exact resource audience")
	}
	exp, e := claims.GetExpirationTime()
	if e != nil || exp == nil {
		return nil, errors.New("access token expiry required")
	}
	iat, e := claims.GetIssuedAt()
	if e != nil || iat == nil || !exp.After(iat.Time) || exp.Sub(iat.Time) > v.config.MaxTokenLifetime || !now.Before(exp.Time) {
		return nil, errors.New("invalid access token lifetime")
	}
	sub, e := claims.GetSubject()
	if e != nil || sub == "" || len(sub) > 256 {
		return nil, errors.New("access token subject required")
	}
	scope, ok := claims["scope"].(string)
	if !ok || len(scope) > 2048 {
		return nil, errors.New("access token scopes required")
	}
	scopes := strings.Fields(scope)
	if len(scopes) == 0 || len(scopes) > 32 {
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
	g := &ExternalGrant{issuer: v.config.Issuer, subject: sub, tenant: authority.TenantID, permissions: scopes, projects: authority.Projects, all: authority.ProjectScope == "all", operator: authority.Operator, expires: exp.Time}
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

func (v *ExternalVerifier) key(ctx context.Context, kid, algorithm string, now time.Time) (any, error) {
	for {
		v.mu.Lock()
		if now.Before(v.validUntil) {
			if k := v.keys[kid+"\x00"+algorithm]; k != nil {
				v.mu.Unlock()
				return k, nil
			}
		}
		if pending := v.refreshing; pending != nil {
			v.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-pending:
				continue
			}
		}
		if now.Sub(v.lastRefresh) < 10*time.Second {
			fresh := now.Before(v.validUntil)
			v.mu.Unlock()
			if fresh {
				return nil, errors.New("unknown access token signing key")
			}
			return nil, ErrExternalKeysUnavailable
		}
		v.lastRefresh = now
		v.refreshing = make(chan struct{})
		v.mu.Unlock()
		// A canceled caller must not cancel a refresh shared by other requests.
		go v.refresh(now)
	}
}

func (v *ExternalVerifier) refresh(now time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	keys, err := v.fetchKeys(ctx)
	v.mu.Lock()
	defer v.mu.Unlock()
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
	var set jose.JSONWebKeySet
	if uniqueJSON(data) != nil || json.Unmarshal(data, &set) != nil || len(set.Keys) == 0 || len(set.Keys) > 32 {
		return nil, ErrExternalKeysUnavailable
	}
	var raw struct {
		Keys []map[string]json.RawMessage `json:"keys"`
	}
	if json.Unmarshal(data, &raw) != nil || len(raw.Keys) != len(set.Keys) {
		return nil, ErrExternalKeysUnavailable
	}
	keys := map[string]any{}
	seen := map[string]bool{}
	for i, k := range set.Keys {
		if k.KeyID == "" || len(k.KeyID) > 128 || seen[k.KeyID] || k.IsPublic() == false || !k.Valid() || (k.Use != "" && k.Use != "sig") {
			return nil, ErrExternalKeysUnavailable
		}
		seen[k.KeyID] = true
		if operations, exists := raw.Keys[i]["key_ops"]; exists {
			var ops []string
			if len(operations) == 0 || operations[0] != '[' || json.Unmarshal(operations, &ops) != nil || !slices.Equal(ops, []string{"verify"}) {
				return nil, ErrExternalKeysUnavailable
			}
		}
		if !slices.Contains(v.config.Algorithms, k.Algorithm) {
			continue
		}
		switch p := k.Key.(type) {
		case *rsa.PublicKey:
			if p.N.BitLen() < 2048 || !slices.Contains([]string{"RS256", "RS384", "RS512"}, k.Algorithm) {
				return nil, ErrExternalKeysUnavailable
			}
		case *ecdsa.PublicKey:
			if map[string]int{"ES256": 256, "ES384": 384, "ES512": 521}[k.Algorithm] != p.Curve.Params().BitSize {
				return nil, ErrExternalKeysUnavailable
			}
		case ed25519.PublicKey:
			if k.Algorithm != "EdDSA" {
				return nil, ErrExternalKeysUnavailable
			}
		default:
			return nil, ErrExternalKeysUnavailable
		}
		if method := jwt.GetSigningMethod(k.Algorithm); method == nil {
			return nil, ErrExternalKeysUnavailable
		}
		keys[k.KeyID+"\x00"+k.Algorithm] = k.Key
	}
	return keys, nil
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
