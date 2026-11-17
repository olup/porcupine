package porcupine

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/olup/porcupine/internal/auth"
)

type mcpDispatchKey struct{}

func (s *Service) authenticateResource(ctx context.Context, raw, resource string) (auth.Principal, error) {
	if s.resources == nil {
		return s.authenticate(ctx, raw)
	}
	if s.externalAuthEnabled() && !strings.HasPrefix(raw, serviceKeyPrefix) {
		config := s.resources.external.Config()
		audience := ""
		switch resource {
		case s.resources.apiResource:
			audience = config.APIAudience
		case s.resources.mcpResource:
			audience = config.MCPAudience
		default:
			return auth.Principal{}, errors.New("unknown authentication resource")
		}
		grant, err := s.resources.external.Verify(ctx, raw, audience, s.clock())
		if err != nil {
			return auth.Principal{}, err
		}
		return externalPrincipal(ctx, s.control, grant, s.clock())
	}
	if strings.HasPrefix(raw, "pc_at_") {
		return s.authenticateOAuthToken(ctx, raw, resource)
	}
	if strings.HasPrefix(raw, serviceKeyPrefix) {
		return s.authenticateServiceKey(ctx, raw, resource)
	}
	if s.oauth != nil && s.oauth.config.AllowLegacyKeys {
		return s.authenticate(ctx, raw)
	}
	return auth.Principal{}, errors.New("OAuth access token required")
}

func (s *Service) authChallenge(w http.ResponseWriter, resource string, invalid bool) {
	challenge := `Bearer realm="porcupine"`
	if s.resources != nil {
		suffix := "/api/v1"
		if resource == s.resources.mcpResource {
			suffix = "/mcp"
		}
		challenge += `, resource_metadata="` + s.resources.publicURL + "/.well-known/oauth-protected-resource" + suffix + `"`
	}
	if s.interactiveOAuthEnabled() && s.interactiveResourceAllowed(resource) {
		challenge += `, scope="events:read management:read"`
	}
	if invalid {
		challenge += `, error="invalid_token"`
	}
	w.Header().Set("WWW-Authenticate", challenge)
	w.Header().Set("Cache-Control", "no-store")
}

func (s *Service) registerOAuth(m *http.ServeMux) {
	if s.resources == nil {
		return
	}
	if s.externalAuthEnabled() {
		for _, entry := range []struct{ suffix, resource string }{{"/api/v1", s.resources.apiResource}, {"/mcp", s.resources.mcpResource}} {
			m.HandleFunc("GET /.well-known/oauth-protected-resource"+entry.suffix, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Cache-Control", "no-store")
				write(w, 200, map[string]any{"resource": entry.resource, "authorization_servers": []string{s.resources.external.Config().Issuer}, "scopes_supported": auth.Permissions(), "bearer_methods_supported": []string{"header"}})
			})
		}
		return
	}
	m.HandleFunc("POST /oauth/token", limitConcurrent(s.tokenSlots, s.oauthToken))
	m.HandleFunc("POST /oauth/revoke", limitConcurrent(s.tokenSlots, s.oauthRevoke))
	if s.interactiveOAuthEnabled() {
		m.HandleFunc("GET /oauth/authorize", limitConcurrent(s.tokenSlots, s.oauthAuthorize))
		s.registerInteractiveOIDC(m)
	}
	metadata := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		metadata := map[string]any{"issuer": s.oauth.issuer, "token_endpoint": s.oauth.issuer + "/oauth/token", "grant_types_supported": []string{"client_credentials"}, "token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post"}, "scopes_supported": append([]string(nil), auth.Permissions()...), "resource_indicators_supported": true, "revocation_endpoint": s.oauth.issuer + "/oauth/revoke"}
		if s.interactiveOAuthEnabled() {
			// MCP clients often request every scope returned by discovery. Point tools
			// that need explicit write or administrator grants to the complete contract.
			metadata["service_documentation"] = s.oauth.issuer + "/openapi.json"
			metadata["scopes_supported"] = append([]string{}, auth.InteractiveReadScopes()...)
			metadata["authorization_endpoint"] = s.oauth.issuer + "/oauth/authorize"
			metadata["revocation_endpoint"] = s.oauth.issuer + "/oauth/revoke"
			metadata["grant_types_supported"] = []string{"client_credentials", "authorization_code", "refresh_token"}
			metadata["response_types_supported"] = []string{"code"}
			metadata["code_challenge_methods_supported"] = []string{"S256"}
			metadata["token_endpoint_auth_methods_supported"] = []string{"client_secret_basic", "client_secret_post", "none"}
			metadata["authorization_response_iss_parameter_supported"] = true
		}
		write(w, 200, metadata)
	}
	m.HandleFunc("GET /.well-known/oauth-authorization-server", metadata)
	for _, suffix := range []string{"/api/v1", "/mcp"} {
		resource := s.oauth.issuer + suffix
		m.HandleFunc("GET /.well-known/oauth-protected-resource"+suffix, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			scopes := append([]string(nil), auth.Permissions()...)
			if s.interactiveOAuthEnabled() && s.interactiveResourceAllowed(resource) {
				scopes = append([]string{}, auth.InteractiveReadScopes()...)
			}
			write(w, 200, map[string]any{"resource": resource, "authorization_servers": []string{s.oauth.issuer}, "scopes_supported": scopes, "bearer_methods_supported": []string{"header"}})
		})
	}
}
