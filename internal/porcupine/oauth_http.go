package porcupine

import (
	"context"
	"errors"
	"net/http"
	"strings"
)

type mcpDispatchKey struct{}

func bearerCredential(r *http.Request) (string, error) {
	if len(r.Header.Values("Authorization")) != 1 {
		return "", errors.New("one authorization header is required")
	}
	fields := strings.Fields(r.Header.Get("Authorization"))
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || len(fields[1]) > 512 {
		return "", errors.New("invalid bearer authorization")
	}
	return fields[1], nil
}

func (s *Service) authenticateResource(ctx context.Context, raw, resource string) (actor, error) {
	if s.oauth == nil {
		return s.authenticate(ctx, raw)
	}
	if strings.HasPrefix(raw, "pc_at_") {
		return s.authenticateOAuthToken(ctx, raw, resource)
	}
	if s.oauth.config.AllowLegacyKeys {
		return s.authenticate(ctx, raw)
	}
	return actor{}, errors.New("OAuth access token required")
}

func (s *Service) authChallenge(w http.ResponseWriter, resource string, invalid bool) {
	challenge := `Bearer realm="porcupine"`
	if s.oauth != nil {
		suffix := "/api/v1"
		if resource == s.oauth.mcpResource {
			suffix = "/mcp"
		}
		challenge += `, resource_metadata="` + s.oauth.issuer + "/.well-known/oauth-protected-resource" + suffix + `"`
	}
	if s.interactiveOAuthEnabled() && resource == s.oauth.mcpResource {
		challenge += `, scope="events:read management:read"`
	}
	if invalid {
		challenge += `, error="invalid_token"`
	}
	w.Header().Set("WWW-Authenticate", challenge)
	w.Header().Set("Cache-Control", "no-store")
}

func (s *Service) registerOAuth(m *http.ServeMux) {
	if s.oauth == nil {
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
		metadata := map[string]any{"issuer": s.oauth.issuer, "token_endpoint": s.oauth.issuer + "/oauth/token", "grant_types_supported": []string{"client_credentials"}, "token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post"}, "scopes_supported": append([]string(nil), validPermissions...), "resource_indicators_supported": true, "revocation_endpoint": s.oauth.issuer + "/oauth/revoke"}
		if s.interactiveOAuthEnabled() {
			metadata["scopes_supported"] = append([]string{}, interactiveOAuthScopes...)
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
			scopes := append([]string(nil), validPermissions...)
			if s.interactiveOAuthEnabled() && resource == s.oauth.mcpResource {
				scopes = append([]string{}, interactiveOAuthScopes...)
			}
			write(w, 200, map[string]any{"resource": resource, "authorization_servers": []string{s.oauth.issuer}, "scopes_supported": scopes, "bearer_methods_supported": []string{"header"}})
		})
	}
}
