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
	metadata := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		write(w, 200, map[string]any{"issuer": s.oauth.issuer, "token_endpoint": s.oauth.issuer + "/oauth/token", "grant_types_supported": []string{"client_credentials"}, "token_endpoint_auth_methods_supported": []string{"client_secret_basic"}, "scopes_supported": append([]string(nil), validPermissions...), "resource_indicators_supported": true})
	}
	m.HandleFunc("GET /.well-known/oauth-authorization-server", metadata)
	for _, suffix := range []string{"/api/v1", "/mcp"} {
		resource := s.oauth.issuer + suffix
		m.HandleFunc("GET /.well-known/oauth-protected-resource"+suffix, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			write(w, 200, map[string]any{"resource": resource, "authorization_servers": []string{s.oauth.issuer}, "scopes_supported": append([]string(nil), validPermissions...), "bearer_methods_supported": []string{"header"}})
		})
	}
}
