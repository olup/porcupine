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

func (s *Service) registerResourceMetadata(m *http.ServeMux) {
	if s.resources == nil {
		return
	}
	issuer := s.resources.publicURL
	if s.externalAuthEnabled() {
		issuer = s.resources.external.Config().Issuer
	}
	for _, entry := range []struct{ suffix, resource string }{{"/api/v1", s.resources.apiResource}, {"/mcp", s.resources.mcpResource}} {
		m.HandleFunc("GET /.well-known/oauth-protected-resource"+entry.suffix, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			scopes := auth.Permissions()
			if s.interactiveOAuthEnabled() && s.interactiveResourceAllowed(entry.resource) {
				scopes = auth.InteractiveReadScopes()
			}
			write(w, 200, map[string]any{"resource": entry.resource, "authorization_servers": []string{issuer}, "scopes_supported": scopes, "bearer_methods_supported": []string{"header"}})
		})
	}
}
