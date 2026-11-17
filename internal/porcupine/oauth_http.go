package porcupine

import (
	"github.com/olup/porcupine/internal/auth"
	"net/http"
)

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
}
