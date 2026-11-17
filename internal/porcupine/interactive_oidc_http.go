package porcupine

import (
	"github.com/olup/porcupine/internal/auth"
	"net/http"
)

func (s *Service) registerInteractiveOIDC(m *http.ServeMux) {
	login := s.oauth.browser
	accounts := s.loginAccounts()
	if s.oauth.config.Email != nil && s.oauth.config.Email.SendCode != nil {
		m.HandleFunc("POST /oauth/email/request", func(w http.ResponseWriter, r *http.Request) {
			peer := ""
			if ip, err := s.clientIP(r); err == nil {
				peer = hash(ip.String())
			}
			login.RequestEmailCode(w, r, peer, accounts)
		})
		m.HandleFunc("POST /oauth/email/verify", func(w http.ResponseWriter, r *http.Request) { login.VerifyEmailCode(w, r, accounts) })
	}
	if login.HasOIDC() {
		m.HandleFunc("GET /oauth/oidc/callback", func(w http.ResponseWriter, r *http.Request) { login.OIDCCallback(w, r, accounts) })
		m.HandleFunc("POST /oauth/oidc/start", login.StartOIDC)
	}
	m.HandleFunc("GET /oauth/consent", s.oauthOIDCConsent)
	m.HandleFunc("POST /oauth/consent", s.oauthOIDCConsent)
}
func (s *Service) loginAccounts() loginAccounts {
	issuer := ""
	if s.oauth != nil && s.oauth.browser != nil {
		issuer = s.oauth.browser.ProviderIssuer()
	}
	return loginAccounts{control: s.control, providerIssuer: issuer}
}

var _ auth.LoginAccounts = loginAccounts{}
