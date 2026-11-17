package porcupine

import (
	"crypto/subtle"
	"html/template"
	"net/http"
	"net/url"
	"slices"
)

var tenantConsentPage = template.Must(template.New("tenant-consent").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authorize tenant administration</title><style>body{max-width:38rem;margin:3rem auto;padding:0 1rem;font:17px/1.5 system-ui;color:#161616;background:white}fieldset{margin:1rem 0}label{display:block;padding:.4rem}button{font:inherit;padding:.5rem 1rem;margin:.5rem .5rem 0 0}code{overflow-wrap:anywhere}</style><main><h1>Authorize tenant administration</h1><p><strong>{{.ClientName}}</strong> requests permission to manage one tenant.</p><p>Client: <code>{{.ClientID}}</code></p><p>Return address: <code>{{.RedirectURI}}</code></p><p>Resource: <code>{{.Resource}}</code></p><p>Permissions: <code>tenant:admin</code></p><p>This grants management of the selected tenant's members, service credentials, and current and future projects.</p><form method="post" action="/oauth/consent"><input type="hidden" name="csrf" value="{{.CSRF}}"><fieldset><legend>Select one tenant</legend>{{range .Tenants}}<label><input type="radio" name="tenant" value="{{.ID}}"> {{.Name}} <code>{{.ID}}</code></label>{{end}}</fieldset><button type="submit" name="decision" value="approve">Approve access</button><button type="submit" name="decision" value="deny">Deny</button></form></main></html>`))

func (s *Service) oauthTenantConsent(w http.ResponseWriter, r *http.Request, sessionKey string, session *oidcBrowserSession, request oauthAuthorizationRequest, human actor) {
	options, err := s.tenantAdminOptions(r.Context(), s.control, human)
	if err != nil {
		http.Error(w, "Tenant access unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(options) == 0 {
		http.Error(w, "No tenant administration is granted", http.StatusForbidden)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Referrer-Policy", "same-origin")
		callback, _ := url.Parse(request.RedirectURI)
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' "+callback.Scheme+"://"+callback.Host+"; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = tenantConsentPage.Execute(w, struct {
			ClientName, ClientID, RedirectURI, Resource, CSRF string
			Tenants                                           []tenantConsentOption
		}{request.ClientName, request.ClientID, request.RedirectURI, request.Resource, session.csrf, options})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if r.Header.Get("Origin") != s.oauth.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil || len(r.PostForm["csrf"]) != 1 || subtle.ConstantTimeCompare([]byte(r.PostForm.Get("csrf")), []byte(session.csrf)) != 1 || len(r.PostForm["decision"]) != 1 || len(r.PostForm["project"]) != 0 {
		http.Error(w, "Invalid consent submission", http.StatusBadRequest)
		return
	}
	decision := r.PostForm.Get("decision")
	if decision != "deny" && decision != "approve" {
		http.Error(w, "Invalid consent decision", http.StatusBadRequest)
		return
	}
	tenant := r.PostForm.Get("tenant")
	if decision == "approve" {
		if len(r.PostForm["tenant"]) != 1 || tenant == "" {
			http.Error(w, "Select one tenant", http.StatusBadRequest)
			return
		}
		if !slices.ContainsFunc(options, func(option tenantConsentOption) bool { return option.ID == tenant }) {
			http.Error(w, "Tenant administration is not granted", http.StatusForbidden)
			return
		}
	}
	if !s.oauth.browser.consume(sessionKey) {
		http.Error(w, "Consent already used", http.StatusBadRequest)
		return
	}
	s.oauth.browser.cookie(w, "", -1)
	if decision == "deny" {
		if _, err := s.control.ExecContext(r.Context(), `UPDATE oauth_authorization_requests SET consumed=1 WHERE request_hash=$1 AND consumed=0`, hash(request.ID)); err != nil {
			http.Error(w, "Authorization could not be completed", http.StatusServiceUnavailable)
			return
		}
		target, _ := url.Parse(request.RedirectURI)
		query := target.Query()
		query.Set("error", "access_denied")
		query.Set("state", request.State)
		query.Set("iss", s.oauth.issuer)
		target.RawQuery = query.Encode()
		http.Redirect(w, r, target.String(), http.StatusSeeOther)
		return
	}
	approved := actor{ID: human.ID, Permissions: []string{tenantAdminScope}, Projects: []string{}, tenantAdmins: []string{tenant}}
	location, err := s.issueOAuthAuthorizationCode(r.Context(), request, human.ID, session.proof, approved)
	if err != nil {
		http.Error(w, "Authorization could not be completed", http.StatusForbidden)
		return
	}
	http.Redirect(w, r, location, http.StatusSeeOther)
}
