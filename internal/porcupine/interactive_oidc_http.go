package porcupine

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const oidcSessionLifetime = 10 * time.Minute
const oidcSessionCapacity = 1024

type oidcBrowserSession struct {
	emailChallenge emailChallenge
	requestID      string
	state          string
	nonce          string
	verifier       string
	csrf           string
	humanID        string
	expires        time.Time
}

func oidcRandom() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func (o *oauthBrowser) cookieName() string {
	if o.secure {
		return "__Host-porcupine_oidc"
	}
	return "porcupine_oidc"
}
func (o *oauthBrowser) cookie(w http.ResponseWriter, value string, age int) {
	http.SetCookie(w, &http.Cookie{Name: o.cookieName(), Value: value, Path: "/", HttpOnly: true, Secure: o.secure, SameSite: http.SameSiteLaxMode, MaxAge: age})
}
func (o *oauthBrowser) save(session *oidcBrowserSession) (string, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for key, value := range o.sessions {
		if time.Now().After(value.expires) {
			delete(o.sessions, key)
		}
	}
	if len(o.sessions) >= oidcSessionCapacity {
		return "", false
	}
	raw := oidcRandom()
	o.sessions[hash(raw)] = session
	return raw, true
}
func (o *oauthBrowser) session(r *http.Request) (string, *oidcBrowserSession) {
	cookie, err := r.Cookie(o.cookieName())
	if err != nil || len(cookie.Value) > 128 {
		return "", nil
	}
	key := hash(cookie.Value)
	o.mu.Lock()
	defer o.mu.Unlock()
	session := o.sessions[key]
	if session == nil || time.Now().After(session.expires) {
		delete(o.sessions, key)
		return "", nil
	}
	copy := *session
	return key, &copy
}
func (o *oauthBrowser) consume(key string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.sessions[key] == nil {
		return false
	}
	delete(o.sessions, key)
	return true
}
func oidcPageHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
}
func (s *Service) registerInteractiveOIDC(m *http.ServeMux) {
	if s.oauth.config.Email != nil {
		s.registerEmailLogin(m)
	}
	if s.oauth.config.LocalInvitations {
		m.HandleFunc("POST /oauth/invitation", s.oauthInvitationLogin)
	} else if s.oauth.oidc != nil {
		m.HandleFunc("GET /oauth/oidc/callback", s.oauthOIDCCallback)
	}
	m.HandleFunc("GET /oauth/consent", s.oauthOIDCConsent)
	m.HandleFunc("POST /oauth/consent", s.oauthOIDCConsent)
}
func (s *Service) oauthHumanAuthorize(w http.ResponseWriter, r *http.Request, request oauthAuthorizationRequest) {
	oidcPageHeaders(w)
	if s.oauth.config.Email != nil {
		s.startEmailLogin(w, request)
		return
	}
	if s.oauth.config.LocalInvitations {
		s.startInvitationLogin(w, request)
		return
	}
	o := s.oauth.oidc
	session := &oidcBrowserSession{requestID: request.ID, state: oidcRandom(), nonce: oidcRandom(), verifier: oauth2.GenerateVerifier(), expires: time.Now().Add(oidcSessionLifetime)}
	raw, ok := o.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	o.cookie(w, raw, int(oidcSessionLifetime.Seconds()))
	http.Redirect(w, r, o.oauth.AuthCodeURL(session.state, oidc.Nonce(session.nonce), oauth2.S256ChallengeOption(session.verifier)), http.StatusSeeOther)
}
func (s *Service) oauthOIDCCallback(w http.ResponseWriter, r *http.Request) {
	oidcPageHeaders(w)
	o := s.oauth.oidc
	key, session := o.session(r)
	query, queryErr := oauthSingleQuery(r)
	if queryErr != nil || session == nil || session.humanID != "" || len(query["state"]) != 1 || subtle.ConstantTimeCompare([]byte(query.Get("state")), []byte(session.state)) != 1 {
		http.Error(w, "Invalid login state", 400)
		return
	}
	if !o.consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	o.cookie(w, "", -1)
	if query.Get("error") != "" || query.Get("iss") != "" && query.Get("iss") != o.issuer || len(query["code"]) != 1 || query.Get("code") == "" || len(query.Get("code")) > 4096 {
		http.Error(w, "Login was not completed", 403)
		return
	}
	ctx := oidc.ClientContext(r.Context(), o.client)
	token, err := o.oauth.Exchange(ctx, query.Get("code"), oauth2.VerifierOption(session.verifier))
	if err != nil {
		http.Error(w, "Login verification failed", 403)
		return
	}
	rawToken, ok := token.Extra("id_token").(string)
	if !ok || len(rawToken) > 65536 {
		http.Error(w, "Login verification failed", 403)
		return
	}
	identity, err := o.verifier.Verify(ctx, rawToken)
	if err != nil || identity.Subject == "" || len(identity.Subject) > 512 || subtle.ConstantTimeCompare([]byte(identity.Nonce), []byte(session.nonce)) != 1 {
		http.Error(w, "Login verification failed", 403)
		return
	}
	var claims struct {
		AuthorizedParty string `json:"azp"`
	}
	if identity.Claims(&claims) != nil || claims.AuthorizedParty != "" && claims.AuthorizedParty != o.oauth.ClientID || len(identity.Audience) > 1 && claims.AuthorizedParty != o.oauth.ClientID {
		http.Error(w, "Login verification failed", 403)
		return
	}
	human, err := s.oidcHuman(r.Context(), identity.Subject)
	if err != nil {
		http.Error(w, "This account has no project access", 403)
		return
	}
	session.humanID = human.ID
	session.csrf = oidcRandom()
	session.state = ""
	session.nonce = ""
	session.verifier = ""
	raw, ok := o.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	o.cookie(w, raw, int(time.Until(session.expires).Seconds()))
	http.Redirect(w, r, "/oauth/consent", http.StatusSeeOther)
}

var oidcConsentPage = template.Must(template.New("consent").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Authorize Porcupine access</title><style>body{max-width:38rem;margin:3rem auto;padding:0 1rem;font:17px/1.5 system-ui;color:#161616;background:white}fieldset{margin:1rem 0}label{display:block;padding:.4rem}button{font:inherit;padding:.5rem 1rem;margin:.5rem .5rem 0 0}code{overflow-wrap:anywhere}</style><main><h1>Authorize access</h1><p><strong>{{.ClientName}}</strong> requests access to Porcupine.</p><p>Client: <code>{{.ClientID}}</code></p><p>Return address: <code>{{.RedirectURI}}</code></p><p>Resource: <code>{{.Resource}}</code></p><p>Permissions: <code>{{.Scope}}</code></p>{{if .Admin}}<p><strong>Grant full instance administration, including management of all people, clients and projects.</strong></p>{{end}}<form method="post" action="/oauth/consent"><input type="hidden" name="csrf" value="{{.CSRF}}"><fieldset><legend>Select projects to share</legend>{{range .Projects}}<label><input type="checkbox" name="project" value="{{.}}"> {{if eq . "*"}}All projects (instance-wide administrator){{else}}{{.}}{{end}}</label>{{end}}</fieldset><button type="submit" name="decision" value="approve">Approve access</button><button type="submit" name="decision" value="deny">Deny</button></form></main></html>`))

func (s *Service) oauthOIDCConsent(w http.ResponseWriter, r *http.Request) {
	oidcPageHeaders(w)
	o := s.oauth.browser
	key, session := o.session(r)
	if session == nil || session.humanID == "" {
		http.Error(w, "Login session expired", 401)
		return
	}
	request, err := s.loadOAuthAuthorizationRequest(r.Context(), session.requestID)
	if err != nil {
		http.Error(w, "Authorization request expired", 400)
		return
	}
	human, err := s.loadOAuthHuman(r.Context(), s.db, session.humanID)
	if err != nil {
		http.Error(w, "Project access revoked", 403)
		return
	}
	client, err := loadOAuthClient(r.Context(), s.db, request.ClientID)
	if err != nil {
		http.Error(w, "Client access revoked", 403)
		return
	}
	permissions := strings.Fields(request.Scope)
	if human.emailHuman && onlyInteractiveReadScopes(permissions) {
		permissions = slices.DeleteFunc(permissions, func(permission string) bool { return !human.can(permission) })
	}
	if !s.interactiveScopesAllowed(human, client, request.Resource, permissions) {
		http.Error(w, "Requested permissions are not granted", 403)
		return
	}
	projectsOffered := oauthProjectIntersection(human.Projects, client.Projects)
	admin := slices.Contains(permissions, "instance:admin")
	if slices.Contains(projectsOffered, "*") && !admin {
		// A wildcard in both upstream grants still requires choosing concrete projects here.
		var encoded string
		err = s.db.QueryRowContext(r.Context(), `SELECT json_group_array(id) FROM (SELECT id FROM projects ORDER BY id LIMIT 1000)`).Scan(&encoded)
		if err != nil || json.Unmarshal([]byte(encoded), &projectsOffered) != nil {
			http.Error(w, "Project access unavailable", 503)
			return
		}
	}
	projectsOffered = slices.DeleteFunc(projectsOffered, func(project string) bool {
		return !interactiveProjectsAllowed(human, client.actor, permissions, []string{project})
	})
	if len(projectsOffered) == 0 {
		http.Error(w, "No shared projects are granted", 403)
		return
	}
	if r.Method == http.MethodGet {
		w.Header().Set("Referrer-Policy", "same-origin")
		callback, _ := url.Parse(request.RedirectURI)
		w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self' "+callback.Scheme+"://"+callback.Host+"; frame-ancestors 'none'; base-uri 'none'")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_ = oidcConsentPage.Execute(w, struct {
			ClientName, ClientID, RedirectURI, Resource, Scope, CSRF string
			Admin                                                    bool
			Projects                                                 []string
		}{request.ClientName, request.ClientID, request.RedirectURI, request.Resource, strings.Join(permissions, " "), session.csrf, admin, projectsOffered})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if r.Header.Get("Origin") != s.oauth.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil || len(r.PostForm["csrf"]) != 1 || subtle.ConstantTimeCompare([]byte(r.PostForm.Get("csrf")), []byte(session.csrf)) != 1 || len(r.PostForm["decision"]) != 1 {
		http.Error(w, "Invalid consent submission", 400)
		return
	}
	decision := r.PostForm.Get("decision")
	if decision != "deny" && decision != "approve" {
		http.Error(w, "Invalid consent decision", 400)
		return
	}
	projects := []string{}
	for _, project := range r.PostForm["project"] {
		if !slices.Contains(projectsOffered, project) || slices.Contains(projects, project) {
			http.Error(w, "Project is not granted", 403)
			return
		}
		projects = append(projects, project)
	}
	if decision == "approve" && len(projects) == 0 {
		http.Error(w, "Select a project", 400)
		return
	}
	if len(projects) > 64 {
		http.Error(w, "Select at most 64 projects", 400)
		return
	}
	if !o.consume(key) {
		http.Error(w, "Consent already used", 400)
		return
	}
	o.cookie(w, "", -1)
	if decision == "deny" {
		if _, err := s.db.ExecContext(r.Context(), `UPDATE oauth_authorization_requests SET consumed=1 WHERE request_hash=? AND consumed=0`, hash(request.ID)); err != nil {
			http.Error(w, "Authorization could not be completed", 503)
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
	approved := actor{ID: human.ID, Permissions: permissions, Projects: projects}
	location, err := s.issueOAuthAuthorizationCode(r.Context(), request, human.ID, approved)
	if err != nil {
		http.Error(w, "Authorization could not be completed", 403)
		return
	}
	http.Redirect(w, r, location, http.StatusSeeOther)
}
