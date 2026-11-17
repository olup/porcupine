package porcupine

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
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
	if o.sharedDB != nil {
		return o.saveSharedSession(session)
	}
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
	if o.sharedDB != nil {
		return key, o.loadSharedSession(r.Context(), key)
	}
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
	if o.sharedDB != nil {
		return o.consumeSharedSession(key)
	}
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
	if s.oauth.config.Email != nil && s.oauth.config.Email.SendCode != nil {
		s.registerEmailLogin(m)
	}
	if s.oauth.oidc != nil {
		m.HandleFunc("GET /oauth/oidc/callback", s.oauthOIDCCallback)
		m.HandleFunc("POST /oauth/oidc/start", s.oauthOIDCStart)
	}
	m.HandleFunc("GET /oauth/consent", s.oauthOIDCConsent)
	m.HandleFunc("POST /oauth/consent", s.oauthOIDCConsent)
}
func (s *Service) oauthHumanAuthorize(w http.ResponseWriter, r *http.Request, request oauthAuthorizationRequest) {
	oidcPageHeaders(w)
	if s.oauth.config.Email != nil && s.oauth.config.Email.SendCode != nil {
		s.startEmailLogin(w, request)
		return
	}
	s.startOIDCLogin(w, r, request.ID, time.Now().Add(oidcSessionLifetime))
}

func (s *Service) oauthOIDCStart(w http.ResponseWriter, r *http.Request) {
	key, session, ok := s.emailSubmission(w, r, "csrf")
	if !ok {
		return
	}
	if len(r.PostForm) != 1 || !s.oauth.browser.consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	s.startOIDCLogin(w, r, session.requestID, session.expires)
}

func (s *Service) startOIDCLogin(w http.ResponseWriter, r *http.Request, requestID string, expires time.Time) {
	o := s.oauth.oidc
	session := &oidcBrowserSession{requestID: requestID, state: oidcRandom(), nonce: oidcRandom(), verifier: oauth2.GenerateVerifier(), expires: expires}
	raw, ok := o.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	o.cookie(w, raw, int(time.Until(expires).Seconds()))
	http.Redirect(w, r, o.oauth.AuthCodeURL(session.state, oidc.Nonce(session.nonce), oauth2.S256ChallengeOption(session.verifier)), http.StatusSeeOther)
}

// A trusted provider proves identity; local human accounts supply all authority.
type upstreamOIDCClaims struct {
	AuthorizedParty string `json:"azp"`
	Email           string `json:"email"`
	EmailVerified   bool   `json:"email_verified"`
	HostedDomain    string `json:"hd"`
}

func oidcVerifiedEmail(issuer string, claims upstreamOIDCClaims) (string, error) {
	if !claims.EmailVerified {
		return "", errors.New("identity provider did not verify email")
	}
	email, err := normalizeEmail(claims.Email)
	if err != nil {
		return "", errors.New("identity provider did not provide a valid email")
	}
	if issuer == "https://accounts.google.com" {
		domain := email[strings.LastIndexByte(email, '@')+1:]
		if domain != "gmail.com" && claims.HostedDomain == "" {
			return "", errors.New("Google does not establish current email ownership; use email sign-in")
		}
	}
	return email, nil
}

func (s *Service) oauthOIDCCallback(w http.ResponseWriter, r *http.Request) {
	oidcPageHeaders(w)
	o := s.oauth.oidc
	key, session := o.session(r)
	query, queryErr := oauthSingleQuery(r)
	if queryErr != nil || session == nil || session.humanID != "" || session.state == "" || session.nonce == "" || session.verifier == "" || len(query["state"]) != 1 || subtle.ConstantTimeCompare([]byte(query.Get("state")), []byte(session.state)) != 1 {
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
	var claims upstreamOIDCClaims
	if identity.Claims(&claims) != nil || claims.AuthorizedParty != "" && claims.AuthorizedParty != o.oauth.ClientID || len(identity.Audience) > 1 && claims.AuthorizedParty != o.oauth.ClientID {
		http.Error(w, "Login verification failed", 403)
		return
	}
	email, err := oidcVerifiedEmail(o.issuer, claims)
	if err != nil {
		http.Error(w, "A verified invited email is required; use email sign-in if available", 403)
		return
	}
	human, err := s.oidcHuman(r.Context(), identity.Subject, email)
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
