package porcupine

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"golang.org/x/oauth2"
)

const oidcSessionLifetime = 10 * time.Minute
const oidcSessionCapacity = 1024

type oidcBrowserSession struct {
	proof          humanLoginProof
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
	http.Redirect(w, r, o.provider.AuthorizationURL(session.state, session.nonce, session.verifier), http.StatusSeeOther)
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
	if query.Get("error") != "" || query.Get("iss") != "" && query.Get("iss") != o.provider.Issuer() || len(query["code"]) != 1 || query.Get("code") == "" || len(query.Get("code")) > 4096 {
		http.Error(w, "Login was not completed", 403)
		return
	}
	identity, err := o.provider.ExchangeIdentity(r.Context(), query.Get("code"), session.verifier, session.nonce)
	if err != nil {
		message := "Login verification failed"
		if errors.Is(err, auth.ErrOIDCEmail) {
			message = "A verified invited email is required; use email sign-in if available"
		}
		http.Error(w, message, 403)
		return
	}
	human, err := s.oidcHuman(r.Context(), identity.Subject, identity.Email)
	if err != nil {
		http.Error(w, "This account has no project access", 403)
		return
	}
	session.humanID = human.ID
	session.proof = human.proof
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
