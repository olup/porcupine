package auth

import (
	"crypto/subtle"
	"errors"
	"golang.org/x/oauth2"
	"net/http"
	"time"
)

func LoginPageHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
}
func (o *Login) StartOIDC(w http.ResponseWriter, r *http.Request) {
	key, session, ok := o.emailSubmission(w, r, "csrf")
	if !ok {
		return
	}
	if len(r.PostForm) != 1 || !o.Consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	o.startOIDCLogin(w, r, session.RequestID, session.Expires)
}

func (o *Login) startOIDCLogin(w http.ResponseWriter, r *http.Request, requestID string, expires time.Time) {
	session := &browserSession{RequestID: requestID, state: RandomNonce(), nonce: RandomNonce(), verifier: oauth2.GenerateVerifier(), Expires: expires}
	raw, ok := o.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	o.Cookie(w, raw, int(time.Until(expires).Seconds()))
	http.Redirect(w, r, o.provider.AuthorizationURL(session.state, session.nonce, session.verifier), http.StatusSeeOther)
}

func (o *Login) OIDCCallback(w http.ResponseWriter, r *http.Request, accounts LoginAccounts) {
	LoginPageHeaders(w)
	key, session := o.browserSession(r)
	query, queryErr := ReadOAuthQuery(r)
	if queryErr != nil || session == nil || session.HumanID != "" || session.state == "" || session.nonce == "" || session.verifier == "" || len(query["state"]) != 1 || subtle.ConstantTimeCompare([]byte(query.Get("state")), []byte(session.state)) != 1 {
		http.Error(w, "Invalid login state", 400)
		return
	}
	if !o.Consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	o.Cookie(w, "", -1)
	if query.Get("error") != "" || query.Get("iss") != "" && query.Get("iss") != o.provider.Issuer() || len(query["code"]) != 1 || query.Get("code") == "" || len(query.Get("code")) > 4096 {
		http.Error(w, "Login was not completed", 403)
		return
	}
	identity, err := o.provider.ExchangeIdentity(r.Context(), query.Get("code"), session.verifier, session.nonce)
	if err != nil {
		message := "Login verification failed"
		if errors.Is(err, ErrOIDCEmail) {
			message = "A verified invited email is required; use email sign-in if available"
		}
		http.Error(w, message, 403)
		return
	}
	human, err := accounts.LinkOIDCIdentity(r.Context(), identity.Subject, identity.Email)
	if err != nil {
		http.Error(w, "This account has no project access", 403)
		return
	}
	session.HumanID = human.ID
	session.Proof = human.Proof
	session.CSRF = RandomNonce()
	session.state = ""
	session.nonce = ""
	session.verifier = ""
	raw, ok := o.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	o.Cookie(w, raw, int(time.Until(session.Expires).Seconds()))
	http.Redirect(w, r, "/oauth/consent", http.StatusSeeOther)
}
