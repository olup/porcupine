package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"time"
)

const LoginLifetime = 10 * time.Minute
const loginSessionCapacity = 1024

type browserSession struct {
	Proof          LoginProof
	emailChallenge emailChallenge
	RequestID      string
	state          string
	nonce          string
	verifier       string
	CSRF           string
	HumanID        string
	Expires        time.Time
}

func RandomNonce() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:])
}
func (o *Login) CookieName() string {
	if o.secure {
		return "__Host-porcupine_oidc"
	}
	return "porcupine_oidc"
}
func (o *Login) Cookie(w http.ResponseWriter, value string, age int) {
	http.SetCookie(w, &http.Cookie{Name: o.CookieName(), Value: value, Path: "/", HttpOnly: true, Secure: o.secure, SameSite: http.SameSiteLaxMode, MaxAge: age})
}
func (o *Login) save(session *browserSession) (string, bool) {
	if o.sharedDB != nil {
		return o.saveSharedSession(session)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for key, value := range o.sessions {
		if time.Now().After(value.Expires) {
			delete(o.sessions, key)
		}
	}
	if len(o.sessions) >= loginSessionCapacity {
		return "", false
	}
	raw := RandomNonce()
	o.sessions[loginHash(raw)] = session
	return raw, true
}
func (o *Login) browserSession(r *http.Request) (string, *browserSession) {
	cookie, err := r.Cookie(o.CookieName())
	if err != nil || len(cookie.Value) > 128 {
		return "", nil
	}
	key := loginHash(cookie.Value)
	if o.sharedDB != nil {
		return key, o.loadSharedSession(r.Context(), key)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	session := o.sessions[key]
	if session == nil || time.Now().After(session.Expires) {
		delete(o.sessions, key)
		return "", nil
	}
	copy := *session
	return key, &copy
}
func (o *Login) Consume(key string) bool {
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

func loginHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func (o *Login) Session(r *http.Request) (string, *LoginSession) {
	key, session := o.browserSession(r)
	if session == nil {
		return key, nil
	}
	return key, &LoginSession{Proof: session.Proof, RequestID: session.RequestID, CSRF: session.CSRF, HumanID: session.HumanID, Expires: session.Expires}
}
func (o *Login) SaveSession(session *LoginSession) (string, bool) {
	return o.save(&browserSession{Proof: session.Proof, RequestID: session.RequestID, CSRF: session.CSRF, HumanID: session.HumanID, Expires: session.Expires})
}
