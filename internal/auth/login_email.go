package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const (
	emailCodeLifetime = 5 * time.Minute
	emailCodeAttempts = 5
)

// An OTP is valid only for one browser's outstanding OAuth authorization request.
type emailChallenge struct {
	loginVersion int64
	digest       [32]byte
	principal    string
	attemptsLeft int
	Expires      time.Time
}

var emailLoginPage = template.Must(template.New("email-login").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in to Porcupine</title><main>
<h1>Connect {{.Client}} to Porcupine</h1>
<form method="post" action="/oauth/email/request"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Email address <input type="email" name="email" autocomplete="email" required></label> <button>Send sign-in code</button></form>
{{if .Provider}}<p>Or</p><form method="post" action="/oauth/oidc/start"><input type="hidden" name="csrf" value="{{.CSRF}}"><button>Continue with {{.Provider}}</button></form>{{end}}
</main></html>`))

var emailVerificationPage = template.Must(template.New("email-verification").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Verify your email</title><main>
<h1>Verify your email</h1>{{if .Rejected}}<p>The code is invalid or expired. If necessary, restart the connection to request another code.</p>{{else}}<p>If your email address has access, check your inbox for an eight-digit code.</p>{{end}}
<form method="post" action="/oauth/email/verify"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Sign-in code <input type="text" name="code" inputmode="numeric" autocomplete="one-time-code" maxlength="8" required></label> <button>Continue</button></form>
</main></html>`))

func emailPageHeaders(w http.ResponseWriter) {
	LoginPageHeaders(w)
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
}

func (o *Login) startEmailLogin(w http.ResponseWriter, request LoginRequest) {
	emailPageHeaders(w)
	browser := o
	session := &browserSession{RequestID: request.ID, CSRF: RandomNonce(), Expires: time.Now().Add(LoginLifetime)}
	raw, ok := browser.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.Cookie(w, raw, int(LoginLifetime.Seconds()))
	provider := ""
	if o.provider != nil {
		provider = "identity provider"
		if o.provider.Issuer() == "https://accounts.google.com" {
			provider = "Google"
		}
	}
	_ = emailLoginPage.Execute(w, struct {
		Client, CSRF, Provider string
	}{request.ClientName, session.CSRF, provider})
}

// All login forms carry a session-bound token as well as an exact browser origin.
func (o *Login) emailSubmission(w http.ResponseWriter, r *http.Request, field string) (string, *browserSession, bool) {
	emailPageHeaders(w)
	key, session := o.browserSession(r)
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if session == nil || session.HumanID != "" || len(session.CSRF) == 0 ||
		r.Header.Get("Origin") != o.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil ||
		len(r.PostForm["csrf"]) != 1 || len(r.PostForm[field]) != 1 ||
		subtle.ConstantTimeCompare([]byte(session.CSRF), []byte(r.PostForm.Get("csrf"))) != 1 {
		http.Error(w, "Invalid login session", 400)
		return "", nil, false
	}
	return key, session, true
}

func (o *Login) RequestEmailCode(w http.ResponseWriter, r *http.Request, peer string, accounts LoginAccounts) {
	key, session, ok := o.emailSubmission(w, r, "email")
	if !ok {
		return
	}
	browser := o
	if !session.emailChallenge.Expires.IsZero() || !browser.Consume(key) {
		http.Error(w, "Login already used", 400)
		return
	}
	n, err := rand.Int(rand.Reader, big.NewInt(100000000))
	if err != nil {
		http.Error(w, "Login unavailable", 503)
		return
	}
	code := fmt.Sprintf("%08d", n.Int64())
	now := time.Now()
	session.CSRF = RandomNonce()
	expiry := now.Add(emailCodeLifetime)
	if session.Expires.Before(expiry) {
		expiry = session.Expires
	}
	challenge := emailChallenge{attemptsLeft: emailCodeAttempts, Expires: expiry, digest: sha256.Sum256([]byte(session.CSRF + code))}
	email, err := NormalizeEmail(r.PostForm.Get("email"))
	send := false
	var finishDelivery func()
	if err == nil && peer != "" {
		finishDelivery, _ = browser.reserveEmailAttempt(now, email, peer)
	}
	if finishDelivery != nil {
		// Unknown accounts, throttled requests and SMTP failures produce the same browser response.
		identity, lookupErr := accounts.LookupEmail(r.Context(), email)
		if lookupErr == nil && identity.ID != "" {
			challenge.principal = identity.ID
			challenge.loginVersion = identity.Proof.Version
			send = true
		} else {
			finishDelivery()
		}
	}
	session.emailChallenge = challenge
	raw, saved := browser.save(session)
	if !saved {
		if send {
			finishDelivery()
		}
		http.Error(w, "Login capacity reached", 503)
		return
	}
	if send {
		go func() {
			defer finishDelivery()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if o.sendCode(ctx, email, code) != nil {
				slog.Warn("Porcupine could not deliver a sign-in email")
			}
		}()
	}
	browser.Cookie(w, raw, int(time.Until(session.Expires).Seconds()))
	_ = emailVerificationPage.Execute(w, struct {
		CSRF     string
		Rejected bool
	}{session.CSRF, false})
}

// Check and decrement the live session atomically, including when browsers race or replay a form.
func (o *Login) redeemEmailCode(key, code string, now time.Time) (*browserSession, bool) {
	if o.sharedDB != nil {
		return o.redeemSharedEmailCode(key, code, now)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	session := o.sessions[key]
	if session == nil || session.emailChallenge.Expires.IsZero() {
		return nil, false
	}
	challenge := &session.emailChallenge
	if !now.Before(challenge.Expires) || !now.Before(session.Expires) {
		delete(o.sessions, key)
		return nil, false
	}
	sum := sha256.Sum256([]byte(session.CSRF + code))
	if subtle.ConstantTimeCompare(sum[:], challenge.digest[:]) != 1 || challenge.principal == "" {
		challenge.attemptsLeft--
		if challenge.attemptsLeft <= 0 {
			delete(o.sessions, key)
		}
		return nil, false
	}
	delete(o.sessions, key)
	return &browserSession{RequestID: session.RequestID, Expires: session.Expires, HumanID: challenge.principal, Proof: LoginProof{Version: challenge.loginVersion, Verified: true}, CSRF: RandomNonce()}, true
}

func (o *Login) VerifyEmailCode(w http.ResponseWriter, r *http.Request, accounts LoginAccounts) {
	key, session, ok := o.emailSubmission(w, r, "code")
	if !ok {
		return
	}
	browser := o
	verified, ok := browser.redeemEmailCode(key, strings.TrimSpace(r.PostForm.Get("code")), time.Now())
	if !ok {
		w.WriteHeader(http.StatusForbidden)
		_ = emailVerificationPage.Execute(w, struct {
			CSRF     string
			Rejected bool
		}{session.CSRF, true})
		return
	}
	if err := accounts.ValidateLogin(r.Context(), LoginIdentity{ID: verified.HumanID, Proof: verified.Proof}); err != nil {
		message := "Sign-in invalidated; restart sign-in"
		if errors.Is(err, ErrLoginRevoked) {
			message = "Access revoked"
		}
		http.Error(w, message, 403)
		return
	}
	raw, ok := browser.save(verified)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.Cookie(w, raw, int(time.Until(verified.Expires).Seconds()))
	http.Redirect(w, r, "/oauth/consent", http.StatusSeeOther)
}
