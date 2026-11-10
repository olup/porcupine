package porcupine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"html/template"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"
)

const (
	emailCodeLifetime   = 5 * time.Minute
	emailCodeAttempts   = 5
	emailDeliveryPeriod = 10 * time.Minute
)

// An OTP is valid only for one browser's outstanding OAuth authorization request.
type emailChallenge struct {
	digest       [32]byte
	principal    string
	attemptsLeft int
	expires      time.Time
}

type emailDeliveryWindow struct {
	until         time.Time
	addressCounts map[string]int
	peerCounts    map[string]int
	total         int
	inFlight      int
}

func (o *oauthBrowser) reserveEmailDelivery(now time.Time, address, peer string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	window := &o.emailDelivery
	if !now.Before(window.until) {
		*window = emailDeliveryWindow{until: now.Add(emailDeliveryPeriod), addressCounts: make(map[string]int), peerCounts: make(map[string]int), inFlight: window.inFlight}
	}
	// The global budget also bounds both maps, including entries for unknown addresses.
	address = hash(address)
	if window.total >= 120 || window.peerCounts[peer] >= 20 || window.addressCounts[address] >= 3 || window.inFlight >= 8 {
		return false
	}
	window.addressCounts[address]++
	window.peerCounts[peer]++
	window.total++
	window.inFlight++
	return true
}

func (o *oauthBrowser) finishEmailDelivery() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.emailDelivery.inFlight--
}

var emailLoginPage = template.Must(template.New("email-login").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in to Porcupine</title><main>
<h1>Connect {{.Client}} to Porcupine</h1>
<form method="post" action="/oauth/email/request"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Email address <input type="email" name="email" autocomplete="email" required></label> <button>Send sign-in code</button></form>
</main></html>`))

var emailVerificationPage = template.Must(template.New("email-verification").Parse(`<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Verify your email</title><main>
<h1>Verify your email</h1>{{if .Rejected}}<p>The code is invalid or expired. If necessary, restart the connection to request another code.</p>{{else}}<p>If your email address has access, check your inbox for an eight-digit code.</p>{{end}}
<form method="post" action="/oauth/email/verify"><input type="hidden" name="csrf" value="{{.CSRF}}"><label>Sign-in code <input type="text" name="code" inputmode="numeric" autocomplete="one-time-code" maxlength="8" required></label> <button>Continue</button></form>
</main></html>`))

func emailPageHeaders(w http.ResponseWriter) {
	oidcPageHeaders(w)
	w.Header().Set("Referrer-Policy", "same-origin")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
}

func (s *Service) registerEmailLogin(m *http.ServeMux) {
	m.HandleFunc("POST /oauth/email/request", s.requestEmailCode)
	m.HandleFunc("POST /oauth/email/verify", s.verifyEmailCode)
}

func (s *Service) startEmailLogin(w http.ResponseWriter, request oauthAuthorizationRequest) {
	emailPageHeaders(w)
	browser := s.oauth.browser
	session := &oidcBrowserSession{requestID: request.ID, csrf: oidcRandom(), expires: time.Now().Add(oidcSessionLifetime)}
	raw, ok := browser.save(session)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.cookie(w, raw, int(oidcSessionLifetime.Seconds()))
	_ = emailLoginPage.Execute(w, struct {
		Client, CSRF string
	}{request.ClientName, session.csrf})
}

// All login forms carry a session-bound token as well as an exact browser origin.
func (s *Service) emailSubmission(w http.ResponseWriter, r *http.Request, field string) (string, *oidcBrowserSession, bool) {
	emailPageHeaders(w)
	key, session := s.oauth.browser.session(r)
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if session == nil || session.humanID != "" || len(session.csrf) == 0 ||
		r.Header.Get("Origin") != s.oauth.issuer || r.URL.RawQuery != "" || r.ParseForm() != nil ||
		len(r.PostForm["csrf"]) != 1 || len(r.PostForm[field]) != 1 ||
		subtle.ConstantTimeCompare([]byte(session.csrf), []byte(r.PostForm.Get("csrf"))) != 1 {
		http.Error(w, "Invalid login session", 400)
		return "", nil, false
	}
	return key, session, true
}

func (s *Service) requestEmailCode(w http.ResponseWriter, r *http.Request) {
	key, session, ok := s.emailSubmission(w, r, "email")
	if !ok {
		return
	}
	browser := s.oauth.browser
	if !session.emailChallenge.expires.IsZero() || !browser.consume(key) {
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
	session.csrf = oidcRandom()
	expiry := now.Add(emailCodeLifetime)
	if session.expires.Before(expiry) {
		expiry = session.expires
	}
	challenge := emailChallenge{attemptsLeft: emailCodeAttempts, expires: expiry, digest: sha256.Sum256([]byte(session.csrf + code))}
	email, err := normalizeEmail(r.PostForm.Get("email"))
	ip, ipErr := s.clientIP(r)
	send := false
	if err == nil && ipErr == nil && browser.reserveEmailDelivery(now, email, hash(ip.String())) {
		// Unknown accounts, throttled requests and SMTP failures produce the same browser response.
		id, lookupErr := s.lookupActiveEmailID(r.Context(), email)
		if lookupErr == nil && id != "" {
			challenge.principal = id
			send = true
		} else {
			browser.finishEmailDelivery()
		}
	}
	session.emailChallenge = challenge
	raw, saved := browser.save(session)
	if !saved {
		if send {
			browser.finishEmailDelivery()
		}
		http.Error(w, "Login capacity reached", 503)
		return
	}
	if send {
		go func() {
			defer browser.finishEmailDelivery()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if s.oauth.config.Email.SendCode(ctx, email, code) != nil {
				slog.Warn("Porcupine could not deliver a sign-in email")
			}
		}()
	}
	browser.cookie(w, raw, int(time.Until(session.expires).Seconds()))
	_ = emailVerificationPage.Execute(w, struct {
		CSRF     string
		Rejected bool
	}{session.csrf, false})
}

// Check and decrement the live session atomically, including when browsers race or replay a form.
func (o *oauthBrowser) redeemEmailCode(key, code string, now time.Time) (*oidcBrowserSession, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	session := o.sessions[key]
	if session == nil || session.emailChallenge.expires.IsZero() {
		return nil, false
	}
	challenge := &session.emailChallenge
	if !now.Before(challenge.expires) || !now.Before(session.expires) {
		delete(o.sessions, key)
		return nil, false
	}
	sum := sha256.Sum256([]byte(session.csrf + code))
	if subtle.ConstantTimeCompare(sum[:], challenge.digest[:]) != 1 || challenge.principal == "" {
		challenge.attemptsLeft--
		if challenge.attemptsLeft <= 0 {
			delete(o.sessions, key)
		}
		return nil, false
	}
	delete(o.sessions, key)
	return &oidcBrowserSession{requestID: session.requestID, expires: session.expires, humanID: challenge.principal, csrf: oidcRandom()}, true
}

func (s *Service) verifyEmailCode(w http.ResponseWriter, r *http.Request) {
	key, session, ok := s.emailSubmission(w, r, "code")
	if !ok {
		return
	}
	browser := s.oauth.browser
	verified, ok := browser.redeemEmailCode(key, strings.TrimSpace(r.PostForm.Get("code")), time.Now())
	if !ok {
		w.WriteHeader(http.StatusForbidden)
		_ = emailVerificationPage.Execute(w, struct {
			CSRF     string
			Rejected bool
		}{session.csrf, true})
		return
	}
	if _, err := s.loadOAuthHuman(r.Context(), s.db, verified.humanID); err != nil {
		http.Error(w, "Access revoked", 403)
		return
	}
	raw, ok := browser.save(verified)
	if !ok {
		http.Error(w, "Login capacity reached", 503)
		return
	}
	browser.cookie(w, raw, int(time.Until(verified.expires).Seconds()))
	http.Redirect(w, r, "/oauth/consent", http.StatusSeeOther)
}
