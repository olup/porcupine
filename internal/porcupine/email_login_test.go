package porcupine

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmailCodeProofIsSingleUseAndBoundToOneSession(t *testing.T) {
	now := time.Now()
	browser := newOAuthBrowser(false)
	add := func(key, code string) {
		csrf := oidcRandom()
		browser.sessions[key] = &oidcBrowserSession{requestID: "one", expires: now.Add(time.Minute * 10), csrf: csrf, emailChallenge: emailChallenge{
			principal: "invited", digest: sha256.Sum256([]byte(csrf + code)), expires: now.Add(time.Minute), attemptsLeft: emailCodeAttempts,
		}}
	}
	add("request-a", "92345678")
	add("request-b", "82345678")
	if _, ok := browser.redeemEmailCode("request-a", "82345678", now); ok {
		t.Fatal("code was accepted in another request")
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			next, ok := browser.redeemEmailCode("request-a", "92345678", now)
			if ok {
				if next.humanID != "invited" || next.requestID != "one" || next.csrf == "" || !next.emailChallenge.expires.IsZero() {
					t.Error("invalid verified browser session")
				}
				successes.Add(1)
			}
		})
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("successful concurrent redemptions = %d", successes.Load())
	}
	if _, ok := browser.redeemEmailCode("request-b", "82345678", now.Add(time.Minute)); ok {
		t.Fatal("expired OTP accepted")
	}
	add("tries", "52345678")
	for range emailCodeAttempts {
		if _, ok := browser.redeemEmailCode("tries", "invalid", now); ok {
			t.Fatal("invalid proof accepted")
		}
	}
	if _, ok := browser.redeemEmailCode("tries", "52345678", now); ok {
		t.Fatal("proof accepted after attempts exhausted")
	}
}

func TestEmailSendAbuseLimits(t *testing.T) {
	start := time.Now()
	b := newOAuthBrowser(false)
	for i := range 4 {
		ok := b.reserveEmailDelivery(start, "a@example.test", "peer")
		if ok != (i < 3) {
			t.Fatal("address budget")
		}
		if ok {
			b.finishEmailDelivery()
		}
	}
	for i := range 18 {
		ok := b.reserveEmailDelivery(start, fmt.Sprint(i), "peer")
		if ok != (i < 17) {
			t.Fatal("peer budget")
		}
		if ok {
			b.finishEmailDelivery()
		}
	}
	for i := range 101 {
		ok := b.reserveEmailDelivery(start, fmt.Sprint(i+20), fmt.Sprint(i))
		if ok != (i < 100) {
			t.Fatal("global budget")
		}
		if ok {
			b.finishEmailDelivery()
		}
	}
	for i := range 9 {
		ok := b.reserveEmailDelivery(start.Add(emailDeliveryPeriod), fmt.Sprint(i), "peer")
		if ok != (i < 8) {
			t.Fatal("in-flight budget")
		}
	}
	for range 8 {
		b.finishEmailDelivery()
	}
}

func TestEmailChallengeRejectsForgeryAndHidesMembership(t *testing.T) {
	type delivery struct{ to, code string }
	deliveries := make(chan delivery, 8)
	finishSMTP := make(chan struct{})
	defer close(finishSMTP)
	email := "owner@example.test"
	const origin = "http://127.0.0.1"
	cfg := OAuthConfig{Issuer: origin, Email: &EmailConfig{AdminEmail: email, SendCode: func(ctx context.Context, to, code string) error {
		deliveries <- delivery{to, code}
		// An SMTP round trip must not leak account existence through the HTTP response time.
		select {
		case <-finishSMTP:
		case <-ctx.Done():
		}
		return nil
	}}}
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "otp.db"), BootstrapToken: adminToken, OAuth: &cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	hidden := regexp.MustCompile(`name="csrf" value="([^"]+)"`)
	post := func(path string, cookie *http.Cookie, values url.Values, from string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", origin+path, strings.NewReader(values.Encode()))
		r.Header.Set("Origin", from)
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		return w
	}
	start := func() (token string, cookie *http.Cookie) {
		rec := httptest.NewRecorder()
		s.startEmailLogin(rec, oauthAuthorizationRequest{ID: "test", ClientName: "Tool"})
		match := hidden.FindStringSubmatch(rec.Body.String())
		if len(match) != 2 {
			t.Fatal("missing csrf")
		}
		if strings.Contains(rec.Body.String(), `action="/oauth/invitation"`) {
			t.Fatal("removed manual login option present")
		}
		return match[1], rec.Result().Cookies()[0]
	}
	ask := func(address string) (string, *http.Cookie, string) {
		csrf, c := start()
		form := url.Values{"email": {address}, "csrf": {csrf}}
		if post("/oauth/email/request", c, form, "https://attacker.example").Code != 400 {
			t.Fatal("forged origin accepted")
		}
		form.Set("csrf", "wrong")
		if post("/oauth/email/request", c, form, origin).Code != 400 {
			t.Fatal("forged CSRF accepted")
		}
		form.Set("csrf", csrf)
		res := post("/oauth/email/request", c, form, origin)
		if res.Code != 200 {
			t.Fatalf("request code status %d", res.Code)
		}
		if post("/oauth/email/request", c, form, origin).Code != 400 {
			t.Fatal("address submission reused")
		}
		body := res.Body.String()
		match := hidden.FindStringSubmatch(body)
		if len(match) != 2 {
			t.Fatal("missing verification form")
		}
		c2 := res.Result().Cookies()[0]
		if c2.Value == c.Value || csrf == match[1] {
			t.Fatal("session and csrf must rotate")
		}
		return match[1], c2, hidden.ReplaceAllString(body, "CSRF")
	}
	challenge, c, knownBody := ask(email)
	var message delivery
	select {
	case message = <-deliveries:
	case <-time.After(time.Second * 2):
		t.Fatal("missing OTP")
	}
	if message.to != email || !regexp.MustCompile(`^[0-9]{8}$`).MatchString(message.code) {
		t.Fatal("incorrect OTP envelope")
	}
	unknown, other, unknownBody := ask("unknown@example.test")
	if knownBody != unknownBody {
		t.Fatal("response reveals email membership")
	}
	form := url.Values{"code": {message.code}, "csrf": {unknown}}
	if post("/oauth/email/verify", other, form, origin).Code != 403 {
		t.Fatal("unknown account accepted")
	}
	select {
	case <-deliveries:
		t.Fatal("dispatched mail to uninvited address")
	default:
	}
	form.Set("csrf", challenge)
	if post("/oauth/invitation", c, form, origin).Code != 404 {
		t.Fatal("email challenge used as manual invitation session")
	}
	if post("/oauth/email/verify", c, form, "https://attacker.example").Code != 400 {
		t.Fatal("cross-origin OTP accepted")
	}
	form.Set("csrf", "wrong")
	if post("/oauth/email/verify", c, form, origin).Code != 400 {
		t.Fatal("OTP accepted without CSRF")
	}
	form.Set("csrf", challenge)
	if r := post("/oauth/email/verify", c, form, origin); r.Code != 303 || r.Header().Get("Location") != "/oauth/consent" {
		t.Fatalf("correct proof status %d", r.Code)
	}
	if post("/oauth/email/verify", c, form, origin).Code != 400 {
		t.Fatal("OTP replay accepted")
	}
}
