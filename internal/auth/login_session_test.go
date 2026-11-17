package auth

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newLoginTest(t *testing.T, secure bool) *Login {
	t.Helper()
	return &Login{secure: secure, sessions: map[string]*browserSession{}}
}
func TestEmailCodeProofIsSingleUseAndBoundToOneSession(t *testing.T) {
	t.Parallel()
	now := time.Now()
	browser := newLoginTest(t, false)
	add := func(key, code string) {
		csrf := RandomNonce()
		browser.sessions[key] = &browserSession{RequestID: "one", Expires: now.Add(time.Minute * 10), CSRF: csrf, emailChallenge: emailChallenge{
			principal: "invited", digest: sha256.Sum256([]byte(csrf + code)), Expires: now.Add(time.Minute), attemptsLeft: emailCodeAttempts,
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
				if next.HumanID != "invited" || next.RequestID != "one" || next.CSRF == "" || !next.emailChallenge.Expires.IsZero() {
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
	t.Parallel()
	start := time.Now()
	b := newLoginTest(t, false)
	for i := range 4 {
		ok := b.emailDelivery.Reserve(start, "a@example.test", "peer")
		if ok != (i < 3) {
			t.Fatal("address budget")
		}
		if ok {
			b.emailDelivery.Finish()
		}
	}
	for i := range 18 {
		ok := b.emailDelivery.Reserve(start, fmt.Sprint(i), "peer")
		if ok != (i < 17) {
			t.Fatal("peer budget")
		}
		if ok {
			b.emailDelivery.Finish()
		}
	}
	for i := range 101 {
		ok := b.emailDelivery.Reserve(start, fmt.Sprint(i+20), fmt.Sprint(i))
		if ok != (i < 100) {
			t.Fatal("global budget")
		}
		if ok {
			b.emailDelivery.Finish()
		}
	}
	for i := range 9 {
		ok := b.emailDelivery.Reserve(start.Add(EmailDeliveryPeriod), fmt.Sprint(i), "peer")
		if ok != (i < 8) {
			t.Fatal("in-flight budget")
		}
	}
	for range 8 {
		b.emailDelivery.Finish()
	}
}

func TestInteractiveOIDCSessionBoundAndSecureCookie(t *testing.T) {
	t.Parallel()
	o := newLoginTest(t, true)
	recorder := httptest.NewRecorder()
	o.Cookie(recorder, "opaque", 600)
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != "__Host-porcupine_oidc" || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode || cookies[0].Path != "/" || cookies[0].Domain != "" {
		t.Fatal("HTTPS login cookie lost protections")
	}
	for range loginSessionCapacity {
		if _, ok := o.save(&browserSession{Expires: time.Now().Add(time.Minute)}); !ok {
			t.Fatal("premature session rejection")
		}
	}
	if _, ok := o.save(&browserSession{Expires: time.Now().Add(time.Minute)}); ok {
		t.Fatal("unbounded browser session allocation")
	}
}
