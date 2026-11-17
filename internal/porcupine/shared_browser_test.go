package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sharedBrowserPair(t *testing.T) (*oauthBrowser, *oauthBrowser) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	first, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sql.Open("pgx", dsn)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close(); second.Close() })
	a, b := newOAuthBrowser(false), newOAuthBrowser(false)
	s := &Service{control: first, shared: &sharedRuntime{}, oauth: &oauthRuntime{browser: a}}
	if err := s.initializeSharedBrowser(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.sharedDB = second
	return a, b
}

func TestSharedBrowserSessionTransferAndConsume(t *testing.T) {
	a, b := sharedBrowserPair(t)
	want := &oidcBrowserSession{requestID: "request-a", state: "state-a", nonce: "nonce-a", verifier: "verifier-a", csrf: "csrf-a", expires: time.Now().Add(time.Minute)}
	raw, ok := a.save(want)
	if !ok {
		t.Fatal("first worker could not save browser session")
	}
	request := httptest.NewRequest("GET", "http://localhost/oauth/oidc/callback", nil)
	request.AddCookie(&http.Cookie{Name: b.cookieName(), Value: raw})
	key, got := b.session(request)
	if got == nil || got.requestID != want.requestID || got.state != want.state || got.nonce != want.nonce || got.verifier != want.verifier || key != hash(raw) {
		t.Fatal("second worker could not recover complete OIDC session")
	}
	var storedKey string
	if err := a.sharedDB.QueryRow(`SELECT session_hash FROM oauth_browser_sessions`).Scan(&storedKey); err != nil || storedKey == raw {
		t.Fatal("raw browser cookie persisted", err)
	}
	var consumed atomic.Int32
	var wg sync.WaitGroup
	for _, browser := range []*oauthBrowser{a, b} {
		wg.Add(1)
		go func(o *oauthBrowser) {
			defer wg.Done()
			if o.consume(key) {
				consumed.Add(1)
			}
		}(browser)
	}
	wg.Wait()
	if consumed.Load() != 1 || a.loadSharedSession(context.Background(), key) != nil {
		t.Fatal("browser state could be consumed twice across workers")
	}
	if _, ok := a.save(&oidcBrowserSession{expires: time.Now().Add(-time.Second)}); ok {
		t.Fatal("expired browser session accepted")
	}
	if _, err := a.sharedDB.Exec(`INSERT INTO oauth_browser_sessions(session_hash,payload,expires_at_ms) SELECT 'capacity-'||i,'{}',$1 FROM generate_series(1,$2) i`, time.Now().Add(time.Minute).UnixMilli(), oidcSessionCapacity); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.save(want); ok {
		t.Fatal("browser capacity multiplied by worker count")
	}
}

func TestSharedBrowserOTPAtomicAttemptsAndOneUse(t *testing.T) {
	a, b := sharedBrowserPair(t)
	now := time.Now()
	const code = "82345678"
	session := &oidcBrowserSession{requestID: "oauth-request", csrf: "challenge", expires: now.Add(time.Minute), emailChallenge: emailChallenge{
		digest: sha256.Sum256([]byte("challenge" + code)), principal: "human-a", attemptsLeft: emailCodeAttempts, expires: now.Add(time.Minute)}}
	raw, ok := a.save(session)
	if !ok {
		t.Fatal("could not save OTP session")
	}
	key := hash(raw)
	var persisted string
	if err := a.sharedDB.QueryRow(`SELECT payload FROM oauth_browser_sessions WHERE session_hash=$1`, key).Scan(&persisted); err != nil || strings.Contains(persisted, code) {
		t.Fatal("plaintext code persisted", err)
	}
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			browser := a
			if i%2 == 1 {
				browser = b
			}
			next, ok := browser.redeemEmailCode(key, code, now)
			if ok {
				if next.humanID != "human-a" || next.requestID != "oauth-request" || next.csrf == session.csrf {
					t.Error("verified session lost identity or CSRF rotation")
				}
				successes.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatalf("OTP succeeded %d times", successes.Load())
	}
	raw, ok = a.save(session)
	if !ok {
		t.Fatal("could not save attempts session")
	}
	key = hash(raw)
	for i := 0; i < emailCodeAttempts; i++ {
		browser := a
		if i%2 == 1 {
			browser = b
		}
		if _, ok := browser.redeemEmailCode(key, "wrong", now); ok {
			t.Fatal("wrong OTP accepted")
		}
	}
	if _, ok := b.redeemEmailCode(key, code, now); ok {
		t.Fatal("attempt limit reset by moving to another worker")
	}
	raw, ok = a.save(session)
	if !ok {
		t.Fatal("could not save expiry session")
	}
	if _, ok := b.redeemEmailCode(hash(raw), code, now.Add(2*time.Minute)); ok {
		t.Fatal("expired OTP accepted")
	}
}

func TestSharedBrowserEmailDeliveryBudgetsAndCrashExpiry(t *testing.T) {
	a, b := sharedBrowserPair(t)
	now := time.Now()
	for i := 0; i < 3; i++ {
		browser := a
		if i%2 == 1 {
			browser = b
		}
		release, ok := browser.reserveEmailAttempt(now, "person@example.test", "peer")
		if !ok {
			t.Fatal("valid email attempt refused")
		}
		release()
		release()
	}
	if _, ok := b.reserveEmailAttempt(now, "person@example.test", "other-peer"); ok {
		t.Fatal("address limit multiplied by replica count")
	}
	var releases []func()
	for i := 0; i < 8; i++ {
		release, ok := a.reserveEmailAttempt(now, fmt.Sprintf("person-%d@example.test", i), fmt.Sprint(i))
		if !ok {
			t.Fatalf("concurrent email attempt %d refused", i)
		}
		releases = append(releases, release)
	}
	if _, ok := b.reserveEmailAttempt(now, "overflow@example.test", "overflow"); ok {
		t.Fatal("in-flight mail budget multiplied by replica count")
	}
	// An abandoned sending process cannot consume a place permanently.
	release, ok := b.reserveEmailAttempt(now.Add(21*time.Second), "after-crash@example.test", "recovery")
	if !ok {
		t.Fatal("expired sending permits were not recovered")
	}
	release()
	for _, release := range releases {
		release()
	}
}
