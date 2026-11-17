package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func sharedBrowserPair(t *testing.T) (*Login, *Login) {
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
	// Use the production DDL, without importing the native server into pure Go
	// auth tests. Service-level shared journeys separately verify initialization.
	for _, file := range []string{"control_schema.sql", "shared_control_schema.sql"} {
		schema, err := os.ReadFile(filepath.Join("..", "porcupine", file))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		_, err = first.ExecContext(ctx, string(schema))
		cancel()
		if err != nil {
			t.Fatal(err)
		}
	}
	a, b := newLoginTest(t, false), newLoginTest(t, false)
	a.ShareSessions(first)
	b.ShareSessions(second)
	return a, b
}

func TestSharedBrowserSessionTransferAndConsume(t *testing.T) {
	t.Parallel()
	a, b := sharedBrowserPair(t)
	want := &browserSession{RequestID: "request-a", state: "state-a", nonce: "nonce-a", verifier: "verifier-a", CSRF: "csrf-a", Expires: time.Now().Add(time.Minute)}
	raw, ok := a.save(want)
	if !ok {
		t.Fatal("first worker could not save browser session")
	}
	request := httptest.NewRequest("GET", "http://localhost/oauth/oidc/callback", nil)
	request.AddCookie(&http.Cookie{Name: b.CookieName(), Value: raw})
	key, got := b.browserSession(request)
	if got == nil || got.RequestID != want.RequestID || got.state != want.state || got.nonce != want.nonce || got.verifier != want.verifier || key != loginHash(raw) {
		t.Fatal("second worker could not recover complete OIDC session")
	}
	var storedKey string
	if err := a.sharedDB.QueryRow(`SELECT session_hash FROM oauth_browser_sessions`).Scan(&storedKey); err != nil || storedKey == raw {
		t.Fatal("raw browser cookie persisted", err)
	}
	var consumed atomic.Int32
	var wg sync.WaitGroup
	for _, browser := range []*Login{a, b} {
		wg.Add(1)
		go func(o *Login) {
			defer wg.Done()
			if o.Consume(key) {
				consumed.Add(1)
			}
		}(browser)
	}
	wg.Wait()
	if consumed.Load() != 1 || a.loadSharedSession(context.Background(), key) != nil {
		t.Fatal("browser state could be consumed twice across workers")
	}
	if _, ok := a.save(&browserSession{Expires: time.Now().Add(-time.Second)}); ok {
		t.Fatal("expired browser session accepted")
	}
	if _, err := a.sharedDB.Exec(`INSERT INTO oauth_browser_sessions(session_hash,payload,expires_at_ms) SELECT 'capacity-'||i,'{}',$1 FROM generate_series(1,$2) i`, time.Now().Add(time.Minute).UnixMilli(), loginSessionCapacity); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.save(want); ok {
		t.Fatal("browser capacity multiplied by worker count")
	}
}

func TestSharedBrowserOTPAtomicAttemptsAndOneUse(t *testing.T) {
	t.Parallel()
	a, b := sharedBrowserPair(t)
	now := time.Now()
	const code = "82345678"
	session := &browserSession{RequestID: "oauth-request", CSRF: "challenge", Expires: now.Add(time.Minute), emailChallenge: emailChallenge{
		digest: sha256.Sum256([]byte("challenge" + code)), principal: "human-a", attemptsLeft: emailCodeAttempts, Expires: now.Add(time.Minute)}}
	raw, ok := a.save(session)
	if !ok {
		t.Fatal("could not save OTP session")
	}
	key := loginHash(raw)
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
				if next.HumanID != "human-a" || next.RequestID != "oauth-request" || next.CSRF == session.CSRF {
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
	key = loginHash(raw)
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
	if _, ok := b.redeemEmailCode(loginHash(raw), code, now.Add(2*time.Minute)); ok {
		t.Fatal("expired OTP accepted")
	}
}

func TestSharedBrowserEmailDeliveryBudgetsAndCrashExpiry(t *testing.T) {
	t.Parallel()
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
