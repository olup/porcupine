package porcupine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

func recoveryOperatorContext(t *testing.T, s *Service) context.Context {
	t.Helper()
	var id string
	if err := s.control.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	current, err := s.loadCredentialActorFrom(t.Context(), s.control, id)
	if err != nil {
		t.Fatal(err)
	}
	return context.WithValue(t.Context(), actorContextKey{}, current)
}

func unlinkFixtureIdentity(t *testing.T, f *oidcFixture, s *Service) string {
	t.Helper()
	var id string
	if err := s.control.QueryRow(`SELECT id FROM oidc_identities WHERE principal_id=$1`, f.humanID).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := s.managementUnlinkHumanIdentity(recoveryOperatorContext(t, s), &HumanIdentityDeleteInput{ID: f.humanID, IdentityID: id}); err != nil {
		t.Fatal(err)
	}
	return id
}

func pendingFixtureSession(t *testing.T, f *oidcFixture) *oidcBrowserSession {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, f.host.URL+"/oauth/consent", nil)
	target, _ := url.Parse(f.host.URL)
	for _, cookie := range f.client.Jar.Cookies(target) {
		request.AddCookie(cookie)
	}
	_, session := f.s.oauth.browser.session(request)
	if session == nil || session.humanID == "" || !session.proof.Verified {
		t.Fatal("missing authenticated proof")
	}
	return session
}

func assertLateSavedProofCannotIssue(t *testing.T, f *oidcFixture, unlinkWorker, issueWorker *Service) {
	t.Helper()
	csrf := f.login(t)
	old := *pendingFixtureSession(t, f)
	request, err := f.s.loadOAuthRequest(t.Context(), f.s.control, old.requestID)
	if err != nil {
		t.Fatal(err)
	}
	unlinkFixtureIdentity(t, f, unlinkWorker)
	// Simulate the callback having verified/bound before unlink, but saving its
	// authenticated browser session only after the operator's transaction commits.
	raw, ok := issueWorker.oauth.browser.save(&old)
	if !ok {
		t.Fatal("could not persist delayed authenticated session")
	}
	f.client.Jar.SetCookies(mustRecoveryURL(t, f.host.URL), []*http.Cookie{{Name: issueWorker.oauth.browser.cookieName(), Value: raw, Path: "/"}})
	f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, http.StatusForbidden)
	approved := actor{ID: f.humanID, Permissions: []string{"events:read", "management:read"}, Projects: []string{"project-a"}}
	if _, err := issueWorker.issueOAuthAuthorizationCode(t.Context(), request, f.humanID, old.proof, approved); err == nil {
		t.Fatal("stale proof minted authorization code")
	}
	if _, err := issueWorker.issueOAuthAuthorizationCode(t.Context(), request, f.humanID, humanLoginProof{Version: 1}, approved); err == nil {
		t.Fatal("missing proof accepted at version1")
	}
}

func mustRecoveryURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestOIDCRecoveryInvalidatesDelayedAuthenticatedSession(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	assertLateSavedProofCannotIssue(t, f, f.s, f.s)
	// Unlink is recovery rather than a ban: a fresh signed provider journey works.
	f.issueToken(t, "project-a")
}

func assertPendingOTPInvalidated(t *testing.T, f *oidcFixture, worker *Service) {
	t.Helper()
	f.login(t)
	_, page := f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	_, verification := f.postLogin(t, "/oauth/email/request", url.Values{"csrf": {csrfFromJourneyForm(t, page)}, "email": {"operator@example.test"}}, f.host.URL, http.StatusOK)
	var code string
	select {
	case code = <-f.mail:
	case <-time.After(5 * time.Second):
		t.Fatal("OTP missing")
	}
	unlinkFixtureIdentity(t, f, worker)
	f.postLogin(t, "/oauth/email/verify", url.Values{"csrf": {csrfFromJourneyForm(t, verification)}, "code": {code}}, f.host.URL, http.StatusForbidden)
	// The next challenge snapshots the new epoch and remains usable without OIDC.
	_, page = f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	_, verification = f.postLogin(t, "/oauth/email/request", url.Values{"csrf": {csrfFromJourneyForm(t, page)}, "email": {"operator@example.test"}}, f.host.URL, http.StatusOK)
	select {
	case code = <-f.mail:
	case <-time.After(5 * time.Second):
		t.Fatal("fresh OTP missing")
	}
	consent, _ := f.postLogin(t, "/oauth/email/verify", url.Values{"csrf": {csrfFromJourneyForm(t, verification)}, "code": {code}}, f.host.URL, http.StatusSeeOther)
	_, body := f.get(t, f.host.URL+consent, http.StatusOK)
	f.submit(t, url.Values{"csrf": {csrfFromJourneyForm(t, body)}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, http.StatusSeeOther)
}

func TestOIDCRecoveryInvalidatesPendingEmailChallenge(t *testing.T) {
	t.Parallel()
	f := newOIDCFixtureWithOTP(t, true)
	assertPendingOTPInvalidated(t, f, f.s)
}

func TestSharedOIDCRecoveryFencesProofsAcrossWorkers(t *testing.T) {
	t.Parallel()
	t.Run("late-session-save", func(t *testing.T) {
		f, a, b, _ := newSharedOIDCFixture(t)
		tokens := identityRecoveryIssue(t, f, "project-a")
		unredeemed := identityRecoveryCode(t, f, "project-a")
		assertLateSavedProofCannotIssue(t, f, a, b)
		for _, worker := range []*Service{a, b} {
			if _, err := worker.authenticateResource(t.Context(), tokens.access, f.resource); err == nil {
				t.Fatal("old token survived cross-worker unlink")
			}
		}
		interactiveEndpoint(t, b, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {tokens.refresh}, "resource": {f.resource}}, "", "", http.StatusBadRequest)
		interactiveEndpoint(t, b, "/oauth/token", unredeemed, "", "", http.StatusBadRequest)
		f.issueToken(t, "project-a")
	})
	t.Run("pending-email-challenge", func(t *testing.T) {
		f, _, b, _ := newSharedOIDCFixture(t)
		assertPendingOTPInvalidated(t, f, b)
	})
}

func TestOIDCRecoveryInvalidatesPendingTenantConsent(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	tenant := request(t, f.s, "POST", "/api/v1/tenants", operator, `{"name":"Recovery team"}`, 201)["tenant"].(map[string]any)["id"].(string)
	request(t, f.s, "POST", "/api/v1/tenants/"+tenant+"/members", operator, `{"email":"operator@example.test","role":"admin"}`, 201)
	f.clientID, f.callback, f.scope, f.resource = "porcupine-local-tenant-admin", localOAuthCallback, tenantAdminScope, f.host.URL+"/api/v1"
	csrf := f.login(t)
	unlinkFixtureIdentity(t, f, f.s)
	f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "tenant": {tenant}}, f.host.URL, http.StatusForbidden)
	f.issueSelectionToken(t, "tenant", tenant)
}
