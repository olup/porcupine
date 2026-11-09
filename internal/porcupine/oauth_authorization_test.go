package porcupine

import (
	"context"
	"encoding/json"
	"golang.org/x/time/rate"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func interactiveCode(t *testing.T, f *oidcFixture) string {
	t.Helper()
	csrf := f.login(t)
	target := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, 303)
	redirect, err := url.Parse(target)
	if err != nil || redirect.Query().Get("code") == "" {
		t.Fatal("missing authorization code")
	}
	return redirect.Query().Get("code")
}
func interactiveTokenForm(f *oidcFixture, code string) url.Values {
	return url.Values{"grant_type": {"authorization_code"}, "client_id": {f.clientID}, "redirect_uri": {f.callback}, "code": {code}, "code_verifier": {f.verifier}, "resource": {f.host.URL + "/mcp"}}
}
func interactiveEndpoint(t *testing.T, s *Service, path string, form url.Values, basicID, secret string, status int) map[string]any {
	t.Helper()
	r := httptest.NewRequest("POST", path, strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if basicID != "" {
		r.SetBasicAuth(url.QueryEscape(basicID), url.QueryEscape(secret))
	}
	w := httptest.NewRecorder()
	if path == "/oauth/revoke" {
		s.oauthRevoke(w, r)
	} else {
		s.oauthToken(w, r)
	}
	if w.Code != status {
		t.Fatalf("OAuth endpoint got %d want %d: %s", w.Code, status, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("Pragma") != "no-cache" {
		t.Fatal("cacheable token response")
	}
	value := map[string]any{}
	if w.Body.Len() > 0 && json.Unmarshal(w.Body.Bytes(), &value) != nil {
		t.Fatal("invalid OAuth response")
	}
	return value
}

func TestOAuthAuthorizationRejectsCallbackForgeryAndPKCEDowngrade(t *testing.T) {
	f := newOIDCFixture(t)
	for _, change := range []func(url.Values){
		func(q url.Values) { q.Set("redirect_uri", f.callback+"/unregistered") },
		func(q url.Values) { q.Set("redirect_uri", "https://attacker.example/callback") },
		func(q url.Values) { q.Set("code_challenge_method", "plain") },
		func(q url.Values) { q.Del("code_challenge_method") },
		func(q url.Values) { q.Set("code_challenge", "bad") },
		func(q url.Values) { q.Set("scope", "events:write") },
		func(q url.Values) { q.Set("resource", f.host.URL+"/api/v1") },
		func(q url.Values) { q.Add("redirect_uri", f.callback) },
	} {
		target, _ := url.Parse(f.authorizeURL(f.callback))
		q := target.Query()
		change(q)
		target.RawQuery = q.Encode()
		location, _ := f.get(t, target.String(), 400)
		if location != "" {
			t.Fatal("invalid request redirected browser")
		}
	}
	for _, raw := range []string{"https://user:secret@example.com/cb", "https://example.com/cb#fragment", "http://example.com/cb", "https://example.com/cb?code=preloaded", "https://*.example.com/cb"} {
		if validOAuthRedirect(raw) {
			t.Fatalf("unsafe registered redirect accepted: %s", raw)
		}
	}
}

func TestOAuthAuthorizationCodePKCEOnceOnlyAndHashedStorage(t *testing.T) {
	f := newOIDCFixture(t)
	code := interactiveCode(t, f)
	wrong := interactiveTokenForm(f, code)
	wrong.Set("code_verifier", strings.Repeat("x", 43))
	interactiveEndpoint(t, f.s, "/oauth/token", wrong, "", "", 400)
	correct := interactiveTokenForm(f, code)
	response := interactiveEndpoint(t, f.s, "/oauth/token", correct, "", "", 200)
	access, refresh := response["access_token"].(string), response["refresh_token"].(string)
	var storedCode, storedRefresh string
	if f.s.db.QueryRow(`SELECT code_hash FROM oauth_authorization_codes`).Scan(&storedCode) != nil || storedCode != hash(code) || storedCode == code {
		t.Fatal("authorization code not hash-only")
	}
	if f.s.db.QueryRow(`SELECT token_hash FROM oauth_refresh_tokens`).Scan(&storedRefresh) != nil || storedRefresh != hash(refresh) || storedRefresh == refresh {
		t.Fatal("refresh token not hash-only")
	}
	if _, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp"); err != nil {
		t.Fatal(err)
	}
	interactiveEndpoint(t, f.s, "/oauth/token", correct, "", "", 400)
	if _, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("reused code did not revoke its issued authority")
	}
}

func TestOAuthRefreshRotationReplayRevokesFamilyAndCannotExpand(t *testing.T) {
	f := newOIDCFixture(t)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	first := tokens["refresh_token"].(string)
	form := url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {first}, "scope": {"events:read"}}
	second := interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 200)
	if second["refresh_token"] == first || second["scope"] != "events:read" {
		t.Fatal("refresh did not rotate/downscope")
	}
	expanded := url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {second["refresh_token"].(string)}, "scope": {"events:read management:read"}}
	interactiveEndpoint(t, f.s, "/oauth/token", expanded, "", "", 400)
	interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 400)
	if _, err := f.s.authenticateOAuthToken(context.Background(), second["access_token"].(string), f.host.URL+"/mcp"); err == nil {
		t.Fatal("replay left descendant access token active")
	}
	expanded.Del("scope")
	interactiveEndpoint(t, f.s, "/oauth/token", expanded, "", "", 400)
}

func TestOAuthInteractiveCredentialsAndRedirectAreClientBound(t *testing.T) {
	for _, method := range []string{"client_secret_basic", "client_secret_post"} {
		t.Run(method, func(t *testing.T) {
			f := newOIDCFixture(t)
			code := interactiveCode(t, f)
			if _, err := f.s.db.Exec(`UPDATE oauth_clients SET token_endpoint_auth_method=?,secret_hash=? WHERE client_id=?`, method, hash("confidential-secret"), f.clientID); err != nil {
				t.Fatal(err)
			}
			form := interactiveTokenForm(f, code)
			interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 401)
			if method == "client_secret_basic" {
				interactiveEndpoint(t, f.s, "/oauth/token", form, f.clientID, "wrong-secret", 401)
				interactiveEndpoint(t, f.s, "/oauth/token", form, f.clientID, "confidential-secret", 200)
			} else {
				form.Set("client_secret", "confidential-secret")
				interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 200)
			}
		})
	}
	f := newOIDCFixture(t)
	code := interactiveCode(t, f)
	wrong := interactiveTokenForm(f, code)
	wrong.Set("redirect_uri", f.callback+"/wrong")
	interactiveEndpoint(t, f.s, "/oauth/token", wrong, "", "", 400)
	wrong = interactiveTokenForm(f, code)
	wrong.Set("client_secret", "")
	interactiveEndpoint(t, f.s, "/oauth/token", wrong, "", "", 400)
	interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, code), "", "", 200)
}

func TestOAuthInteractiveGrantExpiryAppliesToWorkerReadAndTransaction(t *testing.T) {
	f := newOIDCFixture(t)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	access := tokens["access_token"].(string)
	principal, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.exportActor(context.Background(), principal.ID); err != nil {
		t.Fatal("new grant not authorized", err)
	}
	if _, err = f.s.db.Exec(`UPDATE oauth_interactive_grants SET expires_at_ms=? WHERE principal_id=?`, f.s.clock().Add(-time.Second).UnixMilli(), principal.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.exportActor(context.Background(), principal.ID); err == nil {
		t.Fatal("expired grant retained background read authority")
	}
	tx, err := f.s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = f.s.exportActorFrom(context.Background(), tx, principal.ID); err == nil {
		t.Fatal("expired grant retained publication transaction authority")
	}
}

func TestOAuthInteractiveRevocationAndAdminGrantManagement(t *testing.T) {
	f := newOIDCFixture(t)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	access := tokens["access_token"].(string)
	principal, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	admin := oauthProtocolRequest(t, f.s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + "/api/v1"}}, 200)["access_token"].(string)
	grants := request(t, f.s, "GET", "/api/v1/oauth-grants", admin, "", 200)
	entries := grants["grants"].([]any)
	if len(entries) != 1 || entries[0].(map[string]any)["id"] != principal.ID || entries[0].(map[string]any)["subject"] != "operator-subject" {
		t.Fatal("admin cannot inspect human grant identity", grants)
	}
	request(t, f.s, "DELETE", "/api/v1/oauth-grants/"+principal.ID, admin, "", 204)
	if _, err = f.s.exportActor(context.Background(), principal.ID); err == nil {
		t.Fatal("grant revoke retained background authority")
	}
	if _, err = f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("grant revoke retained bearer authority")
	}
	interactiveEndpoint(t, f.s, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {tokens["refresh_token"].(string)}}, "", "", 400)
}

func TestOAuthInteractiveTokenRevocationEndpointAndAudienceIsolation(t *testing.T) {
	f := newOIDCFixture(t)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	access := tokens["access_token"].(string)
	if _, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/api/v1"); err == nil {
		t.Fatal("interactive MCP token authorized API/import resource")
	}
	request(t, f.s, "GET", "/api/v1/oauth-grants", access, "", 401)
	interactiveEndpoint(t, f.s, "/oauth/revoke", url.Values{"client_id": {f.clientID}, "token": {"unknown-token"}}, "", "", 200)
	if _, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp"); err != nil {
		t.Fatal("unknown revocation invalidated unrelated token")
	}
	interactiveEndpoint(t, f.s, "/oauth/revoke", url.Values{"client_id": {f.clientID}, "token": {tokens["refresh_token"].(string)}}, "", "", 200)
	if _, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("refresh revocation left grant active")
	}
}

func TestOAuthInteractiveDiscoveryRequiresOIDCAndPKCES256(t *testing.T) {
	s, _ := oauthProtocolService(t)
	machine := request(t, s, "GET", "/.well-known/oauth-authorization-server", "", "", 200)
	if machine["authorization_endpoint"] != nil || machine["code_challenge_methods_supported"] != nil {
		t.Fatal("machine-only instance advertises interactive login")
	}
	f := newOIDCFixture(t)
	enabled := request(t, f.s, "GET", "/.well-known/oauth-authorization-server", "", "", 200)
	if enabled["authorization_endpoint"] != f.host.URL+"/oauth/authorize" || enabled["authorization_response_iss_parameter_supported"] != true {
		t.Fatal("interactive discovery missing issuer-bound endpoint")
	}
	pkce := enabled["code_challenge_methods_supported"].([]any)
	if len(pkce) != 1 || pkce[0] != "S256" {
		t.Fatal("discovery permits PKCE downgrade")
	}
}

func TestOAuthInteractiveMetadataRecommendsOnlyReadScopes(t *testing.T) {
	f := newOIDCFixture(t)
	for _, path := range []string{"/.well-known/oauth-authorization-server", "/.well-known/oauth-protected-resource/mcp"} {
		result := request(t, f.s, "GET", path, "", "", 200)
		scopes := result["scopes_supported"].([]any)
		if len(scopes) != 2 || scopes[0] != "events:read" || scopes[1] != "management:read" {
			t.Fatal("interactive discovery advertises write/import/admin scopes", result)
		}
	}
	recorder := httptest.NewRecorder()
	f.s.authChallenge(recorder, f.host.URL+"/mcp", false)
	if !strings.Contains(recorder.Header().Get("WWW-Authenticate"), `scope="events:read management:read"`) {
		t.Fatal("MCP challenge omitted least-privilege recommendation")
	}
	code := interactiveCode(t, f)
	form := interactiveTokenForm(f, code)
	form.Set("resource", "")
	if result := interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 400); result["error"] != "invalid_target" {
		t.Fatal("explicit empty resource fell back")
	}
	form.Set("resource", f.host.URL+"/api/v1")
	if result := interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 400); result["error"] != "invalid_target" {
		t.Fatal("API resource accepted interactive grant")
	}
	form.Del("resource")
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", form, "", "", 200)
	if tokens["resource"] != f.host.URL+"/mcp" {
		t.Fatal("omitted interactive resource fell back to API")
	}
}

func TestOAuthInteractiveMissingAuxiliaryMappingCannotBroadenAuthority(t *testing.T) {
	f := newOIDCFixture(t)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, interactiveCode(t, f)), "", "", 200)
	raw := tokens["access_token"].(string)
	before, err := f.s.authenticateOAuthToken(context.Background(), raw, f.host.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.db.Exec(`DELETE FROM oauth_interactive_access`); err != nil {
		t.Fatal(err)
	}
	after, err := f.s.authenticateOAuthToken(context.Background(), raw, f.host.URL+"/mcp")
	if err != nil || after.ID != before.ID || authorize(after, "events:read", "project-b") {
		t.Fatal("missing auxiliary map changed human token to machine authority", after, err)
	}
	interactiveEndpoint(t, f.s, "/oauth/revoke", url.Values{"client_id": {f.clientID}, "token": {raw}}, "", "", 200)
	if _, err = f.s.authenticateOAuthToken(context.Background(), raw, f.host.URL+"/mcp"); err == nil {
		t.Fatal("access token revocation required missing auxiliary map")
	}
}

func TestOAuthAuthorizationStartAdmissionIsIndependentAndBounded(t *testing.T) {
	f := newOIDCFixture(t)
	f.s.oauth.limiter = rate.NewLimiter(0, 1)
	accepted, rejected := 0, 0
	for range 30 {
		r := httptest.NewRequest("GET", f.authorizeURL(f.callback), nil)
		r.RemoteAddr = "192.0.2.88:1234"
		w := httptest.NewRecorder()
		f.s.oauthAuthorize(w, r)
		if w.Code == 303 {
			accepted++
		} else if w.Code == 429 {
			rejected++
		} else {
			t.Fatalf("unexpected admission status %d", w.Code)
		}
	}
	var pending int
	if f.s.db.QueryRow(`SELECT count(*) FROM oauth_authorization_requests`).Scan(&pending) != nil || accepted > 5 || accepted == 0 || rejected == 0 || pending != accepted {
		t.Fatal("authorization start flood not bounded", accepted, rejected, pending)
	}
	if !f.s.oauth.limiter.Allow() {
		t.Fatal("unauthenticated authorization flood consumed token issuance budget")
	}
	var peerHash string
	if f.s.db.QueryRow(`SELECT peer_hash FROM oauth_authorization_requests LIMIT 1`).Scan(&peerHash) != nil || len(peerHash) != 64 || strings.Contains(peerHash, "192.0.2") {
		t.Fatal("raw IP stored in pending authorization")
	}
}

func TestOAuthInteractiveCrossClientProofsCannotRedeemOrRevoke(t *testing.T) {
	f := newOIDCFixture(t)
	code := interactiveCode(t, f)
	other, _ := oauthProtocolClient(t, f.s, []string{"events:read"}, []string{"*"})
	redirects, _ := json.Marshal([]string{f.callback})
	if _, err := f.s.db.Exec(`UPDATE oauth_clients SET grant_types='["authorization_code","refresh_token"]',redirect_uris=?,token_endpoint_auth_method='none' WHERE client_id=?`, string(redirects), other); err != nil {
		t.Fatal(err)
	}
	wrong := interactiveTokenForm(f, code)
	wrong.Set("client_id", other)
	interactiveEndpoint(t, f.s, "/oauth/token", wrong, "", "", 400)
	tokens := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, code), "", "", 200)
	refresh := tokens["refresh_token"].(string)
	access := tokens["access_token"].(string)
	interactiveEndpoint(t, f.s, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {other}, "refresh_token": {refresh}}, "", "", 400)
	interactiveEndpoint(t, f.s, "/oauth/revoke", url.Values{"client_id": {other}, "token": {refresh}}, "", "", 200)
	principal, err := f.s.authenticateOAuthToken(context.Background(), access, f.host.URL+"/mcp")
	if err != nil {
		t.Fatal("cross-client invalid proof revoked victim", err)
	}
	if _, err = f.s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), f.clientID); err != nil {
		t.Fatal(err)
	}
	var revoked *string
	if f.s.db.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=?`, principal.ID).Scan(&revoked) != nil || revoked == nil {
		t.Fatal("client revocation did not durably revoke consent principal")
	}
	if _, err = f.s.exportActor(context.Background(), principal.ID); err == nil {
		t.Fatal("client revoked but background actor active")
	}
}

func TestOAuthInteractiveUnusedCodeExpires(t *testing.T) {
	f := newOIDCFixture(t)
	code := interactiveCode(t, f)
	later := f.s.clock().Add(oauthCodeTTL + time.Second)
	f.s.clock = func() time.Time { return later }
	response := interactiveEndpoint(t, f.s, "/oauth/token", interactiveTokenForm(f, code), "", "", 400)
	if response["error"] != "invalid_grant" {
		t.Fatal("expired code accepted")
	}
}
