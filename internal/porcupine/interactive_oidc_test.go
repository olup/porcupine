package porcupine

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
)

type oidcMock struct {
	host               *httptest.Server
	key                *rsa.PrivateKey
	mu                 sync.Mutex
	codes              map[string]url.Values
	subject            string
	email              string
	verified           bool
	lastIDToken        string
	expired            bool
	badNonce           bool
	badAudience        bool
	badIssuer          bool
	badSignature       bool
	badAuthorizedParty bool
	multipleAudience   bool
}

func newOIDCMock(t *testing.T) *oidcMock {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &oidcMock{key: key, codes: map[string]url.Values{}, subject: "operator-subject", email: "operator@example.test", verified: true}
	p.host = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			write(w, 200, map[string]any{"issuer": p.host.URL, "authorization_endpoint": p.host.URL + "/authorize", "token_endpoint": p.host.URL + "/token", "jwks_uri": p.host.URL + "/keys", "response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"}, "id_token_signing_alg_values_supported": []string{"RS256"}})
		case "/keys":
			write(w, 200, map[string]any{"keys": []any{map[string]any{"kty": "RSA", "use": "sig", "kid": "test-key", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
		case "/authorize":
			q := r.URL.Query()
			scopes := strings.Fields(q.Get("scope"))
			for _, scope := range scopes {
				if !slices.Contains([]string{"openid", "email", "profile"}, scope) {
					http.Error(w, "unexpected upstream scope", 400)
					return
				}
			}
			if q.Get("nonce") == "" || q.Get("code_challenge_method") != "S256" || q.Get("client_id") != "upstream-client" || !slices.Contains(scopes, "openid") || !slices.Contains(scopes, "email") {
				http.Error(w, "invalid mock authorize", 400)
				return
			}
			code := auth.RandomNonce()
			p.mu.Lock()
			p.codes[code] = q
			p.mu.Unlock()
			target, _ := url.Parse(q.Get("redirect_uri"))
			query := target.Query()
			query.Set("code", code)
			query.Set("state", q.Get("state"))
			target.RawQuery = query.Encode()
			http.Redirect(w, r, target.String(), 303)
		case "/token":
			_ = r.ParseForm()
			id, secret, ok := r.BasicAuth()
			p.mu.Lock()
			q := p.codes[r.PostForm.Get("code")]
			delete(p.codes, r.PostForm.Get("code"))
			subject, email, verified, expired := p.subject, p.email, p.verified, p.expired
			nonceBad, audBad, issuerBad, signatureBad := p.badNonce, p.badAudience, p.badIssuer, p.badSignature
			partyBad, multipleAudience := p.badAuthorizedParty, p.multipleAudience
			p.mu.Unlock()
			challenge := sha256.Sum256([]byte(r.PostForm.Get("code_verifier")))
			if !ok || id != "upstream-client" || secret != "mock-client-secret" || q == nil || q.Get("code_challenge") != base64.RawURLEncoding.EncodeToString(challenge[:]) || q.Get("redirect_uri") != r.PostForm.Get("redirect_uri") {
				http.Error(w, "invalid mock code", 400)
				return
			}
			nonce := q.Get("nonce")
			if nonceBad {
				nonce = "wrong-nonce"
			}
			aud := "upstream-client"
			if audBad {
				aud = "other-client"
			}
			issuer := p.host.URL
			if issuerBad {
				issuer = "https://wrong-issuer.example"
			}
			expiry := time.Now().Add(time.Minute)
			if expired {
				expiry = time.Now().Add(-time.Minute)
			}
			claims := jwt.MapClaims{"iss": issuer, "sub": subject, "aud": aud, "exp": expiry.Unix(), "iat": time.Now().Unix(), "nonce": nonce, "email": email, "email_verified": verified}
			if partyBad {
				claims["azp"] = "other-client"
			}
			if multipleAudience {
				claims["aud"] = []string{"upstream-client", "other-client"}
			}
			token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
			token.Header["kid"] = "test-key"
			signingKey := p.key
			if signatureBad {
				var err error
				signingKey, err = rsa.GenerateKey(rand.Reader, 2048)
				if err != nil {
					t.Error(err)
					http.Error(w, "signing key", 500)
					return
				}
			}
			signed, err := token.SignedString(signingKey)
			if err != nil {
				t.Error(err)
				http.Error(w, "sign", 500)
				return
			}
			p.mu.Lock()
			p.lastIDToken = signed
			p.mu.Unlock()
			write(w, 200, map[string]any{"access_token": "unused-upstream-access", "token_type": "Bearer", "id_token": signed, "expires_in": 60})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(p.host.Close)
	return p
}

type oidcFixture struct {
	s        *Service
	host     *httptest.Server
	provider *oidcMock
	client   *http.Client
	clientID string
	callback string
	verifier string
	humanID  string
	mail     chan string
	resource string
	scope    string
}

func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	return newOIDCFixtureWithOTP(t, false)
}

func newOIDCFixtureWithOTP(t *testing.T, otp bool) *oidcFixture {
	t.Helper()
	host := httptest.NewUnstartedServer(nil)
	issuer := "http://" + host.Listener.Addr().String()
	p := newOIDCMock(t)
	cfg := &OAuthConfig{Issuer: issuer}
	cfg.OIDC = &OIDCConfig{Issuer: p.host.URL, ClientID: "upstream-client", ClientSecret: "mock-client-secret"}
	mail := make(chan string, 8)
	if otp {
		cfg.Email = &EmailConfig{SendCode: func(_ context.Context, email, code string) error {
			mail <- code
			return nil
		}}
	}
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "auth.db"), BootstrapToken: adminToken, OAuth: cfg})
	if err != nil {
		host.Close()
		t.Fatal(err)
	}
	s.oauth.limiter = rate.NewLimiter(rate.Inf, 1000)
	for _, id := range []string{"project-a", "project-b"} {
		if _, err := s.control.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES(?,?,?,?)`, id, defaultTenantID, id, now()); err != nil {
			t.Fatal(err)
		}
	}
	human := EmailHuman{ID: eventapi.NewID(), Email: "operator@example.test", CreatedAt: now(), Permissions: []string{"events:read", "management:read"}, Projects: []string{"project-a", "project-b"}, Grants: []HumanProjectGrant{
		{ProjectID: "project-a", Permissions: []string{"events:read", "management:read"}},
		{ProjectID: "project-b", Permissions: []string{"events:read", "management:read"}},
	}}
	tx, err := s.control.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertEmailHuman(t.Context(), tx, human); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	id, _ := oauthProtocolClient(t, s, []string{"events:read", "management:read"}, []string{"*"})
	callback := issuer + "/client/callback"
	redirects, _ := json.Marshal([]string{callback, "https://chatgpt.com/connector_platform_oauth_redirect", "https://claude.ai/api/mcp/auth_callback"})
	_, err = s.control.Exec(`UPDATE oauth_clients SET grant_types='["authorization_code","refresh_token"]',redirect_uris=?,token_endpoint_auth_method='none' WHERE client_id=?`, string(redirects), id)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /client/callback", func(w http.ResponseWriter, r *http.Request) { write(w, 200, r.URL.Query()) })
	mux.Handle("/", s.Handler())
	host.Config.Handler = mux
	host.Start()
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(func() { host.Close(); client.CloseIdleConnections(); s.Close() })
	return &oidcFixture{s: s, host: host, provider: p, client: client, clientID: id, callback: callback, verifier: oauth2.GenerateVerifier(), humanID: human.ID, mail: mail, resource: issuer + "/mcp", scope: "events:read management:read"}
}
func (f *oidcFixture) authorizeURL(callback string) string {
	challenge := sha256.Sum256([]byte(f.verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {f.clientID}, "redirect_uri": {callback}, "scope": {f.scope}, "resource": {f.resource}, "state": {"downstream-state"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}}
	return f.host.URL + "/oauth/authorize?" + q.Encode()
}
func (f *oidcFixture) get(t *testing.T, target string, status int) (string, string) {
	t.Helper()
	response, err := f.client.Get(target)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != status {
		t.Fatalf("GET %s got %d want %d: %s", response.Request.URL.Path, response.StatusCode, status, body)
	}
	if response.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("OIDC response cacheable")
	}
	return response.Header.Get("Location"), string(body)
}

func (f *oidcFixture) beginOIDC(t *testing.T) string {
	t.Helper()
	if f.s.oauth.config.Email == nil || f.s.oauth.config.Email.SendCode == nil {
		upstream, _ := f.get(t, f.authorizeURL(f.callback), http.StatusSeeOther)
		return upstream
	}
	_, body := f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	if !strings.Contains(body, `/oauth/email/request`) || !strings.Contains(body, `/oauth/oidc/start`) {
		t.Fatal("combined login does not offer both methods")
	}
	upstream, _ := f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {csrfFromJourneyForm(t, body)}}, f.host.URL, http.StatusSeeOther)
	return upstream
}

func (f *oidcFixture) postLogin(t *testing.T, path string, form url.Values, origin string, status int) (string, string) {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, f.host.URL+path, strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", origin)
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != status {
		t.Fatalf("POST %s got %d want %d: %s %v", path, response.StatusCode, status, body, err)
	}
	return response.Header.Get("Location"), string(body)
}

func (f *oidcFixture) browserHuman(t *testing.T) string {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, f.host.URL+"/oauth/consent", nil)
	target, _ := url.Parse(f.host.URL)
	for _, cookie := range f.client.Jar.Cookies(target) {
		request.AddCookie(cookie)
	}
	_, session := f.s.oauth.browser.Session(request)
	if session == nil || session.HumanID == "" {
		t.Fatal("login did not authenticate a human")
	}
	return session.HumanID
}

func (f *oidcFixture) login(t *testing.T) string {
	t.Helper()
	upstream := f.beginOIDC(t)
	callback, _ := f.get(t, upstream, 303)
	consent, _ := f.get(t, callback, 303)
	_, body := f.get(t, f.host.URL+consent, 200)
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("missing consent CSRF")
	}
	return match[1]
}

func TestInteractiveOIDCSameInvitedHumanThroughOTPAndProvider(t *testing.T) {
	t.Parallel()
	f := newOIDCFixtureWithOTP(t, true)
	f.login(t)
	if got := f.browserHuman(t); got != f.humanID {
		t.Fatalf("OIDC authenticated a different principal: %s", got)
	}
	_, page := f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	_, verification := f.postLogin(t, "/oauth/email/request", url.Values{"csrf": {csrfFromJourneyForm(t, page)}, "email": {"operator@example.test"}}, f.host.URL, http.StatusOK)
	var code string
	select {
	case code = <-f.mail:
	case <-time.After(5 * time.Second):
		t.Fatal("OTP was not delivered")
	}
	consent, _ := f.postLogin(t, "/oauth/email/verify", url.Values{"csrf": {csrfFromJourneyForm(t, verification)}, "code": {code}}, f.host.URL, http.StatusSeeOther)
	f.get(t, f.host.URL+consent, http.StatusOK)
	if got := f.browserHuman(t); got != f.humanID {
		t.Fatalf("OTP authenticated a different principal: %s", got)
	}
	var humans, identities int
	if err := f.s.control.QueryRow(`SELECT count(*) FROM email_humans`).Scan(&humans); err != nil || humans != 1 {
		t.Fatalf("login created an extra human: %d %v", humans, err)
	}
	if err := f.s.control.QueryRow(`SELECT count(*) FROM oidc_identities WHERE principal_id=?`, f.humanID).Scan(&identities); err != nil || identities != 1 {
		t.Fatalf("identity link missing or duplicated: %d %v", identities, err)
	}
}

func TestInteractiveOIDCRejectsIdentityRelinking(t *testing.T) {
	t.Parallel()
	for _, changed := range []string{"subject", "email"} {
		t.Run(changed, func(t *testing.T) {
			f := newOIDCFixture(t)
			f.login(t)
			f.provider.mu.Lock()
			if changed == "subject" {
				f.provider.subject = "different-subject"
			} else {
				f.provider.email = "different@example.test"
			}
			f.provider.mu.Unlock()
			upstream := f.beginOIDC(t)
			callback, _ := f.get(t, upstream, http.StatusSeeOther)
			f.get(t, callback, http.StatusForbidden)
		})
	}
}

func TestInteractiveOIDCUpstreamTokensAreNotPorcupineCredentials(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	f.login(t)
	f.provider.mu.Lock()
	raw := f.provider.lastIDToken
	f.provider.mu.Unlock()
	for _, resource := range []string{f.host.URL + "/api/v1", f.host.URL + "/mcp"} {
		for _, upstream := range []string{raw, "unused-upstream-access"} {
			if _, err := f.s.authenticateResource(t.Context(), upstream, resource); err == nil {
				t.Fatal("upstream credential accepted by Porcupine")
			}
		}
	}
}

func TestInteractiveOIDCCallbackIsSingleUse(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	upstream := f.beginOIDC(t)
	callback, _ := f.get(t, upstream, http.StatusSeeOther)
	f.get(t, callback, http.StatusSeeOther)
	f.get(t, callback, http.StatusBadRequest)
}

func TestInteractiveOIDCStartRequiresSessionCSRFAndOrigin(t *testing.T) {
	t.Parallel()
	f := newOIDCFixtureWithOTP(t, true)
	_, page := f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	csrf := csrfFromJourneyForm(t, page)
	f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {csrf}}, "https://attacker.example", http.StatusBadRequest)
	f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {"forged"}}, f.host.URL, http.StatusBadRequest)
	f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {csrf, csrf}}, f.host.URL, http.StatusBadRequest)
	f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {csrf}}, f.host.URL, http.StatusSeeOther)
	f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {csrf}}, f.host.URL, http.StatusBadRequest)
}
func (f *oidcFixture) submit(t *testing.T, form url.Values, origin string, status int) string {
	t.Helper()
	request, _ := http.NewRequest("POST", f.host.URL+"/oauth/consent", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Origin", origin)
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != status {
		t.Fatalf("consent got %d want %d: %s", response.StatusCode, status, body)
	}
	return response.Header.Get("Location")
}

func TestInteractiveOIDCConsentProjectIsolationAndRevocation(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	csrf := f.login(t)
	form := url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}
	f.submit(t, form, "https://attacker.example", 400)
	forged, _ := url.ParseQuery(form.Encode())
	forged.Set("project", "project-other")
	f.submit(t, forged, f.host.URL, 403)
	badCSRF, _ := url.ParseQuery(form.Encode())
	badCSRF.Set("csrf", "forged")
	f.submit(t, badCSRF, f.host.URL, 400)
	redirect := f.submit(t, form, f.host.URL, 303)
	target, _ := url.Parse(redirect)
	if target.Query().Get("code") == "" || target.Query().Get("state") != "downstream-state" {
		t.Fatal("missing downstream code/state")
	}
	f.submit(t, form, f.host.URL, 401)
	values := url.Values{"grant_type": {"authorization_code"}, "client_id": {f.clientID}, "redirect_uri": {f.callback}, "code": {target.Query().Get("code")}, "code_verifier": {f.verifier}, "resource": {f.host.URL + "/mcp"}}
	response, err := http.PostForm(f.host.URL+"/oauth/token", values)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var tokens map[string]any
	_ = json.NewDecoder(response.Body).Decode(&tokens)
	if response.StatusCode != 200 {
		t.Fatalf("downstream exchange got %d: %v", response.StatusCode, tokens)
	}
	access, _ := tokens["access_token"].(string)
	principal, err := f.s.authenticateResource(context.Background(), access, f.host.URL+"/mcp")
	if err != nil {
		t.Fatal(err)
	}
	if !auth.Authorize(principal, "events:read", "project-a") || auth.Authorize(principal, "events:read", "project-b") || auth.Authorize(principal, "events:write", "project-a") {
		t.Fatalf("project or permission escalation: %+v", principal)
	}
	human, err := f.s.loginAccounts().LinkOIDCIdentity(context.Background(), "operator-subject", "operator@example.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), human.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.authenticateResource(context.Background(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("human revocation did not revoke token")
	}
}
func TestInteractiveOIDCRejectsInvalidIdentityAndState(t *testing.T) {
	t.Parallel()
	for _, which := range []string{"nonce", "audience", "authorized-party", "multiple-audience", "issuer", "signature", "expired", "unverified", "missing-email", "uninvited", "state", "cookie"} {
		t.Run(which, func(t *testing.T) {
			f := newOIDCFixture(t)
			f.provider.mu.Lock()
			switch which {
			case "nonce":
				f.provider.badNonce = true
			case "audience":
				f.provider.badAudience = true
			case "issuer":
				f.provider.badIssuer = true
			case "authorized-party":
				f.provider.badAuthorizedParty = true
			case "multiple-audience":
				f.provider.multipleAudience = true
			case "signature":
				f.provider.badSignature = true
			case "expired":
				f.provider.expired = true
			case "unverified":
				f.provider.verified = false
			case "missing-email":
				f.provider.email = ""
			case "uninvited":
				f.provider.email = "uninvited@example.test"
			}
			f.provider.mu.Unlock()
			upstream, _ := f.get(t, f.authorizeURL(f.callback), 303)
			callback, _ := f.get(t, upstream, 303)
			status := 403
			if which == "state" {
				u, _ := url.Parse(callback)
				q := u.Query()
				q.Set("state", "forged")
				u.RawQuery = q.Encode()
				callback = u.String()
				status = 400
			}
			if which == "cookie" {
				f.client.Jar, _ = cookiejar.New(nil)
				status = 400
			}
			f.get(t, callback, status)
		})
	}
}
func TestInteractiveOIDCDenialAndExpiry(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	csrf := f.login(t)
	location := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"deny"}}, f.host.URL, 303)
	target, _ := url.Parse(location)
	if target.Query().Get("error") != "access_denied" || target.Query().Get("code") != "" || target.Query().Get("state") != "downstream-state" {
		t.Fatal("denial did not preserve safe redirect state")
	}
	csrf = f.login(t)
	_ = csrf
	session := pendingFixtureSession(t, f)
	session.Expires = time.Now().Add(-time.Second)
	raw, ok := f.s.oauth.browser.SaveSession(session)
	if !ok {
		t.Fatal("could not save expired consent session")
	}
	f.client.Jar.SetCookies(mustRecoveryURL(t, f.host.URL), []*http.Cookie{{Name: f.s.oauth.browser.CookieName(), Value: raw, Path: "/"}})
	f.get(t, f.host.URL+"/oauth/consent", 401)
}
func TestInteractiveOIDCRealBrowser(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_OIDC_BROWSER") != "1" {
		t.Skip("set PORCUPINE_OIDC_BROWSER=1 after npm ci in compat for Chromium consent test")
	}
	f := newOIDCFixture(t)
	script := `const {chromium}=require(process.argv[1]);(async()=>{
const [local,provider,...flows]=process.argv.slice(2);const allowed=new Set([local,provider]);
const browser=await chromium.launch({headless:true});try{const page=await browser.newPage();let unexpected=[];
await page.route('**/*',async route=>{const u=new URL(route.request().url());if(!allowed.has(u.origin)){unexpected.push(u.origin);return route.abort();}const response=await route.fetch({maxRedirects:0});const location=response.headers()['location'];if(location&&!allowed.has(new URL(location,u).origin)){unexpected.push(new URL(location,u).origin);return route.fulfill({status:403,body:'Blocked non-loopback redirect'});}return route.fulfill({response});});
async function login(url){await page.goto(url);await page.getByRole('heading',{name:'Authorize access'}).waitFor();if(await page.getByRole('checkbox').count()!==2)throw Error('unexpected projects');}
async function decide(name){await Promise.all([page.waitForURL(u=>u.pathname.includes('callback')||u.pathname.includes('oauth_redirect'),{timeout:5000}),page.getByRole('button',{name,exact:true}).click()]);return new URL(page.url());}
await login(flows[0]);await page.evaluate(()=>{const i=document.createElement('input');i.type='checkbox';i.name='project';i.value='project-other';i.checked=true;document.querySelector('form').append(i)});await page.getByRole('button',{name:'Approve access',exact:true}).click();await page.getByText('Project is not granted',{exact:true}).waitFor();
await login(flows[0]);let u=await decide('Deny');if(u.searchParams.get('error')!=='access_denied'||u.searchParams.get('code'))throw Error('denial failed');
for(const flow of flows){await login(flow);await page.getByRole('checkbox',{name:'project-a',exact:true}).check();u=await decide('Approve access');if(!u.searchParams.get('code')||u.searchParams.get('state')!=='downstream-state'||u.searchParams.get('iss')!==local)throw Error('bad redirect');}
if(unexpected.length)throw Error('unexpected network origin');
}finally{await browser.close();}})().catch(e=>{console.error(e.message);process.exit(1)});`
	packagePath, err := filepath.Abs("../../compat/node_modules/playwright")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "-e", script, packagePath, f.host.URL, f.provider.host.URL, f.authorizeURL(f.callback))
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("Chromium OIDC consent failed: %v %s", err, output)
	}
}

func TestInteractiveOIDCLinkedIdentityCannotReviveRevokedHuman(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	f.login(t)
	human, err := f.s.loginAccounts().LinkOIDCIdentity(t.Context(), "operator-subject", "operator@example.test")
	if err != nil || human.ID != f.humanID {
		t.Fatal("OIDC did not link invited human", err)
	}
	if _, err := f.s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), human.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.loginAccounts().LinkOIDCIdentity(t.Context(), "operator-subject", "operator@example.test"); err == nil {
		t.Fatal("known subject revived revoked human")
	}
	upstream := f.beginOIDC(t)
	callback, _ := f.get(t, upstream, 303)
	f.get(t, callback, 403)
}

func TestInteractiveOIDCClientProjectIntersection(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	if _, err := f.s.control.Exec(`UPDATE access_keys SET projects='["project-b"]' WHERE id=?`, f.clientID); err != nil {
		t.Fatal(err)
	}
	csrf := f.login(t)
	f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, 403)
	location := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-b"}}, f.host.URL, 303)
	if target, err := url.Parse(location); err != nil || target.Query().Get("code") == "" {
		t.Fatal("shared project did not authorize")
	}
}
func TestInteractiveOIDCRegisteredExternalCallbacks(t *testing.T) {
	t.Parallel()
	for _, callback := range []string{"https://chatgpt.com/connector_platform_oauth_redirect", "https://claude.ai/api/mcp/auth_callback"} {
		t.Run(callback, func(t *testing.T) {
			f := newOIDCFixture(t)
			f.callback = callback
			csrf := f.login(t)
			location := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, 303)
			target, _ := url.Parse(location)
			registered, _ := url.Parse(callback)
			if target.Scheme != registered.Scheme || target.Host != registered.Host || target.Path != registered.Path || target.Query().Get("code") == "" || target.Query().Get("iss") != f.host.URL {
				t.Fatal("authorization did not use exact registered callback")
			}
		})
	}
}
func TestInteractiveOIDCRejectsIncompleteOrInsecureProvider(t *testing.T) {
	t.Parallel()
	for _, cfg := range []*OIDCConfig{
		{Issuer: "http://login.example", ClientID: "client", ClientSecret: "secret"},
		{Issuer: "https://login.example", ClientSecret: "secret"},
		{Issuer: "https://login.example", ClientID: "client"},
	} {
		if _, err := auth.NewLogin(t.Context(), auth.LoginConfig{Issuer: "https://analytics.example", OIDC: &auth.OIDCConfig{Issuer: cfg.Issuer, ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret}}); err == nil {
			t.Fatal("invalid provider accepted")
		}
	}
}

func TestInteractiveOIDCCanonicalIssuerCookies(t *testing.T) {
	t.Parallel()
	for _, issuer := range []string{"HTTPS://analytics.example.test", "https://ANALYTICS.example.test", "https://analytics.example.test:443", "http://localhost:80", "http://localhost:080", "https://analytics.example.test:0443", "https://analytics.example.test:65536"} {
		t.Run(issuer, func(t *testing.T) {
			if _, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer, OIDC: &OIDCConfig{}}); err == nil {
				t.Fatal("accepted issuer differing from browser canonical Origin")
			}
		})
	}
	provider := newOIDCMock(t)
	for _, issuer := range []string{"https://analytics.example.test/", "http://127.0.0.1:9000/"} {
		runtime, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer, OIDC: &OIDCConfig{Issuer: provider.host.URL, ClientID: "upstream-client", ClientSecret: "mock-client-secret"}})
		if err != nil {
			t.Fatal(err)
		}
		expected := strings.TrimSuffix(issuer, "/")
		if runtime.issuer != expected || runtime.config.Issuer != expected || runtime.browser.ProviderIssuer() != provider.host.URL {
			t.Fatal("metadata, Origin comparison and callback issuer disagree")
		}
		recorder := httptest.NewRecorder()
		runtime.browser.Start(recorder, httptest.NewRequest("GET", expected+"/oauth/authorize", nil), auth.LoginRequest{ID: "test", ClientName: "Tool"})
		if !strings.Contains(recorder.Header().Get("Location"), "redirect_uri="+url.QueryEscape(expected+"/oauth/oidc/callback")) {
			t.Fatal("provider callback differs from canonical issuer")
		}
		cookie := recorder.Result().Cookies()[0]
		wantSecure := strings.HasPrefix(expected, "https://")
		if cookie.Secure != wantSecure || wantSecure && cookie.Name != "__Host-porcupine_oidc" {
			t.Fatal("canonical HTTPS issuer lost Secure host cookie")
		}
	}
}

func TestLegacyOAuthPreservesIssuerSpelling(t *testing.T) {
	t.Parallel()
	const issuer = "https://analytics.example.test:443"
	runtime, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer})
	if err != nil || runtime.issuer != issuer || runtime.issuer+"/mcp" != issuer+"/mcp" {
		t.Fatal("machine client audience changed on upgrade", err)
	}
}

func (f *oidcFixture) issueToken(t *testing.T, projects ...string) string {
	t.Helper()
	return f.issueSelectionToken(t, "project", projects...)
}

func (f *oidcFixture) issueSelectionToken(t *testing.T, field string, selected ...string) string {
	t.Helper()
	csrf := f.login(t)
	redirect := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, field: selected}, f.host.URL, http.StatusSeeOther)
	target, err := url.Parse(redirect)
	if err != nil || target.Query().Get("code") == "" {
		t.Fatal("missing downstream authorization code")
	}
	response, err := http.PostForm(f.host.URL+"/oauth/token", url.Values{
		"grant_type": {"authorization_code"}, "client_id": {f.clientID}, "redirect_uri": {f.callback},
		"code": {target.Query().Get("code")}, "code_verifier": {f.verifier}, "resource": {f.resource},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var tokens struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&tokens); err != nil || response.StatusCode != http.StatusOK || tokens.AccessToken == "" {
		t.Fatalf("downstream token exchange failed: HTTP %d %v", response.StatusCode, err)
	}
	return tokens.AccessToken
}

func TestInteractiveOIDCTenantAdminIsolationAndMembershipRevocation(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	operator := accountTestToken(t, f.s)
	create := func(name string) string {
		return request(t, f.s, "POST", "/api/v1/tenants", operator, tenantOAuthJSON(t, map[string]any{"name": name}), 201)["tenant"].(map[string]any)["id"].(string)
	}
	a, b := create("OIDC team A"), create("OIDC team B")
	projectA := request(t, f.s, "POST", "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": "A project", "tenant_id": a}), 201)["project"].(map[string]any)["id"].(string)
	projectB := request(t, f.s, "POST", "/api/v1/projects", operator, tenantOAuthJSON(t, map[string]any{"name": "B project", "tenant_id": b}), 201)["project"].(map[string]any)["id"].(string)
	human := request(t, f.s, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"lea@example.test","role":"admin"}`, 201)["human_id"].(string)
	request(t, f.s, "POST", "/api/v1/tenants/"+b+"/members", operator, `{"email":"lea@example.test","role":"member","project_scope":"all","project_role":"reader"}`, 201)
	f.provider.mu.Lock()
	f.provider.email = "lea@example.test"
	f.provider.mu.Unlock()
	readerClient, readerCallback := f.clientID, f.callback
	f.clientID, f.callback, f.scope, f.resource = "porcupine-local-tenant-admin", localOAuthCallback, tenantAdminScope, f.host.URL+"/api/v1"
	access := f.issueSelectionToken(t, "tenant", a)
	request(t, f.s, "GET", "/api/v1/tenants/"+a, access, "", 200)
	request(t, f.s, "GET", "/api/v1/tenants/"+b, access, "", 403)
	request(t, f.s, "GET", "/api/v1/projects/"+projectA+"/schema", access, "", 200)
	request(t, f.s, "GET", "/api/v1/projects/"+projectB+"/schema", access, "", 403)
	request(t, f.s, "POST", "/api/v1/tenants/"+a+"/members", operator, `{"email":"backup@example.test","role":"admin"}`, 201)
	request(t, f.s, "PUT", "/api/v1/tenants/"+a+"/members/"+human, operator, `{"role":"member","project_scope":"all","project_role":"reader"}`, 200)
	request(t, f.s, "GET", "/api/v1/tenants/"+a, access, "", 401)
	f.clientID, f.callback, f.scope = readerClient, readerCallback, "events:read management:read"
	reader := f.issueToken(t, projectB)
	request(t, f.s, "GET", "/api/v1/projects/"+projectB+"/schema", reader, "", 200)
	request(t, f.s, "PATCH", "/api/v1/projects/"+projectB, reader, `{"name":"forbidden"}`, 403)
	request(t, f.s, "GET", "/api/v1/projects/"+projectA+"/schema", reader, "", 403)
}

func TestInteractiveOIDCAPIResourceUsesInvitedPermissions(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	f.resource = f.host.URL + "/api/v1"
	access := f.issueToken(t, "project-a")
	principal, err := f.s.authenticateResource(t.Context(), access, f.resource)
	if err != nil || principal.ID == f.humanID || !auth.Authorize(principal, "events:read", "project-a") || auth.Authorize(principal, "events:read", "project-b") || auth.Authorize(principal, "events:write", "project-a") {
		t.Fatalf("invalid attenuated API grant: %+v %v", principal, err)
	}
	if _, err := f.s.authenticateResource(t.Context(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("API token accepted for MCP")
	}
}

func TestInteractiveOIDCAdministratorUsesSameDurableAccount(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	if _, err := f.s.control.Exec(`UPDATE access_keys SET permissions='["instance:admin"]', projects='["*"]' WHERE id IN (?,?)`, f.humanID, f.clientID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.control.Exec(`UPDATE email_humans SET deployment_admin=1, project_grants='[{"project_id":"*","permissions":["instance:admin"]}]' WHERE principal_id=?`, f.humanID); err != nil {
		t.Fatal(err)
	}
	f.scope = "instance:admin"
	f.resource = f.host.URL + "/api/v1"
	access := f.issueToken(t, "*")
	principal, err := f.s.authenticateResource(t.Context(), access, f.resource)
	if err != nil || !principal.Admin() {
		t.Fatal("invited administrator could not authorize a tool", err)
	}
	if _, err := f.s.control.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), f.humanID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.authenticateResource(t.Context(), access, f.resource); err == nil {
		t.Fatal("administrator token survived human revocation")
	}
}

func TestInteractiveOIDCOnlyDoesNotExposeEmailOTP(t *testing.T) {
	t.Parallel()
	f := newOIDCFixture(t)
	f.postLogin(t, "/oauth/email/request", url.Values{"csrf": {"anything"}, "email": {"operator@example.test"}}, f.host.URL, http.StatusNotFound)
	f.login(t)
}

func TestInteractiveOIDCCallbackRequiresStartedProviderLogin(t *testing.T) {
	t.Parallel()
	f := newOIDCFixtureWithOTP(t, true)
	_, body := f.get(t, f.authorizeURL(f.callback), http.StatusOK)
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("missing login-choice CSRF")
	}
	f.get(t, f.host.URL+"/oauth/oidc/callback?state=&code=unsolicited", http.StatusBadRequest)
	upstream, _ := f.postLogin(t, "/oauth/oidc/start", url.Values{"csrf": {match[1]}}, f.host.URL, http.StatusSeeOther)
	callback, _ := f.get(t, upstream, http.StatusSeeOther)
	consent, _ := f.get(t, callback, http.StatusSeeOther)
	f.get(t, f.host.URL+consent, http.StatusOK)
}

func TestInteractiveOIDCStatusTracksConfiguredLoginMethods(t *testing.T) {
	t.Parallel()
	for _, otp := range []bool{false, true} {
		f := newOIDCFixtureWithOTP(t, otp)
		ctx := context.WithValue(t.Context(), actorContextKey{}, auth.Principal{Permissions: []string{"management:read"}})
		status, err := f.s.getInstanceStatus(ctx, &struct{}{})
		if err != nil {
			t.Fatal(err)
		}
		if !status.Body.Configuration.OIDCSignIn || status.Body.Configuration.EmailSignIn != otp {
			t.Fatalf("login methods reported incorrectly: otp=%v configuration=%+v", otp, status.Body.Configuration)
		}
	}
}

func TestInteractiveOIDCConfiguredAdministratorWithoutSMTP(t *testing.T) {
	t.Parallel()
	provider := newOIDCMock(t)
	s, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "oidc-admin.db"), BootstrapToken: adminToken,
		OAuth: &OAuthConfig{
			Issuer: "https://analytics.example.test",
			OIDC:   &OIDCConfig{Issuer: provider.host.URL, ClientID: "upstream-client", ClientSecret: "mock-client-secret"},
			Email:  &EmailConfig{AdminEmail: "operator@example.test"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	human, err := s.loginAccounts().LinkOIDCIdentity(t.Context(), "operator-subject", "operator@example.test")
	if err != nil {
		t.Fatal(err)
	}
	current, err := loadEmailHuman(t.Context(), s.control, human.ID)
	if err != nil || !current.Admin() || s.oauth.config.Email.SendCode != nil {
		t.Fatal("configured OIDC-only administrator unavailable", err)
	}
	if _, err := s.loginAccounts().LinkOIDCIdentity(t.Context(), "uninvited-subject", "uninvited@example.test"); err == nil {
		t.Fatal("configured provider provisioned an uninvited administrator")
	}
}

func TestOIDCVerifiedEmailAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, issuer, email, domain string
		verified, allowed           bool
	}{
		{"gmail", "https://accounts.google.com", "reader@gmail.com", "", true, true},
		{"workspace", "https://accounts.google.com", "reader@company.example", "company.example", true, true},
		{"third-party-email", "https://accounts.google.com", "reader@company.example", "", true, false},
		{"workspace-secondary-domain", "https://accounts.google.com", "reader@company.example", "other.example", true, true},
		{"unverified", "https://accounts.google.com", "reader@gmail.com", "", false, false},
		{"missing", "https://accounts.google.com", "", "", true, false},
		{"configured-provider", "https://sso.example", "reader@company.example", "", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			email, err := auth.VerifiedOIDCEmail(tc.issuer, auth.OIDCClaims{Email: tc.email, EmailVerified: tc.verified, HostedDomain: tc.domain})
			if (err == nil) != tc.allowed || tc.allowed && email != tc.email {
				t.Fatalf("email authority result=%q, %v", email, err)
			}
		})
	}
}
