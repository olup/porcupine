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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/oauth2"
	"golang.org/x/time/rate"
)

type oidcMock struct {
	host        *httptest.Server
	key         *rsa.PrivateKey
	mu          sync.Mutex
	codes       map[string]url.Values
	subject     string
	badNonce    bool
	badAudience bool
	badIssuer   bool
}

func newOIDCMock(t *testing.T) *oidcMock {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &oidcMock{key: key, codes: map[string]url.Values{}, subject: "operator-subject"}
	p.host = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			write(w, 200, map[string]any{"issuer": p.host.URL, "authorization_endpoint": p.host.URL + "/authorize", "token_endpoint": p.host.URL + "/token", "jwks_uri": p.host.URL + "/keys", "response_types_supported": []string{"code"}, "subject_types_supported": []string{"public"}, "id_token_signing_alg_values_supported": []string{"RS256"}})
		case "/keys":
			write(w, 200, map[string]any{"keys": []any{map[string]any{"kty": "RSA", "use": "sig", "kid": "test-key", "alg": "RS256", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
		case "/authorize":
			q := r.URL.Query()
			if q.Get("nonce") == "" || q.Get("code_challenge_method") != "S256" || q.Get("client_id") != "upstream-client" {
				http.Error(w, "invalid mock authorize", 400)
				return
			}
			code := oidcRandom()
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
			subject, nonceBad, audBad, issuerBad := p.subject, p.badNonce, p.badAudience, p.badIssuer
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
			token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{"iss": issuer, "sub": subject, "aud": aud, "exp": time.Now().Add(time.Minute).Unix(), "iat": time.Now().Unix(), "nonce": nonce})
			token.Header["kid"] = "test-key"
			signed, err := token.SignedString(p.key)
			if err != nil {
				t.Error(err)
				http.Error(w, "sign", 500)
				return
			}
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
}

func newOIDCFixture(t *testing.T) *oidcFixture {
	t.Helper()
	p := newOIDCMock(t)
	host := httptest.NewUnstartedServer(nil)
	issuer := "http://" + host.Listener.Addr().String()
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "oidc.db"), BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: issuer, OIDC: &OIDCConfig{Issuer: p.host.URL, ClientID: "upstream-client", ClientSecret: "mock-client-secret", Bindings: []OIDCBinding{{Subject: "operator-subject", Permissions: []string{"events:read", "management:read"}, Projects: []string{"project-a", "project-b"}}}}}})
	if err != nil {
		host.Close()
		t.Fatal(err)
	}
	s.oauth.limiter = rate.NewLimiter(rate.Inf, 1000)
	id, _ := oauthProtocolClient(t, s, []string{"events:read", "management:read"}, []string{"*"})
	callback := issuer + "/client/callback"
	redirects, _ := json.Marshal([]string{callback, "https://chatgpt.com/connector_platform_oauth_redirect", "https://claude.ai/api/mcp/auth_callback"})
	_, err = s.db.Exec(`UPDATE oauth_clients SET grant_types='["authorization_code","refresh_token"]',redirect_uris=?,token_endpoint_auth_method='none' WHERE client_id=?`, string(redirects), id)
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
	return &oidcFixture{s: s, host: host, provider: p, client: client, clientID: id, callback: callback, verifier: oauth2.GenerateVerifier()}
}
func (f *oidcFixture) authorizeURL(callback string) string {
	challenge := sha256.Sum256([]byte(f.verifier))
	q := url.Values{"response_type": {"code"}, "client_id": {f.clientID}, "redirect_uri": {callback}, "scope": {"events:read management:read"}, "resource": {f.host.URL + "/mcp"}, "state": {"downstream-state"}, "code_challenge": {base64.RawURLEncoding.EncodeToString(challenge[:])}, "code_challenge_method": {"S256"}}
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
func (f *oidcFixture) login(t *testing.T) string {
	t.Helper()
	upstream, _ := f.get(t, f.authorizeURL(f.callback), 303)
	callback, _ := f.get(t, upstream, 303)
	consent, _ := f.get(t, callback, 303)
	_, body := f.get(t, f.host.URL+consent, 200)
	match := regexp.MustCompile(`name="csrf" value="([^"]+)"`).FindStringSubmatch(body)
	if len(match) != 2 {
		t.Fatal("missing consent CSRF")
	}
	return match[1]
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
	if !authorize(principal, "events:read", "project-a") || authorize(principal, "events:read", "project-b") || authorize(principal, "events:write", "project-a") {
		t.Fatalf("project or permission escalation: %+v", principal)
	}
	human, err := f.s.oidcHuman(context.Background(), "operator-subject")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.db.Exec(`UPDATE access_keys SET revoked_at=? WHERE id=?`, now(), human.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s.authenticateResource(context.Background(), access, f.host.URL+"/mcp"); err == nil {
		t.Fatal("human revocation did not revoke token")
	}
}
func TestInteractiveOIDCRejectsInvalidIdentityAndState(t *testing.T) {
	for _, which := range []string{"nonce", "audience", "issuer", "unbound", "state", "cookie"} {
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
			case "unbound":
				f.provider.subject = "unbound-subject"
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
	f := newOIDCFixture(t)
	csrf := f.login(t)
	location := f.submit(t, url.Values{"csrf": {csrf}, "decision": {"deny"}}, f.host.URL, 303)
	target, _ := url.Parse(location)
	if target.Query().Get("error") != "access_denied" || target.Query().Get("code") != "" || target.Query().Get("state") != "downstream-state" {
		t.Fatal("denial did not preserve safe redirect state")
	}
	csrf = f.login(t)
	_ = csrf
	f.s.oauth.oidc.mu.Lock()
	for _, session := range f.s.oauth.oidc.sessions {
		session.expires = time.Now().Add(-time.Second)
	}
	f.s.oauth.oidc.mu.Unlock()
	f.get(t, f.host.URL+"/oauth/consent", 401)
}
func TestInteractiveOIDCRealBrowser(t *testing.T) {
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

func TestInteractiveOIDCBindingRemovalRevokesDurablePrincipals(t *testing.T) {
	f := newOIDCFixture(t)
	human, err := f.s.oidcHuman(context.Background(), "operator-subject")
	if err != nil {
		t.Fatal(err)
	}
	f.s.oauth.oidc.bindings = map[string]actor{}
	if err := f.s.initializeOIDCHumans(context.Background()); err != nil {
		t.Fatal(err)
	}
	var revoked *string
	if err := f.s.db.QueryRow(`SELECT revoked_at FROM access_keys WHERE id=?`, human.ID).Scan(&revoked); err != nil || revoked == nil {
		t.Fatal("removed subject did not revoke durable human")
	}
	f.s.oauth.oidc.bindings["operator-subject"] = actor{Permissions: []string{"events:read"}, Projects: []string{"project-a"}}
	if err := f.s.initializeOIDCHumans(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.oidcHuman(context.Background(), "operator-subject"); err == nil {
		t.Fatal("readding binding silently restored revoked human")
	}
}
func TestInteractiveOIDCClientProjectIntersection(t *testing.T) {
	f := newOIDCFixture(t)
	if _, err := f.s.db.Exec(`UPDATE access_keys SET projects='["project-b"]' WHERE id=?`, f.clientID); err != nil {
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
func TestInteractiveOIDCSessionBoundAndSecureCookie(t *testing.T) {
	o := &interactiveOIDC{secure: true, sessions: map[string]*oidcBrowserSession{}}
	recorder := httptest.NewRecorder()
	o.cookie(recorder, "opaque", 600)
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != "__Host-porcupine_oidc" || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode || cookies[0].Path != "/" || cookies[0].Domain != "" {
		t.Fatal("HTTPS login cookie lost protections")
	}
	for range oidcSessionCapacity {
		if _, ok := o.save(&oidcBrowserSession{expires: time.Now().Add(time.Minute)}); !ok {
			t.Fatal("premature session rejection")
		}
	}
	if _, ok := o.save(&oidcBrowserSession{expires: time.Now().Add(time.Minute)}); ok {
		t.Fatal("unbounded browser session allocation")
	}
}

func TestInteractiveOIDCRejectsAmbientGrants(t *testing.T) {
	for _, which := range []string{"wildcard", "admin", "unknown-permission", "duplicate-subject", "insecure-issuer"} {
		t.Run(which, func(t *testing.T) {
			config := &OIDCConfig{Issuer: "https://login.example", ClientID: "client", ClientSecret: "secret", Bindings: []OIDCBinding{{Subject: "subject", Permissions: []string{"events:read"}, Projects: []string{"a"}}}}
			switch which {
			case "wildcard":
				config.Bindings[0].Projects = []string{"*"}
			case "admin":
				config.Bindings[0].Permissions = []string{"instance:admin"}
			case "unknown-permission":
				config.Bindings[0].Permissions = []string{"all:projects"}
			case "duplicate-subject":
				config.Bindings = append(config.Bindings, config.Bindings[0])
			case "insecure-issuer":
				config.Issuer = "http://login.example"
			}
			if _, err := newInteractiveOIDC(config, "https://analytics.example"); err == nil {
				t.Fatal("invalid operator binding accepted")
			}
		})
	}
}

func TestInteractiveOIDCCanonicalIssuerCookies(t *testing.T) {
	for _, issuer := range []string{"HTTPS://analytics.example.test", "https://ANALYTICS.example.test", "https://analytics.example.test:443", "http://localhost:80", "http://localhost:080", "https://analytics.example.test:0443", "https://analytics.example.test:65536"} {
		t.Run(issuer, func(t *testing.T) {
			if _, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer}); err == nil {
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
		if runtime.issuer != expected || runtime.config.Issuer != expected || runtime.oidc.oauth.RedirectURL != expected+"/oauth/oidc/callback" {
			t.Fatal("metadata, Origin comparison and callback issuer disagree")
		}
		recorder := httptest.NewRecorder()
		runtime.oidc.cookie(recorder, "opaque", 600)
		cookie := recorder.Result().Cookies()[0]
		wantSecure := strings.HasPrefix(expected, "https://")
		if cookie.Secure != wantSecure || wantSecure && cookie.Name != "__Host-porcupine_oidc" {
			t.Fatal("canonical HTTPS issuer lost Secure host cookie")
		}
	}
}
