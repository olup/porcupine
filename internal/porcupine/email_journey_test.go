package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

type journeyMail struct{ to, code string }

type emailJourney struct {
	service *Service
	issuer  string
	mail    chan journeyMail
	notices chan string
}

func newEmailJourney(t *testing.T) *emailJourney {
	t.Helper()
	host := httptest.NewUnstartedServer(nil)
	j := &emailJourney{issuer: "http://" + host.Listener.Addr().String(), mail: make(chan journeyMail, 8), notices: make(chan string, 8)}
	s, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "journey.db"), BootstrapToken: adminToken,
		OAuth: &OAuthConfig{Issuer: j.issuer, Email: &EmailConfig{
			AdminEmail: "operator@example.test",
			SendInvitation: func(ctx context.Context, to string) error {
				select {
				case j.notices <- to:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			SendCode: func(ctx context.Context, to, code string) error {
				select {
				case j.mail <- journeyMail{to, code}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		}},
	})
	if err != nil {
		host.Close()
		t.Fatal(err)
	}
	j.service = s
	host.Config.Handler = s.Handler()
	host.Start()
	t.Cleanup(func() { host.Close(); s.Close() })

	return j
}

func (j *emailJourney) register(t *testing.T, adminToken, name string, scopes, projects []string) *oauth2.Config {
	t.Helper()
	return j.registerCallback(t, adminToken, name, j.issuer+"/client/"+name, scopes, projects)
}

func (j *emailJourney) registerCallback(t *testing.T, adminToken, name, callback string, scopes, projects []string) *oauth2.Config {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"name": name, "permissions": scopes, "projects": projects,
		"grant_types": []string{"authorization_code", "refresh_token"}, "redirect_uris": []string{callback},
		"token_endpoint_auth_method": "none",
	})
	if err != nil {
		t.Fatal(err)
	}
	result := request(t, j.service, "POST", "/api/v1/oauth-clients", adminToken, string(body), 201)
	return j.client(result["client_id"].(string), callback, scopes)
}

func (j *emailJourney) client(id, callback string, scopes []string) *oauth2.Config {
	return &oauth2.Config{
		ClientID: id, RedirectURL: callback, Scopes: scopes,
		Endpoint: oauth2.Endpoint{AuthURL: j.issuer + "/oauth/authorize", TokenURL: j.issuer + "/oauth/token", AuthStyle: oauth2.AuthStyleInParams},
	}
}

var journeyCSRF = regexp.MustCompile(`name="csrf" value="([^"]+)"`)

func csrfFromJourneyForm(t *testing.T, page string) string {
	t.Helper()
	match := journeyCSRF.FindStringSubmatch(page)
	if len(match) != 2 {
		t.Fatal("browser did not receive a CSRF-protected form")
	}
	return match[1]
}

func (j *emailJourney) browserLogin(t *testing.T, app *oauth2.Config, email, resource, project, forbiddenProject string, consentStatus int) *oauth2.Token {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	browser := &http.Client{Jar: jar, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	defer browser.CloseIdleConnections()
	step := func(target string, form url.Values, status int) (string, string) {
		t.Helper()
		method := http.MethodGet
		var body io.Reader
		if form != nil {
			method = http.MethodPost
			body = strings.NewReader(form.Encode())
		}
		r, err := http.NewRequest(method, target, body)
		if err != nil {
			t.Fatal(err)
		}
		if form != nil {
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.Header.Set("Origin", j.issuer)
		}
		resp, err := browser.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != status {
			t.Fatalf("%s %s: got %d, want %d, read error %v", method, r.URL.Path, resp.StatusCode, status, err)
		}
		return string(b), resp.Header.Get("Location")
	}

	verifier := oauth2.GenerateVerifier()
	state := oauth2.GenerateVerifier()
	audience := oauth2.SetAuthURLParam("resource", j.issuer+resource)
	page, _ := step(app.AuthCodeURL(state, audience, oauth2.S256ChallengeOption(verifier)), nil, 200)
	page, _ = step(j.issuer+"/oauth/email/request", url.Values{"email": {email}, "csrf": {csrfFromJourneyForm(t, page)}}, 200)
	var mail journeyMail
	select {
	case mail = <-j.mail:
	case <-time.After(4 * time.Second):
		t.Fatal("OTP did not reach the invited mailbox")
	}
	if mail.to != email || mail.code == "" {
		t.Fatal("OTP did not reach the expected recipient")
	}
	_, location := step(j.issuer+"/oauth/email/verify", url.Values{"code": {mail.code}, "csrf": {csrfFromJourneyForm(t, page)}}, 303)
	if location != "/oauth/consent" {
		t.Fatal("verified address did not reach consent")
	}
	page, _ = step(j.issuer+location, nil, consentStatus)
	if consentStatus != 200 {
		return nil
	}
	csrf := csrfFromJourneyForm(t, page)
	consent := url.Values{"decision": {"approve"}, "project": {project}, "csrf": {csrf}}
	if forbiddenProject != "" {
		if strings.Contains(page, `value="`+forbiddenProject+`"`) {
			t.Fatal("consent page offered an unassigned project")
		}
		consent.Set("project", forbiddenProject)
		step(j.issuer+location, consent, 403)
		consent.Set("project", project)
	}
	_, callback := step(j.issuer+location, consent, 303)
	u, err := url.Parse(callback)
	if err != nil || strings.SplitN(callback, "?", 2)[0] != app.RedirectURL || u.Query().Get("iss") != j.issuer || u.Query().Get("state") != state {
		t.Fatal("unexpected OAuth callback")
	}
	// A client cannot exchange a valid PKCE code against the other resource.
	other := "/mcp"
	if resource == other {
		other = "/api/v1"
	}
	_, err = app.Exchange(t.Context(), u.Query().Get("code"), oauth2.VerifierOption(verifier), oauth2.SetAuthURLParam("resource", j.issuer+other))
	var oauthError *oauth2.RetrieveError
	if !errors.As(err, &oauthError) || oauthError.ErrorCode != "invalid_target" {
		t.Fatalf("code exchange with wrong resource: %v", err)
	}
	token, err := app.Exchange(t.Context(), u.Query().Get("code"), oauth2.VerifierOption(verifier), audience)
	if err != nil || !token.Valid() || token.RefreshToken == "" {
		t.Fatalf("OAuth exchange did not return working tokens: %v", err)
	}
	return token
}

func (j *emailJourney) checkRefresh(t *testing.T, app *oauth2.Config, token *oauth2.Token, resource string, status int) *oauth2.Token {
	t.Helper()
	resp, err := http.PostForm(app.Endpoint.TokenURL, url.Values{
		"grant_type": {"refresh_token"}, "client_id": {app.ClientID},
		"refresh_token": {token.RefreshToken}, "resource": {j.issuer + resource},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != status {
		t.Fatalf("refresh returned %d, expected %d", resp.StatusCode, status)
	}
	if status == 200 {
		var rotated oauth2.Token
		if err := json.NewDecoder(resp.Body).Decode(&rotated); err != nil || !rotated.Valid() || rotated.RefreshToken == "" {
			t.Fatalf("refresh did not return a usable token: %v", err)
		}
		return &rotated
	}
	return nil
}

func TestEmailHumanAuthorizesTwoToolsAndRevokesAccess(t *testing.T) {
	j := newEmailJourney(t)
	s := j.service
	// An empty deployment has no operator-created clients. Start with its fixed loopback client.
	const loopback = "http://127.0.0.1:43179/callback"
	operatorApp := j.client("porcupine-local-admin", loopback, []string{"instance:admin"})
	operator := j.browserLogin(t, operatorApp, "operator@example.test", "/api/v1", "*", "", 200)
	createProject := func(name string) string {
		v := request(t, s, "POST", "/api/v1/projects", operator.AccessToken, `{"name":"`+name+`"}`, 201)
		return v["project"].(map[string]any)["id"].(string)
	}
	a, b := createProject("A"), createProject("B")
	human := "lea@example.test"
	body, _ := json.Marshal(map[string]any{"email": human, "permissions": []string{"events:read", "management:read"}, "projects": []string{a}})
	invited := request(t, s, "POST", "/api/v1/humans", operator.AccessToken, string(body), 201)
	humanID := invited["id"].(string)
	select {
	case recipient := <-j.notices:
		if recipient != human {
			t.Fatal("invitation notice sent to different mailbox")
		}
	default:
		t.Fatal("missing invitation notice")
	}
	retried := request(t, s, "POST", "/api/v1/humans", operator.AccessToken, string(body), 201)
	if retried["id"] != humanID || len(j.notices) != 0 {
		t.Fatal("repeat invite did not reuse account and its delivered notice")
	}
	scopes := []string{"events:read", "management:read"}
	// Both applications can service either project. The invited human can only share A.
	mcpApp := j.client("porcupine-local", loopback, scopes)
	apiApp := j.register(t, operator.AccessToken, "cli", scopes, []string{a, b})
	mcpToken := j.browserLogin(t, mcpApp, human, "/mcp", a, b, 200)
	cliToken := j.browserLogin(t, apiApp, human, "/api/v1", a, b, 200)
	ctx, session := mcpTestSession(t, j.issuer, mcpToken.AccessToken, "2025-11-25")
	for _, project := range []struct {
		id     string
		status int
	}{{a, 200}, {b, 403}} {
		mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": project.id}}, project.status)
		request(t, s, "GET", "/api/v1/projects/"+project.id+"/schema", cliToken.AccessToken, "", project.status)
	}
	request(t, s, "GET", "/api/v1/projects/"+a+"/schema", mcpToken.AccessToken, "", 401)
	request(t, s, "POST", "/mcp", cliToken.AccessToken, "{}", 401)
	request(t, s, "POST", "/api/v1/humans", cliToken.AccessToken, string(body), 403)
	j.browserLogin(t, operatorApp, human, "/api/v1", "*", "", 403)

	grants := request(t, s, "GET", "/api/v1/oauth-grants", operator.AccessToken, "", 200)["grants"].([]any)
	matching := map[string]string{}
	for _, value := range grants {
		g := value.(map[string]any)
		if g["human_id"] == humanID {
			matching[g["client_id"].(string)] = g["id"].(string)
		}
	}
	if len(matching) != 2 || matching[mcpApp.ClientID] == "" || matching[apiApp.ClientID] == "" {
		t.Fatal("two client connections did not belong to the same invited person")
	}
	request(t, s, "DELETE", "/api/v1/oauth-grants/"+matching[mcpApp.ClientID], operator.AccessToken, "", 204)
	request(t, s, "POST", "/mcp", mcpToken.AccessToken, "{}", 401)
	j.checkRefresh(t, mcpApp, mcpToken, "/mcp", 400)
	j.checkRefresh(t, apiApp, cliToken, "/mcp", 400)
	cliToken = j.checkRefresh(t, apiApp, cliToken, "/api/v1", 200)
	request(t, s, "GET", "/api/v1/projects/"+a+"/schema", cliToken.AccessToken, "", 200)

	// An operator-provisioned job lives independently of this person's connections.
	serviceBody, _ := json.Marshal(map[string]any{"name": "nightly report", "projects": []string{a}, "permissions": []string{"events:read"}})
	job := request(t, s, "POST", "/api/v1/oauth-clients", operator.AccessToken, string(serviceBody), 201)
	request(t, s, "DELETE", "/api/v1/humans/"+humanID, operator.AccessToken, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+a+"/schema", cliToken.AccessToken, "", 401)
	j.checkRefresh(t, apiApp, cliToken, "/api/v1", 400)
	machine := oauthProtocolRequest(t, s, job["client_id"].(string), job["client_secret"].(string), url.Values{
		"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"},
	}, 200)["access_token"].(string)
	request(t, s, "GET", "/api/v1/projects/"+a+"/schema", machine, "", 200)
	request(t, s, "GET", "/api/v1/projects/"+b+"/schema", machine, "", 403)
}

// A real browser supplies Origin and enforces CSP, cookie attributes and form actions.
func TestEmailLoginChromium(t *testing.T) {
	if os.Getenv("PORCUPINE_OIDC_BROWSER") != "1" {
		t.Skip("set PORCUPINE_OIDC_BROWSER=1 with Playwright installed in compat")
	}
	j := newEmailJourney(t)
	callbackHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		_, _ = io.WriteString(w, "Authorized")
	}))
	t.Cleanup(callbackHost.Close)
	bootstrap := oauthProtocolRequest(t, j.service, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	app := j.registerCallback(t, bootstrap, "browser-app", callbackHost.URL+"/callback", []string{"instance:admin"}, []string{"*"})
	verifier, state := oauth2.GenerateVerifier(), oauth2.GenerateVerifier()
	audience := oauth2.SetAuthURLParam("resource", j.issuer+"/api/v1")
	authorizeURL := app.AuthCodeURL(state, audience, oauth2.S256ChallengeOption(verifier))
	pkg, err := filepath.Abs("../../compat/node_modules/playwright")
	if err != nil {
		t.Fatal(err)
	}
	script := `const {chromium}=require(process.argv[1]);
 const [url,callback]=process.argv.slice(2);
 (async()=>{
   const browser=await chromium.launch();
   try {
     const page=await browser.newPage();
     await page.goto(url);
     await page.getByLabel('Email address').fill('operator@example.test');
     await page.getByRole('button',{name:'Send sign-in code'}).click();
     let code='';
     for await(const chunk of process.stdin){code+=chunk;}
     await page.getByLabel('Sign-in code').fill(code.trim());
     await page.getByRole('button',{name:'Continue'}).click();
     await page.getByRole('checkbox').check();
     await Promise.all([page.waitForURL(u=>u.origin+u.pathname===callback&&u.searchParams.has('code'),{timeout:5000}),page.getByRole('button',{name:'Approve access'}).click()]);
     process.stdout.write(page.url());
   } finally {await browser.close();}
 })().catch(err=>{console.error(err);process.exit(1)});`
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "-e", script, pkg, authorizeURL, app.RedirectURL)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	var callback, stderr bytes.Buffer
	cmd.Stdout = &callback
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case msg := <-j.mail:
		if msg.to != "operator@example.test" {
			t.Fatal("mail delivered to unexpected address")
		}
		if _, err := io.WriteString(stdin, msg.code); err != nil {
			t.Fatal(err)
		}
		if err := stdin.Close(); err != nil {
			t.Fatal(err)
		}
	case err := <-done:
		t.Fatalf("Chromium exited before delivery: %v %s", err, stderr.String())
	case <-ctx.Done():
		t.Fatal("browser failed to request OTP")
	}
	if err := <-done; err != nil {
		t.Fatalf("browser did not complete consent: %v %s", err, stderr.String())
	}
	target, err := url.Parse(callback.String())
	if err != nil || target.Query().Get("state") != state || target.Query().Get("iss") != j.issuer {
		t.Fatal("unexpected Chromium callback")
	}
	token, err := app.Exchange(t.Context(), target.Query().Get("code"), oauth2.VerifierOption(verifier), audience)
	if err != nil {
		t.Fatal(err)
	}
	request(t, j.service, "GET", "/api/v1/humans", token.AccessToken, "", 200)
}

func TestEmailConsentRestrictsWriteToItsGrantedProject(t *testing.T) {
	j := newEmailJourney(t)
	admin := oauthProtocolRequest(t, j.service, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {j.issuer + "/api/v1"}}, 200)["access_token"].(string)
	makeProject := func(name string) string {
		body, _ := json.Marshal(map[string]string{"name": name, "ingestion_mode": "server_only"})
		v := request(t, j.service, "POST", "/api/v1/projects", admin, string(body), 201)
		return v["project"].(map[string]any)["id"].(string)
	}
	a, b := makeProject("A"), makeProject("B")
	const email = "writer@example.test"
	scopes := []string{"events:read", "events:write"}
	invite, _ := json.Marshal(map[string]any{"email": email, "grants": []map[string]any{
		{"project_id": a, "permissions": []string{"events:read"}},
		{"project_id": b, "permissions": scopes},
	}})
	h := request(t, j.service, "POST", "/api/v1/humans", admin, string(invite), 201)
	app := j.register(t, admin, "writer", scopes, []string{a, b})
	token := j.browserLogin(t, app, email, "/api/v1", b, a, 200)
	request(t, j.service, "GET", "/api/v1/projects/"+a+"/schema", token.AccessToken, "", 403)
	event := `{"type":"track","actor_id":"e2e","name":"consented","message_id":"consented"}`
	request(t, j.service, "POST", "/ingest/v1/events", token.AccessToken, event, 200)
	// Removing write access from B also invalidates a token that was already issued for it.
	grants, _ := json.Marshal(map[string]any{"grants": []map[string]any{
		{"project_id": a, "permissions": []string{"events:read"}},
		{"project_id": b, "permissions": []string{"events:read"}},
	}})
	request(t, j.service, "PUT", "/api/v1/humans/"+h["id"].(string), admin, string(grants), 200)
	request(t, j.service, "GET", "/api/v1/projects/"+b+"/schema", token.AccessToken, "", 401)
	j.checkRefresh(t, app, token, "/api/v1", 400)
}

func TestEmailInvitedInstanceAdmin(t *testing.T) {
	j := newEmailJourney(t)
	app := j.client("porcupine-local-admin", localOAuthCallback, []string{"instance:admin"})
	owner := j.browserLogin(t, app, "operator@example.test", "/api/v1", "*", "", 200)
	project := request(t, j.service, "POST", "/api/v1/projects", owner.AccessToken, `{"name":"Demo"}`, 201)["project"].(map[string]any)["id"].(string)
	const member = "second@example.test"
	const adminInvite = `{"email":"second@example.test","projects":["*"],"permissions":["instance:admin"]}`
	invited := request(t, j.service, "POST", "/api/v1/humans", owner.AccessToken, adminInvite, 201)
	id := invited["id"].(string)
	if invited["deployment_admin"] != false || <-j.notices != member {
		t.Fatal("wrong invitation recipient or seed identity")
	}
	retry := request(t, j.service, "POST", "/api/v1/humans", owner.AccessToken, adminInvite, 201)
	if retry["id"] != id || len(j.notices) != 0 {
		t.Fatal("duplicate invitation")
	}
	admin := j.browserLogin(t, app, member, "/mcp", "*", "", 200)
	ctx, session := mcpTestSession(t, j.issuer, admin.AccessToken, "2025-11-25")
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "invite-human", Body: map[string]any{"email": "reader@example.test", "projects": []string{project}, "permissions": []string{"events:read"}}}, 201)
	if <-j.notices != "reader@example.test" {
		t.Fatal("second administrator could not invite a member")
	}
	// Replacing the role also revokes previously issued administrative grants.
	request(t, j.service, "PUT", "/api/v1/humans/"+id, owner.AccessToken, `{"projects":["`+project+`"],"permissions":["events:read"]}`, 200)
	request(t, j.service, "POST", "/mcp", admin.AccessToken, "{}", 401)
	j.checkRefresh(t, app, admin, "/mcp", 400)
	j.browserLogin(t, app, member, "/mcp", "*", "", 403)
	request(t, j.service, "PUT", "/api/v1/humans/"+id, owner.AccessToken, `{"grants":[{"project_id":"*","permissions":["instance:admin"]}]}`, 200)
	restored := j.browserLogin(t, app, member, "/api/v1", "*", "", 200)
	request(t, j.service, "DELETE", "/api/v1/humans/"+id, owner.AccessToken, "", 204)
	request(t, j.service, "GET", "/api/v1/humans", restored.AccessToken, "", 401)
	request(t, j.service, "GET", "/api/v1/humans", owner.AccessToken, "", 200)
}
