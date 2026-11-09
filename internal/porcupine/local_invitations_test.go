package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func (f *oidcFixture) enterInvitation(t *testing.T, code string, want int) string {
	t.Helper()
	_, body := f.get(t, f.authorizeURL(f.callback), 200)
	field := regexp.MustCompile(`name="csrf" value="([^"]+)"`)
	csrf := field.FindStringSubmatch(body)
	if len(csrf) != 2 {
		t.Fatal("no invitation form")
	}
	form := url.Values{"csrf": {csrf[1]}, "code": {code}}
	r, _ := http.NewRequest("POST", f.host.URL+"/oauth/invitation", strings.NewReader(form.Encode()))
	r.Header.Set("Origin", f.host.URL)
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, err := f.client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != want {
		t.Fatalf("invitation login %d want %d", res.StatusCode, want)
	}
	if want != 303 {
		return ""
	}
	_, body = f.get(t, f.host.URL+"/oauth/consent", 200)
	csrf = field.FindStringSubmatch(body)
	if len(csrf) != 2 {
		t.Fatal("no consent form")
	}
	return csrf[1]
}

func TestLocalInvitationMCP(t *testing.T) {
	f := newLoginFixture(t, true)
	for _, id := range []string{"project-a", "project-b"} {
		_, err := f.s.db.Exec("INSERT INTO projects(id,name,created_at) VALUES(?,?,?)", id, id, now())
		if err != nil {
			t.Fatal(err)
		}
	}
	adminTokenFor := func(resource string) string {
		return oauthProtocolRequest(t, f.s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + resource}}, 200)["access_token"].(string)
	}
	apiAdmin := adminTokenFor("/api/v1")
	ctx, admin := mcpTestSession(t, f.host.URL, adminTokenFor("/mcp"), "2025-11-25")
	invite := mcpTestCall(t, ctx, admin, mcpCallInput{OperationID: "create-local-invitation", Path: map[string]string{"id": "project-a"}}, 201)
	code := invite["code"].(string)
	id := invite["id"].(string)
	expires, err := time.Parse(time.RFC3339Nano, invite["expires_at"].(string))
	if err != nil || time.Until(expires) < 24*time.Hour-time.Minute || time.Until(expires) > 24*time.Hour {
		t.Fatal("wrong invitation lifetime")
	}
	var digest string
	if err = f.s.db.QueryRow("SELECT code_hash FROM local_invitations WHERE principal_id=?", id).Scan(&digest); err != nil || digest != hash(code) {
		t.Fatal("code not securely stored")
	}
	csrf := f.enterInvitation(t, code, 303)
	other := *f
	other.client = &http.Client{CheckRedirect: f.client.CheckRedirect}
	other.client.Jar, _ = cookiejar.New(nil)
	otherCSRF := other.enterInvitation(t, code, 303)
	form := url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-b"}}
	f.submit(t, form, f.host.URL, 403)
	// Both browsers passed login before either approved. Release their consent requests together.
	type approval struct {
		status   int
		location string
		err      error
	}
	results := make(chan approval, 2)
	start := make(chan struct{})
	for _, attempt := range []struct {
		fixture *oidcFixture
		csrf    string
	}{{f, csrf}, {&other, otherCSRF}} {
		go func() {
			<-start
			values := url.Values{"csrf": {attempt.csrf}, "decision": {"approve"}, "project": {"project-a"}}
			req, err := http.NewRequest("POST", f.host.URL+"/oauth/consent", strings.NewReader(values.Encode()))
			if err != nil {
				results <- approval{err: err}
				return
			}
			req.Header.Set("Origin", f.host.URL)
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			res, err := attempt.fixture.client.Do(req)
			if err != nil {
				results <- approval{err: err}
				return
			}
			res.Body.Close()
			results <- approval{status: res.StatusCode, location: res.Header.Get("Location")}
		}()
	}
	close(start)
	var location string
	statuses := map[int]int{}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		statuses[result.status]++
		if result.status == 303 {
			location = result.location
		}
	}
	if statuses[303] != 1 || statuses[403] != 1 {
		t.Fatalf("concurrent approvals: %v", statuses)
	}

	other.enterInvitation(t, code, 403)
	target, _ := url.Parse(location)
	response, err := http.PostForm(f.host.URL+"/oauth/token", interactiveTokenForm(f, target.Query().Get("code")))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var tokens map[string]any
	if json.NewDecoder(response.Body).Decode(&tokens) != nil || response.StatusCode != 200 {
		t.Fatalf("exchange failed: %d", response.StatusCode)
	}
	bearer := tokens["access_token"].(string)
	ctx, client := mcpTestSession(t, f.host.URL, bearer, "2025-11-25")
	for project, want := range map[string]int{"project-a": 200, "project-b": 403} {
		mcpTestCall(t, ctx, client, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": project}}, want)
	}
	request(t, f.s, "GET", "/api/v1/projects/project-a/schema", bearer, "", 401)
	grants := request(t, f.s, "GET", "/api/v1/oauth-grants", apiAdmin, "", 200)["grants"].([]any)
	if len(grants) != 1 || grants[0].(map[string]any)["human_id"] != id {
		t.Fatal("local grant not listed")
	}
	mcpTestCall(t, ctx, admin, mcpCallInput{OperationID: "revoke-local-invitation", Path: map[string]string{"id": id}}, 204)
	request(t, f.s, "POST", "/mcp", bearer, "{}", 401)
	interactiveEndpoint(t, f.s, "/oauth/token", url.Values{"grant_type": {"refresh_token"}, "client_id": {f.clientID}, "refresh_token": {tokens["refresh_token"].(string)}}, "", "", 400)
}

func TestLocalInvitationExpiryAndCancellation(t *testing.T) {
	f := newLoginFixture(t, true)
	if _, err := f.s.db.Exec("INSERT INTO projects(id,name,created_at) VALUES('project-a','A',?)", now()); err != nil {
		t.Fatal(err)
	}
	admin := oauthProtocolRequest(t, f.s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {f.host.URL + "/api/v1"}}, 200)["access_token"].(string)
	makeInvite := func() map[string]any {
		return request(t, f.s, "POST", "/api/v1/projects/project-a/invitations", admin, "", 201)
	}
	invite := makeInvite()
	request(t, f.s, "DELETE", "/api/v1/invitations/"+invite["id"].(string), admin, "", 204)
	f.enterInvitation(t, invite["code"].(string), 403)
	invite = makeInvite()
	csrf := f.enterInvitation(t, invite["code"].(string), 303)
	if _, err := f.s.db.Exec("UPDATE local_invitations SET expires_at_ms=? WHERE principal_id=?", f.s.clock().UnixMilli(), invite["id"]); err != nil {
		t.Fatal(err)
	}
	f.submit(t, url.Values{"csrf": {csrf}, "decision": {"approve"}, "project": {"project-a"}}, f.host.URL, 403)
	f.enterInvitation(t, invite["code"].(string), 403)
}

func TestLocalInvitationChromium(t *testing.T) {
	if os.Getenv("PORCUPINE_OIDC_BROWSER") != "1" {
		t.Skip("set PORCUPINE_OIDC_BROWSER=1 with Playwright installed in compat")
	}
	f := newLoginFixture(t, true)
	if _, err := f.s.db.Exec("INSERT INTO projects(id,name,created_at) VALUES('project-a','A',?)", now()); err != nil {
		t.Fatal(err)
	}
	invite, err := f.s.createLocalInvitation(context.Background(), &InvitationCreateInput{ID: "project-a"})
	if err != nil {
		t.Fatal(err)
	}
	script := `const {chromium}=require(process.argv[1]);
 (async()=>{const browser=await chromium.launch();try{
 const page=await browser.newPage();await page.goto(process.argv[2]);
 await page.getByLabel('Invitation code').fill(process.env.TEST_INVITATION);
 await page.getByRole('button',{name:'Review access'}).click();
 await page.getByLabel('project-a').check();
 await page.getByRole('button',{name:'Approve access'}).click();
 await page.waitForURL(url=>url.pathname==='/client/callback'&&url.searchParams.has('code'));
 }finally{await browser.close();}})().catch(err=>{console.error(err);process.exit(1)});`
	pkg, err := filepath.Abs("../../compat/node_modules/playwright")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "node", "-e", script, pkg, f.authorizeURL(f.callback))
	cmd.Env = append(os.Environ(), "TEST_INVITATION="+invite.Body.Code)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Chromium consent failed: %v %s", err, out)
	}
}
