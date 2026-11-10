package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestEmailAddressIdentity(t *testing.T) {
	for input, want := range map[string]string{
		" Lea+ops@EXAMPLE.COM ": "Lea+ops@example.com", "a.b@company.internal": "a.b@company.internal",
	} {
		normalized, err := normalizeEmail(input)
		if err != nil || normalized != want {
			t.Fatalf("address %q returned %q, %v", input, normalized, err)
		}
	}
	for _, input := range []string{"Name <a@example.com>", "a@example.com\r\nBcc: abuse@example.com", "a@example☃.com", "a@ex_ample.com", "a@localhost..", "a@.local", "a@[127.0.0.1]", "a@example.com,a@evil.com", "a@-bad.example", "a@bad-.example", strings.Repeat("a", 65) + "@example.com"} {
		if _, err := normalizeEmail(input); err == nil {
			t.Errorf("accepted malformed mailbox %q", input)
		}
	}
}
func openAccountTest(t *testing.T, dbpath, adminEmail string) *Service {
	t.Helper()
	cfg := Config{DBPath: dbpath, BootstrapToken: adminToken, OAuth: &OAuthConfig{
		Issuer: "https://test.example", Email: &EmailConfig{AdminEmail: adminEmail, SendCode: func(context.Context, string, string) error { return nil }},
	}}
	s, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func accountTestToken(t *testing.T, s *Service) string {
	t.Helper()
	return oauthProtocolRequest(t, s, "bootstrap", adminToken, url.Values{"grant_type": {"client_credentials"}, "resource": {s.oauth.apiResource}}, 200)["access_token"].(string)
}
func TestEmailHumansGrantLifecycle(t *testing.T) {
	ctx := context.Background()
	s := openAccountTest(t, filepath.Join(t.TempDir(), "db"), "operator@example.com")
	admin := accountTestToken(t, s)
	p1 := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"First"}`, 201)["project"].(map[string]any)["id"].(string)
	p2 := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"Second"}`, 201)["project"].(map[string]any)["id"].(string)
	principalID, err := s.lookupActiveEmailID(ctx, "operator@EXAMPLE.COM")
	if err != nil {
		t.Fatal(err)
	}
	operator, err := loadEmailHuman(ctx, s.db, principalID)
	if err != nil || !operator.admin() || !operator.emailHuman {
		t.Fatalf("seed admin: %+v %v", operator, err)
	}
	input := `{"email":"lea@example.com","projects":["` + p1 + `"],"permissions":["events:read"]}`
	h := request(t, s, "POST", "/api/v1/humans", admin, input, 201)
	id := h["id"].(string)
	if h["token"] != nil || h["code"] != nil || h["email"] != "lea@example.com" || h["deployment_admin"] != false {
		t.Fatal("unexpected account metadata")
	}
	h = request(t, s, "POST", "/api/v1/humans", admin, strings.Replace(input, "@example.com", "@EXAMPLE.COM", 1), 201)
	if id != h["id"] {
		t.Fatal("retry assigned a different principal ID")
	}
	actor, err := loadEmailHuman(ctx, s.db, id)
	if err != nil || !authorize(actor, "events:read", p1) || authorize(actor, "events:read", p2) || authorize(actor, "events:write", p1) {
		t.Fatal("account project or permission escaped")
	}
	request(t, s, "POST", "/api/v1/humans", admin, strings.Replace(input, p1, p2, 1), 409)
	request(t, s, "POST", "/api/v1/humans", admin, strings.Replace(input, p1, "*", 1), 400)
	request(t, s, "POST", "/api/v1/humans", admin, strings.Replace(input, "events:read", "instance:admin", 1), 400)
	request(t, s, "POST", "/api/v1/humans", admin, strings.Replace(input, p1, "unknown", 1), 400)
	request(t, s, "PUT", "/api/v1/humans/"+id, admin, `{"projects":["`+p2+`"],"permissions":["events:read"]}`, 200)
	actor, err = loadEmailHuman(ctx, s.db, id)
	if err != nil || !slices.Equal(actor.Projects, []string{p2}) {
		t.Fatal("rights were not replaced")
	}
	request(t, s, "DELETE", "/api/v1/humans/"+id, admin, "", 204)
	if _, err = s.lookupActiveEmailID(ctx, "lea@example.com"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("revoked email can still start login")
	}
	request(t, s, "POST", "/api/v1/humans", admin, input, 409)
	request(t, s, "PUT", "/api/v1/humans/"+id, admin, `{"projects":["`+p2+`"],"permissions":["events:read"]}`, 404)
	got := request(t, s, "GET", "/api/v1/humans?limit=1", admin, "", 200)
	if got["next_offset"] != float64(1) || len(got["humans"].([]any)) != 1 {
		t.Fatal("invalid humans pagination")
	}
}
func TestEmailBootstrapPinAndBreakGlass(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "state.db")
	s := openAccountTest(t, db, "owner@example.com")
	api := accountTestToken(t, s)
	owner, err := s.lookupActiveEmailID(ctx, "owner@example.com")
	if err != nil {
		t.Fatal(err)
	}
	var bootstrap string
	if err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&bootstrap); err != nil {
		t.Fatal(err)
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+bootstrap, api, "", 409)
	request(t, s, "DELETE", "/api/v1/humans/"+owner, api, "", 204)
	s2 := openAccountTest(t, db, "owner@EXAMPLE.COM")
	if _, err := s2.lookupActiveEmailID(ctx, "owner@example.com"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("restart reactivated revoked admin")
	}
	cfg := Config{DBPath: db, BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: "https://test.example", Email: &EmailConfig{AdminEmail: "different@example.com", SendCode: func(context.Context, string, string) error { return nil }}}}
	if other, err := Open(cfg); err == nil {
		other.Close()
		t.Fatal("reconfigured admin email created a new administrator")
	}
}
func TestEmailBootstrapCannotPromoteExistingAccount(t *testing.T) {
	db := filepath.Join(t.TempDir(), "state.db")
	s := openAccountTest(t, db, "")
	api := accountTestToken(t, s)
	pid := request(t, s, "POST", "/api/v1/projects", api, `{"name":"P"}`, 201)["project"].(map[string]any)["id"].(string)
	request(t, s, "POST", "/api/v1/humans", api, `{"email":"member@example.com","projects":["`+pid+`"],"permissions":["events:read"]}`, 201)
	cfg := Config{DBPath: db, BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: "https://test.example", Email: &EmailConfig{AdminEmail: "member@example.com", SendCode: func(context.Context, string, string) error { return nil }}}}
	if other, err := Open(cfg); err == nil {
		other.Close()
		t.Fatal("ordinary email account elevated by bootstrap")
	}
}

func TestEmailInvitationDeliveryRecovery(t *testing.T) {
	s := openAccountTest(t, filepath.Join(t.TempDir(), "db"), "")
	admin := accountTestToken(t, s)
	pid := request(t, s, "POST", "/api/v1/projects", admin, `{"name":"Analytics"}`, 201)["project"].(map[string]any)["id"].(string)
	ctx := oauthManagementAdmin(t, s)
	in := &InviteHumanInput{}
	in.Body.Email = "lea@example.test"
	in.Body.Permissions = []string{"events:read"}
	in.Body.Projects = []string{pid}
	var calls atomic.Int32
	enter := make(chan struct{})
	release := make(chan struct{})
	s.oauth.config.Email.SendInvitation = func(ctx context.Context, to string, _ bool) error {
		if to != in.Body.Email {
			return errors.New("wrong recipient")
		}
		if calls.Add(1) == 1 {
			close(enter)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
			return errors.New("mail relay temporarily unavailable")
		}
		return nil
	}
	first := make(chan error, 1)
	go func() { _, err := s.inviteHuman(ctx, in); first <- err }()
	select {
	case <-enter:
	case <-time.After(3 * time.Second):
		t.Fatal("mail sender did not receive invitation")
	}
	if _, err := s.inviteHuman(ctx, in); queryStatus(err) != 429 {
		t.Fatal("parallel invitation did not report pending notice", err)
	}
	id, err := s.lookupActiveEmailID(ctx, in.Body.Email)
	if err != nil {
		t.Fatal("database locked or account not durable while mail in flight", err)
	}
	close(release)
	if err := <-first; queryStatus(err) != 503 {
		t.Fatal("delivery failure not reported", err)
	}
	if _, err := s.inviteHuman(ctx, in); queryStatus(err) != 429 {
		t.Fatal("retry cooldown missing", err)
	}
	if _, err = s.db.Exec(`UPDATE email_humans SET notice_retry_after_ms=0 WHERE principal_id=?`, id); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		r, err := s.inviteHuman(ctx, in)
		if err != nil || r.Body.ID != id {
			t.Fatal("idempotent delivery retry failed", err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("wanted one failed and one successful delivery; got %d calls", calls.Load())
	}
}

func TestEmailHumanProjectGrantChanges(t *testing.T) {
	s := openAccountTest(t, filepath.Join(t.TempDir(), "db"), "")
	admin := accountTestToken(t, s)
	mk := func(name string) string {
		return request(t, s, "POST", "/api/v1/projects", admin, `{"name":"`+name+`"}`, 201)["project"].(map[string]any)["id"].(string)
	}
	a, b := mk("A"), mk("B")
	grants := func(read, write string) string {
		return `[{"project_id":"` + read + `","permissions":["events:read"]},{"project_id":"` + write + `","permissions":["events:write"]}]`
	}
	all := grants(a, b)
	input := `{"email":"lea@example.test","grants":` + all + `}`
	result := request(t, s, "POST", "/api/v1/humans", admin, input, 201)
	id := result["id"].(string)
	if len(result["grants"].([]any)) != 2 {
		t.Fatal("missing human project grants")
	}
	ctx := context.Background()
	h, err := loadEmailHuman(ctx, s.db, id)
	if err != nil || !slices.Equal(h.emailProjectPermissions[a], []string{"events:read"}) || !slices.Equal(h.emailProjectPermissions[b], []string{"events:write"}) {
		t.Fatal("incorrect stored project grants", err)
	}
	request(t, s, "POST", "/api/v1/humans", admin, `{"email":"lea@example.test","grants":`+grants(b, a)+`}`, 409)
	request(t, s, "POST", "/api/v1/humans", admin, `{"email":"lea@example.test","grants":`+all+`,"projects":["`+a+`"]}`, 400)

	// Observe the durable consent principal used by OAuth token validation.
	grantID := newID()
	_, err = s.db.Exec(`INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'test grant',?,'["events:read"]',?,?)`, grantID, hash(token()), `["`+a+`"]`, now())
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.db.Exec(`INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,created_at_ms,expires_at_ms,resource) VALUES(?,?,?,?,?,?,?)`, grantID, grantID, id, principal(oauthManagementAdmin(t, s)).ID, s.clock().UnixMilli(), s.clock().Add(time.Hour).UnixMilli(), s.oauth.mcpResource)
	if err != nil {
		t.Fatal(err)
	}
	active := func() bool {
		var yes bool
		if err := s.db.QueryRow(`SELECT revoked_at IS NULL FROM access_keys WHERE id=?`, grantID).Scan(&yes); err != nil {
			t.Fatal(err)
		}
		return yes
	}
	request(t, s, "PUT", "/api/v1/humans/"+id, admin, `{"grants":`+all+`}`, 200)
	if !active() {
		t.Fatal("unchanged grant revoked")
	}
	request(t, s, "PUT", "/api/v1/humans/"+id, admin, `{"grants":`+grants(b, a)+`}`, 200)
	if active() {
		t.Fatal("swapping rights across projects kept a stale consent grant")
	}
}

func TestEmailAdminInviteWithoutSeedMailbox(t *testing.T) {
	s := openAccountTest(t, filepath.Join(t.TempDir(), "db"), "")
	ctx := context.Background()
	client, err := loadOAuthClient(ctx, s.db, "porcupine-local-admin")
	if err != nil || !client.permitsInteractiveScope("instance:admin") {
		t.Fatal("admin PKCE client must be available without a seeded mailbox")
	}
	admin := accountTestToken(t, s)
	for _, grants := range []string{
		`[{"project_id":"*","permissions":["instance:admin","events:read"]}]`,
		`[{"project_id":"*","permissions":["instance:admin"]},{"project_id":"something","permissions":["events:read"]}]`,
	} {
		request(t, s, "POST", "/api/v1/humans", admin, `{"email":"new@example.test","grants":`+grants+`}`, 400)
	}
	request(t, s, "POST", "/api/v1/humans", admin, `{"email":"new@example.test","projects":["*"],"permissions":["instance:admin"]}`, 201)
}
