package porcupine

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

func newSharedOIDCFixture(t *testing.T) (*oidcFixture, *Service, *Service, *[2]atomic.Uint32) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	ctx := t.Context()
	first, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Close() })
	queries, err := normalizeInteractiveQueries(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := initializeSharedControl(ctx, first, adminToken, queries); err != nil {
		t.Fatal(err)
	}
	second, err := openSharedControl(ctx, SharedConfig{DatabaseURL: dsn})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close() })
	provider := newOIDCMock(t)
	host := httptest.NewUnstartedServer(nil)
	issuer := "http://" + host.Listener.Addr().String()
	mail := make(chan string, 8)
	makeWorker := func(control *sql.DB) *Service {
		t.Helper()
		local, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "journal.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { local.Close() })
		if _, err := local.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		runtime, err := normalizeOAuthConfig(OAuthConfig{
			Issuer: issuer,
			OIDC:   &OIDCConfig{Issuer: provider.host.URL, ClientID: "upstream-client", ClientSecret: "mock-client-secret"},
			Email:  &EmailConfig{SendCode: func(_ context.Context, _ string, code string) error { mail <- code; return nil }},
		})
		if err != nil {
			t.Fatal(err)
		}
		service := &Service{db: local, control: control, shared: &sharedRuntime{}, oauth: runtime, eventRules: &eventRules{clock: time.Now}, tokenSlots: make(chan struct{}, 8)}
		if err := service.initializeOAuthClients(ctx); err != nil {
			t.Fatal(err)
		}
		if err := service.initializeServiceKeys(ctx); err != nil {
			t.Fatal(err)
		}
		if err := service.initializeOAuth(ctx); err != nil {
			t.Fatal(err)
		}
		return service
	}
	a, b := makeWorker(first), makeWorker(second)
	for _, project := range []string{"project-a", "project-b"} {
		if _, err := first.Exec(`INSERT INTO projects(id,tenant_id,name,created_at) VALUES($1,'default',$1,$2)`, project, now()); err != nil {
			t.Fatal(err)
		}
	}
	human := EmailHuman{ID: newID(), Email: "operator@example.test", CreatedAt: now(), Permissions: []string{"events:read", "management:read"}, Projects: []string{"project-a"}, Grants: []HumanProjectGrant{{ProjectID: "project-a", Permissions: []string{"events:read", "management:read"}}}}
	tx, err := first.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := insertEmailHuman(ctx, tx, human); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	muxes := []*http.ServeMux{http.NewServeMux(), http.NewServeMux()}
	a.registerOAuth(muxes[0])
	b.registerOAuth(muxes[1])
	var next atomic.Uint32
	var hits [2]atomic.Uint32
	host.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := next.Add(1) % 2
		hits[index].Add(1)
		muxes[index].ServeHTTP(w, r)
	})
	host.Start()
	t.Cleanup(host.Close)
	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(client.CloseIdleConnections)
	fixture := &oidcFixture{s: a, host: host, provider: provider, client: client, clientID: "porcupine-local", callback: localOAuthCallback, verifier: oauth2.GenerateVerifier(), humanID: human.ID, mail: mail, resource: issuer + "/mcp", scope: "events:read management:read"}
	return fixture, a, b, &hits
}

func TestSharedOIDCInvitedIdentityAcrossWorkersAndRevocation(t *testing.T) {
	fixture, a, b, hits := newSharedOIDCFixture(t)
	first, ctx := a.control, t.Context()
	human := EmailHuman{ID: fixture.humanID, Email: "operator@example.test"}
	issuer := fixture.host.URL
	access := fixture.issueToken(t, "project-a")
	for _, worker := range []*Service{a, b} {
		principal, err := worker.authenticateResource(ctx, access, issuer+"/mcp")
		if err != nil || !authorize(principal, "events:read", "project-a") || authorize(principal, "events:read", "project-b") || authorize(principal, "events:write", "project-a") {
			t.Fatalf("cross-worker OIDC grant escaped scope: %+v %v", principal, err)
		}
		linked, err := worker.oidcHuman(ctx, "operator-subject", human.Email)
		if err != nil || linked.ID != human.ID {
			t.Fatalf("cross-worker identity mapping diverged: %+v %v", linked, err)
		}
	}
	var links int
	if err := first.QueryRow(`SELECT count(*) FROM oidc_identities WHERE principal_id=$1`, human.ID).Scan(&links); err != nil || links != 1 {
		t.Fatalf("expected one shared identity link: %d %v", links, err)
	}
	if hits[0].Load() < 2 || hits[1].Load() < 2 {
		t.Fatal("OIDC journey did not cross workers")
	}
	if _, err := first.Exec(`UPDATE access_keys SET revoked_at=$1 WHERE id=$2`, now(), human.ID); err != nil {
		t.Fatal(err)
	}
	for _, worker := range []*Service{a, b} {
		if _, err := worker.authenticateResource(ctx, access, issuer+"/mcp"); err == nil {
			t.Fatal("revoked human retained cross-worker token")
		}
		if _, err := worker.oidcHuman(ctx, "operator-subject", human.Email); err == nil {
			t.Fatal("provider identity revived revoked shared human")
		}
	}
}
