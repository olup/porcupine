package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/oauth2"
)

// This journey deliberately alternates public OAuth requests between independent
// process-local runtimes. Only metadata and authentication state use PostgreSQL.
func TestSharedAuthEmailOAuthAcrossWorkersAndRevocation(t *testing.T) {
	dsn := sharedPostgresDSN(t)
	ctx := context.Background()
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
	host := httptest.NewUnstartedServer(nil)
	issuer := "http://" + host.Listener.Addr().String()
	mail := make(chan journeyMail, 8)
	makeWorker := func(control *sql.DB) *Service {
		t.Helper()
		local, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "local.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { local.Close() })
		if _, err := local.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)`); err != nil {
			t.Fatal(err)
		}
		oauth, err := normalizeOAuthConfig(OAuthConfig{Issuer: issuer, Email: &EmailConfig{SendCode: func(ctx context.Context, to, code string) error {
			select {
			case mail <- journeyMail{to: to, code: code}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}}})
		if err != nil {
			t.Fatal(err)
		}
		s := &Service{db: local, control: control, shared: &sharedRuntime{}, oauth: oauth, eventRules: &eventRules{clock: time.Now}, tokenSlots: make(chan struct{}, 8)}
		if err := s.initializeOAuthClients(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.initializeServiceKeys(ctx); err != nil {
			t.Fatal(err)
		}
		if err := s.initializeOAuth(ctx); err != nil {
			t.Fatal(err)
		}
		return s
	}
	a, b := makeWorker(first), makeWorker(second)
	for _, project := range []string{"project-a", "project-b"} {
		if _, err := first.Exec(`INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES($1,'default',$1,$2,'mixed')`, project, now()); err != nil {
			t.Fatal(err)
		}
	}
	human := EmailHuman{ID: newID(), Email: "person@example.test", CreatedAt: now(), Permissions: []string{"management:read", "events:read"}, Projects: []string{"project-a"}, Grants: []HumanProjectGrant{{ProjectID: "project-a", Permissions: []string{"management:read", "events:read"}}}}
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
	var requests atomic.Uint32
	host.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { muxes[requests.Add(1)%2].ServeHTTP(w, r) })
	host.Start()
	t.Cleanup(host.Close)
	j := &emailJourney{service: a, issuer: issuer, mail: mail}
	app := j.client("porcupine-local", localOAuthCallback, []string{"management:read", "events:read"})
	token := j.browserLogin(t, app, human.Email, "/mcp", "project-a", "project-b", 200)
	for _, worker := range []*Service{a, b} {
		actor, err := worker.authenticateResource(ctx, token.AccessToken, worker.oauth.mcpResource)
		if err != nil || !authorize(actor, "events:read", "project-a") || authorize(actor, "events:read", "project-b") {
			t.Fatalf("shared OAuth lost project isolation: actor=%+v err=%v", actor, err)
		}
	}
	var operatorID string
	if err := first.QueryRow(`SELECT value FROM metadata WHERE key='oauth_bootstrap_id'`).Scan(&operatorID); err != nil {
		t.Fatal(err)
	}
	operator := context.WithValue(ctx, actorContextKey{}, actor{ID: operatorID, Permissions: []string{"instance:admin"}, Projects: []string{"*"}})
	clientInput := &OAuthClientCreateInput{}
	clientInput.Body.Name = "Shared reporting service"
	clientInput.Body.Permissions = []string{"events:read"}
	clientInput.Body.Projects = []string{"project-a"}
	machine, err := a.managementCreateOAuthClient(operator, clientInput)
	if err != nil {
		t.Fatal(err)
	}
	keyInput := &ServiceKeyCreateInput{ID: machine.Body.ClientID}
	keyInput.Body.Name, keyInput.Body.Audience = "Static report reader", "api"
	keyInput.Body.Permissions, keyInput.Body.Projects = []string{"events:read"}, []string{"project-a"}
	key, err := a.managementCreateServiceKey(operator, keyInput)
	if err != nil {
		t.Fatal(err)
	}
	tokenForm := url.Values{"grant_type": {"client_credentials"}, "resource": {issuer + "/api/v1"}}
	tokenRequest, err := http.NewRequest("POST", issuer+"/oauth/token", strings.NewReader(tokenForm.Encode()))
	if err != nil {
		t.Fatal(err)
	}
	tokenRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenRequest.SetBasicAuth(machine.Body.ClientID, machine.Body.ClientSecret)
	response, err := http.DefaultClient.Do(tokenRequest)
	if err != nil {
		t.Fatal(err)
	}
	var machineToken struct {
		AccessToken string `json:"access_token"`
	}
	err = json.NewDecoder(response.Body).Decode(&machineToken)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || machineToken.AccessToken == "" {
		t.Fatalf("shared service token exchange failed: status=%d err=%v", response.StatusCode, err)
	}
	for _, worker := range []*Service{a, b} {
		for _, secret := range []string{machineToken.AccessToken, key.Body.Token} {
			principal, err := worker.authenticateResource(ctx, secret, worker.oauth.apiResource)
			if err != nil || !authorize(principal, "events:read", "project-a") || authorize(principal, "events:read", "project-b") {
				t.Fatalf("machine access lost shared scope: actor=%+v err=%v", principal, err)
			}
		}
	}
	if _, err := first.Exec(`UPDATE access_keys SET revoked_at=$1 WHERE id=$2`, now(), machine.Body.ClientID); err != nil {
		t.Fatal(err)
	}
	for _, worker := range []*Service{a, b} {
		for _, secret := range []string{machineToken.AccessToken, key.Body.Token} {
			if _, err := worker.authenticateResource(ctx, secret, worker.oauth.apiResource); err == nil {
				t.Fatal("revoked service account retained delegated access")
			}
		}
	}
	verifier := oauth2.GenerateVerifier()
	authorizationURL, err := url.Parse(app.AuthCodeURL("race-state", oauth2.S256ChallengeOption(verifier)))
	if err != nil {
		t.Fatal(err)
	}
	requestID, err := oauthRandom("pc_ar_")
	if err != nil {
		t.Fatal(err)
	}
	authorization := oauthAuthorizationRequest{ID: requestID, ClientID: app.ClientID, RedirectURI: app.RedirectURL,
		Scope: "events:read management:read", Resource: issuer + "/mcp", State: "race-state",
		CodeChallenge: authorizationURL.Query().Get("code_challenge"), ExpiresAtMS: time.Now().Add(oauthRequestTTL).UnixMilli()}
	if _, err := first.Exec(`INSERT INTO oauth_authorization_requests(request_hash,client_id,redirect_uri,scope,resource,state,challenge,expires_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		hash(requestID), authorization.ClientID, authorization.RedirectURI, authorization.Scope, authorization.Resource, authorization.State, authorization.CodeChallenge, authorization.ExpiresAtMS); err != nil {
		t.Fatal(err)
	}
	callback, err := a.issueOAuthAuthorizationCode(ctx, authorization, human.ID, actor{ID: human.ID, Permissions: []string{"events:read", "management:read"}, Projects: []string{"project-a"}})
	if err != nil {
		t.Fatal(err)
	}
	callbackURL, err := url.Parse(callback)
	if err != nil {
		t.Fatal(err)
	}
	var exchanges atomic.Int32
	var exchangeWG sync.WaitGroup
	for _, worker := range []*Service{a, b} {
		exchangeWG.Add(1)
		go func(worker *Service) {
			defer exchangeWG.Done()
			client, err := loadOAuthClient(ctx, worker.control, app.ClientID)
			if err != nil {
				t.Error(err)
				return
			}
			form := url.Values{"code": {callbackURL.Query().Get("code")}, "redirect_uri": {app.RedirectURL}, "code_verifier": {verifier}, "resource": {issuer + "/mcp"}}
			if _, err := worker.exchangeInteractiveCode(ctx, client, form); err == nil {
				exchanges.Add(1)
			}
		}(worker)
	}
	exchangeWG.Wait()
	if exchanges.Load() != 1 {
		t.Fatalf("one authorization code produced %d successful exchanges", exchanges.Load())
	}
	if _, err := first.Exec(`UPDATE access_keys SET revoked_at=$1 WHERE id=$2`, now(), human.ID); err != nil {
		t.Fatal(err)
	}
	for _, worker := range []*Service{a, b} {
		if _, err := worker.authenticateResource(ctx, token.AccessToken, worker.oauth.mcpResource); err == nil {
			t.Fatal("revoked human retained access on a worker")
		}
	}
	j.checkRefresh(t, app, token, "/mcp", 400)
	if requests.Load() < 8 {
		t.Fatal("journey did not exercise alternating workers")
	}
}
