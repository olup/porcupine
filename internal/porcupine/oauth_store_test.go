package porcupine

import (
	"context"
	"fmt"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestOAuthAccessTokenSurvivesRestartWithoutBootstrapReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oauth.db")
	config := Config{DBPath: path, BootstrapToken: adminToken, OAuth: &OAuthConfig{Issuer: "https://oauth.example", AllowLegacyKeys: true}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	issued := oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 200)
	raw := issued["access_token"].(string)
	principal, err := s.authenticateOAuthToken(context.Background(), raw, s.oauth.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	config.BootstrapToken = "different-bootstrap-secret-not-a-reset"
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	loaded, err := s.authenticateOAuthToken(context.Background(), raw, s.oauth.apiResource)
	if err != nil || loaded.ID != principal.ID || !loaded.admin() {
		t.Fatal("OAuth persistence lost original principal or grants", err)
	}
	oauthProtocolRequest(t, s, "bootstrap", config.BootstrapToken, oauthGrantForm(), 401)
	oauthProtocolRequest(t, s, "bootstrap", adminToken, oauthGrantForm(), 200)
	if _, err = (oauthTokenStore{s: s}).GetByCode(context.Background(), "code"); err == nil {
		t.Fatal("authorization codes supported unexpectedly")
	}
	if _, err = (oauthTokenStore{s: s}).GetByRefresh(context.Background(), "refresh"); err == nil {
		t.Fatal("refresh tokens supported unexpectedly")
	}
}

func TestOAuthPerClientCapIsAtomicUnderConcurrentIssuance(t *testing.T) {
	s, clock := oauthProtocolService(t)
	id, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	for range 60 {
		oauthProtocolRequest(t, s, id, secret, oauthGrantForm(), 200)
	}
	var group sync.WaitGroup
	statuses := make(chan int, 20)
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			r := httptest.NewRequest("POST", "/oauth/token", strings.NewReader(oauthGrantForm().Encode()))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.SetBasicAuth(url.QueryEscape(id), url.QueryEscape(secret))
			w := httptest.NewRecorder()
			s.oauthToken(w, r)
			statuses <- w.Code
		}()
	}
	group.Wait()
	close(statuses)
	issued, rejected := 0, 0
	for status := range statuses {
		switch status {
		case 200:
			issued++
		case 429:
			rejected++
		default:
			t.Fatalf("unexpected concurrent token status %d", status)
		}
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM oauth_tokens WHERE client_id=?`, id).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if issued != 4 || rejected != 16 || count != 64 {
		t.Fatalf("per-client token cap exceeded: issued=%d rejected=%d rows=%d", issued, rejected, count)
	}
	clock.Add(900000)
	oauthProtocolRequest(t, s, id, secret, oauthGrantForm(), 200)
	if err := s.db.QueryRow(`SELECT count(*) FROM oauth_tokens`).Scan(&count); err != nil || count != 1 {
		t.Fatal("expired tokens were not pruned transactionally", err)
	}
}

func TestOAuthGlobalTokenCapAndExpiryPruning(t *testing.T) {
	s, clock := oauthProtocolService(t)
	ids := []string{}
	for range 157 {
		id, _ := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
		ids = append(ids, id)
	}
	fresh, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	tx, err := s.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	statement, err := tx.Prepare(`INSERT INTO oauth_tokens(token_hash,client_id,resource,scope,projects_json,issued_at_ms,expires_at_ms) VALUES(?,?,?,'events:read','["*"]',?,?)`)
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	for index := range oauthMaxTokens {
		_, err = statement.Exec(hash(fmt.Sprintf("synthetic-token-%d", index)), ids[index/64], s.oauth.apiResource, clock.Load(), clock.Load()+900000)
		if err != nil {
			statement.Close()
			tx.Rollback()
			t.Fatal(err)
		}
	}
	statement.Close()
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	oauthProtocolRequest(t, s, fresh, secret, oauthGrantForm(), 429)
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM oauth_tokens`).Scan(&count); err != nil || count != 10000 {
		t.Fatal("global token cap changed rows", err)
	}
	clock.Add(900000)
	oauthProtocolRequest(t, s, fresh, secret, oauthGrantForm(), 200)
	if err = s.db.QueryRow(`SELECT count(*) FROM oauth_tokens`).Scan(&count); err != nil || count != 1 {
		t.Fatal("global expiry pruning did not reclaim capacity", err)
	}
}

func TestOAuthLibraryVerifierDoesNotExposeOrAcceptStoredDigest(t *testing.T) {
	s, _ := oauthProtocolService(t)
	id, secret := oauthProtocolClient(t, s, []string{"events:read"}, []string{"*"})
	client, err := loadOAuthClient(context.Background(), s.db, id)
	if err != nil {
		t.Fatal(err)
	}
	if client.GetSecret() != "" || !client.VerifyPassword(secret) || client.VerifyPassword(hash(secret)) || client.VerifyPassword("") {
		t.Fatal("client verifier exposed or confused stored hash with a secret")
	}
	malformed := &oauthClient{secretHash: "malformed"}
	if malformed.VerifyPassword(secret) {
		t.Fatal("malformed digest accepted")
	}
	form := oauthGrantForm()
	form.Set("scope", "events:read")
	issued := oauthProtocolRequest(t, s, id, secret, form, 200)
	store := oauthTokenStore{s: s}
	if err = store.RemoveByAccess(context.Background(), issued["access_token"].(string)); err != nil {
		t.Fatal(err)
	}
	if _, err = store.GetByAccess(context.Background(), issued["access_token"].(string)); err == nil {
		t.Fatal("removed token remained readable")
	}
	if err = store.RemoveByAccess(context.Background(), "absent"); err != nil {
		t.Fatal("token removal was not idempotent", err)
	}
}
