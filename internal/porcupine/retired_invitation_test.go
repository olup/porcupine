package porcupine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
)

func TestRetireManualInvitations(t *testing.T) {
	s := openAccountTest(t, filepath.Join(t.TempDir(), "test.db"), "admin@example.test")
	_, err := s.db.Exec(`CREATE TABLE local_invitations(principal_id TEXT PRIMARY KEY,code_hash TEXT,expires_at_ms INTEGER,used_at_ms INTEGER);
 INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at)
 VALUES ('retired-human','Manual','hash1','["events:read"]','["example"]','test'),
 ('old-grant','Grant','hash2','["events:read"]','["example"]','test');
 INSERT INTO local_invitations VALUES('retired-human','unredeemed-code',9999999999999,NULL);
 INSERT INTO oauth_interactive_grants(id,principal_id,human_id,client_id,resource,created_at_ms,expires_at_ms)
 VALUES('old-grant','old-grant','retired-human','porcupine-local','https://test.example/mcp',1,9999999999999)`)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := s.initializeInteractiveOAuth(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var revoked int
	if err = s.db.QueryRow("SELECT count(*) FROM access_keys WHERE id IN ('retired-human','old-grant') AND revoked_at IS NOT NULL").Scan(&revoked); err != nil || revoked != 2 {
		t.Fatalf("manual principals revoked = %d: %v", revoked, err)
	}
	if _, err = s.lookupActiveEmailID(context.Background(), "admin@example.test"); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewRecorder()
	s.Handler().ServeHTTP(api, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if api.Code != 200 || strings.Contains(api.Body.String(), "create-local-invitation") || strings.Contains(api.Body.String(), "revoke-local-invitation") || !strings.Contains(api.Body.String(), "invite-human") {
		t.Fatal("unexpected invitation API contract")
	}
}
