package porcupine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeRestoreMarkerRefusesOpenBeforeCreatingStorage(t *testing.T) {
	dir := t.TempDir()
	database := filepath.Join(dir, "recovery.sqlite")
	if err := os.WriteFile(database+".native-restore-incomplete", []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(Config{DBPath: database, BootstrapToken: adminToken}); err == nil {
		s.Close()
		t.Fatal("incomplete restoration was opened")
	}
	if _, err := os.Stat(database); !os.IsNotExist(err) {
		t.Fatalf("startup changed the incomplete target: %v", err)
	}
	if err := os.WriteFile(database, nil, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias.sqlite")
	if err := os.Symlink(database, alias); err != nil {
		t.Skip("symlinks unavailable", err)
	}
	if err := checkNativeRestoreMarker(alias); err == nil {
		t.Fatal("database symlink bypassed the restore marker")
	}
}

func restoreGuardDatabase(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "acceptance.sqlite")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO metadata(key,value) VALUES('native_restore_credentials_reset','1')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNativeRestoreDoesNotReseedEmailAdminWithoutReview(t *testing.T) {
	for _, reviewed := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked", true: "explicit_opt_in"}[reviewed], func(t *testing.T) {
			cfg := Config{DBPath: restoreGuardDatabase(t), BootstrapToken: adminToken, OAuth: &OAuthConfig{
				Issuer: "https://analytics.example.test", AllowRestoredIdentityBootstrap: reviewed,
				Email: &EmailConfig{AdminEmail: "configured@example.test", SendCode: func(context.Context, string, string) error { return nil }},
			}}
			s, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM email_humans").Scan(&count); err != nil || count != map[bool]int{false: 0, true: 1}[reviewed] {
				t.Fatalf("configured mailbox seeded without review: reviewed=%v count=%d error=%v", reviewed, count, err)
			}
			admin := accountTestToken(t, s)
			request(t, s, "POST", "/api/v1/humans", admin, `{"email":"fresh@example.test","projects":["*"],"permissions":["instance:admin"]}`, 201)
			id, err := s.lookupActiveEmailID(context.Background(), "fresh@example.test")
			if err != nil {
				t.Fatal("fresh administrator invitation failed", err)
			}
			human, err := loadEmailHuman(context.Background(), s.db, id)
			if err != nil || !human.admin() {
				t.Fatal("fresh administrator lost its permissions", err)
			}
		})
	}
}

func TestNativeRestoreDoesNotReseedOIDCBindingsWithoutReview(t *testing.T) {
	for _, reviewed := range []bool{false, true} {
		t.Run(map[bool]string{false: "blocked", true: "explicit_opt_in"}[reviewed], func(t *testing.T) {
			s, err := Open(Config{DBPath: restoreGuardDatabase(t), BootstrapToken: adminToken})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			seeder := &Service{db: s.db, oauth: &oauthRuntime{
				config: OAuthConfig{AllowRestoredIdentityBootstrap: reviewed},
				oidc: &interactiveOIDC{issuer: "https://identity.example.test", bindings: map[string]actor{
					"configured-subject": {Permissions: []string{"events:read"}, Projects: []string{"project-a"}},
				}},
			}}
			if err := seeder.initializeOIDCHumans(context.Background()); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM oidc_humans").Scan(&count); err != nil || count != map[bool]int{false: 0, true: 1}[reviewed] {
				t.Fatalf("configured OIDC subject seeded without review: reviewed=%v count=%d error=%v", reviewed, count, err)
			}
		})
	}
}
