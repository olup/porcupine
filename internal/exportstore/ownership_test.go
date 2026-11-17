package exportstore

import "testing"

func TestOwnershipIDSurvivesCredentialRotationAndTracksNamespace(t *testing.T) {
	t.Parallel()
	config := Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "opaque", Endpoint: "http://127.0.0.1:1", UsePathStyle: true, AccessKeyID: "old-access", SecretAccessKey: "old-secret"}
	open := func(c Config) *Store {
		t.Helper()
		store, err := New(t.Context(), c)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(store.Close)
		return store
	}
	original := open(config).OwnershipID()
	rotated := config
	rotated.AccessKeyID, rotated.SecretAccessKey, rotated.SessionToken = "new-access", "new-secret", "new-session"
	rotated.Prefix += "/"
	if got := open(rotated).OwnershipID(); got != original {
		t.Fatal("credential rotation changed object ownership identity")
	}
	for _, change := range []func(*Config){
		func(c *Config) { c.Bucket = "another-bucket" },
		func(c *Config) { c.Prefix = "another-prefix" },
		func(c *Config) { c.Endpoint = "http://127.0.0.1:2" },
	} {
		different := config
		change(&different)
		if got := open(different).OwnershipID(); got == original {
			t.Fatal("a different object namespace reused ownership identity")
		}
	}
}
