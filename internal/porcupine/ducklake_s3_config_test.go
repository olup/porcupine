package porcupine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validLakeS3Config() DuckLakeS3Config {
	return DuckLakeS3Config{Bucket: "analytics-tests", Prefix: "porcupine/isolated", Region: "eu-west-3", Endpoint: "http://127.0.0.1:9000", UsePathStyle: true, AccessKeyID: "fixture-access-private", SecretAccessKey: "fixture-secret-private"}
}

func TestDuckLakeS3ConfigValidationAndRedaction(t *testing.T) {
	base := validLakeS3Config()
	if normalized, err := normalizeDuckLakeS3(&base); err != nil || normalized.Prefix != base.Prefix {
		t.Fatal("valid analytical object storage rejected", err)
	}
	for _, mutate := range []struct {
		name   string
		change func(*DuckLakeS3Config)
	}{
		{"request-timeout", func(c *DuckLakeS3Config) { c.RequestTimeoutSeconds = 31 }},
		{"bucket", func(c *DuckLakeS3Config) { c.Bucket = "../bucket" }},
		{"prefix", func(c *DuckLakeS3Config) { c.Prefix = "prefix/../other" }},
		{"empty-prefix", func(c *DuckLakeS3Config) { c.Prefix = "" }},
		{"region", func(c *DuckLakeS3Config) { c.Region = "region', SELECT secret" }},
		{"credentials", func(c *DuckLakeS3Config) { c.SecretAccessKey = "" }},
		{"token-only", func(c *DuckLakeS3Config) { c.AccessKeyID = ""; c.SecretAccessKey = ""; c.SessionToken = "token" }},
		{"http-public", func(c *DuckLakeS3Config) { c.Endpoint = "http://untrusted.example" }},
		{"url-credentials", func(c *DuckLakeS3Config) { c.Endpoint = "https://secret:password@storage.example" }},
		{"url-path", func(c *DuckLakeS3Config) { c.Endpoint = "https://storage.example/somewhere" }},
		{"query", func(c *DuckLakeS3Config) { c.Endpoint = "https://storage.example?secret=private" }},
		{"ca-directory", func(c *DuckLakeS3Config) { c.CACertFile = t.TempDir() }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			candidate := base
			mutate.change(&candidate)
			if _, err := normalizeDuckLakeS3(&candidate); err == nil {
				t.Fatal("invalid analytical storage accepted")
			}
		})
	}
	ca := filepath.Join(t.TempDir(), "invalid.pem")
	if err := os.WriteFile(ca, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	candidate := base
	candidate.CACertFile = ca
	if _, err := normalizeDuckLakeS3(&candidate); err == nil {
		t.Fatal("invalid TLS trust certificate accepted")
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	printed := fmt.Sprintf("%+v %#v %s", base, base, encoded)
	for _, secret := range []string{base.AccessKeyID, base.SecretAccessKey} {
		if strings.Contains(printed, secret) {
			t.Fatal("analytical storage config disclosed credentials")
		}
	}
	iam := base
	iam.AccessKeyID = ""
	iam.SecretAccessKey = ""
	if _, err := normalizeDuckLakeS3(&iam); err != nil {
		t.Fatal("IAM default chain was rejected", err)
	}
}
