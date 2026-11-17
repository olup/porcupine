package output

import (
	"bytes"
	"testing"
)

func TestPipelineSecretsBoundToDestination(t *testing.T) {
	t.Parallel()
	rt, err := NewSQLitePipelines(PipelineDependencies{}, &PipelineConfig{Key: bytes.Repeat([]byte{0x37}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	s := rt
	secret := []byte("Bearer unit-test-only")
	blob, err := s.SealSecret("p", "d", secret)
	if err != nil || bytes.Contains(blob, secret) {
		t.Fatalf("unexpected seal result: %v", err)
	}
	second, err := s.SealSecret("p", "d", secret)
	if err != nil || bytes.Equal(blob, second) {
		t.Fatal("nonce reused")
	}
	decoded, err := s.OpenSecret("p", "d", blob)
	if err != nil || !bytes.Equal(decoded, secret) {
		t.Fatalf("open: %v", err)
	}
	for _, ids := range [][2]string{{"other", "d"}, {"p", "other"}} {
		if _, err = s.OpenSecret(ids[0], ids[1], blob); err == nil {
			t.Fatal("accepted credential moved across destinations")
		}
	}
	blob[len(blob)-1] ^= 1
	if _, err = s.OpenSecret("p", "d", blob); err == nil {
		t.Fatal("accepted corrupt credential")
	}
}
func TestPipelineEndpointAllowlist(t *testing.T) {
	t.Parallel()
	rt, err := NewSQLitePipelines(PipelineDependencies{}, &PipelineConfig{Key: make([]byte, 32), AllowedS3Endpoints: []string{"https://STORAGE.example.org/"}})
	if err != nil {
		t.Fatal(err)
	}
	s := rt
	for _, url := range []string{"", "https://storage.example.org"} {
		if err = s.ValidateS3Endpoint(url); err != nil {
			t.Fatal(err)
		}
	}
	for _, url := range []string{"https://api.storage.example.org", "http://storage.example.org", "https://storage.example.org/private", "https://storage.example.org:443", "https://storage.example.org?", "https://user@storage.example.org", "https://storage.example.org#ignored"} {
		if err = s.ValidateS3Endpoint(url); err == nil {
			t.Fatalf("allowed %q", url)
		}
	}
}
func TestPipelineS3RequiresExplicitCredentials(t *testing.T) {
	t.Parallel()
	if validateS3PipelineSecret(S3PipelineSecret{}) == nil || validateS3PipelineSecret(S3PipelineSecret{AccessKeyID: "a"}) == nil {
		t.Fatal("default IAM credentials must never be used")
	}
}
