package s3client

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestExplicitEndpointIgnoresAmbientOverrides(t *testing.T) {
	t.Setenv("AWS_ENDPOINT_URL", "http://ambient.invalid")
	t.Setenv("AWS_ENDPOINT_URL_S3", "http://ambient-s3.invalid")
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/bucket/object" || !strings.Contains(r.Header.Get("Authorization"), "Credential=isolated-key/") {
			t.Error("storage endpoint or credential scope changed")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	c, transport, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: server.URL, UsePathStyle: true, AccessKeyID: "isolated-key", SecretAccessKey: "synthetic-secret", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	if _, err := c.DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String("bucket"), Key: aws.String("object")}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("request escaped explicit endpoint")
	}
	if client := c.Options().HTTPClient.(*http.Client); client.Timeout != time.Second || client.CheckRedirect(&http.Request{}, nil) == nil {
		t.Fatal("request timeout or redirect policy lost")
	}
}

func TestSeparateClientsKeepCustomTLSAndResponseCaps(t *testing.T) {
	t.Parallel()
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(strings.Repeat("x", 100))) }))
	defer server.Close()
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	c, transport, err := New(context.Background(), Config{Region: "us-east-1", Endpoint: server.URL, UsePathStyle: true, AccessKeyID: "key", SecretAccessKey: "secret", CACertFile: ca, Timeout: time.Second, ResponseBytes: 11, GetResponseBytes: 23})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	client := c.Options().HTTPClient.(*http.Client)
	for _, tc := range []struct {
		method string
		bytes  int
	}{{"GET", 23}, {"POST", 11}} {
		r, _ := http.NewRequest(tc.method, server.URL, nil)
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || len(data) != tc.bytes {
			t.Fatalf("response cap %s: %d %v", tc.method, len(data), err)
		}
	}
	other, otherTransport, err := New(context.Background(), Config{Region: "us-east-1", AccessKeyID: "another", SecretAccessKey: "secret", Timeout: 45 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer otherTransport.CloseIdleConnections()
	if otherTransport == transport || other.Options().HTTPClient.(*http.Client).Timeout != 45*time.Second || otherTransport.TLSClientConfig != nil {
		t.Fatal("independent storage clients share settings")
	}
}

func TestInvalidCAFailsWithoutLeakingPath(t *testing.T) {
	t.Parallel()
	_, _, err := New(context.Background(), Config{CACertFile: filepath.Join(t.TempDir(), "private-ca-path")})
	if err == nil || strings.Contains(err.Error(), "private-ca-path") {
		t.Fatal("CA error absent or exposed private path")
	}
}
