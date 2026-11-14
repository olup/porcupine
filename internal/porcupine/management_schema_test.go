package porcupine

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/olup/porcupine/internal/apischema"
)

func TestServedOpenAPIMatchesGeneratedSnapshot(t *testing.T) {
	service, err := Open(Config{
		DBPath: filepath.Join(t.TempDir(), "schema.db"), BootstrapToken: rand.Text(),
		OAuth: &OAuthConfig{Issuer: "https://porcupine.example", Email: &EmailConfig{SendCode: func(context.Context, string, string) error {
			t.Fatal("schema generation tried to send mail")
			return nil
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("openapi.json status %d", response.Code)
	}
	// The snapshot includes every optional OAuth/email route. The live configured
	// instance must publish the same document without serializing access metadata.
	decode := func(raw []byte) map[string]any {
		t.Helper()
		var value map[string]any
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	if !reflect.DeepEqual(decode(apischema.Document()), decode(response.Body.Bytes())) {
		t.Fatal("generated contract differs from the configured server's OpenAPI; run go run ./internal/cmd/genapi")
	}
}
