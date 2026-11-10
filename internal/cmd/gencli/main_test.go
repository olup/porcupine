package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/danielgtaylor/huma/v2"
	"github.com/olup/porcupine/internal/porcupine"
)

func TestCatalogMatchesServedOpenAPI(t *testing.T) {
	service, err := porcupine.Open(porcupine.Config{
		DBPath:         filepath.Join(t.TempDir(), "schema.db"),
		BootstrapToken: rand.Text(),
		OAuth: &porcupine.OAuthConfig{
			Issuer: "https://porcupine.example",
			Email: &porcupine.EmailConfig{SendCode: func(context.Context, string, string) error {
				t.Fatal("schema generation tried to send mail")
				return nil
			}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	w := httptest.NewRecorder()
	service.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("openapi.json: status %d", w.Code)
	}

	// Huma exports different optional routes when email and OAuth are enabled.
	// Compare the complete document so scheme or route registration drift is caught.
	api := porcupine.ManagementOpenAPI()
	offline, err := json.Marshal(api)
	if err != nil {
		t.Fatal(err)
	}
	decode := func(data []byte) map[string]any {
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		return doc
	}
	if !reflect.DeepEqual(decode(offline), decode(w.Body.Bytes())) {
		t.Fatal("offline contract differs from the OpenAPI served by a configured instance")
	}

	first, err := generate(api)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(porcupine.ManagementOpenAPI())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generation is nondeterministic")
	}
	for _, snippet := range []string{
		`ID: "invite-human"`,
		`ID: "create-service-api-key"`,
		`ContentType: "application/octet-stream", BodyRequired: true`,
		`Name: "Upload-Offset", In: "header"`,
		`"sql (string, required)`,
	} {
		if !bytes.Contains(first, []byte(snippet)) {
			t.Errorf("expected generated metadata containing %s", snippet)
		}
	}
}

func TestUnsupportedCommandsFailGeneration(t *testing.T) {
	for _, tt := range []struct {
		message string
		change  func(*huma.OpenAPI)
	}{
		{"duplicate operationId", func(a *huma.OpenAPI) { a.Paths["/api/v1/projects"].Post.OperationID = "list-projects" }},
		{"path-level", func(a *huma.OpenAPI) { a.Paths["/api/v1/projects"].Parameters = []*huma.Param{{}} }},
		{"unsupported parameter type", func(a *huma.OpenAPI) {
			a.Paths["/api/v1/projects"].Get.Parameters = []*huma.Param{{Name: "complex", In: "query", Schema: &huma.Schema{Type: "object"}}}
		}},
		{"unsupported request media type", func(a *huma.OpenAPI) {
			a.Paths["/api/v1/projects"].Post.RequestBody.Content = map[string]*huma.MediaType{"multipart/form-data": {}}
		}},
	} {
		t.Run(tt.message, func(t *testing.T) {
			spec := porcupine.ManagementOpenAPI()
			tt.change(spec)
			_, err := generate(spec)
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("got %v, want %q", err, tt.message)
			}
		})
	}
}
