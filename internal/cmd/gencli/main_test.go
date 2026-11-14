package main

import (
	"bytes"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/apischema"
)

func snapshot(t *testing.T) *document {
	t.Helper()
	api, err := parseDocument(apischema.Document())
	if err != nil {
		t.Fatal(err)
	}
	return api
}

func TestCatalogFromSerializedOpenAPI(t *testing.T) {
	api := snapshot(t)
	first, err := generate(api)
	if err != nil {
		t.Fatal(err)
	}
	second, err := generate(snapshot(t))
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
		`ID: "create-import", Method: "POST", Path: "/api/v1/projects/{id}/imports"`,
	} {
		if !bytes.Contains(first, []byte(snippet)) {
			t.Errorf("expected generated metadata containing %s", snippet)
		}
	}
}

func TestUnsupportedCommandsFailGeneration(t *testing.T) {
	for _, tt := range []struct {
		message string
		change  func(*document)
	}{
		{"duplicate operationId", func(a *document) { a.Paths["/api/v1/projects"].Post.OperationID = "list-projects" }},
		{"path-level", func(a *document) { a.Paths["/api/v1/projects"].Parameters = []*parameter{{}} }},
		{"unsupported parameter type", func(a *document) {
			a.Paths["/api/v1/projects"].Get.Parameters = []*parameter{{Name: "complex", In: "query", Schema: &schema{Type: "object"}}}
		}},
		{"unsupported request media type", func(a *document) {
			a.Paths["/api/v1/projects"].Post.RequestBody.Content = map[string]*mediaType{"multipart/form-data": {}}
		}},
		{"missing parameter", func(a *document) {
			a.Paths["/api/v1/projects"].Get.Parameters = []*parameter{nil, {Name: "value", In: "query", Schema: &schema{Type: "string"}}}
		}},
		{"missing request media schema", func(a *document) {
			a.Paths["/api/v1/projects"].Post.RequestBody.Content = map[string]*mediaType{"application/json": nil}
		}},
	} {
		t.Run(tt.message, func(t *testing.T) {
			api := snapshot(t)
			tt.change(api)
			_, err := generate(api)
			if err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("got %v, want %q", err, tt.message)
			}
		})
	}
}

func TestGeneratorDependenciesExcludeRuntime(t *testing.T) {
	command := exec.Command("go", "list", "-deps", "-test", ".")
	command.Env = append(os.Environ(), "CGO_ENABLED=0")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("pure-Go generator dependencies: %v\n%s", err, output)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == "github.com/olup/porcupine/internal/porcupine" || strings.HasPrefix(dependency, "github.com/duckdb/") {
			t.Fatalf("offline generator imports runtime dependency %s", dependency)
		}
	}
}
