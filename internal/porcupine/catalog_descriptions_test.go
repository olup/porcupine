package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestCatalogDescriptionPersistenceAndProjectIsolation(t *testing.T) {
	t.Parallel()
	var at atomic.Int64
	at.Store(time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC).Unix())
	config := Config{DBPath: filepath.Join(t.TempDir(), "events.db"), BootstrapToken: adminToken, Clock: func() time.Time { return time.Unix(at.Load(), 0) }}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	a, ingest := project(t, s)
	b, _ := project(t, s)
	editorID, editor := key(t, s, []string{"management:write"}, []string{a})
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	endpoint := func(id string) string { return "/api/v1/projects/" + id + "/catalog/description" }
	body := `{"event":"purchase","description":"Payment completed"}`
	request(t, s, "PUT", endpoint(a), editor, body, 204)
	at.Add(60)
	request(t, s, "PUT", endpoint(a), editor, body, 204)
	request(t, s, "PUT", endpoint(a), editor, `{"event":"purchase","property":"","description":"Empty key"}`, 204)
	request(t, s, "PUT", endpoint(a), reader, body, 403)
	request(t, s, "PUT", endpoint(a), ingest, body, 403)
	request(t, s, "PUT", endpoint(b), editor, body, 403)
	request(t, s, "PUT", endpoint(a), editor, `{"event":"purchase"}`, 422)
	request(t, s, "PUT", endpoint(b), adminToken, `{"event":"purchase","description":"Other project"}`, 204)

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	discover := func(id string) lakeapi.EventCatalog {
		t.Helper()
		page := lakeapi.EventCatalog{
			Events:     []lakeapi.EventCatalogEvent{{Event: "purchase"}, {Event: "Purchase"}},
			Properties: []lakeapi.EventCatalogProperty{{Key: ""}},
		}
		if err := s.describeCatalogPage(t.Context(), id, "purchase", &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	page := discover(a)
	want := lakeapi.CatalogDescription{Text: "Payment completed", UpdatedBy: editorID, UpdatedAt: "2026-11-17T12:00:00Z"}
	if d := page.Events[0].Description; d == nil || *d != want || page.Events[1].Description != nil {
		t.Fatalf("event description, authorship or retry mismatch: %+v", d)
	}
	if d := page.Properties[0].Description; d == nil || d.Text != "Empty key" || d.UpdatedAt != "2026-11-17T12:01:00Z" {
		t.Fatalf("empty property key lost: %+v", d)
	}
	if other := discover(b); other.Events[0].Description.Text != "Other project" || other.Properties[0].Description != nil {
		t.Fatal("description crossed projects")
	}
	request(t, s, "PUT", endpoint(a), editor, `{"event":"purchase","description":""}`, 204)
	if cleared := discover(a); cleared.Events[0].Description != nil || cleared.Properties[0].Description == nil {
		t.Fatal("clear must affect only the exact target")
	}
	request(t, s, "DELETE", "/api/v1/projects/"+a, adminToken, "", 204)
	request(t, s, "PUT", endpoint(a), adminToken, body, 404)
	if deleted := discover(a); deleted.Properties[0].Description != nil || discover(b).Events[0].Description == nil {
		t.Fatal("project deletion failed to scope description cleanup")
	}
}

func TestCatalogDescriptionReadOnlyMCPJourney(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	id, ingest := project(t, s)
	editorID, editor := key(t, s, []string{"management:write"}, []string{id})
	_, reader := key(t, s, []string{"events:read"}, []string{id})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone CLI: %s %v", output, err)
	}
	runCLI := func(credential string, args ...string) []byte {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, append([]string{"--url", host.URL}, args...)...)
		command.Env = append(os.Environ(), "PORCUPINE_API_KEY="+credential)
		output, err := command.Output()
		if err != nil {
			t.Fatalf("CLI %s: %v", args[0], err)
		}
		return output
	}
	runCLI(editor, "set-catalog-description", "--id", id, "--body", `{"event":"purchase","description":"Payment completed"}`)

	editorCtx, editorSession := mcpTestSession(t, host.URL, editor, "2025-11-25")
	mcpTestTool(t, editorCtx, editorSession, "porcupine_docs", mcpDocsInput{OperationID: "set-catalog-description"}, false)
	path := map[string]string{"id": id}
	write := mcpCallInput{OperationID: "set-catalog-description", Path: path, Body: map[string]any{
		"event": "purchase", "property": "minor_units", "description": "Total including tax",
	}}
	mcpTestCall(t, editorCtx, editorSession, write, 204)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"p1","name":"purchase","properties":{"minor_units":1234,"currency":"EUR"}}`, 200)
	duckLakeJourneyDrain(t, s)

	readerCtx, readerSession := mcpTestSession(t, host.URL, reader, "2025-11-25")
	mcpTestCall(t, readerCtx, readerSession, write, 403)
	read := mcpSchemaInput{ProjectID: id, Event: "purchase"}
	result := mcpTestTool(t, readerCtx, readerSession, "porcupine_schema", read, false)
	raw, err := json.Marshal(result["catalog"])
	if err != nil {
		t.Fatal(err)
	}
	var catalog lakeapi.EventCatalog
	if err := json.Unmarshal(raw, &catalog); err != nil {
		t.Fatal(err)
	}
	var documented, undocumented bool
	for _, item := range catalog.Properties {
		switch item.Key {
		case "minor_units":
			documented = item.Description != nil && item.Description.Text == "Total including tax" && item.Description.UpdatedBy == editorID
		case "currency":
			undocumented = item.Description == nil
		}
	}
	cli := runCLI(reader, "get-event-catalog", "--id", id)
	var cliCatalog lakeapi.EventCatalog
	if err := json.Unmarshal(cli, &cliCatalog); err != nil {
		t.Fatal(err)
	}
	if len(cliCatalog.Events) != 1 || cliCatalog.Events[0].Description == nil || cliCatalog.Events[0].Description.Text != "Payment completed" {
		t.Fatalf("CLI catalog did not discover event description: %s", cli)
	}
	if !documented || !undocumented {
		t.Fatalf("read-only catalog did not preserve observed and documented properties: %s", raw)
	}
}

func TestSharedCatalogDescriptionsVisibleFromAnotherWorker(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	a, b := sharedJourneyPair(t, nil)
	project, _ := project(t, a.service)
	editorID, editor := key(t, a.service, []string{"management:write"}, []string{project})
	path := "/api/v1/projects/" + project + "/catalog/description"
	request(t, b.service, "PUT", path, editor, `{"event":"purchase","property":"currency","description":"ISO 4217 code"}`, 204)
	read := func(node *sharedJourneyNode) *lakeapi.CatalogDescription {
		t.Helper()
		page := lakeapi.EventCatalog{Properties: []lakeapi.EventCatalogProperty{{Key: "currency"}}}
		if err := node.service.describeCatalogPage(t.Context(), project, "purchase", &page); err != nil {
			t.Fatal(err)
		}
		return page.Properties[0].Description
	}
	if d := read(a); d == nil || d.UpdatedBy != editorID || d.Text != "ISO 4217 code" {
		t.Fatalf("shared description missing: %+v", d)
	}
	request(t, a.service, "PUT", path, editor, `{"event":"purchase","property":"currency","description":""}`, 204)
	if d := read(b); d != nil {
		t.Fatalf("shared description was not cleared: %+v", d)
	}
}
