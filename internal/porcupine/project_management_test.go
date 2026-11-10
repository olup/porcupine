package porcupine

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProjectRenamePermissionsAndIdentity(t *testing.T) {
	s := service(t, 20)
	created := request(t, s, "POST", "/api/v1/projects", adminToken, `{"name":"Original"}`, 201)
	original := created["project"].(map[string]any)
	id := original["id"].(string)
	other, _ := project(t, s)
	_, manager := key(t, s, []string{"management:write"}, []string{id})
	_, reader := key(t, s, []string{"management:read"}, []string{id})
	path := "/api/v1/projects/" + id
	body := `{"name":"  Café archive  "}`
	request(t, s, "PATCH", path, "", body, 401)
	request(t, s, "PATCH", path, reader, body, 403)
	request(t, s, "PATCH", "/api/v1/projects/"+other, manager, body, 403)
	request(t, s, "PATCH", "/api/v1/projects/"+newID(), adminToken, body, 404)
	request(t, s, "PATCH", path, manager, `{"name":"   "}`, 400)
	request(t, s, "PATCH", path, manager, `{"name":"`+strings.Repeat("a", 201)+`"}`, 422)
	request(t, s, "PATCH", path, manager, `{"ingestion_mode":"server_only"}`, 422)
	for range 2 {
		renamed := request(t, s, "PATCH", path, manager, body, 200)["project"].(map[string]any)
		if renamed["name"] != "Café archive" || renamed["id"] != id || renamed["created_at"] != original["created_at"] || renamed["ingestion_mode"] != original["ingestion_mode"] {
			t.Fatalf("unexpected renamed project: %v", renamed)
		}
	}
	request(t, s, "POST", "/capture/", "", captureBody(created["ingest_token"].(string), "after-rename"), 200)
	list := request(t, s, "GET", "/api/v1/projects", reader, "", 200)["projects"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["name"] != "Café archive" {
		t.Fatalf("unexpected scoped project list: %v", list)
	}
}

func TestMCPProjectLifecycleAndDiscovery(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	other, _ := project(t, s)
	_, manager := key(t, s, []string{"management:write"}, []string{id})
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, scoped := mcpTestSession(t, host.URL, manager, "")
	_, admin := mcpTestSession(t, host.URL, adminToken, "")
	docs := mcpTestTool(t, ctx, scoped, "porcupine_docs", mcpDocsInput{OperationID: "rename-project"}, false)
	op := docs["operations"].([]any)[0].(map[string]any)
	if op["permission"] != "management:write" || op["method"] != "PATCH" || op["read_only"] != false {
		t.Fatalf("bad rename contract: %v", op)
	}
	mcpTestTool(t, ctx, scoped, "porcupine_docs", mcpDocsInput{OperationID: "delete-project"}, true)
	path := map[string]string{"id": id}
	body := map[string]any{"name": "Via MCP"}
	renamed := mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "rename-project", Path: path, Body: body}, 200)
	if renamed["project"].(map[string]any)["name"] != body["name"] {
		t.Fatal("MCP returned the wrong name")
	}
	mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "rename-project", Path: map[string]string{"id": other}, Body: body}, 403)
	remove := mcpCallInput{OperationID: "delete-project", Path: path}
	mcpTestCall(t, ctx, scoped, remove, 403)
	mcpTestCall(t, ctx, admin, remove, 204)
	mcpTestCall(t, ctx, admin, remove, 204)
	mcpTestCall(t, ctx, admin, mcpCallInput{OperationID: "rename-project", Path: path, Body: body}, 404)
	mcpTestCall(t, ctx, admin, mcpCallInput{OperationID: "delete-project", Path: map[string]string{"id": newID()}}, 404)
	projects := mcpTestCall(t, ctx, admin, mcpCallInput{OperationID: "list-projects"}, 200)["projects"].([]any)
	if len(projects) != 1 || projects[0].(map[string]any)["id"] != other {
		t.Fatal("deletion affected another project")
	}
}
