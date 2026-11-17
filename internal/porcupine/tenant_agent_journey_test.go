package porcupine

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTenantMCPDiscoveryOperatorJourneyAndRevocation(t *testing.T) {
	s := service(t, 20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, operator := mcpTestSession(t, host.URL, adminToken, "")
	operations := []string{"list-tenants", "create-tenant", "get-tenant", "update-tenant", "update-tenant-budgets", "get-tenant-usage"}
	for _, id := range operations {
		detail := mcpTestTool(t, ctx, operator, "porcupine_docs", mcpDocsInput{OperationID: id}, false)
		op := detail["operations"].([]any)[0].(map[string]any)
		permission := tenantAdminScope
		if id == "create-tenant" || id == "update-tenant-budgets" {
			permission = "instance:admin"
		}
		if op["permission"] != permission || op["operation_id"] != id || detail["contract"] == nil {
			t.Fatalf("missing operator contract: %v", detail)
		}
	}
	created := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "create-tenant", Body: map[string]any{"name": "MCP tenant"}}, 201)
	tenantID := created["tenant"].(map[string]any)["id"].(string)
	path := map[string]string{"tenant_id": tenantID}
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "get-tenant", Path: path}, 200)
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "update-tenant", Path: path, Body: map[string]any{"name": "Agent owned", "ingestion_allowed": true}}, 200)
	budgets := map[string]any{"ingestion_monthly_events": 8, "hot_query_max_concurrent": 1, "hot_query_timeout_ms": 100, "hot_query_max_rows": 5, "hot_query_max_result_bytes": 1024}
	updated := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "update-tenant-budgets", Path: path, Body: budgets}, 200)
	if updated["tenant"].(map[string]any)["budgets"].(map[string]any)["ingestion_monthly_events"] != float64(8) {
		t.Fatalf("budget update lost: %v", updated)
	}
	usage := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "get-tenant-usage", Path: path}, 200)
	if usage["accepted_events"] != "0" || usage["remaining_events"] != "8" {
		t.Fatalf("unexpected usage: %v", usage)
	}
	projectResult := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "Via agent", "tenant_id": tenantID}}, 201)
	projectID := projectResult["project"].(map[string]any)["id"].(string)
	otherTenant := tenantTestCreate(t, s, "Private tenant")
	otherProject, _ := tenantJourneyProject(t, s, otherTenant)
	keyID, reader := key(t, s, []string{"events:read", "management:read", "management:write"}, []string{projectID})
	_, scoped := mcpTestSession(t, host.URL, reader, "")
	for _, id := range operations {
		mcpTestTool(t, ctx, scoped, "porcupine_docs", mcpDocsInput{OperationID: id}, true)
		body := map[string]any{"name": "Denied"}
		if id == "update-tenant-budgets" {
			body = budgets
		}
		mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: id, Path: path, Body: body}, 403)
	}
	catalog := mcpTestTool(t, ctx, scoped, "porcupine_docs", mcpDocsInput{Query: "tenant"}, false)
	for _, raw := range catalog["operations"].([]any) {
		op := raw.(map[string]any)
		if op["permission"] == "instance:admin" {
			t.Fatalf("scoped catalog advertised operator operation: %v", op)
		}
	}
	visible := mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "list-projects", Query: map[string]string{"tenant_id": tenantID}}, 200)["projects"].([]any)
	if len(visible) != 1 || visible[0].(map[string]any)["id"] != projectID {
		t.Fatalf("project discovery failed: %v", visible)
	}
	hidden := mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "list-projects", Query: map[string]string{"tenant_id": otherTenant}}, 200)["projects"].([]any)
	if len(hidden) != 0 {
		t.Fatalf("cross tenant project exposed: %v", hidden)
	}
	mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": otherProject}}, 403)
	mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "create-project", Body: map[string]any{"name": "escape", "tenant_id": otherTenant}}, 403)
	mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "revoke-access-key", Path: map[string]string{"id": keyID}}, 204)
	if _, err := scoped.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("revoked credential retained MCP access: %v", err)
	}
}
