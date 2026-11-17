package porcupine

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestTenantGeneratedCLIManagementJourney(t *testing.T) {
	t.Parallel()
	binary := filepath.Join(t.TempDir(), "porcupine")
	build := exec.Command("go", "build", "-o", binary, "./cmd/porcupine")
	build.Dir = "../../cli"
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build standalone CLI: %s %v", output, err)
	}
	s := service(t, 20)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	run := func(token string, wantSuccess bool, args ...string) map[string]any {
		t.Helper()
		command := exec.Command(binary, append([]string{"--url", host.URL}, args...)...)
		command.Env = append(os.Environ(), "PORCUPINE_API_KEY="+token)
		output, err := command.CombinedOutput()
		if !wantSuccess {
			if err == nil || !strings.Contains(string(output), "403") {
				t.Fatalf("expected forbidden CLI operation %v: %s %v", args, output, err)
			}
			return nil
		}
		if err != nil {
			t.Fatalf("CLI %v: %s %v", args, output, err)
		}
		result := map[string]any{}
		if err := json.Unmarshal(output, &result); err != nil {
			t.Fatalf("CLI output is not JSON: %s %v", output, err)
		}
		return result
	}
	result := run(adminToken, true, "create-tenant", "--body", `{"name":"CLI tenant"}`)
	tenantID := result["tenant"].(map[string]any)["id"].(string)
	run(adminToken, true, "get-tenant", "--tenant-id", tenantID)
	run(adminToken, true, "update-tenant", "--tenant-id", tenantID, "--body", `{"name":"CLI renamed"}`)
	run(adminToken, true, "update-tenant-budgets", "--tenant-id", tenantID, "--body", `{"ingestion_monthly_events":10,"hot_query_max_concurrent":1,"hot_query_timeout_ms":100,"hot_query_max_rows":5,"hot_query_max_result_bytes":1024}`)
	result = run(adminToken, true, "list-tenants", "--limit", "1")
	if len(result["tenants"].([]any)) != 1 || result["next_cursor"] == nil {
		t.Fatalf("CLI pagination missing: %v", result)
	}
	result = run(adminToken, true, "create-project", "--body", tenantTestBody(t, map[string]any{"name": "CLI project", "tenant_id": tenantID}))
	id := result["project"].(map[string]any)["id"].(string)
	result = run(adminToken, true, "list-projects", "--tenant-id", tenantID)
	if rows := result["projects"].([]any); len(rows) != 1 || rows[0].(map[string]any)["id"] != id {
		t.Fatalf("CLI tenant filter lost: %v", result)
	}
	result = run(adminToken, true, "get-tenant-usage", "--tenant-id", tenantID, "--period", "2020-02")
	if result["period_start"] != "2020-02-01T00:00:00Z" || result["period_end"] != "2020-03-01T00:00:00Z" || result["accepted_events"] != "0" {
		t.Fatalf("CLI usage contract lost: %v", result)
	}
	_, scoped := key(t, s, []string{"management:read", "events:read"}, []string{id})
	run(scoped, true, "list-projects", "--tenant-id", tenantID)
	run(scoped, false, "get-tenant-usage", "--tenant-id", tenantID)
	run(scoped, false, "update-tenant", "--tenant-id", tenantID, "--body", `{"ingestion_allowed":false}`)
}
