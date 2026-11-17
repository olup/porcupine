package porcupine

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func tenantTestBody(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func tenantTestCreate(t *testing.T, s *Service, name string) string {
	t.Helper()
	result := request(t, s, "POST", "/api/v1/tenants", adminToken, tenantTestBody(t, map[string]any{"name": name}), 201)
	return result["tenant"].(map[string]any)["id"].(string)
}

func tenantJourneyProject(t *testing.T, s *Service, tenantID string) (string, string) {
	t.Helper()
	result := request(t, s, "POST", "/api/v1/projects", adminToken, tenantTestBody(t, map[string]any{"name": "Tenant project", "tenant_id": tenantID}), 201)
	return result["project"].(map[string]any)["id"].(string), result["ingest_token"].(string)
}

func TestTenantManagementLifecycleAndPagination(t *testing.T) {
	s := service(t, 20)
	first := request(t, s, "GET", "/api/v1/tenants", adminToken, "", 200)["tenants"].([]any)
	if len(first) != 1 || first[0].(map[string]any)["id"] != defaultTenantID {
		t.Fatalf("missing default tenant: %v", first)
	}
	defaultProject, _ := project(t, s)
	got := request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)["projects"].([]any)[0].(map[string]any)
	if got["id"] != defaultProject || got["tenant_id"] != defaultTenantID {
		t.Fatalf("default ownership: %v", got)
	}
	wanted := map[string]bool{defaultTenantID: true}
	for _, name := range []string{" First ", "Second", "Third"} {
		wanted[tenantTestCreate(t, s, name)] = true
	}
	seen := map[string]bool{}
	cursor := ""
	for {
		path := "/api/v1/tenants?limit=2"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		page := request(t, s, "GET", path, adminToken, "", 200)
		rows := page["tenants"].([]any)
		if len(rows) > 2 {
			t.Fatal("unbounded tenant page")
		}
		for _, value := range rows {
			tenant := value.(map[string]any)
			id := tenant["id"].(string)
			if !wanted[id] || seen[id] {
				t.Fatalf("unexpected/repeated tenant %q", id)
			}
			seen[id] = true
			if strings.TrimSpace(tenant["name"].(string)) != tenant["name"] {
				t.Fatal("name not trimmed")
			}
		}
		next, _ := page["next_cursor"].(string)
		if next == "" {
			break
		}
		if len(rows) != 2 || next == cursor {
			t.Fatalf("invalid pagination cursor: %v", page)
		}
		cursor = next
	}
	if len(seen) != len(wanted) {
		t.Fatalf("pagination lost tenants: %v", seen)
	}
	id := tenantTestCreate(t, s, "Original")
	path := "/api/v1/tenants/" + id
	before := request(t, s, "GET", path, adminToken, "", 200)["tenant"].(map[string]any)
	renamed := request(t, s, "PATCH", path, adminToken, `{"name":" Renamed ","ingestion_allowed":false}`, 200)["tenant"].(map[string]any)
	if renamed["name"] != "Renamed" || renamed["ingestion_allowed"] != false || renamed["id"] != id || renamed["created_at"] != before["created_at"] {
		t.Fatalf("update changed tenant identity: %v", renamed)
	}
	request(t, s, "PATCH", path, adminToken, `{"ingestion_allowed":true}`, 200)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/api/v1/tenants?limit=0", "", 422},
		{"GET", "/api/v1/tenants?limit=201", "", 422},
		{"GET", "/api/v1/tenants?cursor=" + strings.Repeat("a", 129), "", 422},
		{"POST", "/api/v1/tenants", `{"name":"   "}`, 400},
		{"POST", "/api/v1/tenants", `{"name":"` + strings.Repeat("x", 201) + `"}`, 422},
		{"PATCH", path, `{}`, 400},
		{"PATCH", path, `{"name":"   "}`, 400},
		{"GET", "/api/v1/tenants/missing", "", 404},
		{"PATCH", "/api/v1/tenants/missing", `{"name":"valid"}`, 404},
		{"POST", "/api/v1/projects", `{"name":"orphan","tenant_id":"missing"}`, 404},
	} {
		request(t, s, tc.method, tc.path, adminToken, tc.body, tc.status)
	}
	projects := request(t, s, "GET", "/api/v1/projects", adminToken, "", 200)["projects"].([]any)
	if len(projects) != 1 {
		t.Fatal("failed project creation left metadata")
	}
}

func TestTenantBudgetChangesPreserveMonthlyUsage(t *testing.T) {
	s := service(t, 20)
	id := tenantTestCreate(t, s, "Metered")
	start, end := tenantMonth(s.clock())
	_, ingest := tenantJourneyProject(t, s, id)
	for i := range 7 {
		request(t, s, "POST", "/capture/", "", tenantTestBody(t, map[string]any{
			"api_key": ingest, "uuid": fmt.Sprintf("usage-%d", i), "event": "usage", "distinct_id": "actor",
		}), 200)
	}
	path := "/api/v1/tenants/" + id
	usagePath := path + "/usage"
	unlimited := request(t, s, "GET", usagePath, adminToken, "", 200)
	if unlimited["accepted_events"] != "7" || unlimited["remaining_events"] != nil || unlimited["period_start"] != start.Format(time.RFC3339) || unlimited["period_end"] != end.Format(time.RFC3339) {
		t.Fatalf("unexpected usage: %v", unlimited)
	}
	budgets := map[string]any{"ingestion_monthly_events": 10, "hot_query_max_concurrent": 1, "hot_query_timeout_ms": 100, "hot_query_max_rows": 5, "hot_query_max_result_bytes": 1024}
	for _, quota := range []int{10, 5, 0, 20} {
		budgets["ingestion_monthly_events"] = quota
		request(t, s, "PUT", path+"/budgets", adminToken, tenantTestBody(t, budgets), 200)
		usage := request(t, s, "GET", usagePath, adminToken, "", 200)
		if usage["accepted_events"] != "7" || usage["remaining_events"] != fmt.Sprint(max(0, quota-7)) || usage["ingestion_allowed"] != (quota > 7) {
			t.Fatalf("budget changed usage incorrectly: %v", usage)
		}
	}
	request(t, s, "PATCH", path, adminToken, `{"ingestion_allowed":false}`, 200)
	suspended := request(t, s, "GET", usagePath, adminToken, "", 200)
	if suspended["accepted_events"] != "7" || suspended["ingestion_allowed"] != false {
		t.Fatalf("suspension reset usage: %v", suspended)
	}
	budgets["ingestion_monthly_events"] = nil
	request(t, s, "PUT", path+"/budgets", adminToken, tenantTestBody(t, budgets), 200)
	if got := request(t, s, "GET", usagePath, adminToken, "", 200); got["remaining_events"] != nil || got["accepted_events"] != "7" || got["ingestion_allowed"] != false {
		t.Fatalf("unlimited budget cleared suspension: %v", got)
	}
	prior := start.AddDate(0, -1, 0)
	old := request(t, s, "GET", usagePath+"?period="+prior.Format("2006-01"), adminToken, "", 200)
	if old["accepted_events"] != "0" || old["period_start"] != prior.Format(time.RFC3339) {
		t.Fatalf("wrong empty period: %v", old)
	}
	for _, period := range []string{"2026-00", "2026-13", "notdate"} {
		request(t, s, "GET", usagePath+"?period="+period, adminToken, "", 400)
	}
	request(t, s, "GET", "/api/v1/tenants/missing/usage", adminToken, "", 404)
	request(t, s, "PUT", "/api/v1/tenants/missing/budgets", adminToken, tenantTestBody(t, budgets), 404)
	for _, field := range []string{"hot_query_max_concurrent", "hot_query_timeout_ms", "hot_query_max_rows", "hot_query_max_result_bytes"} {
		copy := map[string]any{}
		for k, v := range budgets {
			copy[k] = v
		}
		copy[field] = 0
		request(t, s, "PUT", path+"/budgets", adminToken, tenantTestBody(t, copy), 422)
	}
	budgets["ingestion_monthly_events"] = -1
	request(t, s, "PUT", path+"/budgets", adminToken, tenantTestBody(t, budgets), 422)
	request(t, s, "PUT", path+"/budgets", adminToken, `{"ingestion_monthly_events":100}`, 422)
	reset := httptest.NewRequest("POST", usagePath+"/reset", strings.NewReader(`{}`))
	reset.Header.Set("Authorization", "Bearer "+adminToken)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, reset)
	if response.Code != 404 {
		t.Fatalf("manual reset route exists: %d %s", response.Code, response.Body.String())
	}
}

func TestTenantProjectScopeOwnershipAndOperatorOnlyManagement(t *testing.T) {
	s := service(t, 20)
	a, b := tenantTestCreate(t, s, "A"), tenantTestCreate(t, s, "B")
	projectA, _ := tenantJourneyProject(t, s, a)
	projectB, _ := tenantJourneyProject(t, s, b)
	keyID, scoped := key(t, s, []string{"management:read", "management:write", "events:read"}, []string{projectA})
	_, span := key(t, s, []string{"management:read", "events:read"}, []string{projectA, projectB})
	_, global := key(t, s, []string{"management:read", "management:write", "events:read"}, []string{"*"})
	for _, tc := range []struct {
		token, filter string
		count         int
	}{
		{scoped, "", 1}, {scoped, "?tenant_id=" + a, 1}, {scoped, "?tenant_id=" + b, 0},
		{span, "", 2}, {span, "?tenant_id=" + b, 1}, {global, "", 2},
	} {
		got := request(t, s, "GET", "/api/v1/projects"+tc.filter, tc.token, "", 200)["projects"].([]any)
		if len(got) != tc.count {
			t.Fatalf("wrong visible projects: %v", got)
		}
	}
	for _, token := range []string{scoped, span, global} {
		for _, tc := range []struct{ method, path, body string }{
			{"GET", "/api/v1/tenants", ""}, {"POST", "/api/v1/tenants", `{"name":"bad"}`},
			{"GET", "/api/v1/tenants/" + a, ""}, {"PATCH", "/api/v1/tenants/" + a, `{"name":"bad"}`},
			{"PUT", "/api/v1/tenants/" + a + "/budgets", `{"ingestion_monthly_events":0,"hot_query_max_concurrent":1,"hot_query_timeout_ms":100,"hot_query_max_rows":1,"hot_query_max_result_bytes":256}`},
			{"GET", "/api/v1/tenants/" + a + "/usage", ""},
			{"POST", "/api/v1/projects", `{"name":"bad","tenant_id":"` + a + `"}`},
		} {
			request(t, s, tc.method, tc.path, token, tc.body, 403)
		}
	}
	request(t, s, "GET", "/api/v1/tenants", "", "", 401)
	renamed := request(t, s, "PATCH", "/api/v1/projects/"+projectA, scoped, `{"name":"A renamed"}`, 200)["project"].(map[string]any)
	if renamed["tenant_id"] != a {
		t.Fatalf("rename moved project: %v", renamed)
	}
	request(t, s, "PATCH", "/api/v1/projects/"+projectA, adminToken, `{"name":"move","tenant_id":"`+b+`"}`, 422)
	var owner string
	if err := s.control.QueryRow(`SELECT tenant_id FROM projects WHERE id=?`, projectA).Scan(&owner); err != nil || owner != a {
		t.Fatalf("project ownership mutated: %q %v", owner, err)
	}
	request(t, s, "DELETE", "/api/v1/access-keys/"+keyID, adminToken, "", 204)
	request(t, s, "GET", "/api/v1/projects", scoped, "", 401)
}
