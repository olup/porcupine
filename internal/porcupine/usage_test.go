package porcupine

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

func usageToday(s *Service) string { return usage.Day(s.clock()) }
func usageRow(t *testing.T, s *Service, tenant string) map[string]any {
	t.Helper()
	path := "/api/v1/usage?start=" + usageToday(s) + "&end=" + usage.Day(s.clock().AddDate(0, 0, 1)) + "&tenant_id=" + tenant
	page := request(t, s, "GET", path, adminToken, "", 200)
	rows := page["rows"].([]any)
	if len(rows) != 1 {
		t.Fatal(page)
	}
	return rows[0].(map[string]any)
}
func TestUsageAcceptanceImportRetryIsolationAndPermissions(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	a, token := project(t, s)
	other := tenantTestCreate(t, s, "Other team")
	b, bToken := tenantTestProject(t, s, other)
	nativeRequest(t, s, token, `{"type":"track","message_id":"one","actor_id":"u","name":"message"}`, 200)
	nativeRequest(t, s, token, `{"type":"track","message_id":"one","actor_id":"u","name":"message"}`, 200)
	nativeRequest(t, s, bToken, `{"type":"track","message_id":"other","actor_id":"u","name":"message"}`, 200)
	_, importerToken := key(t, s, []string{"imports:write", "events:write"}, []string{a})
	data := importTestRow("historical")
	job := importTestCreate(t, s, a, importerToken, "usage-import", []importer.ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	importTestPut(t, s, a, job, job.Files[0], importerToken, 0, data, 200)
	importTestPut(t, s, a, job, job.Files[0], importerToken, 0, data, 200)
	row := usageRow(t, s, defaultTenantID)
	counts := row["counters"].(map[string]any)
	if counts["accepted_live_events"] != "1" || counts["accepted_import_events"] != "1" || row["project_id"] != a {
		t.Fatal(row)
	}
	row = usageRow(t, s, other)
	if row["project_id"] != b || row["counters"].(map[string]any)["accepted_live_events"] != "1" {
		t.Fatal(row)
	}
	_, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	request(t, s, "GET", "/api/v1/usage?tenant_id="+defaultTenantID, reader, "", 403)
	request(t, s, "GET", "/api/v1/usage", token, "", 403)
	request(t, s, "GET", "/api/v1/usage", "", "", 401)
	tenantAllowance(t, s, defaultTenantID, 2)
	nativeRequest(t, s, token, `[{"type":"track","message_id":"denied-a","actor_id":"u","name":"message"},{"type":"track","message_id":"denied-b","actor_id":"u","name":"message"}]`, 429)
	if usageRow(t, s, defaultTenantID)["counters"].(map[string]any)["accepted_live_events"] != "1" {
		t.Fatal("quota rejection was metered")
	}
}

type usageFailingLake struct {
	lakeapi.LakeStore
	s     *Service
	t     *testing.T
	calls int
}

func (l *usageFailingLake) before() error {
	l.t.Helper()
	var count int
	if e := l.s.db.QueryRow(`SELECT COUNT(*) FROM usage_executions WHERE closed=0`).Scan(&count); e != nil || count != 1 {
		l.t.Fatalf("query was dispatched before durable start: %d %v", count, e)
	}
	l.calls++
	return apiError(400, "expected backend error")
}
func (l *usageFailingLake) Query(context.Context, string, string, map[string]string, lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error) {
	return lakeapi.AnalyticsResult{}, l.before()
}
func (l *usageFailingLake) Catalog(context.Context, lakeapi.EventCatalogInput) (lakeapi.EventCatalog, error) {
	return lakeapi.EventCatalog{}, l.before()
}
func (l *usageFailingLake) Profile(context.Context, string, lakeapi.EventProfileOptions, time.Time) (lakeapi.EventProfileRead, error) {
	return lakeapi.EventProfileRead{}, l.before()
}
func (l *usageFailingLake) Close() error { return nil }
func TestUsageQueryNativeLifecycleCountsFailuresAndOperations(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, _ := project(t, s)
	fake := &usageFailingLake{s: s, t: t}
	s.lake = fake
	request(t, s, "POST", "/api/v1/projects/"+id+"/query", adminToken, `{"sql":"SELECT count(*) FROM events"}`, 400)
	if e := s.usage.synchronize(t.Context()); e != nil {
		t.Fatal(e)
	}
	request(t, s, "GET", "/api/v1/projects/"+id+"/catalog", adminToken, "", 400)
	if e := s.usage.synchronize(t.Context()); e != nil {
		t.Fatal(e)
	}
	request(t, s, "GET", profileRequestPath(id, "message"), adminToken, "", 400)
	row := usageRow(t, s, defaultTenantID)
	if fake.calls != 3 || row["counters"].(map[string]any)["interactive_query_count"] != "3" || row["query_duration"].(map[string]any)["unresolved_executions"] != "0" {
		t.Fatal(row)
	}
	var operations int
	if e := s.db.QueryRow(`SELECT COUNT(DISTINCT operation) FROM usage_executions`).Scan(&operations); e != nil || operations != 3 {
		t.Fatal(operations, e)
	}
}
func TestUsageMCPDiscoveryAndRead(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, token := project(t, s)
	nativeRequest(t, s, token, `{"type":"track","message_id":"one","actor_id":"u","name":"message"}`, 200)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, operator := mcpTestSession(t, host.URL, adminToken, "")
	detail := mcpTestTool(t, ctx, operator, "porcupine_docs", mcpDocsInput{OperationID: "list-usage"}, false)
	if detail["operations"].([]any)[0].(map[string]any)["permission"] != "instance:admin" {
		t.Fatal(detail)
	}
	page := mcpTestCall(t, ctx, operator, mcpCallInput{OperationID: "list-usage", Query: map[string]string{"start": usageToday(s), "end": usage.Day(s.clock().AddDate(0, 0, 1))}}, 200)
	if fmt.Sprint(page["rows"].([]any)[0].(map[string]any)["counters"].(map[string]any)["accepted_live_events"]) != "1" {
		t.Fatal(page)
	}
	_, reader := key(t, s, []string{"events:read"}, []string{id})
	_, scoped := mcpTestSession(t, host.URL, reader, "")
	mcpTestTool(t, ctx, scoped, "porcupine_docs", mcpDocsInput{OperationID: "list-usage"}, true)
	mcpTestCall(t, ctx, scoped, mcpCallInput{OperationID: "list-usage"}, 403)
}

func TestUsageDeletionRetainsCountersAndFencesLateStorageSample(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, token := project(t, s)
	nativeRequest(t, s, token, `{"type":"track","message_id":"one","actor_id":"u","name":"message"}`, 200)
	at := s.clock()
	sample := usage.StorageSample{TenantID: defaultTenantID, ProjectID: id, Day: usage.Day(at), ObservedAt: usage.Stamp(at), Bytes: 100}
	if e := s.usage.store.RecordStorageSample(t.Context(), sample); e != nil {
		t.Fatal(e)
	}
	if e := s.beginControlProjectDeletion(t.Context(), id); e != nil {
		t.Fatal(e)
	}
	if row := usageRow(t, s, defaultTenantID); row["storage"].(map[string]any)["current_parquet_bytes"] != "100" {
		t.Fatal("ingestion fence invented zero storage", row)
	}
	request(t, s, "DELETE", "/api/v1/projects/"+id, adminToken, "", 204)
	sample.ObservedAt = usage.Stamp(at.Add(time.Minute))
	sample.Day = usage.Day(at.Add(time.Minute))
	if e := s.usage.store.RecordStorageSample(t.Context(), sample); e != nil {
		t.Fatal(e)
	}
	row := usageRow(t, s, defaultTenantID)
	if row["project_deleted"] != true || row["counters"].(map[string]any)["accepted_live_events"] != "1" || row["storage"].(map[string]any)["current_parquet_bytes"] != "0" {
		t.Fatal(row)
	}
}

func TestUsageIdentityNoopsFollowRetainedUUIDQuotaSemantics(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	_, token := project(t, s)
	identify := `{"type":"identify","message_id":"identify-one","user_id":"person"}`
	nativeRequest(t, s, token, identify, 200)
	nativeRequest(t, s, token, identify, 200)
	nativeRequest(t, s, token, `{"type":"identify","message_id":"identify-two","user_id":"person"}`, 200)
	nativeRequest(t, s, token, `[{"type":"track","message_id":"valid-but-rolled-back","actor_id":"person","name":"message"},{"type":"identify","message_id":"invalid-without-user"}]`, 400)
	row := usageRow(t, s, defaultTenantID)
	if row["counters"].(map[string]any)["accepted_live_events"] != "2" {
		t.Fatal(row)
	}
	var quota int64
	if e := s.db.QueryRow(`SELECT COALESCE(SUM(accepted_events),0) FROM tenant_usage WHERE tenant_id=?`, defaultTenantID).Scan(&quota); e != nil || quota != 2 {
		t.Fatal("meter differs from quota's retained UUID definition", quota, e)
	}
}
