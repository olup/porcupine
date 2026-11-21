package porcupine

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http/httptest"
	"slices"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func queryDeleteJSON(t *testing.T, body map[string]any) string {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func assertQueryDeletion(t *testing.T, result map[string]any, project, state string, matched, applied int) {
	t.Helper()
	if result["project_id"] != project || result["state"] != state || result["matched_count"] != float64(matched) || result["applied_count"] != float64(applied) || result["failed_count"] != float64(0) {
		t.Fatalf("want project %s, state %s, %d matched and %d applied: %v", project, state, matched, applied, result)
	}
}

func queryDeletionSQLCount(t *testing.T, s *Service, project, token string) string {
	t.Helper()
	rows := duckLakeJourneyQuery(t, s, project, token, "SELECT count(*) AS total FROM events", nil)
	return rows[0].(map[string]any)["total"].(string)
}

func TestQueryDeleteStandaloneHTTPAndMCP(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s, _ := nativeCoreService(t)
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	_, manager := key(t, s, []string{"events:read", "events:delete", "events:edit"}, []string{a})
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, deleter := key(t, s, []string{"events:delete"}, []string{a})
	_, other := key(t, s, []string{"events:read", "events:delete"}, []string{b})
	base := "/api/v1/projects/" + a
	path := base + "/event-deletions"
	const from = "2026-11-01T01:00:00Z"
	const to = "2026-11-01T02:00:00Z"
	const channel = "customer's choice"
	row := func(uuid, name, timestamp, channel string) map[string]any {
		return map[string]any{"uuid": uuid, "event": name, "timestamp": timestamp, "distinct_id": "visitor", "properties": map[string]any{"channel": channel}}
	}
	identityCapture(t, s, ingestA, []map[string]any{
		row("match", "purchase", from, channel),
		row("corrected-in", "draft", from, channel),
		row("corrected-out", "purchase", from, channel),
		row("before", "purchase", "2026-11-01T00:59:59Z", channel),
		row("end", "purchase", to, channel),
		row("different-property", "purchase", from, "web"),
		row("already-deleted", "purchase", from, channel),
	}, 200)
	identityCapture(t, s, ingestB, []map[string]any{
		row("match", "purchase", from, channel),
		row("foreign", "purchase", from, channel),
	}, 200)
	control := receiptFrom(t, nativeRequest(t, s, ingestA, `{"type":"identify","message_id":"private-control","anonymous_id":"browser","user_id":"account","set":{"plan":"private"}}`, 200), 0)["uuid"].(string)
	duckLakeJourneyDrain(t, s)
	for uuid, name := range map[string]string{"corrected-in": "purchase", "corrected-out": "draft"} {
		body := map[string]any{
			"expected_revision": "0", "idempotency_key": "edit-" + uuid,
			"snapshot": map[string]any{"event": name, "timestamp": from, "distinct_id": "visitor", "properties": map[string]any{"channel": channel}},
		}
		request(t, s, "PATCH", base+"/events/"+uuid, manager, queryDeleteJSON(t, body), 202)
	}
	request(t, s, "DELETE", base+"/events/already-deleted", manager, `{"expected_revision":"0","idempotency_key":"delete-one"}`, 202)
	duckLakeJourneyDrain(t, s)
	controlPreview := request(t, s, "POST", path, manager, queryDeleteJSON(t, map[string]any{
		"where": "uuid = $target", "parameters": map[string]string{"target": control}, "dry_run": true, "idempotency_key": "private-preview",
	}), 200)
	assertQueryDeletion(t, controlPreview, a, "preview", 0, 0)

	body := map[string]any{
		"where":           "event = $name AND json_extract_string(properties, '$.channel') = $channel",
		"parameters":      map[string]string{"name": "purchase", "channel": channel},
		"time_range":      map[string]string{"from": "2026-11-01T02:00:00+01:00", "to": "2026-11-01T03:00:00+01:00"},
		"idempotency_key": "query-delete",
	}
	previewBody := maps.Clone(body)
	previewBody["dry_run"] = true
	previewJSON := queryDeleteJSON(t, previewBody)
	for _, token := range []string{reader, deleter, other} {
		request(t, s, "POST", path, token, previewJSON, 403)
	}
	identityCapture(t, s, ingestA, []map[string]any{row("queued", "purchase", from, channel)}, 200)
	preview := request(t, s, "POST", path, manager, previewJSON, 200)
	assertQueryDeletion(t, preview, a, "preview", 2, 0)
	if _, exists := preview["mutation_id"]; exists || mutationCount(t, s, "mutation_outbox") != 0 {
		t.Fatalf("preview accepted a mutation: %v", preview)
	}
	sample, ok := preview["sample_uuids"].([]any)
	if !ok || len(sample) != 2 || !slices.Contains(sample, "match") || !slices.Contains(sample, "corrected-in") {
		t.Fatalf("invalid preview sample: %v", preview)
	}
	request(t, s, "GET", base+"/events/match", reader, "", 200)
	duckLakeJourneyDrain(t, s)
	request(t, s, "POST", path, manager, queryDeleteJSON(t, body), 400)
	body["expected_count"] = 2
	if failure := request(t, s, "POST", path, manager, queryDeleteJSON(t, body), 409); failure["code"] != "event_deletion_count_mismatch" {
		t.Fatalf("expected a stale count conflict: %v", failure)
	}
	if count := mutationCount(t, s, "mutation_outbox"); count != 0 {
		t.Fatalf("stale preview queued %d mutations", count)
	}

	host := httptest.NewServer(s.Handler())
	t.Cleanup(host.Close)
	ctx, managerSession := mcpTestSession(t, host.URL, manager, "")
	readerCtx, readerSession := mcpTestSession(t, host.URL, reader, "")
	deleterCtx, deleterSession := mcpTestSession(t, host.URL, deleter, "")
	for _, operation := range []string{"delete-events", "get-event-deletion"} {
		detail := mcpTestTool(t, ctx, managerSession, "porcupine_docs", mcpDocsInput{OperationID: operation}, false)
		entry := detail["operations"].([]any)[0].(map[string]any)
		if entry["read_only"] != (operation == "get-event-deletion") {
			t.Fatalf("invalid MCP discovery metadata: %v", entry)
		}
	}
	mcpTestTool(t, readerCtx, readerSession, "porcupine_docs", mcpDocsInput{OperationID: "delete-events"}, true)
	mcpTestTool(t, readerCtx, readerSession, "porcupine_docs", mcpDocsInput{OperationID: "get-event-deletion"}, false)
	mcpTestTool(t, deleterCtx, deleterSession, "porcupine_docs", mcpDocsInput{OperationID: "delete-events"}, true)
	mcpTestCall(t, readerCtx, readerSession, mcpCallInput{OperationID: "delete-events", Path: map[string]string{"id": a}, Body: previewBody}, 403)
	mcpTestCall(t, ctx, managerSession, mcpCallInput{OperationID: "delete-events", Path: map[string]string{"id": b}, Body: previewBody}, 403)
	assertQueryDeletion(t, mcpTestCall(t, ctx, managerSession, mcpCallInput{OperationID: "delete-events", Path: map[string]string{"id": a}, Body: previewBody}, 200), a, "preview", 3, 0)
	body["expected_count"] = 3
	accepted := mcpTestCall(t, ctx, managerSession, mcpCallInput{OperationID: "delete-events", Path: map[string]string{"id": a}, Body: body}, 202)
	assertQueryDeletion(t, accepted, a, "pending", 3, 0)
	id := accepted["mutation_id"].(string)
	statusPath := path + "/" + id
	assertQueryDeletion(t, request(t, s, "GET", statusPath, reader, "", 200), a, "pending", 3, 0)
	request(t, s, "GET", statusPath, deleter, "", 403)
	request(t, s, "GET", statusPath, other, "", 403)
	request(t, s, "GET", "/api/v1/projects/"+b+"/event-deletions/"+id, other, "", 404)
	for _, uuid := range []string{"match", "corrected-in", "queued"} {
		request(t, s, "GET", base+"/events/"+uuid, reader, "", 410)
	}
	identityCapture(t, s, ingestA, []map[string]any{row("arrived-after-acceptance", "purchase", from, channel)}, 200)
	duckLakeJourneyDrain(t, s)
	status := mcpTestCall(t, readerCtx, readerSession, mcpCallInput{OperationID: "get-event-deletion", Path: map[string]string{"id": a, "mutation_id": id}}, 200)
	assertQueryDeletion(t, status, a, "applied", 3, 3)
	if status["created_at"] == "" || status["created_at"] == nil || status["applied_at"] == "" || status["applied_at"] == nil {
		t.Fatalf("missing mutation timestamps: %v", status)
	}
	if replay := request(t, s, "POST", path, manager, queryDeleteJSON(t, body), 202); replay["mutation_id"] != id {
		t.Fatalf("retry reselected events after acceptance: %v", replay)
	}
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT uuid FROM events ORDER BY uuid", nil)
	got := make([]string, 0, len(rows))
	for _, value := range rows {
		got = append(got, value.(map[string]any)["uuid"].(string))
	}
	want := []string{"arrived-after-acceptance", "before", "corrected-out", "different-property", "end"}
	if !slices.Equal(got, want) {
		t.Fatalf("surviving event UUIDs = %v, want %v", got, want)
	}
	private, err := s.lake.Load(t.Context(), a, control)
	if err != nil || private.IsDeleted || private.IdentitySnapshot == "" {
		t.Fatalf("identity control was deleted or changed: %v", err)
	}
	if _, err := s.control.Exec(`INSERT INTO project_deletions(project_id,completed) VALUES(?,0)`, a); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", path, adminToken, previewJSON, 404)
	request(t, s, "POST", path, adminToken, queryDeleteJSON(t, body), 404)
	request(t, s, "GET", statusPath, adminToken, "", 404)
	if count := queryDeletionSQLCount(t, s, b, other); count != "2" {
		t.Fatalf("other project event count = %s after project deletion fence, want 2", count)
	}
}

func TestQueryDeleteStandaloneAtomicAdmissionAndRestart(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s, config := nativeCoreService(t)
	projectID, ingest := project(t, s)
	base := "/api/v1/projects/" + projectID
	config.Pipelines = testPipelineConfig(s)
	attachTestPipelines(s, config.Pipelines)
	request(t, s, "POST", base+"/pipelines", adminToken, `{"name":"Purchase webhook","kind":"webhook","webhook":{"endpoint":"https://hooks.example.test/purchases","include_events":["purchase"]},"credentials":{}}`, 201)
	for _, uuid := range []string{"atomic-a", "atomic-z"} {
		request(t, s, "POST", "/capture/", "", captureBody(ingest, uuid), 200)
	}
	if count := mutationCount(t, s, "webhook_members"); count != 2 {
		t.Fatalf("expected 2 pending webhook events, found %d", count)
	}
	originals, err := s.deliveryBatch(t.Context())
	if err != nil || len(originals) != 2 {
		t.Fatalf("delivery batch contains %d originals: %v", len(originals), err)
	}
	duckLakeJourneyDrain(t, s)
	path := base + "/event-deletions"
	body := `{"where":"event = $name","parameters":{"name":"purchase"},"idempotency_key":"delete-purchases","expected_count":2}`
	request(t, s, "PATCH", base+"/events/atomic-z", adminToken, `{"expected_revision":"0","idempotency_key":"edit-z","snapshot":{"event":"purchase","distinct_id":"visitor","timestamp":"2026-11-01T01:00:00Z","properties":{}}}`, 202)
	request(t, s, "POST", path, adminToken, body, 409)
	request(t, s, "GET", base+"/events/atomic-a", adminToken, "", 200)
	duckLakeJourneyDrain(t, s)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_query_delete_admission BEFORE INSERT ON mutation_outbox
WHEN (SELECT count(*) FROM mutation_outbox)=1 BEGIN SELECT RAISE(FAIL,'simulated outbox error'); END`); err != nil {
		t.Fatal(err)
	}
	request(t, s, "POST", path, adminToken, body, 503)
	if count := mutationCount(t, s, "mutation_outbox"); count != 0 {
		t.Fatalf("failed batch left %d outbox rows", count)
	}
	if count := mutationCount(t, s, "mutation_audit"); count != 1 {
		t.Fatalf("failed batch changed the audit count to %d", count)
	}
	if count := mutationCount(t, s, "event_deletions"); count != 0 {
		t.Fatalf("failed batch left %d deletion records", count)
	}
	if count := mutationCount(t, s, "webhook_members"); count != 2 {
		t.Fatalf("failed batch changed the pending webhook count to %d", count)
	}
	for _, uuid := range []string{"atomic-a", "atomic-z"} {
		request(t, s, "GET", base+"/events/"+uuid, adminToken, "", 200)
	}
	if _, err := s.db.Exec(`DROP TRIGGER fail_query_delete_admission`); err != nil {
		t.Fatal(err)
	}
	accepted := request(t, s, "POST", path, adminToken, body, 202)
	assertQueryDeletion(t, accepted, projectID, "pending", 2, 0)
	if count := mutationCount(t, s, "webhook_members"); count != 0 {
		t.Fatalf("confirmed deletion left %d pending webhook events", count)
	}
	id := accepted["mutation_id"].(string)
	statusPath := path + "/" + id
	if replay := request(t, s, "POST", path, adminToken, body, 202); replay["mutation_id"] != id {
		t.Fatalf("pending retry returned another mutation: %v", replay)
	}
	request(t, s, "POST", path, adminToken, `{"where":"event = $name","parameters":{"name":"draft"},"idempotency_key":"delete-purchases","expected_count":2}`, 409)
	if _, err := s.db.Exec(`CREATE TRIGGER fail_query_delete_ack BEFORE DELETE ON mutation_outbox BEGIN SELECT RAISE(FAIL,'simulated acknowledgment loss'); END`); err != nil {
		t.Fatal(err)
	}
	if delivered, err := s.deliverMutations(t.Context()); delivered || err == nil {
		t.Fatalf("delivery reported success after a failed acknowledgment: %t, %v", delivered, err)
	}
	assertQueryDeletion(t, request(t, s, "GET", statusPath, adminToken, "", 200), projectID, "pending", 2, 0)
	if count := queryDeletionSQLCount(t, s, projectID, adminToken); count != "0" {
		t.Fatalf("native commit should precede the failed acknowledgment, found %s rows", count)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	stopCoreDelivery(reopened)
	assertQueryDeletion(t, request(t, reopened, "GET", statusPath, adminToken, "", 200), projectID, "pending", 2, 0)
	if _, err := reopened.db.Exec(`DROP TRIGGER fail_query_delete_ack`); err != nil {
		t.Fatal(err)
	}
	duckLakeJourneyDrain(t, reopened)
	assertQueryDeletion(t, request(t, reopened, "GET", statusPath, adminToken, "", 200), projectID, "applied", 2, 2)
	retry := request(t, reopened, "POST", path, adminToken, body, 202)
	assertQueryDeletion(t, retry, projectID, "applied", 2, 2)
	if retry["mutation_id"] != id || retry["created_at"] != accepted["created_at"] {
		t.Fatalf("applied retry lost the original mutation identity: %v", retry)
	}
	request(t, reopened, "POST", path, adminToken, `{"where":"event = $name","parameters":{"name":"purchase"},"idempotency_key":"delete-purchases","expected_count":0}`, 409)
	if err := reopened.lake.Apply(t.Context(), duckLakeJourneyProjections(t, originals)); err != nil {
		t.Fatal(err)
	}
	for _, original := range originals {
		request(t, reopened, "POST", "/capture/", "", captureBody(ingest, original.UUID), 200)
		tombstone, err := reopened.lake.Load(t.Context(), projectID, original.UUID)
		if err != nil || !tombstone.IsDeleted || tombstone.ID != original.ID || tombstone.ReceivedAt != original.ReceivedAt || tombstone.Event.Event != "" || tombstone.OriginalSnapshot != "" {
			t.Fatalf("invalid tombstone for %s: %v", original.UUID, err)
		}
	}
	if count := eventCount(t, reopened); count != 0 {
		t.Fatalf("capture retry queued %d deleted originals", count)
	}
	if count := queryDeletionSQLCount(t, reopened, projectID, adminToken); count != "0" {
		t.Fatalf("late originals resurrected %s deleted events", count)
	}
	zero := request(t, reopened, "POST", path, adminToken, `{"where":"event = 'purchase'","idempotency_key":"empty-delete","expected_count":0}`, 202)
	duckLakeJourneyDrain(t, reopened)
	assertQueryDeletion(t, request(t, reopened, "GET", path+"/"+zero["mutation_id"].(string), adminToken, "", 200), projectID, "applied", 0, 0)
}

func TestQueryDeleteStandaloneMatchLimit(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s, _ := nativeCoreService(t)
	projectID, _ := project(t, s)
	projections := make([]lakeapi.EventProjection, 1001)
	for i := range projections {
		projections[i] = lakeStorageProjection(t, projectID, fmt.Sprintf("limit-%04d", i), int64(i+1))
	}
	if err := s.lake.Apply(t.Context(), projections); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/projects/" + projectID + "/event-deletions"
	const previewAll = `{"where":"event = 'page_viewed'","dry_run":true,"idempotency_key":"preview-all"}`
	if failure := request(t, s, "POST", path, adminToken, previewAll, 422); failure["code"] != "event_deletion_match_limit" {
		t.Fatalf("expected the deletion match limit: %v", failure)
	}
	request(t, s, "POST", path, adminToken, `{"where":"event = 'page_viewed'","idempotency_key":"too-many","expected_count":1000}`, 422)
	preview := request(t, s, "POST", path, adminToken, `{"where":"event = 'page_viewed' AND uuid <> 'limit-1000'","dry_run":true,"idempotency_key":"preview-at-limit"}`, 200)
	assertQueryDeletion(t, preview, projectID, "preview", 1000, 0)
	if sample, ok := preview["sample_uuids"].([]any); !ok || len(sample) != 5 {
		t.Fatalf("preview sample did not respect the five UUID limit: %v", preview)
	}
	budgetPath := "/api/v1/tenants/" + defaultTenantID + "/budgets"
	request(t, s, "PUT", budgetPath, adminToken, `{"ingestion_monthly_events":0,"hot_query_max_concurrent":2,"hot_query_timeout_ms":5000,"hot_query_max_rows":2,"hot_query_max_result_bytes":1048576}`, 200)
	if failure := request(t, s, "POST", path, adminToken, previewAll, 429); failure["code"] != "result_row_limit" {
		t.Fatalf("expected tenant result row limit: %v", failure)
	}
	request(t, s, "PUT", budgetPath, adminToken, `{"ingestion_monthly_events":0,"hot_query_max_concurrent":2,"hot_query_timeout_ms":5000,"hot_query_max_rows":1000,"hot_query_max_result_bytes":256}`, 200)
	if failure := request(t, s, "POST", path, adminToken, previewAll, 429); failure["code"] != "result_byte_limit" {
		t.Fatalf("expected tenant result byte limit: %v", failure)
	}
	if count := mutationCount(t, s, "mutation_outbox"); count != 0 {
		t.Fatalf("limit rejection queued %d mutations", count)
	}
}
