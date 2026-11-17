package porcupine

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func tenantAllowance(t *testing.T, s *Service, tenant string, limit int64) {
	t.Helper()
	if _, err := s.control.Exec(`UPDATE tenants SET ingestion_monthly_events=? WHERE id=?`, limit, tenant); err != nil {
		t.Fatal(err)
	}
}
func tenantAccepted(t *testing.T, s *Service, tenant, period string) int64 {
	t.Helper()
	var used int64
	if err := s.db.QueryRow(`SELECT coalesce(sum(accepted_events),0) FROM tenant_usage WHERE tenant_id=? AND period_start=?`, tenant, period).Scan(&used); err != nil {
		t.Fatal(err)
	}
	return used
}
func tenantTestProject(t *testing.T, s *Service, tenant string) (string, string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"name": "budget project", "tenant_id": tenant})
	result := request(t, s, "POST", "/api/v1/projects", adminToken, string(body), 201)
	return result["project"].(map[string]any)["id"].(string), result["ingest_token"].(string)
}

func TestTenantIngestionSharedProjectsAndIndependentTenant(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	tenantAllowance(t, s, defaultTenantID, 2)
	_, a := project(t, s)
	_, b := project(t, s)
	other, err := s.insertTenant(t.Context(), "other", s.defaultTenantBudgets())
	if err != nil {
		t.Fatal(err)
	}
	tenantAllowance(t, s, other.ID, 1)
	_, c := tenantTestProject(t, s, other.ID)
	for _, token := range []string{a, b, c} {
		result := request(t, s, "POST", "/capture/", "", captureBody(token, "one"), 200)
		if result["accepted"] != float64(1) {
			t.Fatal(result)
		}
	}
	for _, token := range []string{a, b, c} {
		result := request(t, s, "POST", "/capture/", "", captureBody(token, "two"), 200)
		if result["accepted"] != float64(0) || result["code"] != "ingestion_budget_exceeded" || result["quota_limited"] == nil {
			t.Fatal(result)
		}
	}
	if eventCount(t, s) != 3 {
		t.Fatal("allowances were not isolated by tenant")
	}
}

func TestTenantIngestionAtomicBatchAndResponseContracts(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	tenantAllowance(t, s, defaultTenantID, 2)
	_, token := project(t, s)
	old := `{"type":"track","message_id":"old","actor_id":"u","name":"original"}`
	nativeRequest(t, s, token, old, 200)
	denied := nativeRequest(t, s, token, `[{"type":"track","message_id":"old","actor_id":"u","name":"original"},{"type":"track","message_id":"new1","actor_id":"u","name":"new"},{"type":"identify","message_id":"new2","user_id":"person","anonymous_id":"u","set":{"plan":"paid"}}]`, 429)
	if denied["code"] != "ingestion_budget_exceeded" || denied["receipts"] != nil || eventCount(t, s) != 1 {
		t.Fatal("partial acceptance", denied)
	}
	var links int
	if err := s.db.QueryRow(`SELECT count(*) FROM identity_links`).Scan(&links); err != nil || links != 0 {
		t.Fatal("identity side effect committed", err)
	}
	nativeRequest(t, s, token, `{"type":"track","message_id":"second","actor_id":"u","name":"second"}`, 200)
	duplicate := nativeRequest(t, s, token, old, 200)
	if duplicate["duplicates"] != float64(1) {
		t.Fatal(duplicate)
	}
	segment := transportRequest(t, s, "POST", "/v1/track", []byte(`{"messageId":"segment","event":"Blocked","userId":"u"}`), segmentHeaders(token), 429)
	if !strings.Contains(segment.Body.String(), `"code":"ingestion_budget_exceeded"`) || strings.Contains(segment.Body.String(), `"receipts"`) {
		t.Fatal(segment.Body.String())
	}
	period, _ := tenantMonth(s.clock())
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 2 {
		t.Fatal("rolled-back batch was charged", used)
	}
}

func TestTenantIngestionSuspensionDoesNotBlockDeliveryOrRetry(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	_, token := project(t, s)
	body := `{"type":"track","message_id":"stable","actor_id":"u","name":"Original"}`
	first := nativeRequest(t, s, token, body, 200)
	if _, err := s.control.Exec(`UPDATE tenants SET ingestion_allowed=0 WHERE id=?`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	denied := nativeRequest(t, s, token, `{"type":"track","message_id":"new","actor_id":"u","name":"New"}`, 429)
	if denied["code"] != "ingestion_suspended" || strings.Contains(denied["hint"].(string), "month") {
		t.Fatal(denied)
	}
	queued, err := s.deliveryBatch(t.Context())
	if err != nil || len(queued) != 1 {
		t.Fatal(queued, err)
	}
	if err = s.acknowledgeOriginals(t.Context(), queued); err != nil {
		t.Fatal(err)
	}
	replay := nativeRequest(t, s, token, body, 200)
	if replay["duplicates"] != float64(1) || replay["receipts"].([]any)[0].(map[string]any)["event_id"] != first["receipts"].([]any)[0].(map[string]any)["event_id"] {
		t.Fatal("receipt changed on suspended retry", first, replay)
	}
	period, _ := tenantMonth(s.clock())
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 1 {
		t.Fatal(used)
	}
}

func TestTenantIngestionMonthBoundaryAndRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "budget.db")
	now := time.Date(2026, 12, 31, 23, 59, 59, 999999999, time.UTC)
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	_, token := project(t, s)
	tenantAllowance(t, s, defaultTenantID, 1)
	body := `{"type":"track","message_id":"old","actor_id":"u","name":"archived","timestamp":"2020-01-01T00:00:00Z"}`
	first := nativeRequest(t, s, token, body, 200)
	nativeRequest(t, s, token, `{"type":"track","message_id":"denied","actor_id":"u","name":"denied"}`, 429)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	now = time.Date(2027, 1, 1, 1, 0, 0, 0, time.FixedZone("plus-one", 3600))
	s, err = Open(Config{DBPath: path, Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	replay := nativeRequest(t, s, token, body, 200)
	if replay["duplicates"] != float64(1) || replay["receipts"].([]any)[0].(map[string]any)["event_id"] != first["receipts"].([]any)[0].(map[string]any)["event_id"] {
		t.Fatal(first, replay)
	}
	nativeRequest(t, s, token, `{"type":"track","message_id":"new-month","actor_id":"u","name":"new-month"}`, 200)
	nativeRequest(t, s, token, `{"type":"track","message_id":"still-denied","actor_id":"u","name":"denied"}`, 429)
	for _, period := range []string{"2026-12-01T00:00:00Z", "2027-01-01T00:00:00Z"} {
		if used := tenantAccepted(t, s, defaultTenantID, period); used != 1 {
			t.Fatal(period, used)
		}
	}
}

func TestTenantIngestionConcurrentExactLimit(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	tenantAllowance(t, s, defaultTenantID, 7)
	_, token := project(t, s)
	var wg sync.WaitGroup
	failures := make(chan string, 30)
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := httptest.NewRequest("POST", "/capture/", strings.NewReader(captureBody(token, fmt.Sprint(i))))
			w := httptest.NewRecorder()
			s.Handler().ServeHTTP(w, r)
			if w.Code != 200 {
				failures <- w.Body.String()
			}
		}(i)
	}
	wg.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if eventCount(t, s) != 7 {
		t.Fatal("concurrent allowance exceeded", eventCount(t, s))
	}
	period, _ := tenantMonth(s.clock())
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 7 {
		t.Fatal("counter differs from acceptance", used)
	}
}

func TestTenantIngestionImportQuotaRetainsCheckpointAndDates(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	now := time.Date(2026, 11, 30, 23, 59, 59, 0, time.UTC)
	s.clock = func() time.Time { return now }
	projectID, _ := project(t, s)
	tenantAllowance(t, s, defaultTenantID, 1)
	_, token := key(t, s, []string{"imports:write", "events:write"}, []string{projectID})
	first := importTestRow("one")
	data := append(append([]byte(nil), first...), importTestRow("two")...)
	job := importTestCreate(t, s, projectID, token, "quota", []ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data)}})
	blocked := importTestPut(t, s, projectID, job, job.Files[0], token, 0, data, 429)
	if !strings.Contains(blocked.Body.String(), "ingestion_budget_exceeded") {
		t.Fatal(blocked.Body.String())
	}
	status, err := s.imports.loadImportJob(t.Context(), projectID, job.ID)
	if err != nil || status.Files[0].OffsetBytes != 0 || status.Counts.Rows != 0 || status.Counts.Invalid != 0 || eventCount(t, s) != 0 || tenantAccepted(t, s, defaultTenantID, "2026-11-01T00:00:00Z") != 0 {
		t.Fatal("chunk committed partial quota state", status, err)
	}
	accepted := importTestJob(t, importTestPut(t, s, projectID, job, job.Files[0], token, 0, first, 200))
	if accepted.Counts.Accepted != 1 {
		t.Fatal(accepted)
	}
	importTestPut(t, s, projectID, job, job.Files[0], token, int64(len(first)), data[len(first):], 429)
	now = now.Add(time.Second)
	done := importTestJob(t, importTestPut(t, s, projectID, job, job.Files[0], token, int64(len(first)), data[len(first):], 200))
	if done.Counts.Accepted != 2 || done.Counts.Invalid != 0 {
		t.Fatal(done)
	}
	replay := importTestJob(t, importTestPut(t, s, projectID, job, job.Files[0], token, int64(len(first)), data[len(first):], 200))
	if replay.Counts != done.Counts {
		t.Fatal("chunk replay consumed quota", replay)
	}
	for _, period := range []string{"2026-11-01T00:00:00Z", "2026-12-01T00:00:00Z"} {
		if used := tenantAccepted(t, s, defaultTenantID, period); used != 1 {
			t.Fatal(period, used)
		}
	}
	var mismatches int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE timestamp!='2020-01-02T03:04:05Z'`).Scan(&mismatches); err != nil || mismatches != 0 {
		t.Fatal("original dates changed", err)
	}
}

func TestTenantIngestionCountsCanonicalIdentityAndPersonRows(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	p, token := project(t, s)
	period, _ := tenantMonth(s.clock())
	person := func(version int) string {
		return fmt.Sprintf(`{"person_id":"internal-person","distinct_id":"anonymous-original","person_version":%d,"person_distinct_id_version":1,"properties":{"name":"archive"},"created_at":"2024-01-01T00:00:00Z","is_deleted":false}`, version)
	}
	first := importRowsForTest(t, s, p, "persons", person(10))[0]
	stale := importRowsForTest(t, s, p, "persons", person(9))[0]
	repeated := importRowsForTest(t, s, p, "persons", person(10))[0]
	if first.Accepted != 1 || stale.Ignored != 1 || repeated.Duplicates != 1 {
		t.Fatal(first, stale, repeated)
	}
	control := nativeRequest(t, s, token, `{"type":"identify","message_id":"identify","user_id":"person","anonymous_id":"browser","set":{"plan":"paid"}}`, 200)
	if control["accepted"] != float64(1) {
		t.Fatal(control)
	}
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 2 {
		t.Fatal("identity/snapshot accounting differs from canonical rows", used)
	}
	tenantAllowance(t, s, defaultTenantID, 2)
	// An obsolete snapshot is ignored even when the allowance is fully consumed.
	ignored := importRowsForTest(t, s, p, "persons", person(8))[0]
	if ignored.Ignored != 1 {
		t.Fatal(ignored)
	}
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 2 {
		t.Fatal(used)
	}
}

func TestTenantIngestionEveryHistoricalAdapterSharesQuota(t *testing.T) {
	t.Parallel()
	for _, format := range []string{"native", "posthog", "segment"} {
		t.Run(format, func(t *testing.T) {
			s := service(t, 100)
			p, _ := project(t, s)
			_, token := key(t, s, []string{"imports:write", "events:write"}, []string{p})
			tenantAllowance(t, s, defaultTenantID, 2)
			data := historicalJourney(t, format)
			job := createFormatImport(t, s, p, token, format, "limited", data)
			importTestPut(t, s, p, job, job.Files[0], token, 0, data, 429)
			if eventCount(t, s) != 0 {
				t.Fatal("failed adapter chunk leaked acceptance")
			}
			tenantAllowance(t, s, defaultTenantID, 3)
			done := finishFormatImport(t, s, p, token, job, data)
			if done.Counts.Accepted != 3 {
				t.Fatal(done)
			}
			duplicateJob := createFormatImport(t, s, p, token, format, "retry-at-cap", data)
			duplicate := finishFormatImport(t, s, p, token, duplicateJob, data)
			if duplicate.Counts.Duplicates != 3 || duplicate.Counts.Accepted != 0 {
				t.Fatal(duplicate)
			}
			period, _ := tenantMonth(s.clock())
			if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 3 {
				t.Fatal(used)
			}
		})
	}
}

func TestTenantIngestionAcceptanceClockAndNoProjectRefund(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	p, token := project(t, s)
	before := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	s.clock = func() time.Time { return before.Add(time.Second) }
	prepared, err := prepareLiveBatch(p, "", eventBatch{{UUID: "waited-for-lock", Event: "queued", DistinctID: "u"}}, before)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := s.acceptance.Accept(t.Context(), prepared, ingestionPeer{}, false)
	if err != nil || accepted.accepted != 1 {
		t.Fatal(accepted, err)
	}
	if old := tenantAccepted(t, s, defaultTenantID, "2026-12-01T00:00:00Z"); old != 0 {
		t.Fatal("pre-lock preparation date was charged", old)
	}
	if fresh := tenantAccepted(t, s, defaultTenantID, "2027-01-01T00:00:00Z"); fresh != 1 {
		t.Fatal(fresh)
	}
	request(t, s, "DELETE", "/api/v1/projects/"+p, adminToken, "", 204)
	if used := tenantAccepted(t, s, defaultTenantID, "2027-01-01T00:00:00Z"); used != 1 {
		t.Fatal("project deletion refunded usage", used)
	}
	// A new project in the same tenant cannot reuse that consumed allowance.
	tenantAllowance(t, s, defaultTenantID, 1)
	_, token = project(t, s)
	result := nativeRequest(t, s, token, `{"type":"track","message_id":"new-project","actor_id":"u","name":"new"}`, 429)
	if result["code"] != "ingestion_budget_exceeded" {
		t.Fatal(result)
	}
}

func TestTenantIngestionInvalidAndQueueRejectedRowsNotCharged(t *testing.T) {
	t.Parallel()
	s := service(t, 1)
	_, token := project(t, s)
	tenantAllowance(t, s, defaultTenantID, 0)
	nativeRequest(t, s, token, `{"type":"track","message_id":"zero","actor_id":"u","name":"blocked"}`, 429)
	tenantAllowance(t, s, defaultTenantID, 20)
	request(t, s, "POST", "/batch/", "", fmt.Sprintf(`{"api_key":%q,"batch":[{"uuid":"valid","event":"ok","distinct_id":"u"},{"event":"bad"}]}`, token), 400)
	request(t, s, "POST", "/batch/", "", fmt.Sprintf(`{"api_key":%q,"batch":[{"uuid":"a","event":"ok","distinct_id":"u"},{"uuid":"b","event":"ok","distinct_id":"u"}]}`, token), 503)
	period, _ := tenantMonth(s.clock())
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 0 || eventCount(t, s) != 0 {
		t.Fatal("rejected batch consumed allowance", used)
	}
	nativeRequest(t, s, token, `{"type":"track","message_id":"one","actor_id":"u","name":"accepted"}`, 200)
	if used := tenantAccepted(t, s, defaultTenantID, period.Format(time.RFC3339)); used != 1 {
		t.Fatal(used)
	}
}
