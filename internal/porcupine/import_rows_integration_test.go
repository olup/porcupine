package porcupine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestClickHouseIntegrationPostHogImport(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "import.db"), BootstrapToken: adminToken, ClickHouse: &config, Pipelines: &PipelineConfig{Key: bytes.Repeat([]byte{0x74}, 32)}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.stopPipelines()
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	client := &http.Client{Timeout: 15 * time.Second}
	defer client.CloseIdleConnections()
	call := func(method, path, token string, body any, headers map[string]string, status int) map[string]any {
		t.Helper()
		var raw []byte
		if data, ok := body.([]byte); ok {
			raw = data
		} else if body != nil {
			raw, _ = json.Marshal(body)
		}
		request, e := http.NewRequest(method, host.URL+path, bytes.NewReader(raw))
		if e != nil {
			t.Fatal(e)
		}
		request.Header.Set("Content-Type", "application/json")
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		for k, v := range headers {
			request.Header.Set(k, v)
		}
		response, e := client.Do(request)
		if e != nil {
			t.Fatal("import API HTTP failed")
		}
		defer response.Body.Close()
		if response.StatusCode != status {
			t.Fatalf("%s %s status %d want %d", method, path, response.StatusCode, status)
		}
		var value map[string]any
		raw, e = io.ReadAll(io.LimitReader(response.Body, 4<<20))
		if e != nil {
			t.Fatal(e)
		}
		if len(raw) > 0 && json.Unmarshal(raw, &value) != nil {
			t.Fatal("invalid import API JSON")
		}
		return value
	}
	projectID, ingest := project(t, s)
	other, _ := project(t, s)
	_, outsider := key(t, s, []string{"events:write", "imports:write"}, []string{other})
	call("POST", "/api/v1/projects/"+projectID+"/pipelines", adminToken, PipelineCreateBody{Name: "synthetic destination must not receive imports", Kind: "webhook", Webhook: &WebhookPipelineSpec{Endpoint: "https://synthetic.example.test/not-contacted"}}, nil, 201)
	identityCapture(t, s, ingest, []map[string]any{identifyEvent("live-profile", "live-user", "", map[string]any{"name": "fresh live"}, map[string]any{})}, 200)
	call("POST", "/capture/", "", map[string]any{"api_key": ingest, "uuid": "live-first", "event": "live", "distinct_id": "anon"}, nil, 200)
	persons := []byte(`{"person_id":"internal-not-an-app-id","distinct_id":"anon","person_version":5,"person_distinct_id_version":1,"properties":{"plan":"archive"},"created_at":"2020-01-01T00:00:00.123456Z","is_deleted":false}` + "\n" + `{"person_id":"old-internal","distinct_id":"live-user","person_version":9,"person_distinct_id_version":1,"properties":{"name":"stale"},"created_at":"2020-01-01T00:00:00Z","is_deleted":false}`)
	events := []byte(`{"uuid":"live-first","event":"archive duplicate","distinct_id":"anon","timestamp":"2020-01-01T00:00:00Z","properties":{}}` + "\n" + `{"uuid":"archived-click","event":"$autocapture","distinct_id":"anon","timestamp":"2020-01-01T00:00:00.123456Z","properties":{"large":9007199254740993},"elements_chain":"button.synthetic","person_id":"internal-not-an-app-id","person_properties":{"stale":true}}` + "\n" + `{"uuid":"archive-cookie","event":"$pageview","distinct_id":"cookieless_archived_daily_hash","timestamp":"2020-01-02T00:00:00Z","properties":{"cookieless":true,"identity_day":"2020-01-02","$session_id":"must-not-return"}}` + "\n" + `{"uuid":"stripped-control","event":"$set","distinct_id":"live-user","timestamp":"2020-01-01T00:00:00Z","properties":{}}` + "\n" + `{"uuid":"archive-link","event":"$identify","distinct_id":"archive-account","timestamp":"2020-01-01T00:00:00Z","properties":{"$anon_distinct_id":"anon"}}` + "\n" + `{"event":"malformed missing required fields"}`)
	specs := []ImportUploadFileSpec{}
	for _, file := range []struct {
		path, kind string
		data       []byte
	}{{"persons/part-1.jsonl", "persons", persons}, {"events/part-1.jsonl", "events", events}} {
		digest := sha256.Sum256(file.data)
		specs = append(specs, ImportUploadFileSpec{Path: file.path, Kind: file.kind, SizeBytes: int64(len(file.data)), SHA256: hex.EncodeToString(digest[:])})
	}
	base := "/api/v1/projects/" + projectID + "/imports"
	job := call("POST", base, adminToken, map[string]any{"idempotency_key": "synthetic-import", "files": specs}, nil, 201)
	jobID := job["id"].(string)
	call("GET", base+"/"+jobID, outsider, nil, nil, 403)
	for _, rawFile := range job["files"].([]any) {
		file := rawFile.(map[string]any)
		data := events
		if file["kind"] == "persons" {
			data = persons
		}
		filePath := base + "/" + jobID + "/files/" + file["id"].(string)
		digest := sha256.Sum256(data)
		headers := map[string]string{"Content-Type": "application/octet-stream", "Upload-Offset": "0", "X-Chunk-SHA256": hex.EncodeToString(digest[:])}
		call("PUT", filePath, adminToken, data, headers, 200)
		call("PUT", filePath, adminToken, data, headers, 200)
		call("POST", filePath+"/complete", adminToken, map[string]any{}, nil, 200)
	}
	completed := call("POST", base+"/"+jobID+"/complete", adminToken, map[string]any{}, nil, 200)
	counts := completed["counts"].(map[string]any)
	for field, want := range map[string]float64{"rows": 8, "accepted": 5, "duplicates": 1, "ignored": 1, "invalid": 1, "unresolved": 3} {
		if counts[field] != want {
			t.Fatalf("import %s count %v want %v", field, counts[field], want)
		}
	}
	if completed["state"] != "completed" {
		t.Fatal("import job not completed")
	}
	retry := call("POST", "/capture/", "", map[string]any{"api_key": ingest, "uuid": "archived-click", "event": "different live retry", "distinct_id": "anon"}, nil, 200)
	if retry["duplicates"] != float64(1) {
		t.Fatal("live sender duplicated archive UUID")
	}
	var outbound int
	if s.db.QueryRow(`SELECT count(*) FROM webhook_deliveries`).Scan(&outbound) != nil || outbound != 1 {
		t.Fatal("historical import enqueued outbound webhook")
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pending int
		s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&pending)
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("import projection did not drain")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal("import delivery failed")
		}
	}
	result, err := s.analyticsQuery(context.Background(), queryInput(projectID, "SELECT uuid,event,distinct_id,person_id,person_properties,properties,timestamp,received_at,is_imported FROM events ORDER BY uuid"))
	if err != nil || result.Body.Result.Rows != 3 {
		t.Fatalf("imported analytics/control visibility differs: %v", err)
	}
	for _, row := range result.Body.Result.Data {
		var uuid, person, props, timestamp, received string
		var imported uint8
		json.Unmarshal(row["uuid"], &uuid)
		json.Unmarshal(row["person_id"], &person)
		json.Unmarshal(row["properties"], &props)
		json.Unmarshal(row["timestamp"], &timestamp)
		json.Unmarshal(row["received_at"], &received)
		json.Unmarshal(row["is_imported"], &imported)
		if uuid == "live-first" {
			if imported != 0 || string(row["event"]) != `"live"` || person != "archive-account" {
				t.Fatal("first accepted live row overwritten or explicit archive link lost")
			}
		}
		if uuid == "archived-click" {
			if imported != 1 || timestamp != "2020-01-01T00:00:00.123456Z" || person != "archive-account" || !strings.Contains(props, "9007199254740993") || !strings.Contains(props, "$elements_chain") {
				t.Fatal("archive timestamp/properties/identity/import marker lost")
			}
			at, e := time.Parse(time.RFC3339Nano, received)
			if e != nil || at.Year() < 2026 {
				t.Fatal("archive forged local receipt time")
			}
		}
		if uuid == "archive-cookie" && (string(row["distinct_id"]) != `"cookieless_archived_daily_hash"` || strings.Contains(props, "must-not-return")) {
			t.Fatal("historical cookieless identity reconstructed")
		}
	}
	live, _, _ := storedIdentityProfile(t, s, projectID, "live-user")
	if live["name"] != "fresh live" {
		t.Fatal("archive person snapshot overwrote current live profile")
	}
	call("GET", "/api/v1/projects/"+projectID+"/events/stripped-control", adminToken, nil, nil, 404)
	ns, reader, password, err := s.analyticsReader(context.Background(), projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Imported rows have honest current receipt times, yet cannot bypass the
	// dedicated window predicate using a user WITH alias of that same column.
	for _, sql := range []string{"SELECT uuid FROM events ORDER BY uuid", "WITH 0 AS is_imported SELECT uuid FROM events ORDER BY uuid"} {
		windowSQL, e := pipelineWindowSQL(sql, ns)
		if e != nil {
			t.Fatal(e)
		}
		response, e := s.ch.openExport(context.Background(), ns, reader, password, "import_window_"+strings.ReplaceAll(newID(), "-", ""), windowSQL, map[string]string{"window_start": "0", "window_end": "9223372036854775807"})
		if e != nil {
			t.Fatal(e)
		}
		var ids []string
		stream, e := streamExportJSON(context.Background(), response, 1<<20, exportStreamCallbacks{Row: func(values []json.RawMessage) error {
			var id string
			json.Unmarshal(values[0], &id)
			ids = append(ids, id)
			return nil
		}})
		response.Close()
		if e != nil || stream.Rows != 1 || len(ids) != 1 || ids[0] != "live-first" {
			t.Fatalf("automatic receipt-window export included imported history: %v rows=%v", e, ids)
		}
	}
	spoofed, e := pipelineWindowSQL("WITH 0 AS received_at_ms SELECT uuid FROM events", ns)
	if e != nil {
		t.Fatal(e)
	}
	response, e := s.ch.openExport(context.Background(), ns, reader, password, "import_window_alias_"+strings.ReplaceAll(newID(), "-", ""), spoofed, map[string]string{"window_start": "0", "window_end": "10"})
	if e != nil {
		t.Fatal(e)
	}
	stream, e := streamExportJSON(context.Background(), response, 1<<20, exportStreamCallbacks{})
	response.Close()
	if e != nil || stream.Rows != 0 {
		t.Fatal("user WITH alias bypassed trusted receipt-time predicate")
	}
	t.Log("real JSONL HTTP import: counts, resume replay, live dedup, profile safety, controls, archive time, webhook and receipt-window isolation verified")
}

func TestClickHouseIntegrationPostHogBoundImport(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s, err := Open(Config{DBPath: filepath.Join(t.TempDir(), "bound-import.db"), BootstrapToken: adminToken, ClickHouse: &config})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	projectID, _ := project(t, s)
	other, _ := project(t, s)
	bound := func(source string) string {
		t.Helper()
		raw, _ := json.Marshal(map[string]any{"name": "synthetic " + source, "permissions": []string{"events:write", "imports:write"}, "projects": []string{projectID}, "source_id": source})
		return request(t, s, "POST", "/api/v1/access-keys", adminToken, string(raw), 201)["token"].(string)
	}
	warehouse, foreign := bound("warehouse"), bound("foreign-source")
	live := map[string]any{"api_key": warehouse, "uuid": "cross-same", "event": "Live", "distinct_id": "u"}
	encoded, _ := json.Marshal(live)
	request(t, s, "POST", "/capture/", "", string(encoded), 200)
	if result := nativeRequest(t, s, warehouse, `{"type":"track","message_id":"cross-same","name":"Native retry","actor_id":"u"}`, 200); result["duplicates"] != float64(1) {
		t.Fatal("live native/PostHog namespace overlap lost")
	}
	input := []byte(`{"uuid":"cross-same","event":"Archive duplicate","distinct_id":"u","timestamp":"2020-01-01T00:00:00Z","properties":{}}` + "\n" + `{"uuid":"import-first","event":"Imported","distinct_id":"u","timestamp":"2020-01-01T00:00:00Z","source_id":"malicious-other-source","project_id":"` + other + `","properties":{}}` + "\n")
	job := importTestCreate(t, s, projectID, warehouse, "bound-source-import", []ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(input)), SHA256: importTestSHA(input)}})
	rawJob, _ := json.Marshal(job)
	var public map[string]any
	json.Unmarshal(rawJob, &public)
	if public["source_id"] != "warehouse" {
		t.Fatal("job did not freeze source binding")
	}
	importTestPut(t, s, projectID, job, job.Files[0], warehouse, 0, input, 200)
	// A project administrator may resume this job, but its original source is
	// retained rather than replaced with the administrator's default namespace.
	importTestRequest(t, s, "POST", importTestBase(projectID, job.ID)+"/files/"+job.Files[0].ID+"/complete", adminToken, []byte(`{}`), nil, 200)
	completed := importTestJob(t, importTestRequest(t, s, "POST", importTestBase(projectID, job.ID)+"/complete", adminToken, []byte(`{}`), nil, 200))
	if completed.Counts.Accepted != 1 || completed.Counts.Duplicates != 1 {
		t.Fatal("bound import did not deduplicate live source/message")
	}
	if duplicate := nativeRequest(t, s, warehouse, `{"type":"track","message_id":"import-first","name":"Native retry","actor_id":"u"}`, 200); duplicate["duplicates"] != float64(1) {
		t.Fatal("native retry duplicated same-source imported message")
	}
	independent := []byte(`{"uuid":"import-first","event":"Other source","distinct_id":"other-source-user","timestamp":"2020-01-01T00:00:00Z","properties":{}}` + "\n")
	otherJob := importTestCreate(t, s, projectID, foreign, "foreign-source-import", []ImportUploadFileSpec{{Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(independent)), SHA256: importTestSHA(independent)}})
	importTestPut(t, s, projectID, otherJob, otherJob.Files[0], foreign, 0, independent, 200)
	importTestRequest(t, s, "POST", importTestBase(projectID, otherJob.ID)+"/files/"+otherJob.Files[0].ID+"/complete", foreign, []byte(`{}`), nil, 200)
	importTestRequest(t, s, "POST", importTestBase(projectID, otherJob.ID)+"/complete", foreign, []byte(`{}`), nil, 200)
	deadline := time.Now().Add(20 * time.Second)
	for {
		var pending int
		s.db.QueryRow(`SELECT count(*) FROM events`).Scan(&pending)
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("bound import delivery did not drain")
		}
		if err = s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	result, err := s.analyticsQuery(context.Background(), queryInput(projectID, "SELECT source_id,message_id,uuid,is_imported FROM events ORDER BY source_id,message_id"))
	if err != nil || result.Body.Result.Rows != 3 {
		t.Fatalf("bound source imported projection differs: %v", err)
	}
	seen := map[string]bool{}
	for _, row := range result.Body.Result.Data {
		var source, message, uuid string
		json.Unmarshal(row["source_id"], &source)
		json.Unmarshal(row["message_id"], &message)
		json.Unmarshal(row["uuid"], &uuid)
		if !strings.HasPrefix(uuid, sourceEventUUIDPrefix) {
			t.Fatal("bound import did not use existing physical namespace")
		}
		seen[source+":"+message] = true
		if source == "malicious-other-source" {
			t.Fatal("row selected authenticated source")
		}
	}
	if !seen["warehouse:cross-same"] || !seen["warehouse:import-first"] || !seen["foreign-source:import-first"] {
		t.Fatal("same and foreign namespace dedup semantics differ")
	}
	foreignResult, err := s.analyticsQuery(context.Background(), queryInput(other, "SELECT count(*) AS n FROM events"))
	if err != nil || string(foreignResult.Body.Result.Data[0]["n"]) != `"0"` {
		t.Fatal("row selected destination project")
	}
}
