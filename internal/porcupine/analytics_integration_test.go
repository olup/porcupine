package porcupine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func integrationClickHouseConfig(t *testing.T) ClickHouseConfig {
	t.Helper()
	endpoint := os.Getenv("PORCUPINE_TEST_CLICKHOUSE_URL")
	if endpoint == "" {
		t.Skip("run make integration for a real ClickHouse")
	}
	return ClickHouseConfig{URL: endpoint, Database: "porcupine_test_" + strings.ReplaceAll(newID(), "-", ""),
		User: os.Getenv("PORCUPINE_TEST_CLICKHOUSE_USER"), Password: os.Getenv("PORCUPINE_TEST_CLICKHOUSE_PASSWORD"),
		StoragePolicy: os.Getenv("PORCUPINE_TEST_STORAGE_POLICY"), HotStorageDays: 30, ColdVolume: "cold"}
}

func TestClickHouseIntegration(t *testing.T) {
	config := integrationClickHouseConfig(t)
	s := service(t, 100)
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	_, readerA := key(t, s, []string{"events:read", "management:read"}, []string{a})
	_, readerB := key(t, s, []string{"events:read"}, []string{b})
	for _, row := range []struct {
		key, uuid, user, day, client, language string
		duration                               int
	}{
		{ingestA, "a1", "u1", "2026-11-01", "client-1", "fr", 10},
		{ingestA, "a2", "u2", "2026-11-02", "client-1", "fr", 30},
		{ingestA, "a3", "u1", "2026-11-01", "client-2", "en", 50},
		{ingestB, "b1", "secret-user", "2026-11-01", "secret-client", "secret-language", 999},
	} {
		eventName, numeric := "conversation_started", "1"
		if row.key == ingestB {
			eventName, numeric = "secret_event", "not-a-number-secret"
		}
		properties := map[string]any{"client_id": row.client, "language": row.language, "duration_ms": row.duration, "numeric_text": numeric}
		if row.uuid == "a1" {
			properties["legacy_only"] = "sampled-earlier-property"
		}
		body, _ := json.Marshal(map[string]any{"api_key": row.key, "uuid": row.uuid, "event": eventName, "distinct_id": row.user,
			"timestamp": row.day + "T12:00:00Z", "properties": properties})
		request(t, s, "POST", "/capture/", "", string(body), 200)
	}
	batch, err := s.deliveryBatch(ctx)
	if err != nil || len(batch) != 4 {
		t.Fatalf("queue: %d rows, %v", len(batch), err)
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	query := func(project, key, sql string, parameters map[string]string, status int) map[string]any {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"sql": sql, "parameters": parameters})
		return request(t, s, "POST", "/api/v1/projects/"+project+"/query", key, string(body), status)
	}
	summary := query(a, readerA, "SELECT count() AS total, uniqExact(distinct_id) AS users, sum(JSONExtractInt(properties,'duration_ms')) AS duration FROM events WHERE event={event:String}", map[string]string{"event": "conversation_started"}, 200)
	data := summary["result"].(map[string]any)["data"].([]any)
	if len(data) != 1 || data[0].(map[string]any)["total"] != "3" || data[0].(map[string]any)["users"] != "2" || data[0].(map[string]any)["duration"] != "90" {
		t.Fatalf("wrong analytics: %+v", data)
	}
	statistics := summary["result"].(map[string]any)["statistics"].(map[string]any)
	if statistics["rows_read"] != nil || statistics["bytes_read"] != nil {
		t.Fatal("shared-table physical scan statistics escaped the project boundary")
	}
	grouped := query(a, readerA, "SELECT toDate(event_time) AS day, JSONExtractString(properties,'client_id') AS client, count() AS conversations FROM events GROUP BY day,client ORDER BY day,client", nil, 200)
	if grouped["result"].(map[string]any)["rows"] != float64(3) || grouped["delivery"].(map[string]any)["pending"] != float64(0) {
		t.Fatalf("wrong grouping or delivery: %+v", grouped)
	}
	query(b, readerA, "SELECT count() FROM events", nil, 403)
	other := query(b, readerB, "SELECT count() AS total FROM events", nil, 200)
	if other["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "1" {
		t.Fatal("project B reader did not see its own project")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	mcpCtx, session := mcpTestSession(t, host.URL, readerA, "")
	mcpResult := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": a}, Body: map[string]any{"sql": "SELECT count() AS total FROM events"}}, 200)
	if mcpResult["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "3" {
		t.Fatal("MCP analytics mismatch")
	}
	// Replaying acknowledged inserts must not change logical event counts.
	if err := ch.insert(ctx, batch); err != nil {
		t.Fatal(err)
	}
	after := query(a, readerA, "SELECT count() AS total FROM events", nil, 200)
	if after["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "3" {
		t.Fatal("ClickHouse retries double counted events")
	}
	cast := query(a, readerA, "SELECT sum(toInt64(JSONExtractString(properties,'numeric_text'))) AS total FROM events", nil, 200)
	if cast["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "3" {
		t.Fatal("hidden project rows affected an expression")
	}
	cte := query(a, readerA, "WITH selected AS (SELECT distinct_id FROM events WHERE event={event:String}) SELECT count() AS total FROM selected", map[string]string{"event": "conversation_started"}, 200)
	if cte["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "3" {
		t.Fatal("CTE query mismatch")
	}
	bound := query(a, readerA, "SELECT count() AS total FROM events WHERE event={event:String}", map[string]string{"event": "conversation_started' OR 1=1 --"}, 200)
	if bound["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "0" {
		t.Fatal("typed SQL parameter changed query syntax")
	}
	precise := query(a, readerA, "SELECT toUInt64('18446744073709551615') AS total FROM events LIMIT 1", nil, 200)
	if precise["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "18446744073709551615" {
		t.Fatal("64-bit result lost precision")
	}
	query(a, readerA, "SELECT a.id FROM events a CROSS JOIN events b CROSS JOIN events c CROSS JOIN events d CROSS JOIN events e CROSS JOIN events f CROSS JOIN events g", nil, 429)
	for _, sql := range []string{"SELECT * FROM system.query_log", "SELECT * FROM " + ch.config.Database + ".events", "SELECT * FROM url('http://127.0.0.1:1','JSONEachRow')", "SELECT * FROM events SETTINGS readonly=0", "SELECT * FROM events; DROP TABLE events"} {
		query(a, readerA, sql, nil, 400)
	}
	// Bypass the application validator deliberately: ClickHouse itself must scope data.
	namespaceA, accountA, passwordA, err := s.analyticsReader(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	namespaceB, _, _, err := s.analyticsReader(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	physical, err := ch.query(ctx, accountA, passwordA, namespaceA, "SELECT count() AS total FROM "+ch.config.Database+"."+eventTable, nil)
	if err != nil || !strings.Contains(string(physical), `"total": "3"`) {
		t.Fatalf("physical row policy did not scope project A: %v", err)
	}
	if _, err := ch.query(ctx, accountA, passwordA, namespaceA, "SELECT * FROM "+namespaceB+".events", nil); err == nil {
		t.Fatal("project A database account could read project B's view")
	}
	if _, err := ch.query(ctx, accountA, passwordA, namespaceA, "SELECT count() FROM events SETTINGS readonly=0", nil); err == nil {
		t.Fatal("reader changed immutable settings")
	}
	if _, err := ch.query(ctx, accountA, passwordA, namespaceA, "INSERT INTO "+ch.config.Database+"."+eventTable+" SELECT * FROM "+ch.config.Database+"."+eventTable, nil); err == nil {
		t.Fatal("reader wrote events")
	}
	catalog := request(t, s, "GET", "/api/v1/projects/"+a+"/catalog?event=conversation_started", readerA, "", 200)
	events := catalog["events"].([]any)
	if len(events) != 1 || events[0].(map[string]any)["count"] != "3" || events[0].(map[string]any)["latest_event_uuid"] != "a3" {
		t.Fatalf("wrong event discovery or latest pointer: %+v", events)
	}
	foundEarlierProperty := false
	for _, item := range catalog["properties"].([]any) {
		property := item.(map[string]any)
		if property["key"] == "legacy_only" {
			foundEarlierProperty = property["latest_event_uuid"] == "a1"
		}
		if property["key"] == "language" && property["types"].([]any)[0] != "String" {
			t.Fatal("property type discovery failed")
		}
	}
	if !foundEarlierProperty || catalog["sampling"].(map[string]any)["historical_properties_complete"] != false {
		t.Fatal("property discovery must sample earlier events and declare incompleteness")
	}
	encodedCatalog, _ := json.Marshal(catalog)
	for _, forbidden := range []string{"secret-user", "secret-client", "secret_event", "sampled-earlier-property", "client-1"} {
		if strings.Contains(string(encodedCatalog), forbidden) {
			t.Fatal("discovery leaked another project or a property value")
		}
	}
	request(t, s, "GET", "/api/v1/projects/"+b+"/catalog", readerA, "", 403)
	mcpCatalog := mcpTestCall(t, mcpCtx, session, mcpCallInput{OperationID: "get-event-catalog", Path: map[string]string{"id": a}, Query: map[string]string{"event": "conversation_started"}}, 200)
	if len(mcpCatalog["properties"].([]any)) == 0 {
		t.Fatal("MCP property discovery returned no properties")
	}
	pointer := query(a, readerA, "SELECT uuid,event FROM events WHERE uuid={uuid:String}", map[string]string{"uuid": events[0].(map[string]any)["latest_event_uuid"].(string)}, 200)
	if pointer["result"].(map[string]any)["rows"] != float64(1) {
		t.Fatal("latest-event UUID pointer could not be queried")
	}
	query(a, readerA, "SELECT windowFunnel(1800)(toDateTime(event_time),event='conversation_started',event='finished') AS step FROM events", nil, 200)
	query(a, readerA, "SELECT retention(event='conversation_started',event='finished') AS cohort FROM events", nil, 200)
	retry := request(t, s, "POST", "/capture/", "", captureBody(ingestA, "a1"), 200)
	if retry["accepted"] != float64(0) || retry["duplicates"] != float64(1) {
		t.Fatal("receipt did not preserve deduplication after delivery")
	}
	if _, err := ch.admin(ctx, "DROP USER "+accountA); err != nil {
		t.Fatal(err)
	}
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", readerA, "", 503)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", readerA, "", 200)
}

func TestClickHouseIntegrationLostAcknowledgment(t *testing.T) {
	config := integrationClickHouseConfig(t)
	target, err := url.Parse(config.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var dropped atomic.Bool
	proxy.ModifyResponse = func(response *http.Response) error {
		if strings.HasPrefix(response.Request.URL.Query().Get("query"), "INSERT INTO") && dropped.CompareAndSwap(false, true) {
			response.Body.Close()
			return context.DeadlineExceeded
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(503) }
	host := httptest.NewServer(proxy)
	defer host.Close()
	config.URL = host.URL
	ch, err := newClickHouse(config)
	if err != nil {
		t.Fatal(err)
	}
	s := service(t, 2)
	configureAnalyticsTest(s, ch)
	ctx := context.Background()
	if err := ch.initialize(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.admin(ctx, "SYSTEM STOP MERGES "+config.Database+"."+eventTable); err != nil {
		t.Fatal(err)
	}
	a, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "lost-ack"), 200)
	if err := s.flushDelivery(ctx); err == nil {
		t.Fatal("lost acknowledgment was treated as success")
	}
	status, err := s.deliveryStatus(ctx, a)
	if err != nil || status.Pending != 1 || status.Error == "" {
		t.Fatal("uncertain delivery was not retained")
	}
	if err := s.flushDelivery(ctx); err != nil {
		t.Fatal(err)
	}
	physical, err := ch.admin(ctx, "SELECT count() FROM "+config.Database+"."+eventTable)
	if err != nil || strings.TrimSpace(string(physical)) != "2" {
		t.Fatal("test did not create physical duplicates before background merging")
	}
	result := request(t, s, "POST", "/api/v1/projects/"+a+"/query", adminToken, `{"sql":"SELECT count() AS total FROM events"}`, 200)
	if result["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "1" {
		t.Fatal("uncertain delivery double counted a logical event")
	}
	status, err = s.deliveryStatus(ctx, a)
	if err != nil || status.Pending != 0 || status.Error != "" || status.LastSuccess == "" {
		t.Fatal("successful retry did not clear delivery state")
	}
}

func TestClickHouseIntegrationOutageAndRestart(t *testing.T) {
	config := integrationClickHouseConfig(t)
	target, err := url.Parse(config.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	var available atomic.Bool
	host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !available.Load() {
			w.WriteHeader(503)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	defer host.Close()
	config.URL = host.URL
	path := filepath.Join(t.TempDir(), "queue.db")
	open := func() *Service {
		t.Helper()
		s, err := Open(Config{DBPath: path, BootstrapToken: adminToken, QueueMaxEvents: 2, ClickHouse: &config})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { s.Close() })
		return s
	}
	s := open()
	a, ingest := project(t, s)
	instance := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if instance["status"] != "degraded" || instance["checks"].(map[string]any)["clickhouse"] != "unavailable" {
		t.Fatal("instance status did not detect the ClickHouse outage")
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "outage-a"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "outage-b"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "outage-c"), 503)
	status, err := s.deliveryStatus(context.Background(), a)
	if err != nil || status.Pending != 2 {
		t.Fatal("outage did not preserve the bounded durable queue")
	}
	available.Store(true)
	deadline := time.Now().Add(8 * time.Second)
	for {
		status, err = s.deliveryStatus(context.Background(), a)
		if err != nil {
			t.Fatal(err)
		}
		if status.Pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delivery did not recover after transport became available")
		}
		time.Sleep(20 * time.Millisecond)
	}
	instance = request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if instance["status"] != "ok" || instance["checks"].(map[string]any)["clickhouse"] != "ok" {
		t.Fatal("instance status did not recover with ClickHouse")
	}
	_, account, password, err := s.analyticsReader(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	_, nextAccount, nextPassword, err := s.analyticsReader(context.Background(), a)
	if err != nil || account != nextAccount || password != nextPassword {
		t.Fatal("restart changed persistent project reader credentials")
	}
	retry := request(t, s, "POST", "/capture/", "", captureBody(ingest, "outage-a"), 200)
	if retry["duplicates"] != float64(1) {
		t.Fatal("restart lost the delivery receipt")
	}
	result := request(t, s, "POST", "/api/v1/projects/"+a+"/query", adminToken, `{"sql":"SELECT count() AS total FROM events"}`, 200)
	if result["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "2" {
		t.Fatal("restart changed analytical results")
	}
}
