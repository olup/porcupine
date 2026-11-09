package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func queryInput(projectID, sql string) *AnalyticsQueryInput {
	input := &AnalyticsQueryInput{ID: projectID}
	input.Body.SQL = sql
	return input
}

func configureAnalyticsTest(s *Service, ch *clickHouse) {
	s.ch = ch
	s.queryReady = map[string]bool{}
	s.querySlots = make(chan struct{}, 2)
	s.querySecret = []byte("analytics-test-secret-32-bytes!!!")
}

func queryStatus(err error) int {
	if failure, ok := err.(*managementError); ok {
		return failure.GetStatus()
	}
	return 0
}

func TestAnalyticsProjectReaderIsolation(t *testing.T) {
	s := service(t, 20)
	projectA, ingestA := project(t, s)
	projectB, _ := project(t, s)
	var mu sync.Mutex
	var ddl []string
	var readers, databases, passwords []string
	canonical, err := validateSQL("select count() from events -- remove this comment")
	if err != nil {
		t.Fatal(err)
	}
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		user, password := r.Header.Get("X-ClickHouse-User"), r.Header.Get("X-ClickHouse-Key")
		if r.Header.Get("Authorization") != "" || password == adminToken || password == ingestA {
			t.Error("Porcupine access key forwarded to ClickHouse")
		}
		mu.Lock()
		defer mu.Unlock()
		if user == "admin_user" {
			if strings.HasPrefix(strings.TrimSpace(string(body)), "SELECT") {
				t.Error("analytics SQL executed with administrator credentials")
			}
			if password != "admin_secret" {
				t.Error("incorrect DDL credentials")
			}
			ddl = append(ddl, string(body))
			return
		}
		if string(body) != canonical {
			t.Errorf("executed raw or modified SQL: %q", body)
		}
		if !strings.HasPrefix(user, "pc_") || !strings.HasSuffix(user, "_reader") || user == password || password == "" {
			t.Error("query lacks isolated reader")
		}
		if r.URL.Query().Get("database")+"_cached_reader" != user {
			t.Error("reader is not bound to its own namespace")
		}
		readers = append(readers, user)
		databases = append(databases, r.URL.Query().Get("database"))
		passwords = append(passwords, password)
		io.WriteString(w, `{"meta":[{"name":"count()","type":"UInt64"}],"data":[{"count()":"18446744073709551615"}],"rows":1,"statistics":{"elapsed":0.001,"rows_read":1,"bytes_read":100}}`)
	})
	configureAnalyticsTest(s, ch)
	for _, projectID := range []string{projectA, projectA, projectB} {
		result, err := s.analyticsQuery(context.Background(), queryInput(projectID, "select count() from events -- remove this comment"))
		if err != nil {
			t.Fatal(err)
		}
		if string(result.Body.Result.Data[0]["count()"]) != `"18446744073709551615"` {
			t.Fatal("UInt64 precision lost")
		}
	}
	if len(ddl) != 30 {
		t.Fatalf("expected cached provisioning per project, got %d DDL calls", len(ddl))
	}
	if readers[0] != readers[1] || passwords[0] != passwords[1] || readers[0] == readers[2] || databases[0] == databases[2] || passwords[0] == passwords[2] {
		t.Fatal("project credentials were unstable or shared")
	}
	joined := strings.Join(ddl, "\n")
	if strings.Contains(joined, "default_format") {
		t.Fatal("HTTP output format must not be configured as a reader setting")
	}
	for _, required := range []string{
		"IDENTIFIED WITH sha256_hash BY", "readonly=1 READONLY", "max_execution_time=5 READONLY",
		"max_memory_usage=268435456 READONLY", "max_rows_to_read=5000000 READONLY", "max_bytes_to_read=1073741824 READONLY",
		"max_result_rows=1000 READONLY", "max_result_bytes=1048576 READONLY", "result_overflow_mode='throw' READONLY",
		"read_overflow_mode='throw' READONLY", "timeout_overflow_mode='throw' READONLY", "max_threads=2 READONLY",
		"allow_ddl=0 READONLY", "allow_introspection_functions=0 READONLY",
		"output_format_json_quote_64bit_integers=1 READONLY", "SQL SECURITY INVOKER", "CREATE ROW POLICY OR REPLACE", "_admin ON analytics." + eventTable + " USING 1 TO `admin_user`", "GRANT SELECT(event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision) ON analytics." + eventTable + " TO", "." + eventTable + " FINAL WHERE project_id=unhex('" + hex.EncodeToString([]byte(projectA)) + "')",
		"." + eventTable + " FINAL WHERE project_id=unhex('" + hex.EncodeToString([]byte(projectB)) + "')", "REVOKE ALL ON *.* FROM", "GRANT SELECT ON", "final=1 READONLY", "optimize_move_to_prewhere_if_final=0 READONLY",
	} {
		if !strings.Contains(joined, required) {
			t.Errorf("reader restriction missing %q", required)
		}
	}
	if strings.Contains(joined, passwords[0]) || strings.Contains(joined, passwords[2]) {
		t.Fatal("reader plaintext password appeared in DDL")
	}
}

func TestAnalyticsValidationSchemaAndDisabledStorage(t *testing.T) {
	s := service(t, 20)
	id, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "pending"), 200)
	schema, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: id})
	if err != nil || schema.Body.Enabled || schema.Body.Delivery.Pending != 1 || schema.Body.Tables[0].Name != "events" || schema.Body.Limits.MaxRows != 1000 {
		t.Fatalf("incorrect disabled discovery: %#v %v", schema, err)
	}
	if _, err = s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events")); queryStatus(err) != 503 {
		t.Fatal("disabled analytics must return 503")
	}
	if _, err = s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: "missing"}); queryStatus(err) != 404 {
		t.Fatal("missing project schema must return 404")
	}
	if _, err = s.analyticsQuery(context.Background(), queryInput("missing", "SELECT count() FROM events")); queryStatus(err) != 404 {
		t.Fatal("missing project query must return 404")
	}
	var requests atomic.Int64
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) }))
	for _, sql := range []string{"SELECT * FROM system.users", "SELECT * FROM events; SELECT * FROM events", "SELECT * FROM events FORMAT JSON", "SELECT * FROM events SETTINGS readonly=0", "SELECT * FROM url('https://example.com', 'CSV')", "INSERT INTO events VALUES (1)"} {
		if _, err = s.analyticsQuery(context.Background(), queryInput(id, sql)); queryStatus(err) != 400 {
			t.Errorf("SQL was not rejected: %s: %v", sql, err)
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid SQL reached ClickHouse")
	}
}

func TestAnalyticsLimitsAndSanitizedErrors(t *testing.T) {
	for _, tc := range []struct{ code, want int }{{497, 403}, {164, 403}, {241, 429}, {159, 429}, {158, 429}, {47, 400}, {62, 400}, {99999, 503}} {
		err := analyticsQueryError(&clickHouseError{Code: tc.code, Status: 500})
		if queryStatus(err) != tc.want {
			t.Errorf("code %d: %v", tc.code, err)
		}
	}
	s := service(t, 20)
	id, _ := project(t, s)
	var failDDL atomic.Bool
	failDDL.Store(true)
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if failDDL.Load() || r.Header.Get("X-ClickHouse-User") != "admin_user" {
			w.Header().Set("X-ClickHouse-Exception-Code", "497")
			w.WriteHeader(500)
			io.WriteString(w, "Code: 497. administrator password admin_secret pc_private_namespace SQL SELECT leaked")
		}
	}))
	for _, expected := range []int{503, 403} {
		_, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events"))
		if queryStatus(err) != expected {
			t.Fatalf("got %v, want status %d", err, expected)
		}
		for _, secret := range []string{"admin_secret", "pc_private", "SELECT", "497"} {
			if strings.Contains(err.Error(), secret) {
				t.Fatal("ClickHouse error details leaked")
			}
		}
		failDDL.Store(false)
	}
	for i := 0; i < 2; i++ {
		s.querySlots <- struct{}{}
	}
	if _, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events")); queryStatus(err) != 429 {
		t.Fatal("full query slots must reject immediately")
	}
	<-s.querySlots
	<-s.querySlots
}

func TestAnalyticsConcurrentQueriesBounded(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			return
		}
		entered <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, `{"meta":[],"data":[],"rows":0,"statistics":{"elapsed":0,"rows_read":0,"bytes_read":0}}`)
	}))
	finished := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events"))
			finished <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("concurrent query did not start")
		}
	}
	if _, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events")); queryStatus(err) != 429 {
		t.Fatal("third query exceeded concurrency limit")
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
}

func TestAnalyticsRejectsMalformedStorageResponse(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	var response atomic.Value
	response.Store(`{"meta":[],"data":[],"rows":1}`)
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "admin_user" {
			io.WriteString(w, response.Load().(string))
		}
	}))
	for _, value := range []string{`{"meta":[],"data":[],"rows":1}`, `{"meta":[],"rows":0}`, `not json`} {
		response.Store(value)
		if _, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events")); queryStatus(err) != 503 {
			t.Fatal("malformed result accepted")
		}
	}
	data := make([]map[string]any, 1001)
	for i := range data {
		data[i] = map[string]any{"value": i}
	}
	value, _ := json.Marshal(map[string]any{"meta": []any{}, "data": data, "rows": len(data)})
	response.Store(string(value))
	if _, err := s.analyticsQuery(context.Background(), queryInput(id, "SELECT count() FROM events")); queryStatus(err) != 503 {
		t.Fatal("over-limit result accepted")
	}
}

func TestAnalyticsTypedParameters(t *testing.T) {
	s := service(t, 20)
	id, _ := project(t, s)
	var queries atomic.Int64
	input := queryInput(id, "SELECT event FROM events WHERE event={name:String}")
	input.Body.Parameters = map[string]string{"name": "purchase & referral+雪 ' ; DROP TABLE events"}
	canonical, err := validateSQL(input.Body.SQL)
	if err != nil {
		t.Fatal(err)
	}
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			return
		}
		queries.Add(1)
		body, _ := io.ReadAll(r.Body)
		if string(body) != canonical || strings.Contains(string(body), input.Body.Parameters["name"]) {
			t.Error("parameter value interpolated into SQL")
		}
		if r.URL.Query().Get("param_name") != input.Body.Parameters["name"] {
			t.Error("typed parameter was not forwarded as a ClickHouse parameter")
		}
		io.WriteString(w, `{"meta":[],"data":[],"rows":0,"statistics":{"elapsed":0,"rows_read":0,"bytes_read":0}}`)
	}))
	if _, err = s.analyticsQuery(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if queries.Load() != 1 {
		t.Fatal("parameterized query did not execute")
	}
	for _, parameters := range []map[string]string{
		{}, {"other": "missing"}, {"name": "ok", "extra": "unreferenced"}, {"name": strings.Repeat("x", 4097)}, {"bad-name": "unsafe"},
	} {
		input.Body.Parameters = parameters
		if _, err = s.analyticsQuery(context.Background(), input); queryStatus(err) != 400 {
			t.Errorf("invalid parameters accepted: %v", err)
		}
	}
	input = queryInput(id, "SELECT {table:Identifier} FROM events")
	input.Body.Parameters = map[string]string{"table": "project_id"}
	if _, err = s.analyticsQuery(context.Background(), input); queryStatus(err) != 400 {
		t.Fatal("identifier parameter accepted")
	}
	tooMany := map[string]string{}
	for i := 0; i < 33; i++ {
		tooMany[strings.Repeat("x", i+1)] = "value"
	}
	if queryStatus(validateQueryParameters(tooMany)) != 400 {
		t.Fatal("too many parameters accepted")
	}
	if queries.Load() != 1 {
		t.Fatal("invalid parameters reached ClickHouse")
	}
}
