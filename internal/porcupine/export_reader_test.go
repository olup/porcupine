package porcupine

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestExportReaderProjectScopeAndBudgets(t *testing.T) {
	s := service(t, 20)
	projectA, _ := project(t, s)
	projectB, _ := project(t, s)
	var mutex sync.Mutex
	var ddl []string
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-ClickHouse-User") != "admin_user" {
			t.Error("reader provisioning did not use operator")
		}
		mutex.Lock()
		ddl = append(ddl, string(body))
		mutex.Unlock()
	}))
	s.exports = &exportRuntime{config: ExportConfig{MemoryBytes: 512 << 20, MaxExecutionSeconds: 86400, MaxReadBytes: 1 << 40, TemporaryBytes: 1 << 30, BandwidthBytesPerSecond: 5 << 20}}
	namespace, reader, password, err := s.exportReader(context.Background(), projectA)
	if err != nil {
		t.Fatal(err)
	}
	_, interactiveReader, interactivePassword, err := s.analyticsReader(context.Background(), projectA)
	if err != nil {
		t.Fatal(err)
	}
	if reader == interactiveReader || password == interactivePassword || reader != namespace+"_export" {
		t.Fatal("export reused interactive credentials")
	}
	namespace2, reader2, password2, err := s.exportReader(context.Background(), projectB)
	if err != nil {
		t.Fatal(err)
	}
	if namespace == namespace2 || reader == reader2 || password == password2 {
		t.Fatal("projects share export credentials")
	}
	namespaceAgain, readerAgain, passwordAgain, err := s.exportReader(context.Background(), projectA)
	if err != nil || namespaceAgain != namespace || readerAgain != reader || passwordAgain != password {
		t.Fatal("export credentials not stable")
	}
	mutex.Lock()
	statements := append([]string(nil), ddl...)
	mutex.Unlock()
	var profile string
	var policy string
	for _, sql := range statements {
		if strings.HasPrefix(sql, "ALTER USER "+reader+" ") {
			profile = sql
		}
		if strings.Contains(sql, namespace+"_export_scope") {
			policy = sql
		}
		if strings.Contains(sql, password) || strings.Contains(sql, password2) {
			t.Fatal("plaintext export password appeared in DDL")
		}
	}
	for _, required := range []string{
		"readonly=1 READONLY", "DEFAULT ROLE NONE", "allow_ddl=0 READONLY", "allow_introspection_functions=0 READONLY",
		"final=1 READONLY", "optimize_move_to_prewhere_if_final=0 READONLY", "max_threads=1 READONLY", "session_timezone='UTC' READONLY", "use_query_cache=0 READONLY",
		"max_memory_usage=536870912 READONLY", "max_execution_time=86400 READONLY", "max_bytes_to_read=1099511627776 READONLY",
		"max_network_bandwidth=5242880 READONLY", "max_remote_read_network_bandwidth=5242880 READONLY",
		"max_temporary_data_on_disk_size_for_query=1073741824 READONLY", "max_bytes_before_external_group_by=134217728 READONLY", "max_bytes_before_external_sort=134217728 READONLY",
		"max_result_rows=0 READONLY", "max_result_bytes=0 READONLY", "max_rows_to_read=0 READONLY", "cancel_http_readonly_queries_on_client_close=1 READONLY",
	} {
		if !strings.Contains(profile, required) {
			t.Errorf("export reader constraint missing %s", required)
		}
	}
	if strings.Contains(profile, "default_format") || strings.Contains(profile, "max_result_rows=1000") || strings.Contains(profile, "max_result_bytes=1048576") {
		t.Fatal("interactive format/caps leaked into export reader")
	}
	if !strings.Contains(policy, "project_id=unhex('"+hex.EncodeToString([]byte(projectA))+"') TO "+reader) {
		t.Fatal("export row policy does not bind project to export reader")
	}
}

func TestOpenExportStreamingTransport(t *testing.T) {
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") != "project_export" || r.Header.Get("X-ClickHouse-Key") != "project_export_secret" || r.Header.Get("Authorization") != "" {
			t.Error("stream query used incorrect credential boundary")
		}
		q := r.URL.Query()
		if q.Get("default_format") != "JSONCompact" || q.Get("wait_end_of_query") != "0" || q.Get("database") != "project_database" || q.Get("query_id") != "export_attempt-1" || q.Get("param_value") != "snow + 雪 & quoted '" {
			t.Error("stream parameters are incorrect")
		}
		if q.Get("cancel_http_readonly_queries_on_client_close") != "" || strings.Contains(r.URL.RawQuery, "secret") {
			t.Error("stream transport bypassed profile or leaked credential")
		}
		body, _ := io.ReadAll(r.Body)
		if string(body) != "SELECT {value:String}" {
			t.Error("stream changed canonical SQL")
		}
		time.Sleep(20 * time.Millisecond)
		io.WriteString(w, `{"meta":[],"data":[],"rows":0}`)
	})
	c.client.Timeout = time.Millisecond
	original := c.client.Transport.(*http.Transport)
	original.ResponseHeaderTimeout = time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	body, err := c.openExport(ctx, "project_database", "project_export", "project_export_secret", "export_attempt-1", "SELECT {value:String}", map[string]string{"value": "snow + 雪 & quoted '"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := streamExportJSON(ctx, body, 4096, exportStreamCallbacks{}); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	if c.client.Timeout != time.Millisecond || original.ResponseHeaderTimeout != time.Millisecond {
		t.Fatal("export changed interactive transport deadlines")
	}
}

func TestOpenExportRejectsHTTPAndExceptionFailures(t *testing.T) {
	for _, test := range []struct {
		status, code int
		body         string
	}{
		{500, 241, "private SQL secret"}, {200, 47, "private SQL secret"}, {200, 0, "Code: 159. private SQL secret"}, {200, 0, ""}, {302, 0, "redirect"},
	} {
		c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
			if test.code != 0 {
				w.Header().Set("X-ClickHouse-Exception-Code", strconv.Itoa(test.code))
			}
			w.WriteHeader(test.status)
			io.WriteString(w, test.body)
		})
		body, err := c.openExport(context.Background(), "project_database", "project_export", "password", "attempt-1", "SELECT 1", nil)
		if body != nil {
			body.Close()
		}
		if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "secret") {
			t.Fatalf("unsafe response accepted or reflected: %v", err)
		}
	}
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"meta":[],"data":[],"rows":0}Code: 241. private SQL`)
	})
	body, err := c.openExport(context.Background(), "project_database", "project_export", "password", "attempt-1", "SELECT 1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	if _, err := streamExportJSON(context.Background(), body, 4096, exportStreamCallbacks{}); err == nil {
		t.Fatal("late HTTP200 exception became success")
	}
}

func TestCancelExportUsesExactOperatorQueryID(t *testing.T) {
	calls := 0
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		expected := "KILL QUERY WHERE query_id=unhex('" + hex.EncodeToString([]byte("attempt-1")) + "') SYNC"
		if string(body) != expected || r.Header.Get("X-ClickHouse-User") != "admin_user" {
			t.Error("cancel did not target exact server query with operator credential")
		}
	})
	if err := c.cancelExport(context.Background(), "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if err := c.cancelExport(context.Background(), "attempt-1' OR 1"); err == nil {
		t.Fatal("unsafe query ID accepted")
	}
	if calls != 1 {
		t.Fatal("invalid query ID reached ClickHouse")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.openExport(ctx, "project_database", "project_export", "password", "attempt-1", "SELECT 1", nil); err == nil {
		t.Fatal("canceled context opened export")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("context changed")
	}
}
