package porcupine

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAnalyticsCacheModesAndMutationRace(t *testing.T) {
	s := service(t, 20)
	a, ingest := project(t, s)
	b, _ := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "edited"), 200)
	var users, tags []string
	var race atomic.Bool
	ch := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			return
		}
		users = append(users, r.Header.Get("X-ClickHouse-User"))
		tags = append(tags, r.URL.Query().Get("query_cache_tag"))
		if race.Swap(false) {
			if _, err := s.editEvent(mutationContext("actor"), mutationEdit(a, "edited", "racing", "0")); err != nil {
				t.Error(err)
			}
		}
		io.WriteString(w, `{"meta":[],"data":[],"rows":0}`)
	})
	configureAnalyticsTest(s, ch)
	for _, mode := range []*bool{nil, new(true), new(false)} {
		input := queryInput(a, "SELECT event FROM events")
		input.Body.UseCache = mode
		if _, err := s.analyticsQuery(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	if users[0] != users[1] || !strings.HasSuffix(users[0], "_cached_reader") || strings.Contains(users[2], "_cached") || tags[0] == "" || tags[0] != tags[1] || tags[2] != "" {
		t.Fatal("omitted/true must cache; false must use the immutable fresh reader")
	}
	race.Store(true)
	if _, err := s.analyticsQuery(context.Background(), queryInput(a, "SELECT event FROM events")); err != nil {
		t.Fatal(err)
	}
	if len(users) != 5 || !strings.HasSuffix(users[3], "_cached_reader") || strings.Contains(users[4], "_cached") {
		t.Fatal("mutation accepted during cache read did not cause one fresh retry")
	}
	if _, err := s.analyticsQuery(context.Background(), queryInput(a, "SELECT event FROM events")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(users[5], "_cached") {
		t.Fatal("pending mutation allowed cache reads or writes")
	}
	if _, err := s.deliverMutations(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.analyticsQuery(context.Background(), queryInput(a, "SELECT event FROM events")); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(users[6], "_cached_reader") || tags[6] == tags[0] {
		t.Fatal("project did not resume caching under its applied generation")
	}
	if _, err := s.analyticsQuery(context.Background(), queryInput(b, "SELECT event FROM events")); err != nil {
		t.Fatal(err)
	}
	if users[7] == users[6] || tags[7] != tags[0] {
		t.Fatal("project mutation generation or credentials crossed project boundary")
	}
}

func TestAnalyticsCacheAPIAndMCPAndRevocation(t *testing.T) {
	s := service(t, 20)
	a, _ := project(t, s)
	b, _ := project(t, s)
	id, read := key(t, s, []string{"events:read"}, []string{a})
	var cached, fresh atomic.Int64
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		user := r.Header.Get("X-ClickHouse-User")
		if user == "admin_user" {
			return
		}
		if strings.HasSuffix(user, "_cached_reader") {
			cached.Add(1)
		} else {
			fresh.Add(1)
		}
		if r.URL.Query().Get("param_event") != "purchase" {
			t.Error("typed parameter lost")
		}
		io.WriteString(w, `{"meta":[],"data":[],"rows":0}`)
	}))
	for _, extra := range []string{"", `,"use_cache":true`, `,"use_cache":false`} {
		body := `{"sql":"SELECT event FROM events WHERE event={event:String}","parameters":{"event":"purchase"}` + extra + `}`
		request(t, s, "POST", "/api/v1/projects/"+a+"/query", read, body, 200)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, read, "")
	for _, value := range []bool{true, false} {
		mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": a}, Body: map[string]any{"sql": "SELECT event FROM events WHERE event={event:String}", "parameters": map[string]string{"event": "purchase"}, "use_cache": value}}, 200)
	}
	request(t, s, "POST", "/api/v1/projects/"+b+"/query", read, `{"sql":"SELECT event FROM events"}`, 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+id, adminToken, "", 204)
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", read, `{"sql":"SELECT event FROM events"}`, 401)
	if cached.Load() != 3 || fresh.Load() != 2 {
		t.Fatal("API/MCP cache modes differ or unauthorized request reached storage")
	}
	request(t, s, "POST", "/api/v1/projects/"+a+"/query", adminToken, `{"sql":"SELECT event FROM events","use_cache":"false"}`, 422)
}

func TestAnalyticsCacheProfileBounds(t *testing.T) {
	s := service(t, 20)
	projectID, _ := project(t, s)
	var ddl []string
	configureAnalyticsTest(s, testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-ClickHouse-User") == "admin_user" {
			ddl = append(ddl, string(body))
			return
		}
		io.WriteString(w, `{"meta":[],"data":[],"rows":0}`)
	}))
	if _, err := s.analyticsQuery(context.Background(), queryInput(projectID, "SELECT event FROM events")); err != nil {
		t.Fatal(err)
	}
	profiles := 0
	for _, sql := range ddl {
		if !strings.HasPrefix(sql, "ALTER USER ") {
			continue
		}
		profiles++
		for _, required := range []string{"readonly=1 READONLY", "query_cache_ttl=60 READONLY", "query_cache_max_entries=64 READONLY", "query_cache_max_size_in_bytes=8388608 READONLY", "query_cache_share_between_users=0 READONLY", "query_cache_nondeterministic_function_handling='ignore' READONLY"} {
			if !strings.Contains(sql, required) {
				t.Errorf("missing cache restriction %s", required)
			}
		}
		bit := "0"
		if strings.Contains(sql, "_cached_reader ") {
			bit = "1"
		}
		for _, name := range []string{"use_query_cache", "enable_reads_from_query_cache", "enable_writes_to_query_cache"} {
			if !strings.Contains(sql, name+"="+bit+" READONLY") {
				t.Fatal("cache direction not fixed by reader profile")
			}
		}
	}
	if profiles != 2 {
		t.Fatal("both existing fresh and new cached reader must be provisioned")
	}
	encoded, _ := json.Marshal(AnalyticsQueryInput{})
	if strings.Contains(string(encoded), "use_cache") {
		t.Fatal("optional boolean default was collapsed")
	}
}
