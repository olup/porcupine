package porcupine

import (
	"context"
	"encoding/json"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/errtrack"
	"github.com/olup/porcupine/internal/event"
)

func TestErrorGroupsDuckLake(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	s.clock = func() time.Time { return time.Date(2026, 11, 2, 3, 0, 0, 0, time.UTC) }
	a, public := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	_, foreign := key(t, s, []string{"events:read"}, []string{b})
	_, manager := key(t, s, []string{"management:read"}, []string{a})
	groupProperties := func(kind, message, build string) errtrack.Properties {
		return errtrack.Normalize(map[string]any{
			"$exception_build_id": build,
			"$exception_release":  "2026.11",
			"$exception_list": []any{map[string]any{
				"type": kind, "value": message,
				"stacktrace": map[string]any{"frames": []any{map[string]any{"filename": "app.js", "lineno": 1}}},
			}},
		})
	}
	fail := groupProperties("TypeError", "Cannot fetch", "build-a")
	newMessage := groupProperties("TypeError", "Failed again token=secret-xyz", "build-a")
	other := groupProperties("ReferenceError", "Missing %_ widget", "")
	peer := groupProperties("PeerError", "Another project", "")
	degraded := errtrack.Normalize(nil)
	corrupt := strings.ReplaceAll(mustErrorGroupJSON(t, fail), fail.Fingerprint, "bad")
	raw := func(id int64, project, name, at string, properties any) event.Event {
		return event.Event{ID: id, UUID: "error-" + strconv.FormatInt(id, 10), ProjectID: project, DistinctID: "tester", Event: name,
			Properties: json.RawMessage(mustErrorGroupJSON(t, properties)), Timestamp: at, ReceivedAt: "2026-11-02T03:00:00Z"}
	}
	const at = "2026-11-01T23:15:00.123456002Z"
	const newer = "2026-11-02T01:20:00.123456002Z"
	const end = "2026-11-02T02:00:00.123456002Z"
	events := []event.Event{
		raw(1, a, "$exception", at, fail),
		raw(2, a, "$exception", "2026-11-01T23:15:00.123456001Z", fail),
		raw(3, a, "$exception", newer, newMessage),
		raw(9007199254740993, a, "$exception", newer, newMessage),
		raw(5, a, "$exception", "2026-11-02T02:00:00.123456001Z", other),
		raw(6, a, "$exception", end, fail),
		raw(7, a, "ordinary", newer, fail),
		raw(8, a, "$exception", newer, map[string]any{"$exception_list": []any{}, "$exception_fingerprint": []string{fail.Fingerprint}}),
		raw(9, a, "$exception", newer, json.RawMessage(corrupt)),
		raw(10, a, "$exception", newer, map[string]any{"$exception_list": []any{}}),
		raw(11, a, "$exception", newer, map[string]any{"$exception_list": []any{nil}, "$exception_fingerprint": fail.Fingerprint}),
		raw(12, a, "$exception", newer, map[string]any{"$exception_list": []any{}, "$exception_fingerprint": fail.Fingerprint}),
		raw(1, b, "$exception", newer, fail),
		raw(2, b, "$exception", newer, peer),
		raw(3, b, "$exception", newer, degraded),
	}
	if err := s.lake.Apply(context.Background(), duckLakeJourneyProjections(t, events)); err != nil {
		t.Fatal(err)
	}
	path := func(project string) string { return "/api/v1/projects/" + project + "/error-groups" }
	window := "?from=" + url.QueryEscape("2026-11-02T00:15:00.123456002+01:00") + "&to=" + end
	list := func(params string) map[string]any {
		t.Helper()
		return request(t, s, "GET", path(a)+window+params, reader, "", 200)
	}
	fingerprints := func(result map[string]any) []string {
		t.Helper()
		var ids []string
		for _, v := range result["groups"].([]any) {
			ids = append(ids, v.(map[string]any)["fingerprint"].(string))
		}
		return ids
	}
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{"", []string{other.Fingerprint, fail.Fingerprint}},
		{"&sort=count", []string{fail.Fingerprint, other.Fingerprint}},
		{"&limit=1&offset=1", []string{fail.Fingerprint}},
		{"&q=CANNOT%20FETCH", []string{fail.Fingerprint}},
		{"&q=TYPEERROR", []string{fail.Fingerprint}},
		{"&q=%25_", []string{other.Fingerprint}},
		{"&q=secret-xyz", nil},
		{"&q='OR%201=1--", nil},
		{"&q=PeerError", nil},
	} {
		if got := fingerprints(list(tc.query)); !slices.Equal(got, tc.want) {
			t.Fatalf("list %q: got %v, want %v", tc.query, got, tc.want)
		}
	}
	if next := list("&limit=1")["next_offset"]; next != float64(1) {
		t.Fatalf("first page next offset = %v", next)
	}
	if next := list("&limit=1&offset=1")["next_offset"]; next != nil {
		t.Fatalf("last page next offset = %v", next)
	}
	detail := func(params string) map[string]any {
		t.Helper()
		return request(t, s, "GET", path(a)+"/"+fail.Fingerprint+window+params, reader, "", 200)
	}
	group := detail("&limit=1")
	if group["from"] != at || group["to"] != end || group["bucket_seconds"] != float64(3600) || group["next_offset"] != float64(1) {
		t.Fatalf("unexpected detail metadata: %v", group)
	}
	summary := group["group"].(map[string]any)
	if summary["count"] != "3" || summary["first_seen"] != at || summary["last_seen"] != newer ||
		summary["latest_event_id"] != "9007199254740993" || summary["type"] != "TypeError" || summary["message"] != "Failed again [redacted]" {
		t.Fatalf("unexpected summary: %v", summary)
	}
	occurrence := group["occurrences"].([]any)[0].(map[string]any)
	if occurrence["event_id"] != "9007199254740993" || occurrence["occurred_at"] != newer ||
		occurrence["build_id"] != "build-a" || occurrence["release"] != "2026.11" || occurrence["degraded"] != nil {
		t.Fatalf("unexpected occurrence: %v", occurrence)
	}
	if actual := detail("&limit=1&offset=1")["occurrences"].([]any)[0].(map[string]any)["event_id"]; actual != "3" {
		t.Fatalf("tied event page selected %v", actual)
	}
	if actual := detail("&offset=10000")["occurrences"].([]any); len(actual) != 0 {
		t.Fatalf("offset past end returned %v", actual)
	}
	if got := mustErrorGroupJSON(t, group["distribution"]); got != `[{"count":"1","start":"2026-11-01T23:00:00Z"},{"count":"0","start":"2026-11-02T00:00:00Z"},{"count":"2","start":"2026-11-02T01:00:00Z"},{"count":"0","start":"2026-11-02T02:00:00Z"}]` {
		t.Fatalf("unexpected hourly distribution: %s", got)
	}
	daily := request(t, s, "GET", path(a)+"/"+fail.Fingerprint+"?from=2026-10-30T23:30:00Z&to=2026-11-03T00:00:00Z", reader, "", 200)
	if daily["bucket_seconds"] != float64(86400) {
		t.Fatalf("daily bucket seconds: %v", daily)
	}
	if got := mustErrorGroupJSON(t, daily["distribution"]); got != `[{"count":"0","start":"2026-10-30T00:00:00Z"},{"count":"0","start":"2026-10-31T00:00:00Z"},{"count":"2","start":"2026-11-01T00:00:00Z"},{"count":"3","start":"2026-11-02T00:00:00Z"}]` {
		t.Fatalf("unexpected daily distribution: %s", got)
	}
	defaultList := request(t, s, "GET", path(a), reader, "", 200)
	if defaultList["from"] != "2026-10-26T03:00:00Z" || defaultList["to"] != "2026-11-02T03:00:00Z" {
		t.Fatalf("unexpected default window: %v", defaultList)
	}
	for _, query := range []string{"?from=2026-11-01T00:00:00Z", "?to=2026-11-01T00:00:00Z",
		"?from=2026-11-01T00:00:00Z&to=2026-11-01T00:00:00Z", "?from=2026-09-01T00:00:00Z&to=2026-11-01T00:00:00Z"} {
		request(t, s, "GET", path(a)+query, reader, "", 400)
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?offset=-1", "?offset=10001", "?sort=other", "?q=" + strings.Repeat("x", 129)} {
		request(t, s, "GET", path(a)+query, reader, "", 422)
	}
	request(t, s, "GET", path(a), public, "", 401)
	request(t, s, "GET", path(a), manager, "", 403)
	request(t, s, "GET", path(b), reader, "", 403)
	request(t, s, "GET", path(a)+"/"+fail.Fingerprint, foreign, "", 403)
	request(t, s, "GET", path(a)+"/"+peer.Fingerprint, reader, "", 404)
	request(t, s, "GET", path(a)+"/"+fail.Fingerprint+"?from=2026-11-03T00:00:00Z&to=2026-11-04T00:00:00Z", reader, "", 404)
	request(t, s, "GET", path(a)+"/"+strings.ToUpper(fail.Fingerprint), reader, "", 400)
	degradedGroup := request(t, s, "GET", path(b)+"/"+degraded.Fingerprint+window, foreign, "", 200)
	if occurrence := degradedGroup["occurrences"].([]any)[0].(map[string]any); occurrence["degraded"] != true {
		t.Fatalf("degraded metadata missing: %v", occurrence)
	}

	setBudgets := func(concurrent, rows, bytes int) {
		t.Helper()
		if _, err := s.control.Exec(`UPDATE tenants SET hot_query_max_concurrent=?,hot_query_max_rows=?,hot_query_max_result_bytes=? WHERE id=?`, concurrent, rows, bytes, defaultTenantID); err != nil {
			t.Fatal(err)
		}
	}
	setBudgets(2, 1, 1<<20)
	request(t, s, "GET", path(a)+window, reader, "", 429)
	setBudgets(2, 1000, 256)
	request(t, s, "GET", path(a)+"/"+fail.Fingerprint+window, reader, "", 429)
	setBudgets(1, 1000, 1<<20)
	_, _, release, err := s.holdQuery(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	request(t, s, "GET", path(a)+window, reader, "", 429)
	request(t, s, "GET", path(a)+"/"+fail.Fingerprint+window, reader, "", 429)
}

func mustErrorGroupJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
