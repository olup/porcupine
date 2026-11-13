//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func agentAnalyticsFixture(t *testing.T) (*Service, string, string, string, string) {
	t.Helper()
	s := duckLakeJourneyService(t)
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	readerID, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	fixture := []struct {
		id, person, name, timestamp, session, language string
		duration                                       int
	}{
		{"a-page", "a", "page_viewed", "2026-11-01T23:30:00Z", "a-1", "fr", 0},
		{"a-signup", "a", "signup", "2026-11-01T23:31:00Z", "a-1", "fr", 0},
		{"a-purchase", "a", "purchase", "2026-11-01T23:32:00Z", "a-1", "fr", 10},
		{"a-repeat", "a", "purchase", "2026-11-01T23:33:00Z", "a-1", "fr", 50},
		{"a-return", "a", "page_viewed", "2026-11-02T23:40:00Z", "a-2", "fr", 0},
		{"b-page", "b", "page_viewed", "2026-11-01T10:00:00Z", "b-1", "en", 0},
		{"b-signup", "b", "signup", "2026-11-01T10:01:00Z", "b-1", "en", 0},
		{"c-page", "c", "page_viewed", "2026-11-01T12:00:00Z", "c-1", "fr", 0},
		{"c-purchase", "c", "purchase", "2026-11-01T12:02:00Z", "c-1", "fr", 30},
		{"c-repeat", "c", "purchase", "2026-11-01T12:04:00Z", "c-1", "fr", 90},
	}
	batch := []map[string]any{}
	for _, row := range fixture {
		props := map[string]any{"language": row.language, "duration_ms": row.duration, "threadId": row.person, "threadTurnId": row.id, "": "private property value"}
		for i := 0; i < 220; i++ {
			props[fmt.Sprintf("$sdk_%03d", i)] = "do-not-return"
		}
		batch = append(batch, map[string]any{"uuid": row.id, "distinct_id": row.person, "event": row.name, "timestamp": row.timestamp, "properties": props})
		batch[len(batch)-1]["properties"].(map[string]any)["$session_id"] = row.session
	}
	identityCapture(t, s, ingest, batch, 200)
	identityCapture(t, s, otherIngest, []map[string]any{{"uuid": "foreign", "distinct_id": "foreign-person", "event": "page_viewed", "properties": map[string]any{"foreign_only": "foreign secret value"}}}, 200)
	duckLakeJourneyDrain(t, s)
	return s, a, b, readerID, reader
}

func TestDuckLakeAgentDiscoveryRecoveryAndRecipes(t *testing.T) {
	s, a, b, readerID, reader := agentAnalyticsFixture(t)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "2025-11-25")
	instructions := session.InitializeResult().Instructions
	for _, want := range []string{"property_cursor", "code, hint and field", "duckdb-recipes.md"} {
		if !strings.Contains(instructions, want) {
			t.Fatalf("missing agent instructions: %s", want)
		}
	}
	if strings.Contains(instructions, "Identity mutation is not implemented") {
		t.Fatal("stale identity description")
	}
	tools, err := session.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 2 {
		t.Fatalf("expected two progressive-discovery tools: %v %v", tools, err)
	}
	for _, id := range []string{"list-projects", "get-event-schema", "get-event-catalog", "query-events"} {
		docs := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: id}, false)
		if docs["contract"] == nil {
			t.Fatalf("missing operation contract: %s", id)
		}
	}
	schema := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-schema", Path: map[string]string{"id": a}}, 200)
	if schema["sql"].(map[string]any)["dialect"] != "duckdb" {
		t.Fatal("agent cannot discover dialect")
	}
	if schema["delivery"].(map[string]any)["pending"] != float64(0) {
		t.Fatal("fixture not delivered")
	}

	path := map[string]string{"id": a}
	first := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-catalog", Path: path, Query: map[string]string{"event": "page_viewed", "property_kind": "custom", "property_prefix": "thread", "property_limit": "1"}}, 200)
	properties := first["properties"].([]any)
	if len(properties) != 1 || properties[0].(map[string]any)["key"] != "threadId" || properties[0].(map[string]any)["sample_occurrences"] != "4" {
		t.Fatalf("SDK keys masked custom discovery: %v", first)
	}
	cursor, ok := first["next_property_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatal("missing property continuation")
	}
	second := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-catalog", Path: path, Query: map[string]string{"event": "page_viewed", "property_kind": "custom", "property_prefix": "thread", "property_limit": "1", "property_cursor": cursor}}, 200)
	if second["properties"].([]any)[0].(map[string]any)["key"] != "threadTurnId" || second["next_property_cursor"] != nil {
		t.Fatalf("property pagination failed: %v", second)
	}
	encoded, _ := json.Marshal(first)
	if strings.Contains(string(encoded), "do-not-return") || strings.Contains(string(encoded), "foreign") {
		t.Fatal("catalog leaked values or foreign data")
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-catalog", Path: map[string]string{"id": b}, Query: map[string]string{"event": "page_viewed", "property_cursor": cursor}}, 403)
	changed := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-catalog", Path: path, Query: map[string]string{"event": "signup", "property_cursor": cursor}}, 400)
	if changed["code"] != "invalid_property_cursor" {
		t.Fatalf("cursor context was not checked: %v", changed)
	}

	for _, tc := range []struct {
		sql, code, field string
		parameters       map[string]any
	}{
		{"SELECT made_up_column FROM events LIMIT 1", "unknown_column", "made_up_column", nil},
		{"SELECT count(*) FROM events WHERE event=$event", "missing_parameter", "event", nil},
		{"SELECT count(*) FROM events", "unused_parameter", "event", map[string]any{"event": "private parameter value"}},
		{"SELECT toStartOfHour(event_time) FROM events", "unsupported_function", "tostartofhour", nil},
		{"SELECT sum(event_time) FROM events", "type_mismatch", "", nil},
		{`SELECT sum('Referenced column "sensitive" not found') FROM events`, "type_mismatch", "", nil},
		{"SELECT CAST('private constant value' AS BIGINT) FROM events", "invalid_value", "", nil},
		{"SELECT FROM", "invalid_sql", "", nil},
		{"SELECT original_snapshot FROM events", "unsupported_sql", "", nil},
		{"SELECT main.count(*) FROM events", "unsupported_sql", "", nil},
		{"SELECT read_blob('/private/secret') FROM events", "unsupported_function", "read_blob", nil},
	} {
		t.Run(tc.code+tc.field, func(t *testing.T) {
			body := map[string]any{"sql": tc.sql, "parameters": tc.parameters}
			if tc.parameters == nil {
				delete(body, "parameters")
			}
			failure := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: path, Body: body}, 400)
			if failure["code"] != tc.code || failure["hint"] == "" || tc.field != "" && failure["field"] != tc.field {
				t.Fatalf("unhelpful query failure: %v", failure)
			}
			encoded, _ := json.Marshal(failure)
			for _, secret := range []string{"private constant value", "private parameter value", "/private/secret", "Candidate bindings", "current_events"} {
				if strings.Contains(string(encoded), secret) {
					t.Fatalf("raw query failure leaked %s", secret)
				}
			}
		})
	}
	repaired := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: path, Body: map[string]any{"sql": "SELECT count(*) AS total FROM events WHERE event=$event", "parameters": map[string]any{"event": "page_viewed"}}}, 200)
	if repaired["result"].(map[string]any)["data"].([]any)[0].(map[string]any)["total"] != "4" {
		t.Fatal("agent could not recover using error hint")
	}
	limited := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: path, Body: map[string]any{"sql": "SELECT a.event_id FROM events a CROSS JOIN events b CROSS JOIN events c CROSS JOIN events d LIMIT 1001"}}, 429)
	if limited["code"] != "result_row_limit" || !strings.Contains(limited["hint"].(string), "create-event-export") {
		t.Fatalf("missing export recovery: %v", limited)
	}

	doc, err := os.ReadFile("../../docs/duckdb-recipes.md")
	if err != nil {
		t.Fatal(err)
	}
	blocks := regexp.MustCompile("(?s)```sql\\n(.*?)\\n```").FindAllStringSubmatch(string(doc), -1)
	want := map[string]string{
		"daily-volume":       `[{"day":"2026-11-01","people":"3","total":"3"},{"day":"2026-11-02","people":"1","total":"1"}]`,
		"property-breakdown": `[{"language":"en","total":"1"},{"language":"fr","total":"3"}]`,
		"sessions":           `[{"sessions":"4"}]`,
		"latency":            `[{"samples":"4","missing_or_invalid":"0","mean_ms":45,"p95_ms":90}]`,
		"ordered-funnel":     `[{"entered":"3","signed_up":"2","converted":"1"}]`,
		"retention":          `[{"cohort_day":"2026-11-01","day_number":"0","people":"3"},{"cohort_day":"2026-11-01","day_number":"1","people":"1"}]`,
		"local-days":         `[{"day":"2026-11-01","total":"2"},{"day":"2026-11-02","total":"1"},{"day":"2026-11-03","total":"1"}]`,
		"moving-average":     `[{"day":"2026-11-01","total":"3","moving_average":3},{"day":"2026-11-02","total":"1","moving_average":2}]`,
		"property-coverage":  `[{"total":"4","missing":"0","distinct_values":"2"}]`,
	}
	if len(blocks) != len(want) {
		t.Fatalf("recipe count changed: %d", len(blocks))
	}
	for _, block := range blocks {
		lines := strings.SplitN(block[1], "\n", 3)
		if len(lines) != 3 {
			t.Fatal("missing recipe metadata")
		}
		name := strings.TrimPrefix(lines[0], "-- recipe: ")
		t.Run("recipe-"+name, func(t *testing.T) {
			var parameters map[string]any
			if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "-- parameters: ")), &parameters); err != nil {
				t.Fatal(err)
			}
			result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: path, Body: map[string]any{"sql": lines[2], "parameters": parameters}}, 200)
			var expected any
			if err := json.Unmarshal([]byte(want[name]), &expected); err != nil {
				t.Fatal(err)
			}
			if actual := result["result"].(map[string]any)["data"]; !reflect.DeepEqual(actual, expected) {
				t.Fatalf("recipe output differs: got %v want %v", actual, expected)
			}
		})
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "query-events", Path: map[string]string{"id": b}, Body: map[string]any{"sql": "SELECT count(*) FROM events"}}, 403)
	request(t, s, "DELETE", "/api/v1/access-keys/"+readerID, adminToken, "", 204)
	request(t, s, "GET", "/api/v1/projects/"+a+"/catalog", reader, "", 401)
}

func TestDuckLakeCatalogEmptyKeyCursorAndProjectBinding(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 2)
	p := lakeStorageProjection(t, "a", "one", 1)
	p.Properties = json.RawMessage(`{"":"secret","a":1,"b":true}`)
	if err := s.Apply(context.Background(), []eventProjection{p}); err != nil {
		t.Fatal(err)
	}
	input := EventCatalogInput{ID: "a", Event: p.Event.Event, PropertyLimit: 1}
	first, err := s.Catalog(context.Background(), input)
	if err != nil || len(first.Properties) != 1 || first.Properties[0].Key != "" || first.NextPropertyCursor == "" {
		t.Fatalf("empty-key cursor: %v %v", first, err)
	}
	input.PropertyCursor = first.NextPropertyCursor
	next, err := s.Catalog(context.Background(), input)
	if err != nil || next.Properties[0].Key != "a" {
		t.Fatalf("cursor did not advance past empty key: %v %v", next, err)
	}
	input.ID = "b"
	if _, err = s.Catalog(context.Background(), input); queryStatus(err) != 400 {
		t.Fatalf("cross-project cursor accepted: %v", err)
	}
}

func TestDuckLakeParameterDependentScalarMetadata(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 1)
	p := lakeStorageProjection(t, "a", "one", 1)
	p.Properties = json.RawMessage(`{"language":"fr"}`)
	if err := s.Apply(context.Background(), []eventProjection{p}); err != nil {
		t.Fatal(err)
	}
	result, err := s.Query(context.Background(), "a", `WITH extracted AS (SELECT json_extract_string(properties,$path) AS value FROM events) SELECT count(*) AS total,count_if(value IS NULL) AS missing,count(DISTINCT value) AS distinct_values FROM extracted`, map[string]string{"path": "$.language"})
	if err != nil || string(result.Data[0]["total"]) != `"1"` || string(result.Data[0]["missing"]) != `"0"` || string(result.Data[0]["distinct_values"]) != `"1"` {
		t.Fatalf("resolved scalar metadata lost types or values: %+v %v", result, err)
	}
}
