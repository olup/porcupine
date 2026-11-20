package porcupine

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestEventCatalogRefreshJourney(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := duckLakeJourneyService(t)
	s.startCatalogRefresh()
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	path := "/api/v1/projects/" + a + "/catalog"
	request(t, s, "POST", "/api/v1/projects/"+b+"/catalog/refresh", reader, "", 403)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"first","name":"pageview","properties":{"path":"/"}}`, 200)
	duckLakeJourneyDrain(t, s)
	awaitCatalog := func(want int) map[string]any {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			page := request(t, s, "GET", path, reader, "", 200)
			refresh, _ := page["refresh"].(map[string]any)
			if refresh["status"] == "ready" && len(page["events"].([]any)) == want {
				if at, ok := refresh["updated_at"].(string); !ok || at == "" {
					t.Fatal("ready catalog has no update time", refresh)
				}
				return page
			}
			if time.Now().After(deadline) {
				t.Fatalf("catalog did not reach ready with %d names: %v", want, page)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	first := awaitCatalog(1)
	event := first["events"].([]any)[0].(map[string]any)
	if event["event"] != "pageview" || event["count"] != "1" {
		t.Fatal(event)
	}
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"second","name":"signup"}`, 200)
	duckLakeJourneyDrain(t, s)
	page := request(t, s, "GET", path, reader, "", 200)
	if len(page["events"].([]any)) != 1 {
		t.Fatal("add should wait for a refresh during the cooldown", page)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, mcp := mcpTestSession(t, host.URL, reader, "2025-11-25")
	mcpTestCall(t, ctx, mcp, mcpCallInput{OperationID: "refresh-event-catalog", Path: map[string]string{"id": a}}, 202)
	awaitCatalog(2)
	result := mcpTestTool(t, ctx, mcp, "porcupine_schema", mcpSchemaInput{ProjectID: a}, false)
	if len(result["catalog"].(map[string]any)["events"].([]any)) != 2 {
		t.Fatal(result)
	}
	request(t, s, "DELETE", "/api/v1/projects/"+a+"/events/"+event["latest_event_uuid"].(string), adminToken,
		`{"expected_revision":"0","idempotency_key":"remove-old-name"}`, 202)
	duckLakeJourneyDrain(t, s)
	page = request(t, s, "GET", path, reader, "", 200)
	if events := page["events"].([]any); len(events) != 1 || events[0].(map[string]any)["event"] != "signup" {
		t.Fatal("catalog returned deleted event name", page)
	}
	awaitCatalog(1)
}
