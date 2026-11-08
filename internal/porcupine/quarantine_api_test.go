package porcupine

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestQuarantineHTTPAndMCPOperatorRecovery(t *testing.T) {
	f := newOAuthIntegration(t)
	backend := &isolationBackend{}
	backend.code.Store(469)
	backend.status.Store(400)
	backend.allRows.Store(true)
	f.s.ch = isolationCH(t, backend)
	admin := f.token(t, "bootstrap", adminToken, f.s.oauth.apiResource)
	a, ingestA := f.project(t, admin.AccessToken)
	b, ingestB := f.project(t, admin.AccessToken)
	reader := f.createClient(t, admin.AccessToken, []string{"events:read"}, []string{a})
	manager := f.createClient(t, admin.AccessToken, []string{"events:read", "events:edit"}, []string{a})
	other := f.createClient(t, admin.AccessToken, []string{"events:read", "events:edit"}, []string{b})
	readAPI := f.token(t, reader.ClientID, reader.ClientSecret, f.s.oauth.apiResource)
	editAPI := f.token(t, manager.ClientID, manager.ClientSecret, f.s.oauth.apiResource)
	otherAPI := f.token(t, other.ClientID, other.ClientSecret, f.s.oauth.apiResource)
	for _, seed := range []struct{ key, uuid string }{{ingestA, "quarantine-one"}, {ingestA, "quarantine-two"}, {ingestA, "quarantine-three"}, {ingestB, "foreign-quarantine"}} {
		f.call(t, "POST", "/capture/", "", map[string]any{"api_key": seed.key, "uuid": seed.uuid, "event": "poison", "distinct_id": "private-person", "properties": map[string]string{"private-value": "must-not-appear"}}, 200, nil)
	}
	originals, err := f.s.deliveryBatch(context.Background())
	if err != nil || len(originals) != 4 {
		t.Fatal("missing accepted originals")
	}
	if err := f.s.flushDelivery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if quarantineCount(t, f.s) != 4 || receiptCount(t, f.s) != 0 {
		t.Fatal("seed events were not retained in quarantine")
	}
	base := "/api/v1/projects/" + a + "/quarantined-events"
	f.call(t, "GET", base, "", nil, 401, nil)
	f.call(t, "GET", "/api/v1/projects/"+b+"/quarantined-events", readAPI.AccessToken, nil, 403, nil)
	var first map[string]any
	f.call(t, "GET", base+"?limit=2", readAPI.AccessToken, nil, 200, &first)
	items := first["events"].([]any)
	if len(items) != 2 || first["next_after_event_id"] != strconv.FormatInt(originals[1].ID, 10) || first["after_event_id"] != "0" {
		t.Fatal("quarantine keyset first page mismatch")
	}
	allowed := map[string]bool{"event_id": true, "uuid": true, "error_code": true, "quarantined_at": true, "received_at": true}
	for _, raw := range items {
		item := raw.(map[string]any)
		for field := range item {
			if !allowed[field] {
				t.Fatal("quarantine listing exposed payload fields")
			}
		}
		if len(item) != 5 || item["error_code"] != float64(469) || item["quarantined_at"] == "" || item["received_at"] == "" {
			t.Fatal("quarantine metadata incomplete")
		}
	}
	encoded, _ := json.Marshal(first)
	for _, private := range []string{"private-person", "must-not-appear", "private rejected property", "foreign-quarantine", "secret_hash"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("quarantine metadata leaked payload, raw backend errors, or another project")
		}
	}
	var second QuarantinedEvents
	f.call(t, "GET", base+"?limit=2&after_event_id="+first["next_after_event_id"].(string), readAPI.AccessToken, nil, 200, &second)
	if len(second.Events) != 1 || second.Events[0].EventID != strconv.FormatInt(originals[2].ID, 10) || second.NextAfterEventID != nil {
		t.Fatal("quarantine keyset next page mismatch")
	}
	var empty QuarantinedEvents
	f.call(t, "GET", base+"?after_event_id=9223372036854775807", readAPI.AccessToken, nil, 200, &empty)
	if len(empty.Events) != 0 {
		t.Fatal("quarantine cursor did not constrain page")
	}
	for _, query := range []string{"limit=0", "limit=1001", "after_event_id=-1", "after_event_id=not-a-number"} {
		f.call(t, "GET", base+"?"+query, readAPI.AccessToken, nil, 422, nil)
	}
	f.call(t, "GET", base+"?after_event_id=9223372036854775808", readAPI.AccessToken, nil, 400, nil)
	firstID, secondID, foreignID := strconv.FormatInt(originals[0].ID, 10), strconv.FormatInt(originals[1].ID, 10), strconv.FormatInt(originals[3].ID, 10)
	firstRetry := base + "/" + firstID + "/retry"
	f.call(t, "POST", firstRetry, readAPI.AccessToken, nil, 403, nil)
	f.call(t, "POST", firstRetry, otherAPI.AccessToken, nil, 403, nil)
	f.call(t, "POST", base+"/"+foreignID+"/retry", editAPI.AccessToken, nil, 404, nil)
	f.call(t, "POST", base+"/0/retry", editAPI.AccessToken, nil, 422, nil)
	if quarantineCount(t, f.s) != 4 || eventCount(t, f.s) != 4 || receiptCount(t, f.s) != 0 {
		t.Fatal("denied repair changed accepted originals")
	}

	readMCP := f.token(t, reader.ClientID, reader.ClientSecret, f.s.oauth.mcpResource)
	editMCP := f.token(t, manager.ClientID, manager.ClientSecret, f.s.oauth.mcpResource)
	readCtx, readSession := mcpTestSession(t, f.host.URL, readMCP.AccessToken, "")
	editCtx, editSession := mcpTestSession(t, f.host.URL, editMCP.AccessToken, "")
	catalog := mcpTestTool(t, readCtx, readSession, "porcupine_docs", mcpDocsInput{Query: "quarantined"}, false)
	readOps := catalog["operations"].([]any)
	if len(readOps) != 1 || readOps[0].(map[string]any)["operation_id"] != "list-quarantined-events" || readOps[0].(map[string]any)["read_only"] != true {
		t.Fatal("reader MCP discovery exposed retry or hid quarantine listing")
	}
	catalog = mcpTestTool(t, editCtx, editSession, "porcupine_docs", mcpDocsInput{Query: "quarantined"}, false)
	if len(catalog["operations"].([]any)) != 2 {
		t.Fatal("operator MCP discovery missing quarantine operations")
	}
	for _, item := range catalog["operations"].([]any) {
		op := item.(map[string]any)
		if op["operation_id"] == "retry-quarantined-event" && op["read_only"] != false {
			t.Fatal("retry advertised as read-only")
		}
	}
	listed := mcpTestCall(t, readCtx, readSession, mcpCallInput{OperationID: "list-quarantined-events", Path: map[string]string{"id": a}, Query: map[string]string{"limit": "2"}}, 200)
	if len(listed["events"].([]any)) != 2 {
		t.Fatal("MCP quarantine list differs from HTTP")
	}
	mcpTestCall(t, readCtx, readSession, mcpCallInput{OperationID: "list-quarantined-events", Path: map[string]string{"id": b}}, 403)
	mcpTestCall(t, readCtx, readSession, mcpCallInput{OperationID: "retry-quarantined-event", Path: map[string]string{"id": a, "event_id": firstID}}, 403)
	mcpTestCall(t, editCtx, editSession, mcpCallInput{OperationID: "retry-quarantined-event", Path: map[string]string{"id": b, "event_id": foreignID}}, 403)
	mcpTestCall(t, editCtx, editSession, mcpCallInput{OperationID: "retry-quarantined-event", Path: map[string]string{"id": a, "event_id": foreignID}}, 404)

	backend.status.Store(0)
	var retry QuarantinedEventRetry
	f.call(t, "POST", firstRetry, editAPI.AccessToken, nil, 202, &retry)
	if !retry.Requeued || retry.EventID != firstID {
		t.Fatal("HTTP retry did not restore delivery eligibility")
	}
	if eventCount(t, f.s) != 4 || receiptCount(t, f.s) != 0 || quarantineCount(t, f.s) != 3 {
		t.Fatal("retry falsely acknowledged or removed original before delivery")
	}
	f.call(t, "POST", firstRetry, editAPI.AccessToken, nil, 202, &retry)
	if retry.Requeued {
		t.Fatal("repeat retry duplicated eligibility")
	}
	ready, err := f.s.deliveryBatch(context.Background())
	if err != nil || len(ready) != 1 || !reflect.DeepEqual(ready[0], originals[0]) {
		t.Fatal("operator retry changed original payload or identity")
	}
	// The normal worker stays running through subsequent MCP repair and recovery.
	f.s.startDelivery()
	waitReceipts := func(want int) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for receiptCount(t, f.s) != want {
			if time.Now().After(deadline) {
				t.Fatal("normal delivery worker did not acknowledge repaired event")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitReceipts(1)
	f.call(t, "POST", firstRetry, editAPI.AccessToken, nil, 404, nil)
	retried := mcpTestCall(t, editCtx, editSession, mcpCallInput{OperationID: "retry-quarantined-event", Path: map[string]string{"id": a, "event_id": secondID}}, 202)
	if retried["requeued"] != true || retried["event_id"] != secondID {
		t.Fatal("MCP retry did not restore delivery eligibility")
	}
	waitReceipts(2)
	if eventCount(t, f.s) != 2 || quarantineCount(t, f.s) != 2 {
		t.Fatal("successful recovery crossed quarantine or project boundary")
	}
	backend.mu.Lock()
	accepted := append([]clickHouseEvent{}, backend.accepted...)
	backend.mu.Unlock()
	if len(accepted) != 2 || accepted[0].EventID != uint64(originals[0].ID) || accepted[1].EventID != uint64(originals[1].ID) || accepted[0].UUID != originals[0].UUID || accepted[0].ReceivedAt != originals[0].ReceivedAt {
		t.Fatal("recovery changed identity, duplicated, or lost accepted originals")
	}
}
