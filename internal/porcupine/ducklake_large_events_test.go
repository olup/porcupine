//go:build ducklake

package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type largeEventReceipt struct {
	ID   string `json:"event_id"`
	UUID string `json:"uuid"`
}

type largeEventCrashState struct {
	Project, Ingest, OtherProject, OtherIngest string
	Receipts                                   []largeEventReceipt
	Other                                      largeEventReceipt
}

func largeEventPayload(offset, count int) []byte {
	batch := make([]nativeMessage, count)
	for i := range batch {
		batch[i] = nativeMessage{Type: "track", MessageID: fmt.Sprintf("large-%04d", offset+i), ActorID: "large-anonymous", Name: "large_event", Timestamp: "2026-11-13T12:00:00Z", Properties: map[string]any{"app": "same-app", "padding": strings.Repeat("x", 15<<10)}}
	}
	raw, _ := json.Marshal(batch)
	return raw
}

func largeEventHTTP(t *testing.T, host *httptest.Server, method, path, credential string, body []byte, status int) map[string]any {
	t.Helper()
	req, err := http.NewRequest(method, host.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if path == "/ingest/v1/events" {
		req.Header.Set("X-Porcupine-Key", credential)
	} else {
		req.Header.Set("Authorization", "Bearer "+credential)
	}
	response, err := host.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != status {
		t.Fatalf("%s %s: status=%d want=%d error=%v body=%s", method, path, response.StatusCode, status, err, raw)
	}
	var result map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func largeEventQuery(t *testing.T, host *httptest.Server, project, sql string) []any {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"sql": sql, "use_cache": false})
	result := largeEventHTTP(t, host, "POST", "/api/v1/projects/"+project+"/query", adminToken, body, 200)
	return result["result"].(map[string]any)["data"].([]any)
}

func largeEventReceipts(t *testing.T, response map[string]any, count int, duplicate bool) []largeEventReceipt {
	t.Helper()
	items := response["receipts"].([]any)
	if len(items) != count {
		t.Fatalf("receipt count=%d want=%d", len(items), count)
	}
	field := "accepted"
	if duplicate {
		field = "duplicates"
	}
	if response[field] != float64(count) {
		t.Fatalf("unexpected receipt counts: %v", response)
	}
	receipts := make([]largeEventReceipt, count)
	for i, item := range items {
		row := item.(map[string]any)
		receipts[i] = largeEventReceipt{ID: row["event_id"].(string), UUID: row["uuid"].(string)}
		if receipts[i].ID == "" || receipts[i].UUID == "" || row["duplicate"] != duplicate {
			t.Fatalf("invalid receipt: %v", row)
		}
	}
	return receipts
}

func largeEventAssertIDs(t *testing.T, host *httptest.Server, state largeEventCrashState, deleted bool) {
	t.Helper()
	rows := largeEventQuery(t, host, state.Project, "SELECT event_id,uuid FROM events ORDER BY id")
	want := len(state.Receipts)
	if deleted {
		want--
	}
	if len(rows) != want {
		t.Fatalf("visible events=%d want=%d", len(rows), want)
	}
	seen := map[string]bool{}
	uuids := map[string]string{}
	for _, value := range rows {
		row := value.(map[string]any)
		id := row["event_id"].(string)
		if seen[id] {
			t.Fatalf("duplicate analytical event ID %s", id)
		}
		seen[id] = true
		uuids[id] = row["uuid"].(string)
	}
	for i, receipt := range state.Receipts {
		if seen[receipt.ID] != !(deleted && i == 1) {
			t.Fatalf("unexpected visibility for event %d, ID %s", i, receipt.ID)
		}
		if seen[receipt.ID] && uuids[receipt.ID] != receipt.UUID {
			t.Fatalf("analytical UUID changed for event %d", i)
		}
	}
}

func TestDuckLakeLargeEventsHTTPJourney(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := func(stage string) *exec.Cmd {
		cmd := exec.Command(executable, "-test.run=^TestDuckLakeLargeEventsCrashHelper$", "-test.v", "-test.timeout=240s")
		cmd.Env = append(os.Environ(), "PORCUPINE_LARGE_EVENT_ROOT="+root, "PORCUPINE_LARGE_EVENT_STAGE="+stage)
		return cmd
	}
	var output bytes.Buffer
	cmd := command("projected_before_ack")
	cmd.Stdout, cmd.Stderr = &output, &output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	ready := false
	deadline := time.Now().Add(210 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(root, "ready.json")); err == nil {
			ready = true
			break
		}
		select {
		case err := <-done:
			t.Fatalf("large-event child exited before crash checkpoint: %v\n%s", err, output.String())
		default:
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-done
	if !ready {
		t.Fatalf("large-event crash checkpoint timed out\n%s", output.String())
	}
	t.Logf("before hard kill:\n%s", output.String())
	result, err := command("resume").CombinedOutput()
	if err != nil {
		t.Fatalf("large-event restart failed: %v\n%s", err, result)
	}
	t.Logf("after restart:\n%s", result)
}

func TestDuckLakeLargeEventsCrashHelper(t *testing.T) {
	root := os.Getenv("PORCUPINE_LARGE_EVENT_ROOT")
	if root == "" {
		t.Skip("subprocess helper")
	}
	stage := os.Getenv("PORCUPINE_LARGE_EVENT_STAGE")
	config := Config{DBPath: filepath.Join(root, "queue.db"), BootstrapToken: adminToken, QueueMaxEvents: 2000,
		DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "lake"), MemoryLimitMB: 512, Threads: 2, MaintenanceInterval: 24 * time.Hour}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	// Keep the production batch defaults while controlling the crash boundary.
	s.deliveryCancel()
	<-s.deliveryDone
	if s.deliveryMaxEvents != 1000 || s.deliveryMaxBytes != 4<<20 {
		t.Fatalf("unexpected production batch defaults: rows=%d bytes=%d", s.deliveryMaxEvents, s.deliveryMaxBytes)
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	var state largeEventCrashState
	if stage == "resume" {
		raw, err := os.ReadFile(filepath.Join(root, "ready.json"))
		if err != nil || json.Unmarshal(raw, &state) != nil {
			t.Fatal("cannot load accepted large-event receipts")
		}
		if _, err := s.db.Exec("DROP TRIGGER IF EXISTS large_event_block_ack"); err != nil {
			t.Fatal(err)
		}
		s.startDelivery()
		if pending := architectureDrain(t, s, 120*time.Second); pending != 0 {
			t.Fatalf("restarted production delivery worker left %d queued writes", pending)
		}
		largeEventAssertIDs(t, host, state, true)
		for offset := 0; offset < 1000; offset += 10 {
			response := largeEventHTTP(t, host, "POST", "/ingest/v1/events", state.Ingest, largeEventPayload(offset, 10), 200)
			for i, receipt := range largeEventReceipts(t, response, 10, true) {
				if receipt != state.Receipts[offset+i] {
					t.Fatalf("restart changed native receipt %d", offset+i)
				}
			}
		}
		largeEventAssertIDs(t, host, state, true)
		rows := largeEventQuery(t, host, state.Project, "SELECT count(*) AS total,count_if(person_id='large-account') AS identified,count_if(json_extract_string(person_properties,'$.plan')='team') AS traits,min(length(json_extract_string(properties,'$.padding'))) AS smallest,max(length(json_extract_string(properties,'$.padding'))) AS largest FROM events")
		row := rows[0].(map[string]any)
		if row["total"] != "999" || row["identified"] != "999" || row["traits"] != "999" || row["smallest"] != "15360" || row["largest"] != "15360" {
			t.Fatalf("recovery changed payloads or identity: %v", row)
		}
		edited := largeEventQuery(t, host, state.Project, "SELECT event_id,event,revision FROM events WHERE uuid='"+state.Receipts[0].UUID+"'")
		if len(edited) != 1 || edited[0].(map[string]any)["event"] != "corrected_large_event" || edited[0].(map[string]any)["revision"] != "1" || edited[0].(map[string]any)["event_id"] != state.Receipts[0].ID {
			t.Fatalf("late original replaced queued edit: %v", edited)
		}
		other := largeEventQuery(t, host, state.OtherProject, "SELECT event_id FROM events")
		if len(other) != 1 || other[0].(map[string]any)["event_id"] != state.Other.ID {
			t.Fatalf("large-event recovery changed healthy project: %v", other)
		}
		if pending, _ := queueUsage(t, s); pending != 0 {
			t.Fatalf("stable retries requeued %d events", pending)
		}
		t.Log("1000 stable receipts, 999 surviving large events, one durable edit/delete, healthy second project, no resurrection after hard kill")
		return
	}
	if stage != "projected_before_ack" {
		t.Fatal("invalid large-event crash stage")
	}
	state.Project, state.Ingest = project(t, s)
	state.OtherProject, state.OtherIngest = project(t, s)
	for offset := 0; offset < 1000; offset += 10 {
		payload := largeEventPayload(offset, 10)
		response := largeEventHTTP(t, host, "POST", "/ingest/v1/events", state.Ingest, payload, 200)
		state.Receipts = append(state.Receipts, largeEventReceipts(t, response, 10, false)...)
		if offset == 0 {
			t.Logf("HTTP request bytes=%d events=10 padding bytes/event=%d", len(payload), 15<<10)
		}
	}
	if rows := largeEventQuery(t, host, state.Project, "SELECT count(*) AS total FROM events"); rows[0].(map[string]any)["total"] != "0" {
		t.Fatal("SQL exposed accepted events before analytical delivery")
	}
	identify := []byte(`{"type":"identify","message_id":"large-identify","anonymous_id":"large-anonymous","user_id":"large-account","set":{"plan":"team"}}`)
	identity := largeEventReceipts(t, largeEventHTTP(t, host, "POST", "/ingest/v1/events", state.Ingest, identify, 200), 1, false)[0]
	other := []byte(`{"type":"track","message_id":"healthy-project","actor_id":"other-user","name":"healthy_event"}`)
	state.Other = largeEventReceipts(t, largeEventHTTP(t, host, "POST", "/ingest/v1/events", state.OtherIngest, other, 200), 1, false)[0]
	batch, err := s.deliveryBatch(context.Background())
	if err != nil || len(batch) == 0 || len(batch) >= 1000 {
		t.Fatalf("default byte budget did not select bounded large-event batch: rows=%d error=%v", len(batch), err)
	}
	t.Logf("production defaults rows=%d bytes=%d selected rows=%d properties bytes/event=%d", s.deliveryMaxEvents, s.deliveryMaxBytes, len(batch), len(batch[0].Properties))
	if _, err := s.db.Exec("CREATE TRIGGER large_event_block_ack BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'large-event crash checkpoint'); END"); err != nil {
		t.Fatal(err)
	}
	// Every native transaction commits, but the trigger retains its SQLite originals.
	for projected := 0; projected < 1000; {
		if err := s.flushDelivery(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot remove delivered event") {
			t.Fatalf("expected only the controlled acknowledgment failure, got %v", err)
		}
		rows := largeEventQuery(t, host, state.Project, "SELECT count(*) AS total FROM events")
		var total int
		if _, err := fmt.Sscan(rows[0].(map[string]any)["total"].(string), &total); err != nil || total <= projected {
			t.Fatalf("default production batch made no analytical progress: before=%d after=%d error=%v", projected, total, err)
		}
		projected = total
	}
	// One more flush reaches the identity control and the healthy second project.
	if err := s.flushDelivery(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot remove delivered event") {
		t.Fatalf("expected controlled acknowledgment failure for trailing events: %v", err)
	}
	largeEventAssertIDs(t, host, state, false)
	largeEventHTTP(t, host, "GET", "/api/v1/projects/"+state.Project+"/events/"+url.PathEscape(identity.UUID), adminToken, nil, 404)
	// A delete mutation intentionally removes its queued original in SQLite.
	if _, err := s.db.Exec("DROP TRIGGER large_event_block_ack"); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/projects/" + state.Project + "/events/"
	edit, _ := json.Marshal(map[string]any{"expected_revision": "0", "idempotency_key": "large-edit", "snapshot": map[string]any{"event": "corrected_large_event", "distinct_id": "large-anonymous", "timestamp": "2026-11-13T12:00:00Z", "properties": map[string]any{"app": "corrected-app", "padding": strings.Repeat("x", 15<<10)}}})
	first := largeEventHTTP(t, host, "PATCH", path+url.PathEscape(state.Receipts[0].UUID), adminToken, edit, 202)
	retry := largeEventHTTP(t, host, "PATCH", path+url.PathEscape(state.Receipts[0].UUID), adminToken, edit, 202)
	if first["mutation_id"] != retry["mutation_id"] {
		t.Fatal("queued large edit retry allocated a new mutation")
	}
	current := largeEventHTTP(t, host, "GET", path+url.PathEscape(state.Receipts[0].UUID), adminToken, nil, 200)
	if current["pending"] != true || current["event"].(map[string]any)["event"] != "corrected_large_event" {
		t.Fatalf("event detail did not expose the durably queued edit: %v", current)
	}
	largeEventHTTP(t, host, "DELETE", path+url.PathEscape(state.Receipts[1].UUID), adminToken, []byte(`{"expected_revision":"0","idempotency_key":"large-delete"}`), 202)
	largeEventHTTP(t, host, "GET", path+url.PathEscape(state.Receipts[1].UUID), adminToken, nil, 410)
	if rows := largeEventQuery(t, host, state.Project, "SELECT count(*) AS total FROM events"); rows[0].(map[string]any)["total"] != "1000" {
		t.Fatal("SQL applied a queued mutation before analytical delivery")
	}
	var originals, mutations int
	if err := s.db.QueryRow("SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM mutation_outbox)").Scan(&originals, &mutations); err != nil || originals != 1001 || mutations != 2 {
		t.Fatalf("incorrect hard-kill boundary: originals=%d mutations=%d error=%v", originals, mutations, err)
	}
	raw, _ := json.Marshal(state)
	if err := os.WriteFile(filepath.Join(root, "ready.tmp"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(root, "ready.tmp"), filepath.Join(root, "ready.json")); err != nil {
		t.Fatal(err)
	}
	select {}
}
