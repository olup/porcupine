package porcupine

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func projectionFixture(t *testing.T) eventProjection {
	t.Helper()
	event := Event{ID: 42, ProjectID: "project-a", UUID: "event-a", Event: "purchase", DistinctID: "person-a", Properties: json.RawMessage(`{"html":"<original>","price":5}`), Timestamp: "2026-01-01T12:00:00Z", ReceivedAt: "2026-11-07T21:00:00.123Z"}
	snapshot, err := originalSnapshot(event)
	if err != nil {
		t.Fatal(err)
	}
	return eventProjection{Event: event, OriginalSnapshot: snapshot}
}

func TestProjectionOriginalCorrectionAndDeletionRetries(t *testing.T) {
	var bodies []string
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "INSERT INTO analytics.events_v2 FORMAT JSONEachRow" || r.URL.Query().Get("async_insert") != "0" {
			t.Error("projection insert must be synchronous and target v2")
		}
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
	})
	original := projectionFixture(t)
	correction := original
	correction.Revision = 8
	correction.Event.Event = "corrected"
	correction.Event.Properties = json.RawMessage(`{"price":7}`)
	deleted := correction
	deleted.Revision = 9
	deleted.IsDeleted = true
	for range 2 {
		if err := c.insertProjections(context.Background(), []eventProjection{original, correction, deleted}); err != nil {
			t.Fatal(err)
		}
	}
	if len(bodies) != 2 || bodies[0] != bodies[1] {
		t.Fatal("projection retry was not byte-stable")
	}
	lines := strings.Split(strings.TrimSpace(bodies[0]), "\n")
	if len(lines) != 3 {
		t.Fatal("missing projection rows")
	}
	var rows [3]clickHouseEvent
	for i, line := range lines {
		if err := json.Unmarshal([]byte(line), &rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if rows[0].Revision != 0 || rows[1].Revision != 8 || rows[2].Revision != 9 || rows[1].OriginalSnapshot != original.OriginalSnapshot {
		t.Fatal("revision or original evidence was changed")
	}
	if strings.Contains(rows[0].OriginalSnapshot, `\u003c`) || !strings.Contains(rows[0].OriginalSnapshot, "<original>") {
		t.Fatal("original snapshot unexpectedly escaped HTML")
	}
	for _, row := range rows {
		if row.EventID != 42 || row.ProjectID != "project-a" || row.UUID != "event-a" || row.ReceivedAt != "2026-11-07T21:00:00.123Z" || row.ReceivedAtMS != 1794085200123 {
			t.Fatal("projection changed immutable identity or partition anchor")
		}
	}
	row := rows[2]
	if row.IsDeleted != 1 || row.Event != "" || row.DistinctID != "" || row.Properties != "{}" || row.Timestamp != "" || row.TimestampMS != 0 || row.OriginalSnapshot != "" {
		t.Fatal("deletion retained payload")
	}
}

func TestProjectionLoadUsesBoundIdentityAndReturnsTombstone(t *testing.T) {
	project := "project' OR 1=1 --"
	uuid := "uuid' UNION ALL SELECT secret --"
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sql := string(body)
		if strings.Contains(sql, project) || strings.Contains(sql, uuid) || strings.Contains(sql, " FINAL") || !strings.Contains(sql, "ORDER BY revision DESC LIMIT 1") || !strings.Contains(sql, "SETTINGS final=0") {
			t.Error("projection lookup interpolated identity or finalized away tombstones")
		}
		for _, setting := range []string{"use_query_cache=0", "enable_reads_from_query_cache=0", "enable_writes_to_query_cache=0"} {
			if !strings.Contains(sql, setting) {
				t.Error("mandatory projection lookup did not disable cache reads and writes")
			}
		}
		for _, value := range []string{project, uuid} {
			if !strings.Contains(sql, "unhex('"+hex.EncodeToString([]byte(value))+"')") {
				t.Error("lookup did not bind exact identity")
			}
		}
		if r.Header.Get("X-ClickHouse-User") != "admin_user" {
			t.Error("internal lookup lacks server credential")
		}
		json.NewEncoder(w).Encode(map[string]any{"event_id": "42", "project_id": project, "uuid": uuid, "event": "", "distinct_id": "", "properties": "{}", "timestamp": "", "received_at": "2026-11-07T21:00:00Z", "revision": "9", "is_deleted": 1, "original_snapshot": ""})
	})
	projection, err := c.loadProjection(context.Background(), project, uuid)
	if err != nil || !projection.IsDeleted || projection.Revision != 9 || projection.ID != 42 {
		t.Fatalf("tombstone lookup failed: %+v %v", projection, err)
	}
}

func TestProjectionLoadRoundTripAndLegacySize(t *testing.T) {
	original := projectionFixture(t)
	original.Event.Properties = json.RawMessage(`{"legacy":"` + strings.Repeat(`\u003c`, 400000) + `"}`)
	snapshot, err := originalSnapshot(original.Event)
	if err != nil {
		t.Fatal(err)
	}
	original.OriginalSnapshot = snapshot
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"event_id": "42", "project_id": original.ProjectID, "uuid": original.UUID, "event": original.Event.Event, "distinct_id": original.DistinctID, "properties": string(original.Properties), "timestamp": original.Timestamp, "received_at": original.ReceivedAt, "revision": "7", "is_deleted": 0, "original_snapshot": original.OriginalSnapshot})
	})
	loaded, err := c.loadProjection(context.Background(), original.ProjectID, original.UUID)
	if err != nil || loaded.Revision != 7 || string(loaded.Properties) != string(original.Properties) || loaded.OriginalSnapshot != original.OriginalSnapshot {
		t.Fatalf("large legacy projection failed: %v", err)
	}
}

func TestProjectionRejectsInvalidStoredIdentityAndMissingEvent(t *testing.T) {
	for _, body := range []string{"", `{"event_id":"42","project_id":"other-project","uuid":"event-a","revision":"1","is_deleted":1,"received_at":"2026-11-07T21:00:00Z"}`, `{"event_id":"9223372036854775808","project_id":"project-a","uuid":"event-a","revision":"1","is_deleted":1,"received_at":"2026-11-07T21:00:00Z"}`, `{"event_id":"42","project_id":"project-a","uuid":"event-a","revision":"1","is_deleted":2,"received_at":"2026-11-07T21:00:00Z"}`} {
		t.Run(body, func(t *testing.T) {
			c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, body) })
			_, err := c.loadProjection(context.Background(), "project-a", "event-a")
			if body == "" {
				if !errors.Is(err, errEventNotFound) {
					t.Fatal("missing event sentinel lost")
				}
			} else if err == nil {
				t.Fatal("invalid stored projection accepted")
			}
		})
	}
}

func TestProjectionInvalidBatchDoesNotSendPartialInsert(t *testing.T) {
	c := testClickHouse(t, func(w http.ResponseWriter, r *http.Request) { t.Error("invalid projection batch reached ClickHouse") })
	original := projectionFixture(t)
	invalid := original
	invalid.Revision = 1
	invalid.Timestamp = "invalid"
	if err := c.insertProjections(context.Background(), []eventProjection{original, invalid}); err == nil {
		t.Fatal("invalid batch accepted")
	}
	invalid = original
	invalid.IsDeleted = true
	if err := c.insertProjections(context.Background(), []eventProjection{invalid}); err == nil {
		t.Fatal("revision-zero deletion accepted")
	}
}
