package lake

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/event"
)

func TestHostedListEventsDeadline(t *testing.T) {
	t.Parallel()
	reader := HostedReader{store: &DuckLakeStore{}}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := reader.ListEvents(ctx, "project", 10, 1024)
	if queryStatus(err) != http.StatusTooManyRequests {
		t.Fatalf("expired event list returned %v, want a typed time-limit error", err)
	}
}

func TestDuckLakeS3SharedPostgresHostedListEventsMatchesEventsView(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx, writer, cold, config := sharedNativeStores(t)
	reader, err := NewHostedReader(cold)
	if err != nil {
		t.Fatal(err)
	}
	projects := []string{"list-primary", "list-neighbor"}
	for _, project := range projects {
		seedHostedListEvents(t, ctx, writer, ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: project})
	}
	for _, project := range projects {
		expectedIDs := []int64{100, 70, 20}
		expectedUUIDs := []string{project + "-archive", project + "-revised", project + "-first"}
		for _, limit := range []int{0, 1, 2, 10} {
			want := readLegacyHostedListEvents(t, ctx, reader, project, limit)
			byteLimit := len(`{"events":[]}`)
			for _, e := range want {
				encoded, err := json.Marshal(e)
				if err != nil {
					t.Fatal(err)
				}
				byteLimit += len(encoded) + 1
			}
			got, err := reader.ListEvents(ctx, project, limit, byteLimit)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("list for %s with limit %d differs from events view: got %+v, want %+v, err %v", project, limit, got, want, err)
			}
			var ids []int64
			var uuids []string
			for _, e := range got {
				if e.ProjectID != project {
					t.Fatalf("list for %s returned a row from %s", project, e.ProjectID)
				}
				ids, uuids = append(ids, e.ID), append(uuids, e.UUID)
			}
			n := min(limit, len(expectedIDs))
			if !slices.Equal(ids, expectedIDs[:n]) || !slices.Equal(uuids, expectedUUIDs[:n]) {
				t.Fatalf("list for %s with limit %d returned IDs %v and UUIDs %v", project, limit, ids, uuids)
			}
			if n > 0 {
				got, err = reader.ListEvents(ctx, project, limit, byteLimit-1)
				var limitError *apierr.Error
				if got != nil || !errors.As(err, &limitError) ||
					limitError.GetStatus() != http.StatusTooManyRequests || limitError.Code != "result_byte_limit" {
					t.Fatalf("list at one byte below the limit returned %+v, %v", got, err)
				}
			}
		}
	}
}

func seedHostedListEvents(t *testing.T, ctx context.Context, writer *DuckLakeStore, key ProjectWriteKey) {
	t.Helper()
	if _, err := writer.InitializeHostedProject(ctx, key); err != nil {
		t.Fatal(err)
	}
	p, release, err := writer.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = p.DB.ExecContext(ctx, `INSERT INTO lake.current_events (
 id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,
 revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot)
 SELECT id,?,? || '-' || label,'page_viewed','anonymous',properties,occurred,epoch_ms(occurred::TIMESTAMPTZ),occurred::TIMESTAMPTZ,
 '2026-11-13T11:00:00Z',epoch_ms(TIMESTAMPTZ '2026-11-13T11:00:00Z'),(label='revised')::UBIGINT,
 'source-' || label,'message-' || label,'track',body,(label='archive')::UTINYINT,identity_snapshot,''
 FROM (VALUES
  (20,'first','2026-11-13T09:00:00Z','{}','',''),
  (70,'revised','2026-11-13T10:00:00Z','{"revised":true}','{"user_agent":"browser"}',''),
  (80,'hidden-between','2026-11-13T10:00:00Z','{}','','{}'),
  (100,'archive','2001-01-01T00:00:00Z','{"text":"é"}','',''),
  (110,'hidden-last','2026-11-13T10:00:00Z','{}','','{}')
 ) AS fixture(id,label,occurred,properties,body,identity_snapshot)`, key.ProjectID, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
}

func readLegacyHostedListEvents(t *testing.T, ctx context.Context, r *HostedReader, project string, limit int) []event.Event {
	t.Helper()
	p, release, err := r.lease(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := p.DB.QueryContext(ctx, `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,
source_id,message_id,event_type,context,is_imported FROM events ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	events := []event.Event{}
	for rows.Next() {
		var e event.Event
		var properties, body string
		var imported uint8
		if err := rows.Scan(&e.ID, &e.ProjectID, &e.UUID, &e.Event, &e.DistinctID, &properties,
			&e.Timestamp, &e.ReceivedAt, &e.SourceID, &e.MessageID, &e.EventType, &body, &imported); err != nil {
			t.Fatal(err)
		}
		e.Properties, e.Delivered, e.Imported = json.RawMessage(properties), true, imported != 0
		if body != "" {
			e.Context = json.RawMessage(body)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return events
}
