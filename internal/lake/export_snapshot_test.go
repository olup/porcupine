package lake

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestDuckLakeExportRetainsAdmissionSnapshot(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := context.Background()
	initial := lakeStorageProjection(t, "a", "present", 1)
	if err := store.Apply(ctx, []EventProjection{initial, lakeStorageProjection(t, "b", "private", 2)}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.CaptureExportSnapshot(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	initial.Revision, initial.IsDeleted = 2, true
	if err := store.Apply(ctx, []EventProjection{initial, lakeStorageProjection(t, "a", "later", 3)}); err != nil {
		t.Fatal(err)
	}
	var uuid string
	callbacks := ExportStreamCallbacks{
		Columns: func([]AnalyticsResultColumn) error { return nil },
		Row: func(row []json.RawMessage) error {
			return json.Unmarshal(row[0], &uuid)
		},
	}
	result, err := store.ExportAtSnapshot(ctx, "a", "SELECT uuid, person_id FROM events", nil, snapshot, 1024, callbacks)
	if err != nil || result.Rows != 1 || uuid != "present" {
		t.Fatalf("retained export: rows=%d uuid=%q err=%v", result.Rows, uuid, err)
	}
	result, err = store.Export(ctx, "a", "SELECT uuid FROM events", nil, nil, 1024, callbacks)
	if err != nil || result.Rows != 1 || uuid != "later" {
		t.Fatalf("current read after retained export: rows=%d uuid=%q err=%v", result.Rows, uuid, err)
	}
	_, err = store.ExportAtSnapshot(ctx, "b", "SELECT uuid FROM events", nil, snapshot, 1024, callbacks)
	if !errors.Is(err, ErrExportSnapshotUnavailable) {
		t.Fatalf("cross-project catalog accepted: %v", err)
	}
}

func TestDuckLakeScheduledWindowAtSnapshot(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := t.Context()
	start := time.Date(2026, 11, 13, 10, 0, 1, 0, time.UTC)
	end := start.Add(time.Minute)
	initial := lakeStorageProjection(t, "a", "initial", 1)
	outside := lakeStorageProjection(t, "a", "outside", 2)
	outside.Event.ReceivedAt = "2026-11-13T10:01:01Z"
	imported := lakeStorageProjection(t, "a", "imported", 3)
	imported.Event.Imported = true
	if err := store.Apply(ctx, []EventProjection{initial, outside, imported}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.CaptureExportSnapshot(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	initial.Revision, initial.IsDeleted = 2, true
	if err := store.Apply(ctx, []EventProjection{initial, lakeStorageProjection(t, "a", "late", 4)}); err != nil {
		t.Fatal(err)
	}
	var seen []string
	result, err := store.ExportWindowAtSnapshot(ctx, "a", "SELECT uuid FROM events WHERE received_at_ms >= CAST($window_start AS BIGINT)", nil,
		AnalyticalWindow{StartMS: start.UnixMilli(), EndMS: end.UnixMilli()}, snapshot, 1024, ExportStreamCallbacks{
			Columns: func([]AnalyticsResultColumn) error { return nil },
			Row: func(row []json.RawMessage) error {
				var uuid string
				if err := json.Unmarshal(row[0], &uuid); err != nil {
					return err
				}
				seen = append(seen, uuid)
				return nil
			},
		})
	if err != nil || result.Rows != 1 || len(seen) != 1 || seen[0] != "initial" {
		t.Fatalf("retained window rows=%d seen=%q err=%v", result.Rows, seen, err)
	}
}
