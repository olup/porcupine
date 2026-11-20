package lake

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"
)

const snapshotCatalogProject = "snapshot-catalog"

func TestDuckLakeCatalogSnapshotAppendAndRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := openLakeStorageTest(t, dir, 2)
	initial := lakeStorageProjection(t, snapshotCatalogProject, "initial", 2)
	initial.Event.Event = "purchase"
	applyCatalogProjections(t, store, initial)
	base := readCatalogSnapshot(t, store, nil)
	if !base.Replace || len(base.Rows) != 1 || base.Rows[0].FirstID != 2 || base.Rows[0].LastID != 2 {
		t.Fatalf("initial catalog: %+v", base)
	}

	applyCatalogProjections(t, store, initial)
	replayed := readCatalogSnapshot(t, store, &base.Snapshot)
	if replayed.Replace || len(replayed.Rows) != 0 || replayed.Snapshot != base.Snapshot {
		t.Fatalf("retry added rows or advanced the snapshot: %+v", replayed)
	}

	imported := lakeStorageProjection(t, snapshotCatalogProject, "imported", 5)
	imported.Event.Event = "purchase"
	imported.Timestamp = "2001-01-01T00:00:00Z"
	imported.ReceivedAt = "2026-11-13T10:00:05Z"
	imported.Imported = true
	earlier := lakeStorageProjection(t, snapshotCatalogProject, "earlier-id", 1)
	earlier.Event.Event = "purchase"
	earlier.ReceivedAt = "2026-11-13T10:00:03Z"
	newName := lakeStorageProjection(t, snapshotCatalogProject, "signup", 4)
	newName.Event.Event = "signup"
	control := lakeStorageProjection(t, snapshotCatalogProject, "identify", 6)
	control.Event.Event = "$identify"
	control.IdentitySnapshot = `{"version":"6","identified_id":"a","traits":{},"links":[{"anonymous_id":"b","identified_id":"a","version":"6"}]}`
	applyCatalogProjections(t, store, imported, earlier, newName, control)
	assertCatalogSnapshotValid(t, store, base.Snapshot, true)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openLakeStorageTest(t, dir, 2)
	delta := readCatalogSnapshot(t, store, &base.Snapshot)
	if delta.Replace || delta.Snapshot.Version <= base.Snapshot.Version || len(delta.Rows) != 2 {
		t.Fatalf("expected append delta after a cold reopen: %+v", delta)
	}
	wantPurchase := CatalogAggregate{Event: EventCatalogEvent{Event: "purchase", Count: "2", FirstReceivedAt: earlier.ReceivedAt,
		LastReceivedAt: imported.ReceivedAt, LatestEventUUID: imported.UUID, LatestEventTimestamp: imported.Timestamp}, FirstID: 1, LastID: 5}
	if delta.Rows[0] != wantPurchase || delta.Rows[1].Event.Event != newName.Event.Event {
		t.Fatalf("incorrect durable-ID pointers or unexpected identity event: %+v", delta.Rows)
	}
	full := readCatalogSnapshot(t, store, nil)
	wantPurchase.Event.Count = "3"
	if full.Snapshot != delta.Snapshot || full.Rows[0] != wantPurchase || !slices.Equal(delta.Rows[1:], full.Rows[1:]) {
		t.Fatalf("summary after append differs from full catalog: %+v", full)
	}
	assertSummaryMatchesCatalog(t, store, full)
}

func TestDuckLakeCatalogSnapshotCompactionAndInlineFlush(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	a := lakeStorageProjection(t, snapshotCatalogProject, "a", 1)
	applyCatalogProjections(t, store, a)
	seed := readCatalogSnapshot(t, store, nil)
	catalogSnapshotExec(t, store, "CALL ducklake_flush_inlined_data('lake')")
	flushed := readCatalogSnapshot(t, store, &seed.Snapshot)
	if flushed.Replace || len(flushed.Rows) != 0 {
		t.Fatalf("physical flush duplicated a seeded row: %+v", flushed)
	}
	catalogSnapshotExec(t, store, "CALL ducklake_set_option('lake','data_inlining_row_limit','0')")
	b := lakeStorageProjection(t, snapshotCatalogProject, "b", 2)
	c := lakeStorageProjection(t, snapshotCatalogProject, "c", 3)
	applyCatalogProjections(t, store, b)
	appendOnly := readCatalogSnapshot(t, store, &flushed.Snapshot)
	applyCatalogProjections(t, store, c)
	beforeMerge, err := store.CaptureExportSnapshot(t.Context(), snapshotCatalogProject)
	if err != nil {
		t.Fatal(err)
	}
	catalogSnapshotExec(t, store, "CALL ducklake_merge_adjacent_files('lake','current_events')")
	delta := readCatalogSnapshot(t, store, &flushed.Snapshot)
	if delta.Snapshot.Version <= beforeMerge.Version {
		t.Fatalf("test did not exercise current_events compaction: before %+v, after %+v", beforeMerge, delta.Snapshot)
	}
	if delta.Replace || len(delta.Rows) != 1 || delta.Rows[0].Event.Count != "2" || delta.Rows[0].FirstID != 2 || delta.Rows[0].LastID != 3 {
		t.Fatalf("compaction lost or duplicated appends: %+v", delta)
	}
	assertCatalogSnapshotValid(t, store, appendOnly.Snapshot, true)
	maintenanceOnly := readCatalogSnapshot(t, store, &beforeMerge)
	if maintenanceOnly.Replace || maintenanceOnly.Snapshot != delta.Snapshot || len(maintenanceOnly.Rows) != 0 {
		t.Fatalf("physical merge should advance without a delta: %+v", maintenanceOnly)
	}
	full := readCatalogSnapshot(t, store, nil)
	if len(full.Rows) != 1 || full.Rows[0].Event.Count != "3" {
		t.Fatalf("full catalog after compaction: %+v", full)
	}
	assertSummaryMatchesCatalog(t, store, full)

	b.Revision, b.IsDeleted = 1, true
	applyCatalogProjections(t, store, b)
	deleted := readCatalogSnapshot(t, store, nil)
	d := lakeStorageProjection(t, snapshotCatalogProject, "d", 4)
	applyCatalogProjections(t, store, d)
	beforeRewrite, err := store.CaptureExportSnapshot(t.Context(), snapshotCatalogProject)
	if err != nil {
		t.Fatal(err)
	}
	catalogSnapshotExec(t, store, "CALL ducklake_rewrite_data_files('lake','current_events',delete_threshold=>0)")
	rewritten := readCatalogSnapshot(t, store, &deleted.Snapshot)
	if rewritten.Snapshot.Version <= beforeRewrite.Version {
		t.Fatalf("test did not exercise current_events rewrite: before %+v, after %+v", beforeRewrite, rewritten.Snapshot)
	}
	if rewritten.Replace || len(rewritten.Rows) != 1 || rewritten.Rows[0].Event.Count != "1" ||
		rewritten.Rows[0].FirstID != d.ID || rewritten.Rows[0].LastID != d.ID {
		t.Fatalf("rewrite lost or duplicated appends: %+v", rewritten)
	}
}

func TestDuckLakeCatalogSnapshotPinsAndRejectsMutations(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := t.Context()
	a := lakeStorageProjection(t, snapshotCatalogProject, "a", 1)
	b := lakeStorageProjection(t, snapshotCatalogProject, "b", 2)
	applyCatalogProjections(t, store, a)
	base := readCatalogSnapshot(t, store, nil)
	applyCatalogProjections(t, store, b)
	if err := store.withCatalogSnapshot(ctx, snapshotCatalogProject, func(p *ProjectLake, tx *sql.Tx, pinned ExportSnapshot) error {
		c := lakeStorageProjection(t, snapshotCatalogProject, "accepted-after-pin", 3)
		applyCatalogProjections(t, store, c)
		delta, err := p.readCatalogSnapshotAt(ctx, tx, pinned, &base.Snapshot)
		if err != nil {
			return err
		}
		if delta.Replace || delta.Snapshot != pinned || len(delta.Rows) != 1 || delta.Rows[0].Event.LatestEventUUID != b.UUID {
			return fmt.Errorf("delta crossed the pinned snapshot: %+v", delta)
		}
		full, err := p.readCatalogSnapshotAt(ctx, tx, pinned, nil)
		if err != nil {
			return err
		}
		if !full.Replace || len(full.Rows) != 1 || full.Rows[0].Event.Count != "2" || full.Rows[0].Event.LatestEventUUID != b.UUID {
			return fmt.Errorf("full read crossed the pinned snapshot: %+v", full)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	beforeEdit := readCatalogSnapshot(t, store, nil)
	a.Revision = 1
	a.Event.Event = "renamed"
	applyCatalogProjections(t, store, a)
	assertCatalogSnapshotValid(t, store, beforeEdit.Snapshot, false)
	afterEdit := readCatalogSnapshot(t, store, &beforeEdit.Snapshot)
	if !afterEdit.Replace || len(afterEdit.Rows) != 2 {
		t.Fatalf("rename must rebuild both event names: %+v", afterEdit)
	}
	assertSummaryMatchesCatalog(t, store, afterEdit)

	b.Revision, b.IsDeleted = 1, true
	applyCatalogProjections(t, store, b)
	assertCatalogSnapshotValid(t, store, afterEdit.Snapshot, false)
	afterDelete := readCatalogSnapshot(t, store, &afterEdit.Snapshot)
	if !afterDelete.Replace || len(afterDelete.Rows) != 2 || afterDelete.Rows[0].FirstID != 3 || afterDelete.Rows[0].Event.Count != "1" {
		t.Fatalf("deletion must remove the old first pointer: %+v", afterDelete)
	}
	assertSummaryMatchesCatalog(t, store, afterDelete)

	later := lakeStorageProjection(t, snapshotCatalogProject, "later", 4)
	applyCatalogProjections(t, store, later)
	assertCatalogSnapshotValid(t, store, afterDelete.Snapshot, true)
	current := readCatalogSnapshot(t, store, nil)
	catalogSnapshotExec(t, store, "CALL ducklake_expire_snapshots('lake',versions=>["+strconv.FormatInt(afterDelete.Snapshot.Version, 10)+"])")
	other, err := store.CaptureExportSnapshot(ctx, "other-project")
	if err != nil {
		t.Fatal(err)
	}
	future := current.Snapshot
	future.Version++
	for _, invalid := range []ExportSnapshot{other, future, afterDelete.Snapshot} {
		assertCatalogSnapshotValid(t, store, invalid, false)
		replacement := readCatalogSnapshot(t, store, &invalid)
		if !replacement.Replace || !slices.Equal(replacement.Rows, current.Rows) {
			t.Fatalf("invalid checkpoint %+v did not rebuild: %+v", invalid, replacement)
		}
	}
	catalogSnapshotExec(t, store, "CREATE TABLE lake.unknown_change (id BIGINT)")
	assertCatalogSnapshotValid(t, store, current.Snapshot, false)
	replacement := readCatalogSnapshot(t, store, &current.Snapshot)
	if !replacement.Replace || !slices.Equal(replacement.Rows, current.Rows) {
		t.Fatalf("unrecognized metadata changes did not rebuild: %+v", replacement)
	}
}

func TestDuckLakeCatalogSnapshotSummaryCapacity(t *testing.T) {
	t.Parallel()
	store := openLakeStorageTest(t, t.TempDir(), 1)
	base := readCatalogSnapshot(t, store, nil)
	catalogSnapshotExec(t, store, `INSERT INTO lake.current_events(id,event,uuid,identity_snapshot,received_at,timestamp)
SELECT i, 'event-' || i, 'uuid-' || i, '', '2026-11-13T10:00:00Z', '2026-11-13T10:00:00Z'
FROM range(1,`+strconv.Itoa(maxCatalogSummaryNames+2)+`) t(i)`)
	for _, checkpoint := range []*ExportSnapshot{nil, &base.Snapshot} {
		if result, err := store.ReadCatalogSnapshot(t.Context(), snapshotCatalogProject, checkpoint); !errors.Is(err, ErrCatalogSummaryTooLarge) || len(result.Rows) != 0 || result.Snapshot.CatalogID != "" {
			t.Fatalf("large catalog returned a partial summary: %+v, %v", result, err)
		}
	}
}

func readCatalogSnapshot(t *testing.T, store *DuckLakeStore, previous *ExportSnapshot) CatalogSnapshotRead {
	t.Helper()
	result, err := store.ReadCatalogSnapshot(t.Context(), snapshotCatalogProject, previous)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func assertCatalogSnapshotValid(t *testing.T, store *DuckLakeStore, previous ExportSnapshot, want bool) {
	t.Helper()
	if valid, err := store.CatalogSnapshotValid(t.Context(), snapshotCatalogProject, previous); err != nil || valid != want {
		t.Fatalf("checkpoint %+v validity = %t, want %t, err = %v", previous, valid, want, err)
	}
}

func applyCatalogProjections(t *testing.T, store *DuckLakeStore, items ...EventProjection) {
	t.Helper()
	if err := store.Apply(t.Context(), items); err != nil {
		t.Fatal(err)
	}
}

func catalogSnapshotExec(t *testing.T, store *DuckLakeStore, query string) {
	t.Helper()
	ctx := t.Context()
	p, release, err := store.LeaseProject(ctx, snapshotCatalogProject)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if _, err := p.DB.ExecContext(ctx, query); err != nil {
		t.Fatal(err)
	}
}

func assertSummaryMatchesCatalog(t *testing.T, store *DuckLakeStore, summary CatalogSnapshotRead) {
	t.Helper()
	catalog, err := store.Catalog(t.Context(), EventCatalogInput{ID: snapshotCatalogProject})
	if err != nil {
		t.Fatal(err)
	}
	var events []EventCatalogEvent
	for _, row := range summary.Rows {
		events = append(events, row.Event)
	}
	if !slices.Equal(catalog.Events, events) {
		t.Fatalf("summary differs from authoritative catalog: summary %+v, catalog %+v", events, catalog.Events)
	}
}
