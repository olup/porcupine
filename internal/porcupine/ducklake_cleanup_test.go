//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/fs"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func cleanupParquetFiles(t *testing.T, directory string) []string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return paths
}

// Inspect decoded Parquet rows, not compressed file bytes, to prove that the
// deleted payload is no longer in any managed data file.
func assertCleanupPayloadAbsent(t *testing.T, directory, marker string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	files := cleanupParquetFiles(t, directory)
	if len(files) == 0 {
		t.Fatal("expected surviving Parquet data")
	}
	for _, path := range files {
		var found int
		if err := db.QueryRow("SELECT count(*) FROM read_parquet(?) t WHERE contains(to_json(t)::VARCHAR,?)", path, marker).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found != 0 {
			t.Fatalf("deleted payload still in %s", filepath.Base(path))
		}
	}
}

func cleanupEventFiles(t *testing.T, s *duckLakeStore, id string) []string {
	t.Helper()
	p, release, err := s.leaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := p.db.Query("SELECT data_file FROM ducklake_list_files('lake','current_events')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("expected event data file")
	}
	return paths
}

func flushCleanupProject(t *testing.T, s *duckLakeStore, id string) {
	t.Helper()
	p, release, err := s.leaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := p.maintenanceCommand(context.Background(), "CALL ducklake_flush_inlined_data('lake')", &duckLakeMaintenanceStats{}); err != nil {
		t.Fatal(err)
	}
}

func TestDuckLakeCleanupReclaimsDeletedPayloadAndPreservesRevisions(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 2)
	original := lakeStorageProjection(t, "a", "deleted", 1)
	original.Properties = json.RawMessage(`{"marker":"obsolete-cleanup-payload"}`)
	survivor := lakeStorageProjection(t, "a", "survivor", 2)
	other := lakeStorageProjection(t, "b", "deleted", 3)
	if err := s.Apply(ctx, []eventProjection{original, survivor, other}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, s, "a")
	before := cleanupEventFiles(t, s, "a")
	deleted := original
	deleted.Revision = 1
	deleted.IsDeleted = true
	edited := survivor
	edited.Revision = 1
	edited.Event.Event = "corrected"
	if err := s.Apply(ctx, []eventProjection{deleted, edited}); err != nil {
		t.Fatal(err)
	}
	// Advance the scheduler clock instead of sleeping for retention/grace windows.
	now := time.Now().Add(10 * 24 * time.Hour)
	if err := s.cleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	// Repeat subsequent due cycles to exercise idempotent cleanup.
	if err := s.cleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range before {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old file was not reclaimed: %s (%v)", filepath.Base(path), err)
		}
	}
	assertCleanupPayloadAbsent(t, s.projectDirectory("a"), "obsolete-cleanup-payload")
	current, err := s.Load(ctx, "a", "survivor")
	if err != nil || current.Revision != 1 || current.Event.Event != "corrected" {
		t.Fatalf("correction lost: %+v %v", current, err)
	}
	if lakeStorageCount(t, s, "a", "events") != 1 || lakeStorageCount(t, s, "b", "events") != 1 {
		t.Fatal("cleanup lost a survivor or crossed projects")
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.CompletedCatalogs != 2 || status.FailedCatalogs != 0 {
		t.Fatalf("cleanup status: %+v %v", status, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLakeStorageTest(t, directory, 2)
	if err := s.Apply(ctx, []eventProjection{original, survivor}); err != nil {
		t.Fatal(err)
	}
	tombstone, err := s.Load(ctx, "a", "deleted")
	if err != nil || !tombstone.IsDeleted || tombstone.OriginalSnapshot != "" {
		t.Fatalf("cleanup/restart lost retry tombstone: %+v %v", tombstone, err)
	}
	current, err = s.Load(ctx, "a", "survivor")
	if err != nil || current.Event.Event != "corrected" {
		t.Fatal("retry overwrote correction")
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.CompletedCatalogs != 2 {
		t.Fatal("cleanup progress did not survive restart")
	}
}

func TestDuckLakeCleanupProtectsActiveSnapshot(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	e := lakeStorageProjection(t, "a", "old", 1)
	if err := s.Apply(ctx, []eventProjection{e}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, s, "a")
	oldFiles := cleanupEventFiles(t, s, "a")
	p, release, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	// This lease represents the full lifetime of a streaming query/export.
	defer release()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil || count != 1 {
		t.Fatal("reader did not see original snapshot", err)
	}
	e.Revision = 1
	e.IsDeleted = true
	if err := s.Apply(ctx, []eventProjection{e}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(10 * 24 * time.Hour)
	if err := s.cleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := tx.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil || count != 1 {
		t.Fatal("cleanup broke active snapshot", err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("cleanup removed a leased reader's file", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	release()
	// A subsequent short lease must not retain the released reader's horizon.
	_, newRelease, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	newRelease()
	if err := s.cleanupDue(ctx, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("released reader kept obsolete file: %v", err)
		}
	}
}

func TestDuckLakeCleanupColdCatalogsDisableAndInterruptedRecovery(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 1)
	for i, id := range []string{"a", "b", "c"} {
		if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, id, id, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	s.config.DisableCleanup = true
	if err := s.cleanupDue(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.Enabled || status.LastAttempt != "" {
		t.Fatal("disabled cleanup ran")
	}
	s.config.DisableCleanup = false
	// Both resident and cold registered catalogs must progress with one engine slot.
	now := time.Now()
	for range 3 {
		if err := s.cleanupDue(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.CompletedCatalogs != 3 || len(s.projects) != 1 {
		t.Fatalf("cold catalog sweep: %+v %v", status, err)
	}
	if _, err := s.registry.Exec("UPDATE cleanup_progress SET state='running',next_attempt=? WHERE project_id='a'", now.Add(time.Hour).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLakeStorageTest(t, directory, 1)
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 1 || status.RunningCatalogs != 0 {
		t.Fatalf("interrupted cycle not reported: %+v %v", status, err)
	}
	if err := s.cleanupDue(ctx, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 0 {
		t.Fatal("interrupted cycle was not retried", err)
	}
	if err := s.DeleteProject(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := s.cleanupDue(ctx, now.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(s.projectDirectory("a")); !os.IsNotExist(err) {
		t.Fatal("cleanup resurrected deleted project")
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.RegisteredCatalogs != 2 {
		t.Fatal("deleted project left cleanup state")
	}
}

func TestDuckLakeCleanupStatusAgentPermissionsAndLifecycle(t *testing.T) {
	s, a, ingest := maintenanceOpenService(t)
	b, otherIngest := project(t, s)
	_, reader := key(t, s, []string{"management:read"}, []string{a})
	state := lakeMaintenanceLifecycle(t, s, a, ingest, "cleanup", true)
	nativeRequest(t, s, otherIngest, `{"type":"track","message_id":"other","actor_id":"other","name":"Other"}`, 200)
	duckLakeJourneyDrain(t, s)
	lake := s.lake.(*duckLakeStore)
	now := time.Now().Add(10 * 24 * time.Hour)
	for range 2 {
		if err := lake.cleanupDue(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := lake.cleanupDue(context.Background(), now.Add(3*24*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	lakeMaintenanceVerify(t, s, a, state)
	if lakeStorageCount(t, lake, b, "events") != 1 {
		t.Fatal("other project's event changed")
	}
	admin := request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["ducklake_cleanup"].(map[string]any)["completed_catalogs"] != float64(2) {
		t.Fatalf("missing admin cleanup status: %v", admin)
	}
	scoped := request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if _, ok := scoped["ducklake_cleanup"]; ok {
		t.Fatal("project reader saw instance cleanup details")
	}
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, adminToken, "2025-11-25")
	result := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["ducklake_cleanup"] == nil {
		t.Fatal("MCP missing cleanup status")
	}
	ctx, session = mcpTestSession(t, host.URL, reader, "2025-11-25")
	result = mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-instance-status"}, 200)
	if result["ducklake_cleanup"] != nil {
		t.Fatal("MCP leaked cleanup status to scoped reader")
	}
	if _, err := lake.registry.Exec("UPDATE cleanup_progress SET state='failed',failed=1 WHERE project_id=?", a); err != nil {
		t.Fatal(err)
	}
	admin = request(t, s, "GET", "/api/v1/status", adminToken, "", 200)
	if admin["status"] != "degraded" || admin["ducklake_cleanup"].(map[string]any)["failed_catalogs"] != float64(1) {
		t.Fatal("cleanup failure missing from admin health")
	}
	scoped = request(t, s, "GET", "/api/v1/status", reader, "", 200)
	if scoped["status"] != "ok" || scoped["ducklake_cleanup"] != nil {
		t.Fatal("project status disclosed instance cleanup failure")
	}

}

func TestDuckLakeCleanupRetentionAndObsoleteFileGrace(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	original := lakeStorageProjection(t, "a", "deleted", 1)
	if err := s.Apply(ctx, []eventProjection{original}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, s, "a")
	oldFiles := cleanupEventFiles(t, s, "a")
	deleted := original
	deleted.Revision = 1
	deleted.IsDeleted = true
	if err := s.Apply(ctx, []eventProjection{deleted}); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("default snapshot retention erased recent history", err)
		}
	}
	p, release, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// Expire history while leaving the file grace horizon in the past.
	if err := p.cleanupHistory(ctx, time.Now().Add(time.Hour), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("obsolete file grace was ignored", err)
		}
	}
	if err := p.cleanupHistory(ctx, time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("obsolete file not removed after grace", err)
		}
	}
	// A healthy catalog should not rewrite clean files on every hourly pass.
	cleanFiles := strings.Join(cleanupParquetFiles(t, p.directory), "\n")
	if err := p.cleanupHistory(ctx, time.Now().Add(2*time.Hour), time.Now().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if cleanFiles != strings.Join(cleanupParquetFiles(t, p.directory), "\n") {
		t.Fatal("cleanup rewrote clean files without new deletion")
	}
}

func TestDuckLakeCleanupFailureAndBusyCatalogDoNotStarveOthers(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	for i, id := range []string{"a", "b"} {
		if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, id, id, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	_, release, err := s.leaseProject(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := s.cleanupDue(ctx, now); err != nil {
		t.Fatal("busy budget should defer, not fail cleanup", err)
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 0 {
		t.Fatal("busy budget reported storage failure", err)
	}
	if err := s.cleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	release()
	// a is cold. Removing its catalog must fail closed, not initialize an empty one.
	path := filepath.Join(s.projectDirectory("a"), "catalog.ducklake")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, now.Add(2*time.Minute)); err == nil {
		t.Fatal("missing registered catalog accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("cleanup recreated missing registered catalog")
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 1 || status.CompletedCatalogs != 1 {
		t.Fatalf("missing cleanup failure status: %+v %v", status, err)
	}
	// A busy retry must not clear a previously recorded storage failure.
	_, release, err = s.leaseProject(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.cleanupDue(ctx, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	release()
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 1 {
		t.Fatal("busy retry hid an unresolved failure", err)
	}
	// Retrying the failed catalog cannot erase the healthy project's progress/data.
	if lakeStorageCount(t, s, "b", "events") != 1 {
		t.Fatal("failed catalog affected another project")
	}
}

func TestDuckLakeCleanupCancellationIsRetriable(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, "a", "a", 1)}); err != nil {
		t.Fatal(err)
	}
	p := s.projects["a"]
	if err := p.maintenance.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err := s.cleanupDue(deadline, time.Now())
	p.maintenance.Unlock()
	if err == nil {
		t.Fatal("blocked cleanup ignored its deadline")
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 1 || status.RunningCatalogs != 0 {
		t.Fatal("cancelled pass was marked successful", err)
	}
	if err := s.cleanupDue(ctx, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 0 || status.CompletedCatalogs != 1 {
		t.Fatal("cancelled pass did not recover", err)
	}
	if lakeStorageCount(t, s, "a", "events") != 1 {
		t.Fatal("cancelled cleanup lost live data")
	}
}
