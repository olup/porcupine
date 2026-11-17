package lake

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeCleanupReclaimsDeletedPayloadAndPreservesRevisions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 2)
	original := lakeStorageProjection(t, "a", "deleted", 1)
	original.Properties = json.RawMessage(`{"marker":"obsolete-cleanup-payload"}`)
	survivor := lakeStorageProjection(t, "a", "survivor", 2)
	other := lakeStorageProjection(t, "b", "deleted", 3)
	if err := s.Apply(ctx, []EventProjection{original, survivor, other}); err != nil {
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
	if err := s.Apply(ctx, []EventProjection{deleted, edited}); err != nil {
		t.Fatal(err)
	}
	// Advance the scheduler clock instead of sleeping for retention/grace windows.
	now := time.Now().Add(10 * 24 * time.Hour)
	if err := s.CleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	// Repeat subsequent due cycles to exercise idempotent cleanup.
	if err := s.CleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range before {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("old file was not reclaimed: %s (%v)", filepath.Base(path), err)
		}
	}
	assertCleanupPayloadAbsent(t, s.ProjectDirectory("a"), "obsolete-cleanup-payload")
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
	if err := s.Apply(ctx, []EventProjection{original, survivor}); err != nil {
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
	t.Parallel()
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	e := lakeStorageProjection(t, "a", "old", 1)
	if err := s.Apply(ctx, []EventProjection{e}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, s, "a")
	oldFiles := cleanupEventFiles(t, s, "a")
	p, release, err := s.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	// This lease represents the full lifetime of a streaming query/export.
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
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
	if err := s.Apply(ctx, []EventProjection{e}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(10 * 24 * time.Hour)
	if err := s.CleanupDue(ctx, now); err != nil {
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
	_, newRelease, err := s.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	newRelease()
	if err := s.CleanupDue(ctx, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, now.Add(3*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("released reader kept obsolete file: %v", err)
		}
	}
}

func TestDuckLakeCleanupColdCatalogsDisableAndInterruptedRecovery(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 1)
	for i, id := range []string{"a", "b", "c"} {
		if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, id, id, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	s.Config.DisableCleanup = true
	if err := s.CleanupDue(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.Enabled || status.LastAttempt != "" {
		t.Fatal("disabled cleanup ran")
	}
	s.Config.DisableCleanup = false
	// Both resident and cold registered catalogs must progress with one engine slot.
	now := time.Now()
	for range 3 {
		if err := s.CleanupDue(ctx, now); err != nil {
			t.Fatal(err)
		}
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.CompletedCatalogs != 3 || len(s.Projects) != 1 {
		t.Fatalf("cold catalog sweep: %+v %v", status, err)
	}
	if _, err := s.Registry.Exec("UPDATE cleanup_progress SET state='running',next_attempt=? WHERE project_id='a'", now.Add(time.Hour).UnixMilli()); err != nil {
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
	if err := s.CleanupDue(ctx, now.Add(time.Minute)); err != nil {
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
		if err := s.CleanupDue(ctx, now.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(s.ProjectDirectory("a")); !os.IsNotExist(err) {
		t.Fatal("cleanup resurrected deleted project")
	}
	status, err = s.CleanupStatus(ctx)
	if err != nil || status.RegisteredCatalogs != 2 {
		t.Fatal("deleted project left cleanup state")
	}
}

func TestDuckLakeCleanupRetentionAndObsoleteFileGrace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	original := lakeStorageProjection(t, "a", "deleted", 1)
	if err := s.Apply(ctx, []EventProjection{original}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, s, "a")
	oldFiles := cleanupEventFiles(t, s, "a")
	deleted := original
	deleted.Revision = 1
	deleted.IsDeleted = true
	if err := s.Apply(ctx, []EventProjection{deleted}); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, path := range oldFiles {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("default snapshot retention erased recent history", err)
		}
	}
	p, release, err := s.LeaseProject(ctx, "a")
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
	t.Parallel()
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	for i, id := range []string{"a", "b"} {
		if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, id, id, int64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	_, release, err := s.LeaseProject(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if err := s.CleanupDue(ctx, now); err != nil {
		t.Fatal("busy budget should defer, not fail cleanup", err)
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 0 {
		t.Fatal("busy budget reported storage failure", err)
	}
	if err := s.CleanupDue(ctx, now); err != nil {
		t.Fatal(err)
	}
	release()
	// a is cold. Removing its catalog must fail closed, not initialize an empty one.
	path := filepath.Join(s.ProjectDirectory("a"), "catalog.ducklake")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, now.Add(2*time.Minute)); err == nil {
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
	_, release, err = s.LeaseProject(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupDue(ctx, now.Add(4*time.Minute)); err != nil {
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
	t.Parallel()
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "a", 1)}); err != nil {
		t.Fatal(err)
	}
	p := s.Projects["a"]
	if err := p.maintenance.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	err := s.CleanupDue(deadline, time.Now())
	p.maintenance.Unlock()
	if err == nil {
		t.Fatal("blocked cleanup ignored its deadline")
	}
	status, err := s.CleanupStatus(ctx)
	if err != nil || status.FailedCatalogs != 1 || status.RunningCatalogs != 0 {
		t.Fatal("cancelled pass was marked successful", err)
	}
	if err := s.CleanupDue(ctx, time.Now().Add(2*time.Minute)); err != nil {
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
