package lake

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDuckLakeCurrentParquetUsageExcludesObsoleteAndUnknownProjects(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	if _, e := s.CurrentParquetBytes(ctx, "unknown"); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal(e)
	}
	var catalogs int
	s.Registry.QueryRow(`SELECT COUNT(*) FROM catalogs`).Scan(&catalogs)
	if catalogs != 0 {
		t.Fatal("usage sampling created a catalog")
	}
	original := lakeStorageProjection(t, "a", "one", 1)
	if e := s.Apply(ctx, []EventProjection{original}); e != nil {
		t.Fatal(e)
	}
	flushCleanupProject(t, s, "a")
	before, e := s.CurrentParquetBytes(ctx, "a")
	if e != nil || before <= 0 {
		t.Fatal(before, e)
	}
	// A new managed base table must contribute without a hand-maintained list.
	// Checking its exact independently queried increment prevents omitted current
	// files from being mistaken for obsolete physical files later in this test.
	p, release, e := s.LeaseProject(ctx, "a")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = p.DB.Exec(`CREATE TABLE lake.extra_numeric_evidence(n INTEGER); INSERT INTO lake.extra_numeric_evidence VALUES(1)`); e != nil {
		release()
		t.Fatal(e)
	}
	if e = p.MaintenanceCommand(ctx, `CALL ducklake_flush_inlined_data('lake')`, &DuckLakeMaintenanceStats{}); e != nil {
		release()
		t.Fatal(e)
	}
	var extra int64
	if e = p.DB.QueryRow(`SELECT COALESCE(SUM(data_file_size_bytes),0) FROM ducklake_list_files('lake','extra_numeric_evidence')`).Scan(&extra); e != nil || extra <= 0 {
		release()
		t.Fatal(extra, e)
	}
	release()
	withExtra, e := s.CurrentParquetBytes(ctx, "a")
	if e != nil || withExtra != before+extra {
		t.Fatal("new managed table Parquet was omitted", before, extra, withExtra, e)
	}
	edited := original
	edited.Revision = 1
	edited.Event.Event = "corrected"
	if e = s.Apply(ctx, []EventProjection{edited}); e != nil {
		t.Fatal(e)
	}
	flushCleanupProject(t, s, "a")
	p, release, e = s.LeaseProject(ctx, "a")
	if e != nil {
		t.Fatal(e)
	}
	if e = p.MaintenanceCommand(ctx, `CALL ducklake_rewrite_data_files('lake',delete_threshold=>0,max_compacted_files=>100)`, &DuckLakeMaintenanceStats{}); e != nil {
		release()
		t.Fatal(e)
	}
	release()
	current, e := s.CurrentParquetBytes(ctx, "a")
	if e != nil || current <= 0 {
		t.Fatal(current, e)
	}
	var physical int64
	if e = filepath.WalkDir(s.ProjectDirectory("a"), func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			info, e := d.Info()
			if e != nil {
				return e
			}
			physical += info.Size()
		}
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if physical <= current {
		t.Fatalf("obsolete Parquet was included or fixture did not retain history: current=%d physical=%d", current, physical)
	}
	// Checkpoint verification uses the same complete inventory with its archive
	// alias; it must retain and verify the added table as well as the core tables.
	checkpoint, e := s.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	if e = checkpoint.Close(); e != nil {
		t.Fatal(e)
	}
}

func TestDuckLakeS3SharedPostgresCurrentParquetUsage(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx, writer, sampler, _ := sharedNativeStores(t)
	const project = "metered-project"
	if _, err := sampler.HostedCurrentParquetBytes(ctx, project); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("unknown project's Parquet usage: %v", err)
	}
	projection := lakeStorageProjection(t, project, "one", 1)
	if err := writer.Apply(ctx, []EventProjection{projection}); err != nil {
		t.Fatal(err)
	}
	projection.Revision++
	projection.Event.Event = "corrected"
	if err := writer.Apply(ctx, []EventProjection{projection}); err != nil {
		t.Fatal(err)
	}

	p, release, err := writer.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	current, err := p.currentParquetBytes(ctx)
	if err != nil || current <= 0 {
		t.Fatalf("DuckLake current-file inventory = %d bytes: %v", current, err)
	}
	got, err := sampler.HostedCurrentParquetBytes(ctx, project)
	if err != nil || got != current {
		t.Fatalf("metadata sampler = %d bytes, want DuckLake current-file total %d: %v", got, current, err)
	}
	if len(sampler.Projects) != 0 {
		t.Fatal("metadata sampler opened an analytical engine")
	}
}
