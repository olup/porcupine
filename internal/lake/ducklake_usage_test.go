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
	edited := original
	edited.Revision = 1
	edited.Event.Event = "corrected"
	if e = s.Apply(ctx, []EventProjection{edited}); e != nil {
		t.Fatal(e)
	}
	flushCleanupProject(t, s, "a")
	p, release, e := s.LeaseProject(ctx, "a")
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
}
