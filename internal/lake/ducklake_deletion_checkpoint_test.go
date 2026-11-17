package lake

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDuckLakeDeletionTombstoneSurvivesCheckpoint(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 2)
	ctx := t.Context()
	if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "a", 1), lakeStorageProjection(t, "b", "b", 2)}); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteProject(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, "a", "a"); err == nil {
		t.Fatal("deleted project reopened")
	}
	if _, err := os.Stat(s.ProjectDirectory("a")); !os.IsNotExist(err) {
		t.Fatal("deleted project recreated a directory", err)
	}
	checkpoint, err := s.StageCheckpoint(ctx, func(_ context.Context, path string) error { return os.WriteFile(path, nil, 0600) })
	if err != nil {
		t.Fatal("terminal deletion blocked B backup", err)
	}
	if len(checkpoint.Catalogs) != 1 || checkpoint.Catalogs[0].ProjectID != "b" {
		t.Fatal("backup included deleted A or lost B", checkpoint.Catalogs)
	}
	backup := filepath.Join(t.TempDir(), "snapshot")
	if err := os.CopyFS(backup, os.DirFS(filepath.Join(checkpoint.Directory, "ducklake"))); err != nil {
		t.Fatal(err)
	}
	if err := checkpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(directory); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(directory, os.DirFS(backup)); err != nil {
		t.Fatal(err)
	}
	restored := openLakeStorageTest(t, directory, 2)
	if _, err := restored.Load(ctx, "a", "a"); err == nil {
		t.Fatal("restored snapshot lost A deletion fence")
	}
	if b, err := restored.Load(ctx, "b", "b"); err != nil || b.UUID != "b" {
		t.Fatal("restored snapshot lost B", b, err)
	}
	if _, err := restored.Registry.Exec(`INSERT INTO deleting_projects VALUES('uncataloged-pending')`); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := restored.StageCheckpoint(ctx, func(_ context.Context, path string) error { return os.WriteFile(path, nil, 0600) }); err == nil {
		checkpoint.Close()
		t.Fatal("pending deletion without a catalog was mistaken for terminal deletion")
	}
}
