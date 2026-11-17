package lake

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNativeRestoreRefusesUnknownApplicationVersionBeforeTargetWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := openLakeStorageTest(t, t.TempDir(), 1)
	if err := source.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "event", 1)}); err != nil {
		t.Fatal(err)
	}
	p, release, err := source.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.DB.ExecContext(ctx, "UPDATE lake.schema_version SET version=99"); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	checkpoint, err := source.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoint.Close()
	catalog := checkpoint.Catalogs[0]
	archivePath := filepath.Join(checkpoint.Directory, filepath.FromSlash(catalog.Path))
	before := catalogDigest(t, archivePath)
	reader := &LocalNativeBackupReader{directory: filepath.Dir(checkpoint.Directory)}
	manifest := NativeBackupManifest{ID: filepath.Base(checkpoint.Directory)}
	target := openLakeStorageTest(t, t.TempDir(), 1)
	if err := RestoreNativeProject(ctx, target, reader, manifest, catalog, checkpoint.Directory); err == nil {
		t.Fatal("restored unsupported application version with matching columns")
	}
	var catalogs int
	if err := target.Registry.QueryRowContext(ctx, "SELECT count(*) FROM catalogs").Scan(&catalogs); err != nil || catalogs != 0 {
		t.Fatal("rejected archive created target catalog metadata", catalogs, err)
	}
	if _, err := os.Stat(target.ProjectDirectory("a")); !os.IsNotExist(err) {
		t.Fatal("rejected archive created target project files", err)
	}
	if got := catalogDigest(t, archivePath); got != before {
		t.Fatal("rejecting newer archive changed catalog")
	}
}

func TestNativeRestoreCopiesKnownReorderedColumnsByName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := openLakeStorageTest(t, t.TempDir(), 1)
	want := lakeStorageProjection(t, "a", "event", 1)
	if err := source.Apply(ctx, []EventProjection{want}); err != nil {
		t.Fatal(err)
	}
	p, release, err := source.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.DB.ExecContext(ctx, "ALTER TABLE lake.heads RENAME COLUMN received_at TO reordered_received_at; ALTER TABLE lake.heads ADD COLUMN received_at VARCHAR; UPDATE lake.heads SET received_at=reordered_received_at; ALTER TABLE lake.heads DROP COLUMN reordered_received_at; ALTER TABLE lake.current_events RENAME COLUMN event TO reordered_event; ALTER TABLE lake.current_events ADD COLUMN event VARCHAR; UPDATE lake.current_events SET event=reordered_event; ALTER TABLE lake.current_events DROP COLUMN reordered_event"); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	checkpoint, err := source.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoint.Close()
	reader := &LocalNativeBackupReader{directory: filepath.Dir(checkpoint.Directory)}
	target := openLakeStorageTest(t, t.TempDir(), 1)
	if err := RestoreNativeProject(ctx, target, reader, NativeBackupManifest{ID: filepath.Base(checkpoint.Directory)}, checkpoint.Catalogs[0], checkpoint.Directory); err != nil {
		t.Fatal(err)
	}
	got, err := target.Load(ctx, "a", "event")
	if err != nil || got.ID != want.ID || got.Event.Event != want.Event.Event || got.ReceivedAt != want.ReceivedAt {
		t.Fatalf("reordered restore changed row: %+v %v", got, err)
	}
}
