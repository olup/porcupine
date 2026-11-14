//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDuckLakeCheckpointStageRestore(t *testing.T) {
	root := t.TempDir()
	live := filepath.Join(root, "live")
	backup := filepath.Join(root, "backup")
	config := Config{DBPath: filepath.Join(live, "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(live, "lake"), MemoryLimitMB: 128, Threads: 1, MaintenanceInterval: 24 * time.Hour}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	s.deliveryCancel()
	<-s.deliveryDone
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	first := `{"type":"track","message_id":"checkpoint-history","name":"page_viewed","actor_id":"visitor"}`
	queued := `{"type":"track","message_id":"checkpoint-queued","name":"queued","actor_id":"visitor"}`
	firstID := receiptFrom(t, nativeRequest(t, s, ingest, first, 200), 0)["event_id"]
	duckLakeJourneyDrain(t, s)
	ctx := context.Background()
	lake := s.lake.(*duckLakeStore)
	flushCleanupProject(t, lake, a)
	queueID := receiptFrom(t, nativeRequest(t, s, ingest, queued, 200), 0)["event_id"]
	source, err := lake.SourceID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := lake.stageCheckpoint(ctx, func(ctx context.Context, path string) error {
		database, err := sql.Open("sqlite", config.DBPath)
		if err != nil {
			return err
		}
		defer database.Close()
		return snapshotSQLite(ctx, database, path)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(stage)
	// Staged data survives source cleanup/project removal through immutable links.
	if err := lake.DeleteProject(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(backup, os.DirFS(stage)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil
	if err := os.RemoveAll(live); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(live, 0700); err != nil {
		t.Fatal(err)
	}
	if err := copyCheckpointFile(ctx, filepath.Join(backup, "acceptance.sqlite"), config.DBPath); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(config.DuckLake.Directory, os.DirFS(filepath.Join(backup, "ducklake"))); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	duckLakeJourneyDrain(t, s)
	restored, err := s.lake.SourceID(ctx)
	if err != nil || restored != source {
		t.Fatal("recovery source identity changed", err)
	}
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT event_id,event FROM events ORDER BY id", nil)
	if len(rows) != 2 || rows[0].(map[string]any)["event_id"] != firstID || rows[1].(map[string]any)["event_id"] != queueID {
		t.Fatalf("checkpoint lost accepted events: %+v", rows)
	}
	retry := nativeRequest(t, s, ingest, first, 200)
	if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != firstID {
		t.Fatal("checkpoint lost retry identity")
	}
	request(t, s, "POST", "/api/v1/projects/"+b+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 403)
}

func TestDuckLakeCheckpointWaitsForReadersAndCleansFailedStage(t *testing.T) {
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	_, release, err := lake.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = lake.stageCheckpoint(deadline, func(context.Context, string) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("checkpoint did not respect reader deadline", err)
	}
	release()
	injected := errors.New("injected SQLite checkpoint failure")
	_, err = lake.stageCheckpoint(ctx, func(context.Context, string) error { return injected })
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	stages, err := filepath.Glob(filepath.Join(lake.root, ".checkpoint-*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("failed stage leaked: %v %v", stages, err)
	}
	if _, err := lake.Query(ctx, "a", "SELECT count(*) FROM events", nil); err != nil {
		t.Fatal("failed checkpoint prevented reopening", err)
	}
}

func TestDuckLakeCheckpointAbandonedStagesRemovedOnRestart(t *testing.T) {
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(directory, ".checkpoint-12345")
	if err := os.Mkdir(stale, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "old-payload"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	unrelated := filepath.Join(directory, ".checkpoint-not-managed")
	if err := os.Mkdir(unrelated, 0700); err != nil {
		t.Fatal(err)
	}
	s = openLakeStorageTest(t, directory, 1)
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("abandoned checkpoint retained data", err)
	}
	if _, err := os.Stat(unrelated); err != nil {
		t.Fatal("checkpoint recovery removed unrelated directory", err)
	}
}
