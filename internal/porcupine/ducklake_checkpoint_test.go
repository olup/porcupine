package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeCheckpointStageRestore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	live := filepath.Join(root, "live")
	backup := filepath.Join(root, "backup")
	config := Config{DBPath: filepath.Join(live, "queue.db"), BootstrapToken: adminToken, DuckLake: &lakeapi.DuckLakeConfig{Directory: filepath.Join(live, "lake"), MemoryLimitMB: 128, Threads: 1, MaintenanceInterval: 24 * time.Hour}}
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
	lake := s.lake
	flushCleanupProject(t, lake, a)
	queueID := receiptFrom(t, nativeRequest(t, s, ingest, queued, 200), 0)["event_id"]
	source, err := lake.SourceID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := lake.StageCheckpoint(ctx, func(ctx context.Context, path string) error {
		for _, item := range []struct{ source, destination string }{
			{config.DBPath, path},
			{localControlPath(config.DBPath), filepath.Join(filepath.Dir(path), "control.sqlite")},
		} {
			database, err := sql.Open("sqlite", item.source)
			if err != nil {
				return err
			}
			err = errors.Join(lakeapi.SnapshotSQLite(ctx, database, item.destination), database.Close())
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	if len(stage.Catalogs) != 1 || stage.SourceID != source || stage.CapturedAt == "" {
		t.Fatal("checkpoint omitted catalog/source metadata")
	}
	// Staged data survives source cleanup/project removal through immutable links.
	if err := lake.DeleteProject(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(backup, os.DirFS(stage.Directory)); err != nil {
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
	if err := lakeapi.CopyCheckpointFile(ctx, filepath.Join(backup, "acceptance.sqlite"), config.DBPath); err != nil {
		t.Fatal(err)
	}
	if err := lakeapi.CopyCheckpointFile(ctx, filepath.Join(backup, "control.sqlite"), localControlPath(config.DBPath)); err != nil {
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
