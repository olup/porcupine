package lake

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDuckLakeCheckpointWaitsForReadersAndCleansFailedStage(t *testing.T) {
	t.Parallel()
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	_, release, err := lake.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = lake.StageCheckpoint(deadline, func(context.Context, string) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("checkpoint did not respect reader deadline", err)
	}
	release()
	injected := errors.New("injected SQLite checkpoint failure")
	_, err = lake.StageCheckpoint(ctx, func(context.Context, string) error { return injected })
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	stages, err := filepath.Glob(filepath.Join(lake.Root, ".checkpoint-*"))
	if err != nil || len(stages) != 0 {
		t.Fatalf("failed stage leaked: %v %v", stages, err)
	}
	if _, err := lake.Query(ctx, "a", "SELECT count(*) FROM events", nil, DefaultQueryExecutionLimits()); err != nil {
		t.Fatal("failed checkpoint prevented reopening", err)
	}
}

func TestDuckLakeCheckpointAbandonedStagesRemovedOnRestart(t *testing.T) {
	t.Parallel()
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

func TestDuckLakeCheckpointKeepsWriterOpenDuringAcceptanceCapture(t *testing.T) {
	t.Parallel()
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	entered, resume := make(chan struct{}), make(chan struct{})
	injected := errors.New("acceptance snapshot interrupted")
	done := make(chan error, 1)
	go func() {
		_, err := lake.StageCheckpoint(context.Background(), func(context.Context, string) error {
			close(entered)
			<-resume
			return injected
		})
		done <- err
	}()
	<-entered
	closeErr := lake.Close()
	close(resume)
	if err := <-done; !errors.Is(err, injected) {
		t.Fatal("checkpoint lost its interrupted acceptance error", err)
	}
	if closeErr == nil {
		t.Fatal("store closed while the acceptance checkpoint was live")
	}
	if _, err := lake.Query(context.Background(), "a", "SELECT count(*) FROM events", nil, DefaultQueryExecutionLimits()); err != nil {
		t.Fatal("interrupted checkpoint retained its writer guard", err)
	}
}

func TestDuckLakeCheckpointRejectsPendingPartialDeletion(t *testing.T) {
	t.Parallel()
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	ctx := context.Background()
	if _, err := lake.Query(ctx, "a", "SELECT count(*) FROM events", nil, DefaultQueryExecutionLimits()); err != nil {
		t.Fatal(err)
	}
	if _, err := lake.Registry.ExecContext(ctx, "INSERT INTO deleting_projects VALUES('a')"); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := lake.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if err == nil {
		checkpoint.Close()
		t.Fatal("checkpoint accepted a catalog whose deletion was already in progress")
	}
	stages, err := filepath.Glob(filepath.Join(lake.Root, ".checkpoint-*"))
	if err != nil || len(stages) != 0 {
		t.Fatal("rejected partial deletion retained its checkpoint stage", err)
	}
	if _, err := lake.Registry.ExecContext(ctx, "DELETE FROM deleting_projects"); err != nil {
		t.Fatal(err)
	}
	if err := lake.Close(); err != nil {
		t.Fatal("rejected deletion retained its capture guard", err)
	}
}
