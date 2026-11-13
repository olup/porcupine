//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openLakeStorageTest(t *testing.T, directory string, maxOpen int) *duckLakeStore {
	t.Helper()
	lake, err := newDuckLake(DuckLakeConfig{Directory: directory, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: maxOpen})
	if err != nil {
		t.Fatal(err)
	}
	s := lake.(*duckLakeStore)
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func lakeStorageProjection(t *testing.T, project, uuid string, id int64) eventProjection {
	t.Helper()
	e := Event{ID: id, ProjectID: project, UUID: uuid, Event: "page_viewed", DistinctID: "anonymous", Properties: json.RawMessage(`{"page":"/home"}`), Timestamp: "2026-11-13T10:00:00Z", ReceivedAt: "2026-11-13T10:00:01Z"}
	snapshot, err := originalSnapshot(e)
	if err != nil {
		t.Fatal(err)
	}
	return eventProjection{Event: e, OriginalSnapshot: snapshot}
}

func lakeStorageCount(t *testing.T, s *duckLakeStore, project, table string) int {
	t.Helper()
	p, release, err := s.leaseProject(context.Background(), project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var count int
	if err := p.db.QueryRow("SELECT count(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestDuckLakeStorageRevisionsRestartAndIsolation(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 2)
	a := lakeStorageProjection(t, "project-a", "same-uuid", 1)
	b := lakeStorageProjection(t, "project-b", "same-uuid", 2)
	batch := []eventProjection{a, b}
	for range 2 {
		if err := s.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	for _, project := range []string{"project-a", "project-b"} {
		if n := lakeStorageCount(t, s, project, "events"); n != 1 {
			t.Fatalf("%s retry count=%d", project, n)
		}
	}
	edit := a
	edit.Revision = 1
	edit.Event.Event = "corrected"
	edit.Properties = json.RawMessage(`{"corrected":true}`)
	if err := s.Apply(ctx, []eventProjection{edit, a, edit}); err != nil {
		t.Fatal(err)
	}
	current, err := s.Load(ctx, "project-a", a.UUID)
	if err != nil || current.Revision != 1 || current.Event.Event != "corrected" {
		t.Fatalf("lost correction: %+v %v", current, err)
	}
	deleted := edit
	deleted.Revision = 2
	deleted.IsDeleted = true
	if err := s.Apply(ctx, []eventProjection{deleted, a, edit, deleted}); err != nil {
		t.Fatal(err)
	}
	current, err = s.Load(ctx, "project-a", a.UUID)
	if err != nil || !current.IsDeleted || current.ID != a.ID || current.Revision != 2 || len(current.Properties) != 0 || current.OriginalSnapshot != "" {
		t.Fatalf("invalid tombstone: %+v %v", current, err)
	}
	if n := lakeStorageCount(t, s, "project-a", "events"); n != 0 {
		t.Fatalf("deleted event visible: %d", n)
	}
	if n := lakeStorageCount(t, s, "project-a", "lake.current_events"); n != 0 {
		t.Fatalf("deleted payload still current: %d", n)
	}
	if n := lakeStorageCount(t, s, "project-a", "lake.heads"); n != 1 {
		t.Fatalf("missing retry fact: %d", n)
	}
	if n := lakeStorageCount(t, s, "project-b", "events"); n != 1 {
		t.Fatalf("other project affected: %d", n)
	}
	source, err := s.SourceID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLakeStorageTest(t, directory, 2)
	reopened, err := s.SourceID(ctx)
	if err != nil || reopened != source {
		t.Fatalf("source changed: %q %q %v", source, reopened, err)
	}
	if err := s.Apply(ctx, []eventProjection{a}); err != nil {
		t.Fatal(err)
	}
	current, err = s.Load(ctx, "project-a", a.UUID)
	if err != nil || !current.IsDeleted {
		t.Fatalf("restart resurrected event: %+v %v", current, err)
	}
	if err := s.DeleteProject(ctx, "project-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(s.projectDirectory("project-a")); !os.IsNotExist(err) {
		t.Fatalf("project files survived deletion: %v", err)
	}
	if n := lakeStorageCount(t, s, "project-b", "events"); n != 1 {
		t.Fatalf("project deletion crossed boundary: %d", n)
	}
}

func TestDuckLakeStorageIdentityVersioning(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 2)
	page := lakeStorageProjection(t, "project-a", "page", 1)
	newer := lakeStorageProjection(t, "project-a", "identify-new", 3)
	newer.Event.Event = "$identify"
	newer.IdentitySnapshot = `{"version":"3","identified_id":"user-1","traits":{"plan":"paid"},"links":[{"anonymous_id":"anonymous","identified_id":"user-1","version":"3"}]}`
	older := lakeStorageProjection(t, "project-a", "identify-old", 2)
	older.Event.Event = "$identify"
	older.IdentitySnapshot = `{"version":"2","identified_id":"user-1","traits":{"plan":"free"},"links":[{"anonymous_id":"anonymous","identified_id":"user-1","version":"2"}]}`
	if err := s.Apply(ctx, []eventProjection{page, newer, older, newer}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.leaseProject(ctx, "project-a")
	if err != nil {
		t.Fatal(err)
	}
	var person, traits string
	err = p.db.QueryRow("SELECT person_id,person_properties FROM events").Scan(&person, &traits)
	release()
	if err != nil || person != "user-1" || traits != `{"plan":"paid"}` {
		t.Fatalf("identity regressed: %q %q %v", person, traits, err)
	}
	if n := lakeStorageCount(t, s, "project-a", "events"); n != 1 {
		t.Fatalf("identity controls leaked as events: %d", n)
	}
	other := lakeStorageProjection(t, "project-b", "page", 4)
	if err := s.Apply(ctx, []eventProjection{other}); err != nil {
		t.Fatal(err)
	}
	p, release, err = s.leaseProject(ctx, "project-b")
	if err != nil {
		t.Fatal(err)
	}
	err = p.db.QueryRow("SELECT person_id,person_properties FROM events").Scan(&person, &traits)
	release()
	if err != nil || person != "anonymous" || traits != "{}" {
		t.Fatalf("identity crossed project: %q %q %v", person, traits, err)
	}
}

func TestDuckLakeStorageCatalogContinuityAndWorkingSet(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 1)
	if second, err := newDuckLake(DuckLakeConfig{Directory: directory}); err == nil {
		second.Close()
		t.Fatal("second writer acquired same root")
	}
	a := lakeStorageProjection(t, "a", "a", 1)
	b := lakeStorageProjection(t, "b", "b", 2)
	if err := s.Apply(ctx, []eventProjection{a}); err != nil {
		t.Fatal(err)
	}
	_, release, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, []eventProjection{b}); err == nil {
		release()
		t.Fatal("unbounded active project contexts")
	}
	release()
	if err := s.Apply(ctx, []eventProjection{b}); err != nil {
		t.Fatal(err)
	}
	if len(s.projects) != 1 {
		t.Fatalf("working set grew: %d", len(s.projects))
	}
	catalog := filepath.Join(s.projectDirectory("a"), "catalog.ducklake")
	if err := os.Rename(catalog, catalog+".removed"); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, []eventProjection{a}); err == nil {
		t.Fatal("missing archive recreated and acknowledged")
	}
	if n := lakeStorageCount(t, s, "b", "events"); n != 1 {
		t.Fatalf("failed archive affected other project: %d", n)
	}
}

func TestDuckLakeStorageChangedOpenCatalogAndLockdown(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	a := lakeStorageProjection(t, "a", "a", 1)
	if err := s.Apply(ctx, []eventProjection{a}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT * FROM read_blob('/etc/passwd')", "SELECT * FROM read_parquet('https://example.com/data.parquet')", "SET enable_external_access=true"} {
		if _, err := p.db.ExecContext(ctx, query); err == nil {
			release()
			t.Fatalf("native engine allowed escape: %s", query)
		}
	}
	release()
	catalog := filepath.Join(p.directory, "catalog.ducklake")
	if err := os.Rename(catalog, catalog+".removed"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(catalog, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx, "a", "a"); err == nil {
		t.Fatal("open catalog replacement was ignored")
	}
}

func TestDuckLakeStorageCatalogMarkerAndAtomicBatch(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	a := lakeStorageProjection(t, "a", "same", 1)
	conflict := a
	conflict.ID = 2
	if err := s.Apply(ctx, []eventProjection{a, conflict}); err == nil {
		t.Fatal("changed event identity accepted")
	}
	if n := lakeStorageCount(t, s, "a", "events"); n != 0 {
		t.Fatalf("failed project batch partially committed: %d", n)
	}
	if err := s.Apply(ctx, []eventProjection{a}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.leaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.db.ExecContext(ctx, "UPDATE lake.catalog_identity SET id='unrelated-catalog'")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, []eventProjection{lakeStorageProjection(t, "b", "b", 3)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, []eventProjection{a}); err == nil {
		t.Fatal("different catalog identity accepted after reopen")
	}
}

func TestDuckLakeStorageConcurrentLoadAndApply(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	original := lakeStorageProjection(t, "a", "same", 1)
	if err := s.Apply(ctx, []eventProjection{original}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	wg.Go(func() {
		for revision := uint64(1); revision <= 30; revision++ {
			edited := original
			edited.Revision = revision
			if err := s.Apply(ctx, []eventProjection{edited}); err != nil {
				failures <- err
				return
			}
		}
	})
	wg.Go(func() {
		for range 100 {
			if _, err := s.Load(ctx, "a", original.UUID); err != nil {
				failures <- err
				return
			}
		}
	})
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestDuckLakeStorageRegistryReplacementFailsClosed(t *testing.T) {
	ctx := context.Background()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	a := lakeStorageProjection(t, "a", "a", 1)
	if err := s.Apply(ctx, []eventProjection{a}); err != nil {
		t.Fatal(err)
	}
	registry := filepath.Join(s.root, "registry.sqlite")
	if err := os.Rename(registry, registry+".replaced"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registry, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(ctx, []eventProjection{a}); err == nil {
		t.Fatal("projection accepted replaced source registry")
	}
	if _, err := s.Load(ctx, "a", "a"); err == nil {
		t.Fatal("read accepted replaced source registry")
	}
	if err := s.DeleteProject(ctx, "a"); err == nil {
		t.Fatal("project purge accepted replaced source registry")
	}
	if err := s.Maintain(ctx); err == nil {
		t.Fatal("maintenance accepted replaced source registry")
	}
}

func TestDuckLakeStorageDeadlineWhileCatalogOpening(t *testing.T) {
	s := openLakeStorageTest(t, t.TempDir(), 1)
	if err := s.mu.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.mu.Unlock()
	for name, operation := range map[string]func(context.Context) error{
		"source":      func(ctx context.Context) error { _, err := s.SourceID(ctx); return err },
		"project":     func(ctx context.Context) error { _, _, err := s.leaseProject(ctx, "blocked"); return err },
		"delete":      func(ctx context.Context) error { return s.DeleteProject(ctx, "blocked") },
		"maintenance": s.Maintain,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("expected deadline, got %v", err)
				}
			case <-time.After(100 * time.Millisecond):
				t.Error("catalog opening hid the caller deadline")
			}
		})
	}
}
