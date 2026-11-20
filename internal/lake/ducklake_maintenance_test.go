package lake

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDuckLakeMaintenanceDoesNotTakeProjectionLock(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	s := openLakeStorageTest(t, t.TempDir(), 1)
	for batch := 0; batch < 20; batch++ {
		var projections []EventProjection
		for row := 0; row < 25; row++ {
			i := batch*25 + row
			projections = append(projections, lakeStorageProjection(t, "unlocked", fmt.Sprintf("unlocked-%d", i), int64(i+1)))
		}
		if err := s.Apply(context.Background(), projections); err != nil {
			t.Fatal(err)
		}
	}
	p, release, err := s.LeaseProject(context.Background(), "unlocked")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	before, err := lakeMaintenanceFileCount(context.Background(), p, "current_events")
	if err != nil || before < 2 {
		t.Fatalf("missing compaction fixture %d %v", before, err)
	}
	if err := p.write.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer p.write.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Maintain(ctx); err != nil {
		t.Fatalf("maintenance blocked by projection lock: %v", err)
	}
	after, err := lakeMaintenanceFileCount(ctx, p, "current_events")
	if err != nil || after >= before {
		t.Fatalf("default maintenance made no progress: %d -> %d %v", before, after, err)
	}
	var count int
	if err := p.DB.QueryRowContext(ctx, "SELECT count(*) FROM events").Scan(&count); err != nil || count != 500 {
		t.Fatalf("maintenance changed rows: %d %v", count, err)
	}
}

func TestDuckLakeMaintenanceSkipsIdleCatalogsAndResumes(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	directory := t.TempDir()
	s := openLakeStorageTest(t, directory, 4)
	for _, project := range []string{"a", "b", "c", "d"} {
		_, release, err := s.LeaseProject(ctx, project)
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	pass := func() []DuckLakeMaintenanceStats {
		t.Helper()
		stats, err := s.maintainProjects(ctx, 1, false, 1<<20, true)
		if err != nil {
			t.Fatal(err)
		}
		return stats
	}
	drain := func() {
		t.Helper()
		for range 8 {
			if len(pass()) == 0 {
				return
			}
		}
		t.Fatal("maintenance did not settle")
	}
	drain()
	passes := 0
	for range 30 {
		passes += len(pass())
	}
	t.Logf("30 idle ticks, 4 resident catalogs: %d completed project passes, %d native procedure calls", passes, 2*passes)
	if passes != 0 {
		t.Fatalf("clean catalogs ran %d maintenance passes", passes)
	}
	if err := s.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	if status, err := s.CleanupStatus(ctx); err != nil || status.CompletedCatalogs != 1 {
		t.Fatalf("idle maintenance did not run scheduled history cleanup: %+v %v", status, err)
	}

	event := lakeStorageProjection(t, "a", "first-after-idle", 1)
	if err := s.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	if current, err := s.Load(ctx, "a", event.UUID); err != nil || current.ID != event.ID {
		t.Fatalf("first write after idle was not visible before maintenance: %+v %v", current, err)
	}
	if stats := pass(); len(stats) != 1 || !stats[0].Completed {
		t.Fatalf("first write after idle did not schedule one project: %+v", stats)
	}
	drain()

	event.Revision = 1
	event.IsDeleted = true
	if err := s.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openLakeStorageTest(t, directory, 4)
	if current, err := s.Load(ctx, "a", event.UUID); err != nil || !current.IsDeleted || current.Revision != 1 {
		t.Fatalf("reopening lost the deletion: %+v %v", current, err)
	}
	if stats := pass(); len(stats) != 1 || !stats[0].Completed {
		t.Fatalf("reopened catalog did not get a maintenance pass: %+v", stats)
	}
	drain()
}
