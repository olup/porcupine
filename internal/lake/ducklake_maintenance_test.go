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
