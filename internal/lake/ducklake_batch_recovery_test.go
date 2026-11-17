package lake

import (
	"context"
	"errors"
	"testing"
)

func TestDuckLakeBatchNonAllocationFailureKeepsTransactionAtomic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	lake := openLakeStorageTest(t, t.TempDir(), 1)
	existing := lakeStorageProjection(t, "project", "existing", 1)
	if err := lake.Apply(ctx, []EventProjection{existing}); err != nil {
		t.Fatal(err)
	}
	fresh := lakeStorageProjection(t, "project", "fresh", 2)
	conflicting := existing
	conflicting.ID = 3
	err := lake.Apply(ctx, []EventProjection{fresh, conflicting})
	if err == nil {
		t.Fatal("changed stable event ID was accepted")
	}
	if n := lakeStorageCount(t, lake, "project", "events"); n != 1 {
		t.Fatalf("non-allocation failure split the transaction: rows=%d", n)
	}
	if _, err := lake.Load(ctx, "project", fresh.UUID); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("failed transaction applied its healthy prefix: %v", err)
	}
}
