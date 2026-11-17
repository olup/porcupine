package lake

import (
	"context"
	"errors"
	"fmt"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

func TestDuckLakeInlineFlushCreatesRollbackSafeConflict(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	s := openLakeStorageTest(t, t.TempDir(), 1)
	event := lakeStorageProjection(t, "a", "event", 1)
	if err := s.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.current_events WHERE uuid='event'"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.DB.ExecContext(ctx, "CALL ducklake_flush_inlined_data('lake')"); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	var native *duckdb.Error
	if !errors.As(err, &native) || native.Type != duckdb.ErrorTypeTransaction {
		t.Fatal("inline flush did not produce a typed transaction conflict", err)
	}
	t.Logf("rollback-safe native conflict: %v", err)
	if count := lakeStorageCount(t, s, "a", "events"); count != 1 {
		t.Fatal("failed mutation changed committed event visibility", count)
	}
	corrected := event
	corrected.Revision = 1
	corrected.Event.Event = "corrected"
	if err := s.Apply(ctx, []EventProjection{corrected}); err != nil {
		t.Fatal("new transaction could not replay the correction", err)
	}
	if current, err := s.Load(ctx, "a", "event"); err != nil || current.Revision != 1 || current.Event.Event != "corrected" {
		t.Fatal("replayed correction lost its revision", current, err)
	}
}

func TestDuckLakeLocalMutationAndInlineMaintenanceConverge(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	s := openLakeStorageTest(t, t.TempDir(), 1)
	event := lakeStorageProjection(t, "a", "event", 1)
	if err := s.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	p, release, err := s.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	finished := make(chan struct{})
	var maintenanceErr error
	go func() {
		defer close(finished)
		for ctx.Err() == nil {
			var stats DuckLakeMaintenanceStats
			if err := p.MaintenanceCommand(ctx, "CALL ducklake_flush_inlined_data('lake')", &stats); err != nil {
				if ctx.Err() == nil {
					maintenanceErr = err
				}
				return
			}
		}
	}()
	defer func() {
		cancel()
		<-finished
		if maintenanceErr != nil {
			var native *duckdb.Error
			if errors.As(maintenanceErr, &native) {
				t.Errorf("concurrent inline maintenance failed, native type=%v: %v", native.Type, maintenanceErr)
			} else {
				t.Error("concurrent inline maintenance failed", maintenanceErr)
			}
		}
	}()
	for revision := uint64(1); revision <= 40; revision++ {
		event.Revision = revision
		event.Event.Event = fmt.Sprintf("revision-%d", revision)
		if err := s.Apply(ctx, []EventProjection{event}); err != nil {
			t.Fatal("concurrent local mutation failed", revision, err)
		}
	}
	event.Revision++
	event.IsDeleted = true
	if err := s.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal("concurrent local deletion failed", err)
	}
	if err := s.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "event", 1)}); err != nil {
		t.Fatal("stale original retry failed", err)
	}
	if count := lakeStorageCount(t, s, "a", "events"); count != 0 {
		t.Fatal("maintenance or stale replay resurrected the deletion", count)
	}
	if tombstone, err := s.Load(ctx, "a", "event"); err != nil || !tombstone.IsDeleted || tombstone.Revision != 41 {
		t.Fatal("concurrent cleanup lost the deletion revision", tombstone, err)
	}
}
