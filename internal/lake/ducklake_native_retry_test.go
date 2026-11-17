package lake

import (
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
	attempts := 0
	err = retryNativeTransaction(ctx, func() error {
		attempts++
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, "UPDATE lake.current_events SET event='native-retry' WHERE uuid='event'"); err != nil {
			return err
		}
		if attempts == 1 {
			if _, err := p.DB.ExecContext(ctx, "CALL ducklake_flush_inlined_data('lake')"); err != nil {
				return err
			}
		}
		err = tx.Commit()
		if attempts == 1 {
			var native *duckdb.Error
			if !errors.As(err, &native) || native.Type != duckdb.ErrorTypeTransaction {
				t.Fatal("inline flush did not produce a typed transaction conflict", err)
			}
			t.Logf("rollback-safe native conflict: %v", err)
			var name string
			if readErr := p.DB.QueryRowContext(ctx, "SELECT event FROM events WHERE uuid='event'").Scan(&name); readErr != nil || name != event.Event.Event {
				t.Fatal("failed attempt exposed an uncommitted correction", name, readErr)
			}
		}
		return err
	})
	if err != nil || attempts != 2 {
		t.Fatal("real native conflict was not replayed as a new transaction", attempts, err)
	}
	var name string
	if err := p.DB.QueryRowContext(ctx, "SELECT event FROM events WHERE uuid='event'").Scan(&name); err != nil || name != "native-retry" {
		t.Fatal("successful retry did not commit the complete correction", name, err)
	}
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
	finished := make(chan struct{})
	started := make(chan struct{})
	stop := make(chan struct{})
	var maintenanceErr error
	var failedStats DuckLakeMaintenanceStats
	initialStats := DuckLakeMaintenanceStats{RowsFlushed: 7, FilesCreated: 3}
	go func() {
		defer close(finished)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			stats := initialStats
			if err := p.MaintenanceCommand(ctx, "CALL ducklake_flush_inlined_data('lake')", &stats); err != nil {
				maintenanceErr, failedStats = err, stats
				return
			}
		}
	}()
	stopped := false
	stopMaintenance := func() {
		if !stopped {
			close(stop)
			stopped = true
		}
		<-finished
	}
	defer stopMaintenance()
	<-started
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
	stopMaintenance()
	if maintenanceErr != nil {
		var exhausted *nativeConflictRetryLimit
		var native *duckdb.Error
		if !errors.As(maintenanceErr, &exhausted) || exhausted.attempts != 16 || !errors.As(maintenanceErr, &native) || native.Type != duckdb.ErrorTypeTransaction {
			t.Fatal("maintenance returned an unexpected failure", maintenanceErr)
		}
		if failedStats != initialStats {
			t.Fatal("exhausted maintenance published successful-attempt statistics", failedStats)
		}
		t.Logf("bounded maintenance conflict propagated: %v", maintenanceErr)
	}
	var quietStats DuckLakeMaintenanceStats
	if err := p.MaintenanceCommand(ctx, "CALL ducklake_flush_inlined_data('lake')", &quietStats); err != nil {
		t.Fatal("quiescent maintenance did not converge", err)
	}
	if count := lakeStorageCount(t, s, "a", "events"); count != 0 {
		t.Fatal("maintenance or stale replay resurrected the deletion", count)
	}
	if tombstone, err := s.Load(ctx, "a", "event"); err != nil || !tombstone.IsDeleted || tombstone.Revision != 41 {
		t.Fatal("concurrent cleanup lost the deletion revision", tombstone, err)
	}
}
