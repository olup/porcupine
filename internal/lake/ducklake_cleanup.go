package lake

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/apierr"
)

// cleanupDue processes one registered catalog per pass. Registration and the
// lease are checked under the same lock as DeleteProject: a stale selection
// must never recreate a project that was removed in the meantime.
func (s *DuckLakeStore) CleanupDue(ctx context.Context, now time.Time) error {
	// Hosted catalogs need the write fence; CleanupHostedDue owns them.
	if s.Config.DisableCleanup || s.Config.Hosted {
		return nil
	}
	started := time.Now()
	if err := s.cleanup.Lock(ctx); err != nil {
		return err
	}
	defer s.cleanup.Unlock()
	if err := s.Mu.Lock(ctx); err != nil {
		return err
	}
	if _, err := s.sourceIDLocked(ctx); err != nil {
		s.Mu.Unlock()
		return err
	}
	retry := min(s.Config.CleanupInterval, time.Minute)
	attempt, err := s.registry.claimCleanup(ctx, now, now.Add(retry))
	if errors.Is(err, sql.ErrNoRows) {
		s.Mu.Unlock()
		return nil
	}
	if err != nil {
		s.Mu.Unlock()
		return err
	}
	id := attempt.ProjectID
	// A future reader uses the current snapshot. Only readers that already hold
	// a lease can require files scheduled for deletion before this point.
	snapshotBefore, fileBefore := now.Add(-s.Config.SnapshotRetention), now.Add(-s.Config.CleanupGrace)
	if s.SharedCatalog != nil {
		// Remote exports can run for seven days. Local leases cannot describe
		// readers on other workers, so shared cleanup keeps a longer horizon.
		snapshotBefore = minTime(snapshotBefore, now.Add(-sharedNativeReaderHorizon))
		fileBefore = minTime(fileBefore, now.Add(-sharedNativeReaderHorizon))
	}
	if p := s.Projects[id]; p != nil {
		for lease := range p.leases {
			protected := lease.started.Add(-time.Second)
			if protected.Before(snapshotBefore) {
				snapshotBefore = protected
			}
			if protected.Before(fileBefore) {
				fileBefore = protected
			}
		}
	}
	p, release, err := s.leaseProjectLocked(ctx, id)
	if err != nil {
		state := "failed"
		var limit *apierr.Error
		if errors.As(err, &limit) && limit.Code == "active_project_limit" {
			state = "waiting"
		}
		record, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		recordErr := s.registry.finishCleanup(record, attempt, state, time.Time{}, time.Time{})
		cancel()
		s.Mu.Unlock()
		if state == "waiting" {
			return recordErr
		}
		return errors.Join(err, recordErr)
	}
	s.Mu.Unlock()
	defer release()
	err = p.cleanupHistory(ctx, snapshotBefore, fileBefore)
	// Persist an interrupted attempt even when the maintenance deadline expired.
	record, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err != nil {
		recordErr := s.registry.finishCleanup(record, attempt, "failed", time.Time{}, time.Time{})
		return errors.Join(err, recordErr)
	}
	completed := now.Add(time.Since(started))
	err = s.registry.finishCleanup(record, attempt, "ok", completed, completed.Add(s.Config.CleanupInterval))
	return err
}

func (p *ProjectLake) cleanupHistory(ctx context.Context, snapshotBefore, fileBefore time.Time) error {
	if err := p.maintenance.Lock(ctx); err != nil {
		return err
	}
	defer p.maintenance.Unlock()
	var stats DuckLakeMaintenanceStats
	defer func() {
		if !p.shared && (stats.RowsFlushed > 0 || stats.FilesProcessed > 0) {
			p.maintenancePending.Store(true)
		}
	}()
	// Partial files need rewriting even when fewer than 95% of their rows were
	// deleted. One native compaction operation per table bounds each pass.
	for _, statement := range []string{
		"CALL ducklake_flush_inlined_data('lake')",
		"CALL ducklake_rewrite_data_files('lake',delete_threshold=>0,max_compacted_files=>1)",
		"CALL ducklake_expire_snapshots('lake',older_than=>TIMESTAMPTZ " + LakeLiteral(snapshotBefore.UTC().Format(time.RFC3339Nano)) + ")",
		"CALL ducklake_cleanup_old_files('lake',older_than=>TIMESTAMPTZ " + LakeLiteral(fileBefore.UTC().Format(time.RFC3339Nano)) + ")",
	} {
		if err := p.MaintenanceCommand(ctx, statement, &stats); err != nil {
			return err
		}
	}
	return nil
}

func (s *DuckLakeStore) CleanupStatus(ctx context.Context) (DuckLakeCleanupStatus, error) {
	status := DuckLakeCleanupStatus{Enabled: !s.Config.DisableCleanup, SnapshotRetentionSeconds: int64(s.Config.SnapshotRetention / time.Second), ObsoleteFileGraceSeconds: int64(s.Config.CleanupGrace / time.Second), IntervalSeconds: int64(s.Config.CleanupInterval / time.Second)}
	if s.SharedCatalog != nil && !s.Config.Hosted {
		status.SnapshotRetentionSeconds = max(status.SnapshotRetentionSeconds, int64(sharedNativeReaderHorizon/time.Second))
		status.ObsoleteFileGraceSeconds = max(status.ObsoleteFileGraceSeconds, int64(sharedNativeReaderHorizon/time.Second))
	}
	if err := s.Mu.Lock(ctx); err != nil {
		return status, err
	}
	defer s.Mu.Unlock()
	if _, err := s.sourceIDLocked(ctx); err != nil {
		return status, err
	}
	summary, err := s.registry.cleanupSummary(ctx)
	status.RegisteredCatalogs, status.CompletedCatalogs = summary.Registered, summary.Completed
	status.FailedCatalogs, status.RunningCatalogs = summary.Failed, summary.Running
	status.SnapshotsExpired, status.FilesDeleted, status.LastError = summary.SnapshotsExpired, summary.FilesDeleted, summary.LastError
	attempted, succeeded := summary.LastAttempt, summary.LastSuccess
	if attempted > 0 {
		status.LastAttempt = time.UnixMilli(attempted).UTC().Format(time.RFC3339Nano)
	}
	if succeeded > 0 {
		status.LastSuccess = time.UnixMilli(succeeded).UTC().Format(time.RFC3339Nano)
	}
	return status, err
}
