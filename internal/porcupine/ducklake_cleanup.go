//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// cleanupDue processes one registered catalog per pass. Registration and the
// lease are checked under the same lock as DeleteProject: a stale selection
// must never recreate a project that was removed in the meantime.
func (s *duckLakeStore) cleanupDue(ctx context.Context, now time.Time) error {
	if s.config.DisableCleanup {
		return nil
	}
	started := time.Now()
	if err := s.cleanup.Lock(ctx); err != nil {
		return err
	}
	defer s.cleanup.Unlock()
	if err := s.mu.Lock(ctx); err != nil {
		return err
	}
	if _, err := s.sourceIDLocked(ctx); err != nil {
		s.mu.Unlock()
		return err
	}
	var id string
	err := s.registry.QueryRowContext(ctx, `SELECT c.project_id FROM catalogs c
 LEFT JOIN cleanup_progress p ON p.project_id=c.project_id
 WHERE c.ready=1 AND (p.next_attempt IS NULL OR p.next_attempt<=?)
 ORDER BY COALESCE(p.last_attempt,0),c.project_id LIMIT 1`, now.UnixMilli()).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		s.mu.Unlock()
		return nil
	}
	if err != nil {
		s.mu.Unlock()
		return err
	}
	retry := min(s.config.CleanupInterval, time.Minute)
	_, err = s.registry.ExecContext(ctx, `INSERT INTO cleanup_progress(project_id,next_attempt,last_attempt,state)
 VALUES(?,?,?,'running') ON CONFLICT(project_id) DO UPDATE SET next_attempt=excluded.next_attempt,last_attempt=excluded.last_attempt,state='running'`, id, now.Add(retry).UnixMilli(), now.UnixMilli())
	if err != nil {
		s.mu.Unlock()
		return err
	}
	// A future reader uses the current snapshot. Only readers that already hold
	// a lease can require files scheduled for deletion before this point.
	snapshotBefore, fileBefore := now.Add(-s.config.SnapshotRetention), now.Add(-s.config.CleanupGrace)
	if p := s.projects[id]; p != nil {
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
		var limit *managementError
		if errors.As(err, &limit) && limit.Code == "active_project_limit" {
			state = "waiting"
		}
		record, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, recordErr := s.registry.ExecContext(record, "UPDATE cleanup_progress SET state=?,failed=CASE WHEN ?='failed' THEN 1 ELSE failed END WHERE project_id=?", state, state, id)
		cancel()
		s.mu.Unlock()
		if state == "waiting" {
			return recordErr
		}
		return errors.Join(err, recordErr)
	}
	s.mu.Unlock()
	defer release()
	err = p.cleanupHistory(ctx, snapshotBefore, fileBefore)
	// Persist an interrupted attempt even when the maintenance deadline expired.
	record, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err != nil {
		_, recordErr := s.registry.ExecContext(record, "UPDATE cleanup_progress SET state='failed',failed=1 WHERE project_id=?", id)
		return errors.Join(err, recordErr)
	}
	completed := now.Add(time.Since(started))
	_, err = s.registry.ExecContext(record, `UPDATE cleanup_progress SET state='ok',failed=0,last_success=?,next_attempt=? WHERE project_id=?`, completed.UnixMilli(), completed.Add(s.config.CleanupInterval).UnixMilli(), id)
	return err
}

func (p *projectLake) cleanupHistory(ctx context.Context, snapshotBefore, fileBefore time.Time) error {
	if err := p.maintenance.Lock(ctx); err != nil {
		return err
	}
	defer p.maintenance.Unlock()
	var stats duckLakeMaintenanceStats
	// Partial files need rewriting even when fewer than 95% of their rows were
	// deleted. One native compaction operation per table bounds each pass.
	for _, statement := range []string{
		"CALL ducklake_flush_inlined_data('lake')",
		"CALL ducklake_rewrite_data_files('lake',delete_threshold=>0,max_compacted_files=>1)",
		"CALL ducklake_expire_snapshots('lake',older_than=>TIMESTAMPTZ " + lakeLiteral(snapshotBefore.UTC().Format(time.RFC3339Nano)) + ")",
		"CALL ducklake_cleanup_old_files('lake',older_than=>TIMESTAMPTZ " + lakeLiteral(fileBefore.UTC().Format(time.RFC3339Nano)) + ")",
	} {
		if err := p.maintenanceCommand(ctx, statement, &stats); err != nil {
			return err
		}
	}
	return nil
}

func (s *duckLakeStore) CleanupStatus(ctx context.Context) (DuckLakeCleanupStatus, error) {
	status := DuckLakeCleanupStatus{Enabled: !s.config.DisableCleanup, SnapshotRetentionSeconds: int64(s.config.SnapshotRetention / time.Second), ObsoleteFileGraceSeconds: int64(s.config.CleanupGrace / time.Second), IntervalSeconds: int64(s.config.CleanupInterval / time.Second)}
	if err := s.mu.Lock(ctx); err != nil {
		return status, err
	}
	defer s.mu.Unlock()
	if _, err := s.sourceIDLocked(ctx); err != nil {
		return status, err
	}
	var attempted, succeeded int64
	err := s.registry.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(p.last_success>0),0),COALESCE(sum(p.failed),0),COALESCE(sum(p.state='running'),0),COALESCE(max(p.last_attempt),0),COALESCE(max(p.last_success),0)
 FROM catalogs c LEFT JOIN cleanup_progress p ON p.project_id=c.project_id WHERE c.ready=1`).Scan(&status.RegisteredCatalogs, &status.CompletedCatalogs, &status.FailedCatalogs, &status.RunningCatalogs, &attempted, &succeeded)
	if attempted > 0 {
		status.LastAttempt = time.UnixMilli(attempted).UTC().Format(time.RFC3339Nano)
	}
	if succeeded > 0 {
		status.LastSuccess = time.UnixMilli(succeeded).UTC().Format(time.RFC3339Nano)
	}
	return status, err
}
