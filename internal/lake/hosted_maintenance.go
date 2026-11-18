package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// MaintainHostedProject compacts an existing open hosted catalog under the caller's
// already-held coordinator session. Callers own the maintenance cadence and must
// not call Release inside it. CleanupHostedProject owns history expiry.
// One extra engine per store is allowed while cleanup is held. Its normal resource
// limits apply, and its resident project lease prevents store close until it returns.
func (s *DuckLakeStore) MaintainHostedProject(ctx context.Context, session *ProjectWriteSession) (stats DuckLakeMaintenanceStats, err error) {
	err = s.runHostedMaintenance(ctx, session, func(ctx context.Context, p *ProjectLake, fence ProjectWriteFence, _ HostedProjectScope) error {
		started := time.Now()
		for _, statement := range []string{
			fmt.Sprintf("CALL ducklake_merge_adjacent_files('lake',max_compacted_files=>1,max_file_size=>%d)", hostedMergeFileSize),
			"CALL ducklake_rewrite_data_files('lake',delete_threshold=>0,max_compacted_files=>1)",
		} {
			if err := p.hostedMaintenanceStatement(ctx, session, fence, statement, &stats); err != nil {
				return err
			}
		}
		stats.CompactMS = float64(time.Since(started)) / float64(time.Millisecond)
		return nil
	})
	stats.Completed = err == nil
	return stats, err
}

// runHostedMaintenance advances the session's fence and opens a private engine.
// Each native statement still runs through hostedMaintenanceStatement, which
// rereads the fence inside its own transaction.
func (s *DuckLakeStore) runHostedMaintenance(ctx context.Context, session *ProjectWriteSession, work func(context.Context, *ProjectLake, ProjectWriteFence, HostedProjectScope) error) error {
	if session == nil || s.SharedCatalog == nil || session.key.SourceID != s.Config.SharedSourceID {
		return ErrProjectWriteFenceStale
	}
	ctx, cancel := context.WithTimeout(ctx, hostedCompactionTimeout)
	defer cancel()
	return session.RunNative(ctx, func(nativeCtx context.Context) (err error) {
		if err := s.cleanup.Lock(nativeCtx); err != nil {
			return err
		}
		defer s.cleanup.Unlock()
		scope, err := s.LookupHostedProjectExisting(nativeCtx, session.key)
		if err != nil {
			return err
		}
		fence := session.Fence(scope.StreamID)
		if err := s.AdvanceWriteFence(nativeCtx, fence); err != nil {
			return err
		}
		_, release, err := s.leaseHostedProjectExisting(nativeCtx, scope.Key.ProjectID)
		if err != nil {
			return err
		}
		defer release()
		p, err := s.openProjectEngine(nativeCtx, scope.Key.ProjectID, nil, true, true)
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, p.DB.Close()) }()
		return work(nativeCtx, p, fence, scope)
	})
}

func (p *ProjectLake) hostedMaintenanceStatement(ctx context.Context, session *ProjectWriteSession, fence ProjectWriteFence, statement string, stats *DuckLakeMaintenanceStats) error {
	return retryNativeTransaction(ctx, func() error {
		return session.RunNative(ctx, func(attemptCtx context.Context) error {
			next := *stats
			if err := p.hostedMaintenanceAttempt(attemptCtx, fence, statement, &next); err != nil {
				return err
			}
			*stats = next
			return nil
		})
	})
}

// No native retry may refresh the snapshot after this guard read. The first
// commit attempts snapshot+1; a successor fence occupies that ID. External retry
// must revalidate the held physical session and reread the fence in a new tx.
func (p *ProjectLake) hostedMaintenanceAttempt(ctx context.Context, fence ProjectWriteFence, statement string, stats *DuckLakeMaintenanceStats) error {
	if !validProjectWriteFence(fence) || !p.shared || p.projectID != fence.Key.ProjectID || p.sharedStore.Config.SharedSourceID != fence.Key.SourceID {
		return ErrProjectWriteFenceStale
	}
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var retries int
	if err := conn.QueryRowContext(ctx, `SELECT current_setting('ducklake_max_retry_count')`).Scan(&retries); err != nil {
		return err
	}
	if retries != 0 {
		return errors.New("hosted maintenance requires native commit retries disabled")
	}
	var engine, extension string
	if err := conn.QueryRowContext(ctx, `SELECT version(),extension_version FROM ducklake_settings('lake')`).Scan(&engine, &extension); err != nil {
		return err
	}
	if engine != "v2.0.0" || extension != "417a9117f5" {
		return errors.New("hosted maintenance requires the verified DuckDB v2.0.0 and DuckLake 417a9117f5 engine contract")
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := readHostedMaintenanceFence(ctx, tx, fence); err != nil {
		return err
	}
	var guardedSnapshot, afterSnapshot int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM ducklake_current_snapshot('lake')`).Scan(&guardedSnapshot); err != nil {
		return err
	}
	next := *stats
	if err := runLakeMaintenanceCommand(ctx, tx, statement, &next); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM ducklake_current_snapshot('lake')`).Scan(&afterSnapshot); err != nil {
		return err
	}
	if guardedSnapshot != afterSnapshot {
		return errors.New("native maintenance changed its guarded snapshot")
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	*stats = next
	return nil
}

func readHostedMaintenanceFence(ctx context.Context, tx *sql.Tx, fence ProjectWriteFence) error {
	var owned bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*)=1 FROM lake.hosted_write_guard
 WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND state='open' AND identity_ready AND fence_epoch=? AND fence_nonce=?`, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID).Scan(&owned); err != nil {
		return err
	}
	if !owned {
		return ErrProjectWriteFenceStale
	}
	var deleted bool
	if err := tx.QueryRowContext(ctx, `SELECT deleted FROM lake.project_write_guard WHERE id=1`).Scan(&deleted); err != nil {
		return err
	}
	if deleted {
		return errSharedNativeDeleted
	}
	return nil
}
