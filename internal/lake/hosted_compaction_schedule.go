package lake

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

const (
	hostedCompactionSchedule = "pc_native_compaction"
	hostedCompactionInterval = 10 * time.Minute
	hostedCompactionRetry    = time.Minute
	hostedCompactionClaimTTL = 2 * time.Minute
	hostedCompactionTimeout  = 30 * time.Second
	hostedMergeFileSize      = 1 << 20
)

func (s *DuckLakeStore) claimHostedMaintenance(ctx context.Context, registry *postgresNativeRegistry, table string, at, next time.Time) (nativeCleanupAttempt, error) {
	if err := s.Mu.Lock(ctx); err != nil {
		return nativeCleanupAttempt{}, err
	}
	if s.closed {
		s.Mu.Unlock()
		return nativeCleanupAttempt{}, errors.New("DuckLake is closed")
	}
	due, err := registry.hostedMaintenanceDue(ctx, table, at)
	s.Mu.Unlock()
	if err != nil {
		if ctx.Err() != nil {
			return nativeCleanupAttempt{}, ctx.Err()
		}
		return nativeCleanupAttempt{}, errors.New("DuckLake source unavailable")
	}
	if !due {
		return nativeCleanupAttempt{}, sql.ErrNoRows
	}
	return registry.claimDue(ctx, table, at, next)
}

// CompactHostedDue checks one registered project. The callback owns project
// activity registration and executes only while the normal coordinator owns
// the project. A claim permits scheduling, never native writes by itself.
func (s *DuckLakeStore) CompactHostedDue(ctx context.Context, now time.Time, coordinator *PostgresProjectCoordinator, run func(context.Context, *ProjectWriteSession) (DuckLakeMaintenanceStats, error)) error {
	if coordinator == nil || run == nil || s.SharedCatalog == nil {
		return ErrHostedProjectNotReady
	}
	ctx, cancel := context.WithTimeout(ctx, hostedCompactionTimeout)
	defer cancel()
	registry := s.registry.(*postgresNativeRegistry)
	attempt, err := s.claimHostedMaintenance(ctx, registry, hostedCompactionSchedule, now, now.Add(hostedCompactionClaimTTL))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	state := "ok"
	next := now.Add(hostedCompactionInterval)
	layout, work, err := registry.hostedCompactionCandidate(ctx, attempt)
	if err == nil && work {
		key := ProjectWriteKey{SourceID: registry.source, ProjectID: attempt.ProjectID}
		var stats DuckLakeMaintenanceStats
		stats, err = runHostedCompaction(ctx, coordinator, key, run)
		if err == nil && stats.FilesProcessed > 0 {
			// Another group may be eligible. Recheck the new layout next time.
			layout = 0
		}
	}
	switch {
	case errors.Is(err, ErrProjectWriteBusy):
		state, next, err = "waiting", now.Add(hostedCompactionRetry), nil
	case ctx.Err() != nil:
		state, next = "interrupted", now.Add(hostedCompactionRetry)
	case err != nil:
		state, next = "failed", now.Add(hostedCompactionRetry)
	}
	record, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer stop()
	return errors.Join(err, registry.finishHostedCompaction(record, attempt, state, next, layout))
}

func runHostedCompaction(ctx context.Context, coordinator *PostgresProjectCoordinator, key ProjectWriteKey, run func(context.Context, *ProjectWriteSession) (DuckLakeMaintenanceStats, error)) (stats DuckLakeMaintenanceStats, err error) {
	session, err := coordinator.TryAcquire(ctx, key)
	if err != nil {
		return stats, err
	}
	defer func() { err = errors.Join(err, session.Release()) }()
	return run(ctx, session)
}

// Use only PostgreSQL metadata to detect plausible work in an existing
// catalog. Native compaction still decides eligibility, adjacency and the
// candidate groups. Skip a layout already found idle, even if a heuristic
// matched it. The write guards can generate file churn during idle recovery,
// so changes to those tables do not trigger this scheduler.
func (r *postgresNativeRegistry) hostedCompactionCandidate(ctx context.Context, a nativeCleanupAttempt) (layout int64, work bool, err error) {
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	p, err := r.lookup(ctx, a.ProjectID)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (!p.Ready || p.Deleted)) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	schema := SharedNativeSchema(p.CatalogID)
	var exists bool
	var checked int64
	err = r.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL, checked_layout_snapshot
FROM pc_native_compaction WHERE source_id=$2 AND project_id=$3 AND attempt_id=$4 AND state='running'`,
		schema+".ducklake_data_file", r.source, p.ProjectID, a.ID).Scan(&exists, &checked)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !exists) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// SharedNativeSchema derives a fixed-format identifier from the checked
	// catalog ID. Read file additions and current eligibility in one snapshot.
	schema = `"` + schema + `"`
	err = r.db.QueryRowContext(ctx, `WITH tables AS (
	SELECT table_id FROM `+schema+`.ducklake_table
	WHERE end_snapshot IS NULL AND table_name NOT IN ('project_write_guard','hosted_write_guard')
), data AS (
	SELECT table_id,begin_snapshot,end_snapshot,file_size_bytes FROM `+schema+`.ducklake_data_file
	JOIN tables USING(table_id)
), deletes AS (
	SELECT begin_snapshot,end_snapshot FROM `+schema+`.ducklake_delete_file
	JOIN tables USING(table_id)
)
SELECT greatest(coalesce((SELECT max(greatest(begin_snapshot,end_snapshot)) FROM data),0),
	coalesce((SELECT max(greatest(begin_snapshot,end_snapshot)) FROM deletes),0)),
	EXISTS(SELECT 1 FROM deletes WHERE end_snapshot IS NULL)
	OR EXISTS(SELECT 1 FROM data WHERE end_snapshot IS NULL AND file_size_bytes<=$1 GROUP BY table_id HAVING count(*)>1)`, hostedMergeFileSize).Scan(&layout, &work)
	return layout, work && layout != checked, err
}

func (r *postgresNativeRegistry) finishHostedCompaction(ctx context.Context, attempt nativeCleanupAttempt, state string, next time.Time, layout int64) error {
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	_, err := r.db.ExecContext(ctx, `UPDATE pc_native_compaction SET state=$1,next_attempt=$2,
	checked_layout_snapshot=CASE WHEN $1='ok' THEN $3 ELSE checked_layout_snapshot END
WHERE source_id=$4 AND project_id=$5 AND attempt_id=$6 AND state='running'`,
		state, next.UnixMilli(), layout, r.source, attempt.ProjectID, attempt.ID)
	// A newer claim may have superseded this one while native work was joining.
	return err
}
