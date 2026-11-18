package lake

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

const (
	hostedCleanupSchedule     = "pc_native_cleanup"
	hostedCleanupClaimTTL     = 2 * time.Minute
	hostedCleanupMaxSnapshots = 64
	hostedCleanupMaxFiles     = 256
)

// ErrHostedCleanupGuard means the restore policy or the recovery floor did not
// permit physical deletion. The pass deletes nothing and is retried later.
var ErrHostedCleanupGuard = errors.New("hosted history cleanup requires a covered restore policy and a verified recovery floor")

// HostedCleanupStats counts committed work of one bounded pass.
type HostedCleanupStats struct {
	SnapshotsExpired int64
	FilesDeleted     int64
	// More reports eligible work left behind by the per-pass bounds.
	More bool
}

// HostedCleanupHorizon returns the snapshot-expiry and file-deletion cutoffs for
// one pass at now. DuckLake schedules a file for deletion when the last snapshot
// that references it expires. A metadata restore to a target at or after that
// schedule time no longer references the file. Files are therefore deleted only
// when they were scheduled before the verified recovery floor and before
// now-cleanup_grace, where the grace covers the restore window, the longest
// reader and the safety margin.
func HostedCleanupHorizon(c DuckLakeConfig, now, floor time.Time) (snapshotBefore, fileBefore time.Time, err error) {
	if c.DisableCleanup || now.IsZero() || floor.IsZero() || floor.After(now) ||
		c.SnapshotRetention < time.Second || !restoreGraceCovered(c) {
		return time.Time{}, time.Time{}, ErrHostedCleanupGuard
	}
	// Catalog timestamps have microsecond precision. Round cutoffs down.
	snapshotBefore = now.Add(-c.SnapshotRetention).Truncate(time.Microsecond)
	fileBefore = minTime(now.Add(-c.CleanupGrace), floor).Truncate(time.Microsecond)
	return snapshotBefore, fileBefore, nil
}

// CleanupHostedProject expires old snapshots and deletes obsolete files of one
// hosted catalog under the caller's coordinator session and write fence. It
// first rewrites one group of files with deleted rows so deleted payloads leave
// the current snapshot. Each pass is bounded; More asks for another pass.
func (s *DuckLakeStore) CleanupHostedProject(ctx context.Context, session *ProjectWriteSession, now, floor time.Time) (stats HostedCleanupStats, err error) {
	snapshotBefore, fileBefore, err := HostedCleanupHorizon(s.Config, now, floor)
	if err != nil {
		return stats, err
	}
	registry, ok := s.registry.(*postgresNativeRegistry)
	if !ok {
		return stats, ErrHostedProjectNotReady
	}
	err = s.runHostedMaintenance(ctx, session, func(ctx context.Context, p *ProjectLake, fence ProjectWriteFence, scope HostedProjectScope) error {
		var native DuckLakeMaintenanceStats
		if err := p.hostedMaintenanceStatement(ctx, session, fence, "CALL ducklake_rewrite_data_files('lake',delete_threshold=>0,max_compacted_files=>1)", &native); err != nil {
			return err
		}
		schema := `"` + SharedNativeSchema(scope.CatalogID) + `"`
		versions, more, err := registry.expirableSnapshots(ctx, schema, snapshotBefore)
		if err != nil {
			return err
		}
		if len(versions) != 0 {
			ids := make([]string, len(versions))
			for i, version := range versions {
				ids[i] = strconv.FormatInt(version, 10)
			}
			if err := p.hostedMaintenanceStatement(ctx, session, fence, "CALL ducklake_expire_snapshots('lake',versions=>["+strings.Join(ids, ",")+"])", &native); err != nil {
				return err
			}
			stats.SnapshotsExpired = int64(len(versions))
		}
		olderThan, files, moreFiles, err := registry.deletableFiles(ctx, schema, fileBefore)
		if err != nil {
			return err
		}
		if files != 0 {
			statement := "CALL ducklake_cleanup_old_files('lake',older_than=>TIMESTAMPTZ " + LakeLiteral(olderThan.UTC().Format(time.RFC3339Nano)) + ")"
			if err := p.hostedMaintenanceStatement(ctx, session, fence, statement, &native); err != nil {
				return err
			}
			stats.FilesDeleted = files
		}
		stats.More = more || moreFiles
		return nil
	})
	return stats, err
}

// CleanupHostedDue runs one bounded history cleanup pass for the least recently
// attempted due catalog. It uses the same claim table, coordinator session and
// write fence as the other hosted maintenance. The callback supplies the
// recovery floor and owns project activity registration.
func (s *DuckLakeStore) CleanupHostedDue(ctx context.Context, now time.Time, coordinator *PostgresProjectCoordinator, run func(context.Context, *ProjectWriteSession, time.Time) (HostedCleanupStats, error)) error {
	if s.Config.DisableCleanup {
		return nil
	}
	registry, ok := s.registry.(*postgresNativeRegistry)
	if coordinator == nil || run == nil || s.SharedCatalog == nil || !s.Config.Hosted || !ok {
		return ErrHostedProjectNotReady
	}
	ctx, cancel := context.WithTimeout(ctx, hostedCompactionTimeout)
	defer cancel()
	source, err := s.SourceID(ctx)
	if err != nil {
		return err
	}
	attempt, err := registry.claimDue(ctx, hostedCleanupSchedule, now, now.Add(hostedCleanupClaimTTL))
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var stats HostedCleanupStats
	session, err := coordinator.TryAcquire(ctx, ProjectWriteKey{SourceID: source, ProjectID: attempt.ProjectID})
	if err == nil {
		stats, err = run(ctx, session, now)
		err = errors.Join(err, session.Release())
	}
	retry := min(s.Config.CleanupInterval, time.Minute)
	state, code, next := "ok", "", now.Add(s.Config.CleanupInterval)
	if stats.More {
		next = now
	}
	switch {
	case errors.Is(err, ErrHostedCleanupGuard):
		state, code, next, err = "waiting", "restore_guard", now.Add(retry), nil
	case errors.Is(err, ErrProjectWriteBusy):
		state, next, err = "waiting", now.Add(retry), nil
	case ctx.Err() != nil:
		state, code, next = "interrupted", "interrupted", now.Add(retry)
	case err != nil:
		state, code, next = "failed", "failed", now.Add(retry)
	}
	record, stop := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer stop()
	return errors.Join(err, registry.finishHostedCleanup(record, attempt, state, code, now, next, stats))
}

// expirableSnapshots selects at most hostedCleanupMaxSnapshots of the oldest
// snapshots before the cutoff. The current snapshot is never selected.
func (r *postgresNativeRegistry) expirableSnapshots(ctx context.Context, schema string, before time.Time) ([]int64, bool, error) {
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	// schema is quoted from a checked catalog ID, never request input.
	rows, err := r.db.QueryContext(ctx, `SELECT snapshot_id FROM `+schema+`.ducklake_snapshot
WHERE snapshot_time<$1 AND snapshot_id<(SELECT max(snapshot_id) FROM `+schema+`.ducklake_snapshot)
ORDER BY snapshot_id LIMIT $2`, before, hostedCleanupMaxSnapshots+1)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, false, err
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if len(versions) > hostedCleanupMaxSnapshots {
		return versions[:hostedCleanupMaxSnapshots], true, nil
	}
	return versions, false, nil
}

// deletableFiles bounds ducklake_cleanup_old_files, which has no count limit.
// It returns an older_than cutoff that covers whole schedule-time groups, up to
// hostedCleanupMaxFiles files. The first group is always taken so a pass
// progresses; one expiry pass of bounded snapshots creates each group.
func (r *postgresNativeRegistry) deletableFiles(ctx context.Context, schema string, before time.Time) (time.Time, int64, bool, error) {
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	rows, err := r.db.QueryContext(ctx, `SELECT schedule_start,count(*) FROM `+schema+`.ducklake_files_scheduled_for_deletion
WHERE schedule_start<$1 GROUP BY schedule_start ORDER BY schedule_start LIMIT $2`, before, hostedCleanupMaxFiles+1)
	if err != nil {
		return time.Time{}, 0, false, err
	}
	defer rows.Close()
	var total int64
	var last time.Time
	more := false
	for rows.Next() {
		var start time.Time
		var count int64
		if err := rows.Scan(&start, &count); err != nil {
			return time.Time{}, 0, false, err
		}
		if total != 0 && total+count > hostedCleanupMaxFiles {
			more = true
			break
		}
		total, last = total+count, start
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, 0, false, err
	}
	if !more {
		return before, total, false, nil
	}
	// Schedule times have microsecond precision; the next group starts later.
	return last.Add(time.Microsecond), total, true, nil
}

func (r *postgresNativeRegistry) finishHostedCleanup(ctx context.Context, a nativeCleanupAttempt, state, code string, at, next time.Time, stats HostedCleanupStats) error {
	ctx, cancel := r.metadataContext(ctx)
	defer cancel()
	// A busy project keeps the previous code; contention is not a cleanup error.
	_, err := r.db.ExecContext(ctx, `UPDATE pc_native_cleanup SET state=$1,
last_error=CASE WHEN $1='waiting' AND $2='' THEN last_error ELSE $2 END,
failed=CASE WHEN $1='ok' THEN 0 WHEN $1='failed' THEN 1 ELSE failed END,
last_success=CASE WHEN $1='ok' THEN $3 ELSE last_success END,
next_attempt=$4,snapshots_expired=snapshots_expired+$5,files_deleted=files_deleted+$6
WHERE source_id=$7 AND project_id=$8 AND attempt_id=$9 AND state='running'`,
		state, code, at.UnixMilli(), next.UnixMilli(), stats.SnapshotsExpired, stats.FilesDeleted, r.source, a.ProjectID, a.ID)
	// A newer claim may have superseded this one while native work was joining.
	return err
}
