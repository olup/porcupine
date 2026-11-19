package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

const hostedRecoverySchema = `
CREATE TABLE hosted_recovery_pending (
 project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE, generation BIGINT NOT NULL DEFAULT 1);
INSERT INTO hosted_recovery_pending(project_id)
SELECT p.id FROM projects p JOIN (
 SELECT project_id FROM pipeline_effect_streams
 UNION SELECT project_id FROM hosted_mutation_intents WHERE state='ready'
) work ON work.project_id=p.id;
CREATE INDEX hosted_worker_incarnations_reclaim ON hosted_worker_incarnations(reclaim_after);
CREATE FUNCTION queue_hosted_recovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE project TEXT;
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM hosted_worker_incarnations WHERE incarnation_id=OLD.incarnation_id)
  THEN RETURN NULL; END IF;
  project := OLD.project_id;
 ELSE
  project := NEW.project_id;
 END IF;
 INSERT INTO hosted_recovery_pending(project_id) SELECT id FROM projects WHERE id=project
 ` + hostedRecoveryConflict + `;
 RETURN NULL;
END $$;
CREATE TRIGGER hosted_recovery_reservation AFTER INSERT ON pipeline_effect_reservations
FOR EACH ROW EXECUTE FUNCTION queue_hosted_recovery();
CREATE TRIGGER hosted_recovery_mutation AFTER INSERT OR UPDATE OF state ON hosted_mutation_intents
FOR EACH ROW WHEN (NEW.state='ready') EXECUTE FUNCTION queue_hosted_recovery();
CREATE TRIGGER hosted_recovery_reclaimed AFTER DELETE ON hosted_project_activity
FOR EACH ROW WHEN (OLD.incarnation_id IS NOT NULL) EXECUTE FUNCTION queue_hosted_recovery();`

const hostedRecoveryConflict = `ON CONFLICT(project_id) DO UPDATE SET generation=hosted_recovery_pending.generation+1`

const hostedRecoveryRetryDelay = 30 * time.Second

var errHostedACKPending = errors.New("native effect acknowledgement pending")

func ignoreHostedACKPending(err error) error {
	if errors.Is(err, errHostedACKPending) {
		return nil
	}
	return err
}

func (f *hostedLiveFlusher) acknowledgeHostedEffects(ctx context.Context, fence lakeapi.ProjectWriteFence, scope lakeapi.HostedProjectScope, through int64) error {
	if err := f.native.AcknowledgeNativeEffects(ctx, fence, scope, through); err != nil {
		return errors.Join(errHostedACKPending, err)
	}
	return nil
}

func hostedRecoveryProjects(ctx context.Context, db *sql.DB, after string) ([]string, error) {
	if err := reclaimExpiredHostedIncarnations(ctx, db); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT r.project_id FROM (
SELECT project_id FROM hosted_recovery_pending WHERE project_id>$1
UNION SELECT project_id FROM pipeline_effect_reservations WHERE project_id>$1
) r JOIN projects p ON p.id=r.project_id
LEFT JOIN hosted_recovery_pending pending ON pending.project_id=r.project_id
WHERE (pending.retry_at IS NULL OR pending.retry_at<=clock_timestamp())
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=r.project_id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=p.tenant_id) ORDER BY r.project_id LIMIT 32`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, err
		}
		projects = append(projects, project)
	}
	return projects, rows.Err()
}

func (s *Service) startHostedRecovery(interval time.Duration) {
	if interval == 0 {
		interval = 2 * time.Second
	}
	h := s.hosted
	ctx, stop := context.WithCancel(context.Background())
	h.stopRecovery, h.recoveryDone = stop, make(chan struct{})
	go func() {
		defer close(h.recoveryDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var after string
		for {
			work, cancel := h.incarnation.writeContext(ctx)
			h.flusher.flushReturnedProjectActivities(work)
			projects, err := hostedRecoveryProjects(work, s.control, after)
			if err == nil && len(projects) == 0 {
				after = ""
			}
			for _, project := range projects {
				after = project
				err := h.flusher.recoverProject(work, lakeapi.ProjectWriteKey{SourceID: h.native.Config.SharedSourceID, ProjectID: project})
				if err != nil && !errors.Is(err, lakeapi.ErrProjectWriteBusy) && work.Err() == nil {
					slog.Warn("hosted project reconciliation pending", "project_id", project)
				}
			}
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

func (f *hostedLiveFlusher) recoverProject(ctx context.Context, key lakeapi.ProjectWriteKey) error {
	session, err := f.coordinator.TryAcquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	var generation int64
	var tenant string
	// A candidate selected before another worker's failed attempt may now be deferred.
	if err := f.publisher.control.QueryRowContext(ctx, `SELECT p.tenant_id,coalesce(r.generation,0)
FROM projects p LEFT JOIN hosted_recovery_pending r ON r.project_id=p.id WHERE p.id=$1
AND (r.retry_at IS NULL OR r.retry_at<=clock_timestamp())
AND (r.project_id IS NOT NULL OR EXISTS(SELECT 1 FROM pipeline_effect_reservations WHERE project_id=$1))`, key.ProjectID).Scan(&tenant, &generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	return f.recoverProjectHeld(ctx, session, key, tenant, generation)
}

func (f *hostedLiveFlusher) recoverProjectHeld(ctx context.Context, session *lakeapi.ProjectWriteSession, key lakeapi.ProjectWriteKey, tenant string, generation int64) error {
	return f.runActiveHostedProject(ctx, session, key.ProjectID, func(ctx context.Context) error {
		if err := f.reconcileRecoveryProject(ctx, session, key, tenant); err != nil {
			if ctx.Err() != nil || session.Context().Err() != nil || errors.Is(err, lakeapi.ErrProjectWriteSessionLost) ||
				errors.Is(err, lakeapi.ErrProjectWriteFenceStale) || errors.Is(err, errHostedProjectFenced) {
				return err
			}
			return errors.Join(err, deferHostedRecovery(ctx, session, key.ProjectID, generation))
		}
		// Reservation-only work reads generation zero. A concurrent enqueue
		// creates or advances a positive generation that stays queued.
		return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `DELETE FROM hosted_recovery_pending WHERE project_id=$1 AND generation=$2`, key.ProjectID, generation)
			return err
		})
	})
}

func (f *hostedLiveFlusher) reconcileRecoveryProject(ctx context.Context, session *lakeapi.ProjectWriteSession, key lakeapi.ProjectWriteKey, tenant string) error {
	scope, err := f.native.LookupHostedProjectExisting(ctx, key)
	if err != nil {
		return err
	}
	fence := session.Fence(scope.StreamID)
	if err := f.native.AdvanceRecoveryFence(ctx, fence); err != nil {
		return err
	}
	return f.reconcileAndSettle(ctx, session, scope, fence, tenant)
}

func deferHostedRecovery(ctx context.Context, session *lakeapi.ProjectWriteSession, project string, generation int64) error {
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		const deadline = `clock_timestamp()+$2::bigint*interval '1 second'`
		seconds := int64(hostedRecoveryRetryDelay / time.Second)
		if generation == 0 {
			// A reservation can need another attempt even if publication already
			// consumed it before the attempt failed to acknowledge the proof.
			_, err := tx.ExecContext(ctx, `INSERT INTO hosted_recovery_pending(project_id,retry_at)
SELECT id,`+deadline+` FROM projects WHERE id=$1 ON CONFLICT(project_id) DO NOTHING`, project, seconds)
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE hosted_recovery_pending SET retry_at=`+deadline+`
WHERE project_id=$1 AND generation=$3`, project, seconds, generation)
		return err
	})
}
