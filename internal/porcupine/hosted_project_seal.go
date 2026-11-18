package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// sealDeletedProject starts neither physical cleanup nor tenant finalization.
// A recorded seal is evidence for the deletion recovery worker only.
func (f *hostedLiveFlusher) sealDeletedProject(ctx context.Context, key lakeapi.ProjectWriteKey) (resultErr error) {
	if err := reclaimExpiredHostedIncarnations(ctx, f.publisher.control); err != nil {
		return err
	}
	var tenant string
	var busy bool
	err := f.publisher.control.QueryRowContext(ctx, `SELECT d.tenant_id,
EXISTS(SELECT 1 FROM hosted_project_activity a WHERE a.project_id=d.project_id)
FROM project_deletions d JOIN projects p ON p.id=d.project_id AND p.tenant_id=d.tenant_id
WHERE d.project_id=$1 AND d.completed=0`, key.ProjectID).Scan(&tenant, &busy)
	if err != nil {
		return err
	}
	if busy {
		return errHostedProjectBusy
	}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	ticket := eventapi.NewID()
	defer func() {
		resultErr = errors.Join(resultErr, f.finishProjectActivity(ctx, key.ProjectID, ticket))
	}()
	incarnation, err := f.incarnation.reserve(key.ProjectID, ticket, true)
	if err != nil {
		return err
	}
	var sealed bool
	if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var recordedSource, recordedTenant string
		err := tx.QueryRowContext(ctx, `SELECT source_id,tenant_id FROM hosted_project_seals WHERE project_id=$1`, key.ProjectID).
			Scan(&recordedSource, &recordedTenant)
		if err == nil {
			if recordedSource != key.SourceID || recordedTenant != tenant {
				return lakeapi.ErrNativeEvidenceGap
			}
			sealed = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var registered string
		var current bool
		if err := tx.QueryRowContext(ctx, `SELECT `+hostedIncarnationCurrent("$1"), incarnation).Scan(&current); err != nil || !current {
			return errors.Join(err, errHostedIncarnationLost)
		}
		err = tx.QueryRowContext(ctx, `INSERT INTO hosted_project_activity(project_id,activity_id,incarnation_id)
SELECT d.project_id,$3,NULLIF($4,'') FROM project_deletions d
WHERE d.project_id=$1 AND d.tenant_id=$2 AND d.completed=0
AND NOT EXISTS(SELECT 1 FROM hosted_project_activity a WHERE a.project_id=d.project_id)
RETURNING activity_id`, key.ProjectID, tenant, ticket, incarnation).Scan(&registered)
		if errors.Is(err, sql.ErrNoRows) {
			return errHostedProjectBusy
		}
		return err
	}); err != nil || sealed {
		return err
	}
	return session.RunNative(ctx, func(ctx context.Context) error {
		absent, err := f.native.HostedProjectCatalogAbsent(ctx, key)
		if err != nil {
			return err
		}
		if absent {
			if err := f.verifyNoHostedCatalogEvidence(ctx, key.ProjectID); err != nil {
				return err
			}
			return f.recordHostedProjectSeal(ctx, session, lakeapi.HostedProjectSeal{Scope: lakeapi.HostedProjectScope{Key: key}}, tenant, "", ticket)
		}
		scope, err := f.native.LookupHostedProjectExisting(ctx, key)
		if err != nil {
			return err
		}
		fence := session.Fence(scope.StreamID)
		if err := f.native.AdvanceRecoveryFence(ctx, fence); err != nil {
			return err
		}
		if err := f.reconcileEvidence(ctx, session, scope, fence, tenant, true); err != nil {
			return err
		}
		if err := f.settleReadyHostedMutation(ctx, session, scope, fence, tenant, true); err != nil {
			return err
		}
		var unresolved bool
		err = f.publisher.control.QueryRowContext(ctx, `SELECT EXISTS (
 SELECT 1 FROM hosted_mutation_projects WHERE project_id=$1 AND
 (source_id<>$2 OR catalog_id<>$3 OR stream_id<>$4 OR tenant_id<>$5 OR accepted_through<>settled_through)
 UNION ALL SELECT 1 FROM hosted_mutation_intents WHERE project_id=$1 AND state='ready'
)`, key.ProjectID, key.SourceID, scope.CatalogID, scope.StreamID, tenant).Scan(&unresolved)
		if err != nil {
			return err
		}
		if unresolved {
			return lakeapi.ErrNativeEvidenceGap
		}
		seal, err := f.native.SealHostedProject(ctx, fence)
		if err != nil {
			return err
		}
		if seal.Scope != scope {
			return lakeapi.ErrNativeEvidenceGap
		}
		if err := f.reconcileEvidence(ctx, session, scope, fence, tenant, true); err != nil {
			return err
		}
		sequence, hash, err := f.publisher.checkpoint(ctx, scope, tenant)
		if err != nil {
			return err
		}
		if sequence != seal.FinalSequence {
			return lakeapi.ErrNativeEvidenceGap
		}
		// Ordinary recovery treats native ACK as best effort. A terminal seal
		// requires a confirmed ACK under the current fenced session.
		if err := f.native.AcknowledgeNativeEffects(ctx, fence, scope, sequence); err != nil {
			return err
		}
		return f.recordHostedProjectSeal(ctx, session, seal, tenant, hash, ticket)
	})
}

func (f *hostedLiveFlusher) verifyNoHostedCatalogEvidence(ctx context.Context, project string) error {
	var hasEvidence bool
	err := f.publisher.control.QueryRowContext(ctx, `SELECT EXISTS (
 SELECT 1 FROM pipeline_effect_streams WHERE project_id=$1
 UNION ALL SELECT 1 FROM hosted_mutation_projects WHERE project_id=$1
 UNION ALL SELECT 1 FROM hosted_mutation_intents WHERE project_id=$1
 UNION ALL SELECT 1 FROM import_uploads WHERE project_id=$1
)`, project).Scan(&hasEvidence)
	if err == nil && hasEvidence {
		return lakeapi.ErrNativeEvidenceGap
	}
	return err
}

func (f *hostedLiveFlusher) recordHostedProjectSeal(ctx context.Context, session *lakeapi.ProjectWriteSession, seal lakeapi.HostedProjectSeal, tenant, hash, ticket string) error {
	scope := seal.Scope
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO hosted_project_seals(project_id,tenant_id,source_id,catalog_id,stream_id,final_sequence,proof_sha)
SELECT d.project_id,d.tenant_id,$3,$4,$5,$6,$7 FROM project_deletions d
WHERE d.project_id=$1 AND d.tenant_id=$2 AND d.completed=0
AND NOT EXISTS(SELECT 1 FROM hosted_project_activity a WHERE a.project_id=d.project_id AND a.activity_id<>$8)
ON CONFLICT DO NOTHING`, scope.Key.ProjectID, tenant, scope.Key.SourceID, scope.CatalogID, scope.StreamID, seal.FinalSequence, hash, ticket)
		if err != nil {
			return err
		}
		var matches bool
		err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM hosted_project_seals
WHERE project_id=$1 AND tenant_id=$2 AND source_id=$3 AND catalog_id=$4 AND stream_id=$5 AND final_sequence=$6 AND proof_sha=$7)`,
			scope.Key.ProjectID, tenant, scope.Key.SourceID, scope.CatalogID, scope.StreamID, seal.FinalSequence, hash).Scan(&matches)
		if err == nil && !matches {
			return errors.New("hosted project seal has no matching durable record")
		}
		return err
	})
}

// Bind sealProject at hosted startup, before starting the deletion recovery
// loop. A hosted deployment has no owner journal to dispatch cleanup through.
func (f *hostedLiveFlusher) sealProject(ctx context.Context, project string) error {
	return f.sealDeletedProject(ctx, lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: project})
}

func (s *Service) deleteHostedProject(ctx context.Context, project string) error {
	if err := s.beginControlProjectDeletion(ctx, project); err != nil {
		return err
	}
	var completed bool
	if err := s.control.QueryRowContext(ctx, `SELECT completed FROM project_deletions WHERE project_id=$1`, project).Scan(&completed); err != nil {
		return apierr.New(503, "project deletion status unavailable")
	}
	if completed {
		return nil
	}
	select {
	case s.deletionWake <- struct{}{}:
	default:
	}
	// The existing project endpoint returns 204 only after cleanup completes.
	return apierr.New(503, "project access is fenced; hosted cleanup is pending")
}

func (s *Service) resumeHostedProjectSeals(ctx context.Context) {
	rows, err := s.control.QueryContext(ctx, `SELECT d.project_id FROM project_deletions d
WHERE d.completed=0 AND NOT EXISTS(SELECT 1 FROM hosted_project_seals s WHERE s.project_id=d.project_id)
ORDER BY d.project_id`)
	if err != nil {
		return
	}
	var pending []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			rows.Close()
			return
		}
		pending = append(pending, project)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return
	}
	for _, project := range pending {
		if ctx.Err() != nil {
			return
		}
		work, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := s.sealHostedProject(work, project)
		cancel()
		if err != nil && !errors.Is(err, errHostedProjectBusy) && ctx.Err() == nil {
			slog.Warn("hosted project seal pending", "project_id", project)
		}
	}
}
