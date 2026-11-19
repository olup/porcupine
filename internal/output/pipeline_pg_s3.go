package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const (
	hostedScheduledRulePrefix = "scheduled-query/"
	hostedScheduledMaxBytes   = 64 << 20
	hostedScheduledMaxFiles   = 128
	scheduledMaxAttempts      = 3
)

type HostedScheduledReader interface {
	CaptureExportSnapshot(context.Context, string) (lakeapi.ExportSnapshot, error)
	ExportWindowAtSnapshot(context.Context, string, string, map[string]string, lakeapi.AnalyticalWindow, lakeapi.ExportSnapshot, int64, lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error)
}

type HostedScheduledDependencies struct {
	Reader         HostedScheduledReader
	SnapshotCut    ExportSnapshotCut
	BeginAttemptTx func(context.Context, *sql.Tx, string, string) error
	EndAttempt     func(context.Context, string, string) error
}

func (d HostedScheduledDependencies) ready() bool {
	return d.Reader != nil && d.SnapshotCut != nil && d.BeginAttemptTx != nil && d.EndAttempt != nil
}

func createHostedScheduledState(ctx context.Context, tx *sql.Tx, p Pipeline, raw []byte) error {
	var spec S3PipelineSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return err
	}
	start, err := time.Parse(time.RFC3339, spec.StartAt)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO hosted_s3_pipeline_state(project_id,pipeline_id,cursor_ms,interval_ms) VALUES($1,$2,$3,$4)`,
		p.ProjectID, p.ID, start.UnixMilli(), spec.IntervalSeconds*1000)
	return err
}

type hostedScheduledWork struct {
	s3PipelineWork
	tenant, incarnation, revision, previousRevision string
	previousSecret                                  []byte
	snapshot                                        lakeapi.ExportSnapshot
	reservedBytes                                   int64
}

func (w *hostedScheduledWork) binding() PipelineBinding {
	return PipelineBinding{Scope: PipelineScope{ProjectID: w.pipeline.ProjectID}, TenantID: w.tenant,
		Demand: PipelineDemand{MetadataRows: hostedScheduledMaxFiles + 1, ArtifactBytes: w.reservedBytes + exportstore.MaxManifestBytes, ProofSlots: 1}}
}

type hostedScheduledCandidate struct {
	project, pipeline, run string
	start                  int64
}

func (s *PipelineJobs) hostedScheduledCandidates(ctx context.Context) ([]hostedScheduledCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT q.project_id,q.pipeline_id,q.cursor_ms,COALESCE(r.id,'')
FROM hosted_s3_pipeline_state q
JOIN pipeline_effect_definitions d ON (d.project_id,d.id)=(q.project_id,q.pipeline_id)
LEFT JOIN hosted_s3_pipeline_runs r ON (r.project_id,r.pipeline_id,r.window_start_ms)=(q.project_id,q.pipeline_id,q.cursor_ms)
WHERE d.state='active' AND q.cursor_ms+q.interval_ms <= $1 AND
 (r.id IS NULL OR (r.state='queued' AND r.next_attempt<=clock_timestamp()) OR (r.state='running' AND r.lease_until<=clock_timestamp()))
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=q.project_id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=d.tenant_id)
ORDER BY q.last_attempt,q.project_id,q.pipeline_id LIMIT 32`, s.clock().UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []hostedScheduledCandidate
	for rows.Next() {
		var c hostedScheduledCandidate
		if err = rows.Scan(&c.project, &c.pipeline, &c.start, &c.run); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *PipelineJobs) claimHostedScheduled(ctx context.Context, c hostedScheduledCandidate) (*hostedScheduledWork, error) {
	w := &hostedScheduledWork{}
	w.pipeline.ID, w.pipeline.ProjectID = c.pipeline, c.project
	w.start, w.id = c.start, c.run
	w.claimID = eventapi.NewID()
	didClaim, registered := false, false
	mutate := func(ctx context.Context, tx *sql.Tx) error {
		var config string
		err := tx.QueryRowContext(ctx, `
SELECT d.tenant_id,d.incarnation,d.destination_revision,d.creator_id,c.config,c.secret,q.cursor_ms+q.interval_ms
FROM hosted_s3_pipeline_state q
JOIN pipeline_effect_definitions d ON (d.project_id,d.id)=(q.project_id,q.pipeline_id)
JOIN hosted_pipeline_configs c ON (c.project_id,c.id,c.tenant_id)=(d.project_id,d.id,d.tenant_id)
WHERE q.project_id=$1 AND q.pipeline_id=$2 AND q.cursor_ms=$3 AND d.state='active'`, c.project, c.pipeline, c.start).
			Scan(&w.tenant, &w.incarnation, &w.revision, &w.pipeline.ActorID, &config, &w.pipeline.Secret, &w.end)
		if err != nil {
			return err
		}
		if err = json.Unmarshal([]byte(config), &w.spec); err != nil {
			return err
		}
		// Rotate even when capacity is exhausted or the creator's grant expired.
		if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_state SET last_attempt=clock_timestamp() WHERE project_id=$1 AND pipeline_id=$2`, c.project, c.pipeline); err != nil {
			return err
		}
		if w.id == "" {
			w.reservedBytes = min(s.config.ExportLimits.MaxOutputBytes, int64(hostedScheduledMaxBytes))
			if err = s.pg.effects.lockCapacity(ctx, tx, w.binding(), true); errors.Is(err, ErrPipelineCapacity) {
				return nil
			} else if err != nil {
				return err
			}
		}
		authorized, err := s.pg.effects.authority.ResolveCreatorTx(ctx, tx, w.tenant, c.project, w.pipeline.ActorID)
		if err != nil {
			return err
		}
		if w.id == "" {
			if !authorized || w.snapshot.CatalogID == "" {
				return nil
			}
			w.id = eventapi.NewID()
			_, err = tx.ExecContext(ctx, `INSERT INTO hosted_s3_pipeline_runs(id,project_id,pipeline_id,window_start_ms,window_end_ms,snapshot_catalog_id,snapshot_version,state,created_at_ms,reserved_bytes)
VALUES($1,$2,$3,$4,$5,$6,$7,'queued',$8,$9) ON CONFLICT(project_id,pipeline_id,window_start_ms) DO NOTHING`, w.id, c.project, c.pipeline, w.start, w.end, w.snapshot.CatalogID, w.snapshot.Version, s.clock().UnixMilli(), w.reservedBytes)
			if err != nil {
				return err
			}
		}
		err = tx.QueryRowContext(ctx, `
SELECT attempts,attempt_id,destination_revision,secret,manifest,snapshot_catalog_id,snapshot_version,reserved_bytes
FROM hosted_s3_pipeline_runs WHERE id=$1 AND project_id=$2 AND pipeline_id=$3 AND window_start_ms=$4 AND
 ((state='queued' AND next_attempt<=clock_timestamp()) OR (state='running' AND lease_until<=clock_timestamp())) FOR UPDATE`,
			w.id, c.project, c.pipeline, w.start).Scan(&w.attempts, &w.attemptID, &w.previousRevision, &w.previousSecret, &w.manifest, &w.snapshot.CatalogID, &w.snapshot.Version, &w.reservedBytes)
		if err != nil {
			return err
		}
		if !authorized || w.attempts >= scheduledMaxAttempts {
			code := "attempts_exhausted"
			if !authorized {
				code = "creator_unauthorized"
			}
			_, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET state='failed',lease_until=NULL,error_code=$2,completed_at_ms=$3 WHERE id=$1`, w.id, code, s.clock().UnixMilli())
			return err
		}
		if err = s.pg.scheduled.BeginAttemptTx(ctx, tx, c.project, w.claimID); err != nil {
			return err
		}
		registered = true
		_, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET state='running',attempts=attempts+1,claim_id=$2,lease_until=`+exportLeaseExtension+`,error_code='' WHERE id=$1`, w.id, w.claimID)
		if err != nil {
			return err
		}
		if c.run == "" {
			b := w.binding()
			if err = s.pg.effects.charge(ctx, tx, b, b.Demand, 1); err != nil {
				return err
			}
		}
		w.attempts++
		didClaim = true
		return nil
	}
	var err error
	if c.run != "" {
		err = s.pg.transaction(ctx, c.project, mutate)
	} else {
		err = s.pg.scheduled.SnapshotCut(ctx, c.project, func(ctx context.Context, run ExportControlTx) error {
			var err error
			w.snapshot, err = s.pg.scheduled.Reader.CaptureExportSnapshot(ctx, c.project)
			if err != nil {
				return err
			}
			return run(ctx, mutate)
		})
	}
	if err != nil {
		if registered {
			_ = s.pg.scheduled.EndAttempt(ctx, c.project, w.claimID)
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if !didClaim {
		return nil, nil
	}
	return w, nil
}

func (s *PipelineJobs) runHostedScheduled(ctx context.Context) (bool, error) {
	candidates, err := s.hostedScheduledCandidates(ctx)
	if err != nil {
		return false, err
	}
	var claimErrors error
	for _, c := range candidates {
		w, err := s.claimHostedScheduled(ctx, c)
		if err != nil {
			claimErrors = errors.Join(claimErrors, err)
		}
		if w != nil {
			return true, s.runHostedScheduledAttempt(ctx, w)
		}
		if ctx.Err() != nil {
			break
		}
	}
	return false, claimErrors
}

func (s *PipelineJobs) retryHostedScheduledRun(ctx context.Context, project, pipeline, id string) (bool, error) {
	requeued := false
	err := s.pg.transaction(ctx, project, func(ctx context.Context, tx *sql.Tx) error {
		var tenant, creator, state, runState string
		err := tx.QueryRowContext(ctx, `SELECT d.tenant_id,d.creator_id,d.state,r.state
FROM pipeline_effect_definitions d JOIN hosted_s3_pipeline_runs r ON (r.project_id,r.pipeline_id)=(d.project_id,d.id)
WHERE d.project_id=$1 AND d.id=$2 AND r.id=$3`, project, pipeline, id).Scan(&tenant, &creator, &state, &runState)
		if err != nil || state != "active" || runState != "failed" {
			return err
		}
		allowed, err := s.pg.effects.authority.ResolveCreatorTx(ctx, tx, tenant, project, creator)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrPipelineAuthority
		}
		result, err := tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET state='queued',attempts=0,next_attempt=clock_timestamp(),error_code='',completed_at_ms=0
WHERE id=$1 AND project_id=$2 AND pipeline_id=$3 AND state='failed'`, id, project, pipeline)
		if err = pipelineCheckpoint(result, err); err != nil {
			return err
		}
		requeued = true
		return nil
	})
	return requeued, err
}
