package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

func (s *PipelineJobs) authorizeHostedScheduledTx(ctx context.Context, tx *sql.Tx, w *hostedScheduledWork) error {
	var active bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_effect_definitions
WHERE project_id=$1 AND id=$2 AND tenant_id=$3 AND state='active' AND incarnation=$4 AND destination_revision=$5 AND creator_id=$6)`,
		w.pipeline.ProjectID, w.pipeline.ID, w.tenant, w.incarnation, w.revision, w.pipeline.ActorID).Scan(&active)
	if err != nil {
		return err
	}
	if !active {
		return ErrPipelineConflict
	}
	authorized, err := s.pg.effects.authority.ResolveCreatorTx(ctx, tx, w.tenant, w.pipeline.ProjectID, w.pipeline.ActorID)
	if err != nil {
		return err
	}
	if !authorized {
		return ErrPipelineAuthority
	}
	return nil
}

func lockHostedScheduledRun(ctx context.Context, tx *sql.Tx, w *hostedScheduledWork) error {
	var id string
	return tx.QueryRowContext(ctx, `SELECT id FROM hosted_s3_pipeline_runs WHERE id=$1 AND project_id=$2 AND pipeline_id=$3 AND state='running' AND claim_id=$4`+
		exportLiveLease+` FOR UPDATE`, w.id, w.pipeline.ProjectID, w.pipeline.ID, w.claimID).Scan(&id)
}

func (s *PipelineJobs) withHostedScheduledRun(ctx context.Context, w *hostedScheduledWork, call func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = lockHostedScheduledRun(ctx, tx, w); err != nil {
		return err
	}
	if err = call(tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET lease_until=`+exportLeaseExtension+` WHERE id=$1`, w.id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *PipelineJobs) renewHostedScheduled(ctx context.Context, w *hostedScheduledWork) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.authorizeHostedScheduledTx(ctx, tx, w); err != nil {
		return err
	}
	// An upload checkpoint can hold the row lock during S3 I/O and renews
	// its own lease on completion. Never serialize the monitor behind it.
	const claimed = ` FROM hosted_s3_pipeline_runs WHERE id=$1 AND state='running' AND claim_id=$2`
	result, err := tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET lease_until=`+exportLeaseExtension+`
WHERE id IN (SELECT id`+claimed+` FOR UPDATE SKIP LOCKED)`, w.id, w.claimID)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var current bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1`+claimed+`)`, w.id, w.claimID).Scan(&current); err != nil {
			return err
		}
		if !current {
			return ErrPipelineConflict
		}
	}
	return tx.Commit()
}

func (s *PipelineJobs) runHostedScheduledAttempt(ctx context.Context, w *hostedScheduledWork) error {
	defer s.pg.scheduled.EndAttempt(ctx, w.pipeline.ProjectID, w.claimID)
	attempt, cancel := context.WithTimeout(ctx, time.Duration(s.config.ExportLimits.MaxExecutionSeconds)*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-attempt.Done():
				return
			case <-ticker.C:
				check, stop := context.WithTimeout(attempt, 5*time.Second)
				err := s.renewHostedScheduled(check, w)
				stop()
				if err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err := s.executeHostedScheduled(attempt, w)
	cancel()
	<-done
	if err == nil {
		return nil
	}
	record, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	state, code := "queued", "attempt_failed"
	if w.attempts >= hostedScheduledMaxAttempts {
		state, code = "failed", "attempts_exhausted"
	}
	if errors.Is(err, ErrPipelineAuthority) {
		state, code = "failed", "creator_unauthorized"
	}
	if ctx.Err() != nil {
		code = "interrupted"
	}
	_, err = s.db.ExecContext(record, `UPDATE hosted_s3_pipeline_runs SET state=$3,error_code=$4,lease_until=NULL,
next_attempt=clock_timestamp()+$5::double precision*interval '1 second',completed_at_ms=CASE WHEN $3='failed' THEN $6::bigint ELSE 0 END
WHERE id=$1 AND claim_id=$2 AND state='running'`, w.id, w.claimID, state, code, 1<<w.attempts, s.clock().UnixMilli())
	return err
}

func (s *PipelineJobs) hostedScheduledStore(ctx context.Context, w *hostedScheduledWork, revision string, secret []byte) (*exportstore.Store, error) {
	return s.openPipelineS3Store(ctx, w.pipeline.ProjectID, hostedCredentialBinding(w.tenant, w.pipeline.ID, w.incarnation, revision), secret, w.spec)
}

func (s *PipelineJobs) executeHostedScheduled(ctx context.Context, w *hostedScheduledWork) error {
	store, err := s.hostedScheduledStore(ctx, w, w.revision, w.pipeline.Secret)
	if err != nil {
		return err
	}
	defer store.Close()
	if w.manifest == "" {
		_, err = store.ReadManifest(ctx, pipelineManifestKey(&w.s3PipelineWork))
		if !errors.Is(err, exportstore.ErrNotFound) {
			return ErrPipelineConflict
		}
		if err = s.prepareHostedScheduledAttempt(ctx, w); err != nil {
			return err
		}
		sink := newPipelineS3Sink(ctx, s, &w.s3PipelineWork, store)
		sink.hosted = w
		sink.config.MaxOutputBytes = w.reservedBytes
		window := lakeapi.AnalyticalWindow{StartMS: w.start, EndMS: w.end}
		if _, err = s.pg.scheduled.Reader.ExportWindowAtSnapshot(ctx, w.pipeline.ProjectID, w.spec.SQL, w.spec.Parameters, window, w.snapshot,
			s.config.ExportLimits.MaxRowBytes, lakeapi.ExportStreamCallbacks{Columns: sink.columns, Row: sink.row}); err != nil {
			return err
		}
		manifest, err := sink.finish()
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(manifest)
		if err != nil || len(encoded) > exportstore.MaxManifestBytes {
			return errPipelineOutputLimit
		}
		w.manifest = string(encoded)
		err = s.withHostedScheduledRun(ctx, w, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET manifest=$2 WHERE id=$1`, w.id, w.manifest)
			return err
		})
		if err != nil {
			return err
		}
	}
	return s.publishHostedScheduled(ctx, w, store)
}

func (s *PipelineJobs) prepareHostedScheduledAttempt(ctx context.Context, w *hostedScheduledWork) error {
	if w.attemptID != "" {
		previous, err := s.hostedScheduledStore(ctx, w, w.previousRevision, w.previousSecret)
		if err != nil {
			return err
		}
		defer previous.Close()
		if err = s.withHostedScheduledRun(ctx, w, func(tx *sql.Tx) error {
			return s.cleanupHostedScheduledFiles(ctx, tx, w, previous)
		}); err != nil {
			return err
		}
	}
	w.attemptID = eventapi.NewID()
	return s.withHostedScheduledRun(ctx, w, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET attempt_id=$2,destination_revision=$3,secret=$4 WHERE id=$1`, w.id, w.attemptID, w.revision, w.pipeline.Secret)
		return err
	})
}

func (s *PipelineJobs) publishHostedScheduled(ctx context.Context, w *hostedScheduledWork, store *exportstore.Store) error {
	var manifest pipelineS3Manifest
	encoded := []byte(w.manifest)
	if json.Unmarshal(encoded, &manifest) != nil || manifest.ProjectID != w.pipeline.ProjectID || manifest.PipelineID != w.pipeline.ID ||
		manifest.RunID != w.id || manifest.WindowStartMS != w.start || manifest.WindowEndMS != w.end {
		return ErrPipelineConflict
	}
	// Linearize the immutable marker against pause, rotation and the project
	// fence. A lost database commit is retried using the exact cached marker.
	publish, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return s.pg.transaction(publish, w.pipeline.ProjectID, func(ctx context.Context, tx *sql.Tx) error {
		b := w.binding()
		if err := s.pg.effects.lockCapacity(ctx, tx, b, false); err != nil {
			return err
		}
		if err := s.authorizeHostedScheduledTx(ctx, tx, w); err != nil {
			return err
		}
		if err := lockHostedScheduledRun(ctx, tx, w); err != nil {
			return err
		}
		key := pipelineManifestKey(&w.s3PipelineWork)
		remote, err := store.ReadManifest(ctx, key)
		if errors.Is(err, exportstore.ErrNotFound) {
			if err = store.CommitManifest(ctx, key, encoded); err != nil && !errors.Is(err, exportstore.ErrExists) {
				return err
			}
			remote, err = store.ReadManifest(ctx, key)
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(remote, encoded) {
			return ErrPipelineConflict
		}
		result, err := tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_state SET cursor_ms=$4 WHERE project_id=$1 AND pipeline_id=$2 AND cursor_ms=$3`,
			w.pipeline.ProjectID, w.pipeline.ID, w.start, w.end)
		if err = pipelineCheckpoint(result, err); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE hosted_s3_pipeline_runs SET state='succeeded',lease_until=NULL,manifest='',secret=NULL,
completed_at_ms=$2,row_count=$3,byte_count=$4 WHERE id=$1`, w.id, s.clock().UnixMilli(), manifest.Rows, pipelineManifestBytes(manifest)); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM hosted_s3_pipeline_files WHERE run_id=$1`, w.id); err != nil {
			return err
		}
		if err = pruneHostedScheduledHistory(ctx, tx, w.pipeline.ProjectID, w.pipeline.ID); err != nil {
			return err
		}
		if err = s.pg.effects.charge(ctx, tx, b, b.Demand, -1); err != nil {
			return err
		}
		return s.recordHostedScheduledUsageTx(ctx, tx, w.pipeline.ProjectID, manifest)
	})
}

func (s *PipelineJobs) recordHostedScheduledUsageTx(ctx context.Context, tx *sql.Tx, project string, manifest pipelineS3Manifest) error {
	if s.usage == nil {
		return nil
	}
	return s.usage.AddTx(ctx, tx, project, s.clock(), usage.Counters{PipelineBytes: pipelineManifestBytes(manifest)}, 0)
}
