package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/observability"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

const hostedPolicyRefreshInterval = 10 * time.Second

type hostedLiveFlusher struct {
	native             *lakeapi.DuckLakeStore
	coordinator        *lakeapi.PostgresProjectCoordinator
	publisher          *hostedEffectPublisher
	policy             hostedIngestionPolicy
	incarnation        *hostedIncarnation
	metrics            *operationalMetrics
	returnedActivities sync.Map
}

type hostedPublisherHooks struct {
	ensureCapacity        func(context.Context, *sql.Tx, string, string) error
	loadRule              func(context.Context, string, string, output.PipelineTarget) ([]byte, error)
	cancelMutationWebhook func(context.Context, *sql.Tx, output.PipelineScope, string, string) error
	checkpointImport      func(context.Context, *sql.Tx, lakeapi.NativeCommitEvidence) error
}

func newHostedLiveFlusher(control *sql.DB, native *lakeapi.DuckLakeStore, coordinator *lakeapi.PostgresProjectCoordinator, meter *usage.Store, effects *output.PGPipelineEffects, hooks hostedPublisherHooks) (*hostedLiveFlusher, error) {
	if control == nil || native == nil || native.SharedCatalog == nil || native.Remote == nil || coordinator == nil || meter == nil || meter.DB != control || !meter.Postgres {
		return nil, errors.New("hosted flusher requires real shared providers")
	}
	if effects == nil {
		return nil, errors.New("hosted flusher requires pipeline effects")
	}
	return &hostedLiveFlusher{
		native:      native,
		coordinator: coordinator,
		publisher: &hostedEffectPublisher{
			control:               control,
			effects:               effects,
			usage:                 meter,
			objects:               native.Remote,
			ensureCapacity:        hooks.ensureCapacity,
			loadRule:              hooks.loadRule,
			cancelMutationWebhook: hooks.cancelMutationWebhook,
			checkpointImport:      hooks.checkpointImport,
		},
		policy: hostedIngestionPolicy{db: control},
	}, nil
}

func (f *hostedLiveFlusher) lockLiveAdmission(ctx context.Context, project string) (func(), error) {
	work, cancel := context.WithTimeout(ctx, hostedActivityControlTimeout)
	stop := f.incarnation.cancelOnLoss(cancel)
	tx, err := f.publisher.control.BeginTx(work, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		stop()
		return nil, err
	}
	release := func() { _ = tx.Rollback(); stop() }
	if _, err := lockActiveHostedProject(work, tx, project); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// FlushLive processes batches admitted by the buffered ingestion path.
func (f *hostedLiveFlusher) FlushLive(ctx context.Context, b acceptance.FlushBatch) error {
	if b.ID == "" || b.ProjectID == "" || len(b.Events) == 0 {
		return output.ErrPipelineConflict
	}
	ctx, cancel := f.incarnation.writeContext(ctx)
	defer cancel()
	key := lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: b.ProjectID}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	err = f.runActiveHostedProject(ctx, session, key.ProjectID, func(ctx context.Context) error {
		scope, e := f.native.LookupHostedProjectExisting(ctx, key)
		if e != nil {
			return e
		}
		fence := session.Fence(scope.StreamID)
		var tenant string
		if e = f.publisher.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, b.ProjectID).Scan(&tenant); e != nil {
			return e
		}
		batch := lakeapi.LiveCommitBatch{ID: b.ID, TenantID: tenant, Scope: scope}
		for _, input := range b.Events {
			batch.Inputs = append(batch.Inputs, lakeapi.LiveCommitInput{Event: input.Input.Event, Identity: input.Input.Identity, AdmittedAt: input.AdmittedAt})
		}
		recovery, e := f.needsLiveRecovery(ctx, scope, tenant)
		if e != nil {
			return e
		}
		if !recovery {
			e = f.commitLiveBatch(ctx, session, fence, batch, b.CanSplit())
			if !errors.Is(e, lakeapi.ErrNativeEffectsPending) {
				return e
			}
		}
		// Reservation cleanup and mutation settlement need a committed fence
		// before reading native evidence. Ordinary flushes establish their
		// fence only when the next outcome commits.
		if e = f.native.AdvanceRecoveryFence(ctx, fence); e != nil {
			return e
		}
		if e = f.reconcileHostedWrites(ctx, session, scope, fence, tenant); e != nil {
			return e
		}
		return f.commitLiveBatch(ctx, session, fence, batch, b.CanSplit())
	})
	if err != nil && session.Context().Err() != nil {
		return errors.Join(err, context.Cause(session.Context()))
	}
	return err
}

// Activity settlement can join a known error with a transient failure. Only a
// single cause from the known deterministic checks can advance quarantine.
func hostedDeterministicFlushError(err error) string {
	for err != nil {
		switch err {
		case lakeapi.ErrNativeEvidenceGap:
			return "native_evidence_gap"
		case errHostedEvidenceBinding:
			return "pipeline_evidence_conflict"
		case lakeapi.ErrInvalidLiveProjection:
			return "invalid_live_projection"
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			causes := joined.Unwrap()
			if len(causes) != 1 {
				return ""
			}
			err = causes[0]
		} else {
			err = errors.Unwrap(err)
		}
	}
	return ""
}

func (f *hostedLiveFlusher) needsLiveRecovery(ctx context.Context, scope lakeapi.HostedProjectScope, tenant string) (bool, error) {
	if _, err := f.publisher.reservation(ctx, scope); !errors.Is(err, sql.ErrNoRows) {
		return err == nil, err
	}
	_, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "state", "ready")
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func (f *hostedLiveFlusher) commitLiveBatch(ctx context.Context, session *lakeapi.ProjectWriteSession, fence lakeapi.ProjectWriteFence, batch lakeapi.LiveCommitBatch, canSplit bool) error {
	seq, hash, err := f.publisher.checkpoint(ctx, batch.Scope, batch.TenantID)
	if err != nil {
		return err
	}
	ids, err := session.ReserveEventIDs(ctx, batch.Scope.StreamID, batch.ID, len(batch.Inputs))
	if err != nil {
		return err
	}
	var stager *reservingHostedStager
	stage := liveEffectStageFunc(func(ctx context.Context, selected lakeapi.SelectedLiveEffects) (lakeapi.StagedLiveEffects, error) {
		if stager == nil {
			plan, routing, err := f.publisher.plan(ctx, batch.TenantID, batch.Scope)
			if err != nil {
				return lakeapi.StagedLiveEffects{}, err
			}
			stager = &reservingHostedStager{publisher: f.publisher, session: session, fence: fence, plan: plan, routing: routing, canSplit: canSplit}
		}
		return stager.StageLiveEffects(ctx, selected)
	})
	checkpoint := lakeapi.NativeEffectCheckpoint{Sequence: seq, Fingerprint: hash}
	result, err := f.native.CommitLiveFromCheckpoint(ctx, fence, batch, ids, stage, checkpoint, func(ctx context.Context, commit func() error) error {
		return f.commitLiveIfActive(ctx, batch.Scope.Key.ProjectID, batch.TenantID, commit)
	})
	if err != nil {
		return err
	}
	switch v := result.(type) {
	case lakeapi.DuplicateLiveBatch:
		return nil
	case lakeapi.CommittedLiveBatch:
		if v.Fresh {
			var refused uint64
			for pipeline, count := range stager.refused {
				slog.WarnContext(ctx, "hosted webhook output refused", "project_id", batch.Scope.Key.ProjectID, "batch_id", batch.ID, "pipeline_id", pipeline, "reason", stager.refusedReason, "references", count)
				refused += count
			}
			if f.metrics != nil {
				f.metrics.observeIngestionEvents(acceptance.Buffered, "native_committed", uint64(v.Evidence.AcceptedLive))
				if refused > 0 {
					f.metrics.Add("porcupine_webhook_refused_references_total", refused, observability.Label{Name: "reason", Value: stager.refusedReason})
				}
			}
		}
		if v.Evidence.Sequence <= seq {
			return nil
		}
		if stager == nil || stager.binding == (output.PipelineBinding{}) {
			return output.ErrPipelineConflict
		}
		return f.publisher.publishEvidence(ctx, stager.binding, v.Evidence, nil, false, stager.uploadedManifest)
	default:
		return output.ErrPipelineConflict
	}
}

// The writer's activity ticket and advisory session cover the native commit.
// Deletion may fence further admissions after this check, but sealing must
// drain this ticket and acquire the project session before moving the native fence.
func (f *hostedLiveFlusher) commitLiveIfActive(ctx context.Context, project, tenant string, commit func() error) error {
	if err := f.publisher.transaction(ctx, func(tx *sql.Tx) error {
		currentTenant, err := lockActiveHostedProject(ctx, tx, project)
		if err != nil {
			return err
		}
		if currentTenant != tenant {
			return errHostedProjectFenced
		}
		return nil
	}); err != nil {
		return err
	}
	return commit()
}

func (f *hostedLiveFlusher) commitImportHeld(ctx context.Context, session *lakeapi.ProjectWriteSession, fence lakeapi.ProjectWriteFence, input lakeapi.ImportChunkInput, authorize func(context.Context) error) (lakeapi.NativeCommitEvidence, error) {
	prior, found, err := f.native.FindImportChunk(ctx, fence, input)
	if err != nil || found {
		return prior, err
	}
	if err = f.publisher.transaction(ctx, func(tx *sql.Tx) error {
		if err := f.publisher.effects.PrepareNoOutputStreamTx(ctx, tx, pipelineScope(input.Key.Scope), input.TenantID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO hosted_recovery_pending(project_id) VALUES($1) `+hostedRecoveryConflict, input.Key.Scope.Key.ProjectID)
		return err
	}); err != nil {
		return lakeapi.NativeCommitEvidence{}, err
	}
	var ids lakeapi.EventIDReservation
	if len(input.Candidates) > 0 {
		ids, err = session.ReserveEventIDs(ctx, input.Key.Scope.StreamID, input.Key.ID(), len(input.Candidates))
		if err != nil {
			return lakeapi.NativeCommitEvidence{}, err
		}
	}
	if authorize != nil {
		if err := authorize(ctx); err != nil {
			return lakeapi.NativeCommitEvidence{}, err
		}
	}
	result, err := f.native.CommitImportChunk(ctx, fence, input, ids)
	if err != nil {
		return lakeapi.NativeCommitEvidence{}, err
	}
	if err = f.publisher.publish(ctx, output.PipelineBinding{}, result); err != nil {
		return lakeapi.NativeCommitEvidence{}, err
	}
	_ = f.native.AcknowledgeNativeEffects(ctx, fence, input.Key.Scope, result.Sequence)
	return result, nil
}

// reconcileHostedWrites runs on an acquired project session after the caller
// advances its native fence. Use it before selecting a snapshot or send cut.
func (f *hostedLiveFlusher) reconcileHostedWrites(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string) error {
	return ignoreHostedACKPending(f.reconcileAndSettle(ctx, session, scope, fence, tenant))
}

func (f *hostedLiveFlusher) reconcile(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string) error {
	return ignoreHostedACKPending(f.reconcileEvidence(ctx, session, scope, fence, tenant, false))
}

func (f *hostedLiveFlusher) reconcileAndSettle(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string) error {
	err := f.reconcileEvidence(ctx, session, scope, fence, tenant, false)
	if ignoreHostedACKPending(err) != nil {
		return err
	}
	if settleErr := f.settleReadyHostedMutation(ctx, session, scope, fence, tenant, false); settleErr != nil {
		return settleErr
	}
	return err
}

func (f *hostedLiveFlusher) reconcileEvidence(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string, deleting bool) error {
	seq, checkpointHash, err := f.publisher.checkpoint(ctx, scope, tenant)
	if err != nil {
		return err
	}
	// Initial one-unpublished-batch discipline makes a single proof sufficient.
	page, err := f.native.PendingNativeEffects(ctx, scope, seq, 2)
	if err != nil {
		return err
	}

	if seq > 0 {
		covered, e := f.native.PendingNativeEffects(ctx, scope, seq-1, 1)
		if e != nil {
			return e
		}
		if covered.NativeHighWater != page.NativeHighWater || len(covered.Evidence) != 1 || covered.Evidence[0].Sequence != seq || covered.Evidence[0].ProofFingerprint != checkpointHash {
			return lakeapi.ErrNativeEvidenceGap
		}
	}
	if len(page.Evidence) > 1 {
		return output.ErrPipelineConflict
	}
	binding, resErr := f.publisher.reservation(ctx, scope)
	if resErr != nil && !errors.Is(resErr, sql.ErrNoRows) {
		return resErr
	}
	if len(page.Evidence) == 1 {
		evidence := page.Evidence[0]
		switch evidence.Kind {
		case lakeapi.NativeCommitLive:
			if resErr != nil {
				return resErr
			}
		case lakeapi.NativeCommitImport, lakeapi.NativeCommitMutation:
			if resErr == nil {
				// The previous writer can commit before the live successor's
				// staged transaction wins the native fence. Its proof then
				// occupies the only unpublished sequence. The separate recovery
				// fence rules out any further outcome from the live reservation.
				if binding.Epoch <= evidence.FenceEpoch {
					return output.ErrPipelineConflict
				}
				if err := f.releaseAbsentLiveReservation(ctx, session, fence, binding); err != nil {
					return err
				}
			}
			binding = output.PipelineBinding{}
		default:
			return output.ErrPipelineConflict
		}
		if deleting {
			err = f.publisher.publishEvidence(ctx, binding, evidence, session, true, lakeapi.ImmutableObjectRef{})
		} else if evidence.Kind == lakeapi.NativeCommitMutation {
			err = f.publisher.publishOn(ctx, binding, evidence, session)
		} else {
			err = f.publisher.publish(ctx, binding, evidence)
		}
		if err != nil {
			return err
		}
		seq = page.Evidence[0].Sequence
	} else if resErr == nil {
		if err := f.releaseAbsentLiveReservation(ctx, session, fence, binding); err != nil {
			return err
		}
	}
	if seq > page.AcknowledgedThrough {
		return f.acknowledgeHostedEffects(ctx, fence, scope, seq)
	}
	return nil
}

// Call only after a committed successor fence and a complete proof suffix
// establish absence. Cleanup retains the charge under the original acquisition.
func (f *hostedLiveFlusher) releaseAbsentLiveReservation(ctx context.Context, session *lakeapi.ProjectWriteSession, fence lakeapi.ProjectWriteFence, binding output.PipelineBinding) error {
	if binding.Epoch >= fence.Epoch {
		return output.ErrPipelineConflict
	}
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return f.publisher.effects.ReleaseAbsentReservationTx(ctx, tx, binding, output.PipelineSettlement{SuccessorEpoch: fence.Epoch, Outcome: output.PipelineReleaseReservation})
	})
}

type hostedPolicySnapshot struct {
	allowed bool
	limit   sql.NullInt64
	used    int64
}
type hostedIngestionPolicy struct {
	db        *sql.DB
	mu        sync.Mutex
	refreshed time.Time
	month     string
	tenants   map[string]hostedPolicySnapshot
}

func (p *hostedIngestionPolicy) admit(ctx context.Context, tenant string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now().UTC()
	start, _ := tenantMonth(now)
	month := start.Format(time.RFC3339)
	if p.month != month || time.Since(p.refreshed) >= hostedPolicyRefreshInterval {
		// One grouped shared read, never a per-event control call or escrow.
		rows, err := p.db.QueryContext(ctx, `SELECT t.id,t.ingestion_allowed,t.ingestion_monthly_events,coalesce(u.accepted_events,0) FROM tenants t LEFT JOIN tenant_usage u ON u.tenant_id=t.id AND u.period_start=$1`, month)
		if err != nil {
			return err
		}
		next := map[string]hostedPolicySnapshot{}
		for rows.Next() {
			var id string
			var v hostedPolicySnapshot
			if err = rows.Scan(&id, &v.allowed, &v.limit, &v.used); err != nil {
				rows.Close()
				return err
			}
			next[id] = v
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			return err
		}
		p.tenants = next
		p.month = month
		p.refreshed = time.Now()
	}
	v, ok := p.tenants[tenant]
	if !ok {
		return output.ErrPipelineAuthority
	}
	if !v.allowed {
		return &tenantIngestionError{code: "ingestion_suspended", message: "tenant ingestion is suspended"}
	}
	if v.limit.Valid && v.used >= v.limit.Int64 {
		return &tenantIngestionError{code: "ingestion_budget_exceeded", message: "tenant monthly ingestion budget exceeded"}
	}
	return nil
}
