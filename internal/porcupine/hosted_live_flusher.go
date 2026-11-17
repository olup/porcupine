package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

// hostedLiveFlusher is deliberately absent from Service construction. Enabling
// buffered acceptance also needs the full shared API, consent, assembly and send
// providers. This constructor requires real PG, native and object storage.
type hostedLiveFlusher struct {
	native      *lakeapi.DuckLakeStore
	coordinator *lakeapi.PostgresProjectCoordinator
	publisher   *hostedEffectPublisher
	policy      hostedIngestionPolicy
}

func newHostedLiveFlusher(control *sql.DB, native *lakeapi.DuckLakeStore, coordinator *lakeapi.PostgresProjectCoordinator, meter *usage.Store) (*hostedLiveFlusher, error) {
	if control == nil || native == nil || native.SharedCatalog == nil || native.Remote == nil || coordinator == nil || meter == nil || meter.DB != control || !meter.Postgres {
		return nil, errors.New("hosted flusher requires real shared providers")
	}
	effects, err := output.NewPGPipelineEffects(hostedPipelineNativeAuthority{})
	if err != nil {
		return nil, err
	}
	return &hostedLiveFlusher{native: native, coordinator: coordinator, publisher: &hostedEffectPublisher{control: control, effects: effects, usage: meter, objects: native.Remote}, policy: hostedIngestionPolicy{db: control}}, nil
}
func (f *hostedLiveFlusher) FlushLive(ctx context.Context, b bufferedFlushBatch) error {
	if b.id == "" || b.projectID == "" || len(b.events) == 0 {
		return output.ErrPipelineConflict
	}
	key := lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: b.projectID}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	return session.RunNative(ctx, func(ctx context.Context) error {
		scope, e := f.native.LookupHostedProjectExisting(ctx, key)
		if e != nil {
			return e
		}
		fence := session.Fence(scope.StreamID)
		if e = f.native.AdvanceRecoveryFence(ctx, fence); e != nil {
			return e
		}
		var tenant string
		if e = f.publisher.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, b.projectID).Scan(&tenant); e != nil {
			return e
		}
		batch := lakeapi.LiveCommitBatch{ID: b.id, TenantID: tenant, Scope: scope}
		for _, input := range b.events {
			batch.Inputs = append(batch.Inputs, lakeapi.LiveCommitInput{Event: input.input.event, Identity: input.input.identity, AdmittedAt: input.admittedAt})
		}
		prior, found, e := f.native.FindLiveCommitBatch(ctx, fence, batch)
		if e != nil {
			return e
		}
		if e = f.reconcile(ctx, session, scope, fence, tenant); e != nil {
			return e
		}
		if e := f.settleReadyHostedMutation(ctx, session, scope, fence, tenant); e != nil {
			return e
		}
		if found {
			// Reconciliation has confirmed publication; ACK failure cannot retain input.
			seq, hash, e := f.publisher.checkpoint(ctx, scope, tenant)
			if e != nil {
				return e
			}
			if seq < prior.Sequence || (seq == prior.Sequence && hash != prior.ProofFingerprint) {
				return output.ErrPipelineConflict
			}
			return nil
		}
		plan, routing, e := f.publisher.plan(ctx, tenant, scope)
		if e != nil {
			return e
		}
		ids, e := session.ReserveEventIDs(ctx, scope.StreamID, b.id, len(batch.Inputs))
		if e != nil {
			return e
		}
		stager := &reservingHostedStager{publisher: f.publisher, fence: fence, plan: plan, routing: routing, admission: &f.policy}
		result, e := f.native.CommitLive(ctx, fence, batch, ids, stager)
		if e != nil {
			return e
		}
		switch v := result.(type) {
		case lakeapi.DuplicateLiveBatch:
			return nil
		case lakeapi.CommittedLiveBatch:
			binding, e := f.publisher.reservation(ctx, scope)
			if e != nil {
				return e
			}
			if e = f.publisher.publish(ctx, binding, v.Evidence); e != nil {
				return e
			}
			_ = f.native.AcknowledgeNativeEffects(ctx, fence, scope, v.Evidence.Sequence)
			return nil
		default:
			return output.ErrPipelineConflict
		}
	})
}

// CommitImport coordinates already validated historical input. Upload storage,
// actor authorization and manifest/file validation remain at the upload boundary.
// It preflights the only PG cursor row before any new native import outcome.
func (f *hostedLiveFlusher) CommitImport(ctx context.Context, input lakeapi.ImportChunkInput) (lakeapi.NativeCommitEvidence, error) {
	session, err := f.coordinator.Acquire(ctx, input.Key.Scope.Key)
	if err != nil {
		return lakeapi.NativeCommitEvidence{}, err
	}
	defer session.Release()
	var result lakeapi.NativeCommitEvidence
	err = session.RunNative(ctx, func(ctx context.Context) error {
		scope, err := f.native.LookupHostedProjectExisting(ctx, input.Key.Scope.Key)
		if err != nil {
			return err
		}
		if scope != input.Key.Scope {
			return output.ErrPipelineConflict
		}
		fence := session.Fence(scope.StreamID)
		if err = f.native.AdvanceRecoveryFence(ctx, fence); err != nil {
			return err
		}
		prior, found, err := f.native.FindImportChunk(ctx, fence, input)
		if err != nil {
			return err
		}
		if err = f.reconcile(ctx, session, scope, fence, input.TenantID); err != nil {
			return err
		}
		if err := f.settleReadyHostedMutation(ctx, session, scope, fence, input.TenantID); err != nil {
			return err
		}
		if found {
			result = prior
			return nil
		}
		if err = f.publisher.transaction(ctx, func(tx *sql.Tx) error {
			return f.publisher.effects.PrepareNoOutputStreamTx(ctx, tx, pipelineScope(scope), input.TenantID)
		}); err != nil {
			return err
		}
		var ids lakeapi.EventIDReservation
		if len(input.Candidates) > 0 {
			ids, err = session.ReserveEventIDs(ctx, scope.StreamID, input.Key.ID(), len(input.Candidates))
			if err != nil {
				return err
			}
		}
		result, err = f.native.CommitImportChunk(ctx, fence, input, ids)
		if err != nil {
			return err
		}
		if err = f.publisher.publish(ctx, output.PipelineBinding{}, result); err != nil {
			return err
		}
		_ = f.native.AcknowledgeNativeEffects(ctx, fence, scope, result.Sequence)
		return nil
	})
	if err != nil {
		return lakeapi.NativeCommitEvidence{}, err
	}
	return result, nil
}

// RecoverProject can run cold without producer credentials, payload or worker
// disk. It fences before high-water capture and never creates missing projects.
func (f *hostedLiveFlusher) RecoverProject(ctx context.Context, key lakeapi.ProjectWriteKey) error {
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	return session.RunNative(ctx, func(ctx context.Context) error {
		scope, e := f.native.LookupHostedProjectExisting(ctx, key)
		if e != nil {
			return e
		}
		fence := session.Fence(scope.StreamID)
		if e = f.native.AdvanceRecoveryFence(ctx, fence); e != nil {
			return e
		}
		var tenant string
		if e = f.publisher.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, key.ProjectID).Scan(&tenant); e != nil {
			return e
		}
		if err := f.reconcile(ctx, session, scope, fence, tenant); err != nil {
			return err
		}
		return f.settleReadyHostedMutation(ctx, session, scope, fence, tenant)
	})
}
func (f *hostedLiveFlusher) reconcile(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string) error {
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
		switch page.Evidence[0].Kind {
		case lakeapi.NativeCommitLive:
			if resErr != nil {
				return resErr
			}
		case lakeapi.NativeCommitImport, lakeapi.NativeCommitMutation:
			if resErr == nil {
				return output.ErrPipelineConflict
			}
			binding = output.PipelineBinding{}
		default:
			return output.ErrPipelineConflict
		}
		evidence := page.Evidence[0]
		if evidence.Kind == lakeapi.NativeCommitMutation {
			err = f.publisher.publishOn(ctx, binding, evidence, session)
		} else {
			err = f.publisher.publish(ctx, binding, evidence)
		}
		if err != nil {
			return err
		}
		seq = page.Evidence[0].Sequence
	} else if resErr == nil {
		// Complete retained history after a committed successor fence proves the
		// unresolved batch did not commit. Cleanup remains charged under the original acquisition. A retry
		// owns a separately charged, disjoint acquisition namespace.
		if binding.Epoch >= fence.Epoch {
			return output.ErrPipelineConflict
		}
		err = f.publisher.transaction(ctx, func(tx *sql.Tx) error {
			return f.publisher.effects.ReleaseAbsentReservationTx(ctx, tx, binding, output.PipelineSettlement{SuccessorEpoch: fence.Epoch, Outcome: output.PipelineReleaseReservation})
		})
		if err != nil {
			return err
		}
	}
	if seq > page.AcknowledgedThrough {
		_ = f.native.AcknowledgeNativeEffects(ctx, fence, scope, seq)
	}
	return nil
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
	start, end := tenantMonth(now)
	month := start.Format(time.RFC3339)
	if p.month != month || time.Since(p.refreshed) >= sharedIngestionRefreshInterval {
		// One grouped shared read, never a per-event control call or escrow.
		rows, err := p.db.QueryContext(ctx, `SELECT t.id,t.ingestion_allowed,t.ingestion_monthly_events,coalesce(sum(u.live_events+u.import_events),0) FROM tenants t LEFT JOIN usage_rollups u ON u.tenant_id=t.id AND u.day_start >= $1 AND u.day_start < $2 GROUP BY t.id,t.ingestion_allowed,t.ingestion_monthly_events`, month, end.Format(time.RFC3339))
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
