package porcupine

import (
	"context"
	"database/sql"
	"errors"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

// assembleNext persists one exact, closed-window cross-fragment claim before
// staging its artifacts. Recomposition uses the same durable claim and keeps
// the canceled delivery's original identity and bytes.
func (w *hostedPipelineDelivery) assembleNext(ctx context.Context) (bool, error) {
	f := w.flusher
	p := f.publisher
	first, err := p.effects.NextFragment(ctx, p.control, f.native.Config.SharedSourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	key := lakeapi.ProjectWriteKey{SourceID: first.Claim.Scope.SourceID, ProjectID: first.Claim.Scope.ProjectID}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return false, err
	}
	defer session.Release()
	var progress bool
	err = f.runActiveHostedProject(ctx, session, key.ProjectID, func(ctx context.Context) error {
		scope, err := f.native.LookupHostedProjectExisting(ctx, key)
		if err != nil {
			return err
		}
		if pipelineScope(scope) != first.Claim.Scope {
			return output.ErrPipelineConflict
		}
		fence := session.Fence(scope.StreamID)
		if err := f.native.AdvanceRecoveryFence(ctx, fence); err != nil {
			return err
		}
		if err := f.reconcileHostedWrites(ctx, session, scope, fence, first.Claim.TenantID); err != nil {
			return err
		}
		progress, err = w.assembleUnderFence(ctx, session, scope, first)
		if progress || ctx.Err() != nil {
			return err
		}
		return errors.Join(err, w.deferWork(ctx, session, first.Claim))
	})
	return progress, err
}

func (w *hostedPipelineDelivery) assembleUnderFence(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, first output.PipelineFragment) (progress bool, resultErr error) {
	if w.deleteGate == nil || w.beginAttemptTx == nil || w.endAttempt == nil {
		return false, output.ErrPipelineAuthority
	}
	p := w.flusher.publisher
	tenant := first.Claim.TenantID
	a, err := p.effects.Assembly(ctx, p.control, pipelineScope(scope), tenant)
	newClaim := errors.Is(err, sql.ErrNoRows)
	if err != nil && !newClaim {
		return false, err
	}
	if !newClaim {
		for _, in := range a.Inputs {
			for _, index := range in.Eligible {
				head, err := w.flusher.native.Load(ctx, scope.Key.ProjectID, in.Fragment.Members[index].UUID)
				if err != nil {
					return false, err
				}
				if head.IsDeleted {
					err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
						return p.effects.AbandonAssemblyTx(ctx, tx, a)
					})
					return err == nil, err
				}
			}
		}
	}
	if newClaim {
		a, err = w.prepareAssembly(ctx, scope, first)
		if err != nil {
			return false, err
		}
	}
	ticket := eventapi.NewID()
	registered := false
	err = session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		blocked, err := w.deleteGate(ctx, tx, scope, tenant)
		if err != nil || blocked {
			return err
		}
		if newClaim {
			if err := p.effects.ReserveAssemblyTx(ctx, tx, a); err != nil {
				return err
			}
		} else if err := p.effects.CheckAssemblyTx(ctx, tx, a); err != nil {
			return err
		}
		if err := w.beginAttemptTx(ctx, tx, scope.Key.ProjectID, ticket); err != nil {
			return err
		}
		registered = true
		return nil
	})
	if !newClaim && (errors.Is(err, output.ErrPipelineAuthority) || errors.Is(err, output.ErrPipelineConflict) || errors.Is(err, sql.ErrNoRows)) {
		err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return p.effects.AbandonAssemblyTx(ctx, tx, a)
		})
		return err == nil, err
	}

	if registered {
		defer func() { resultErr = errors.Join(resultErr, w.finishTicket(ctx, scope.Key.ProjectID, ticket)) }()
	}
	if err != nil || !registered {
		return false, err
	}
	r, err := output.StageWebhookAssembly(ctx, p.objects, scope, a)
	if err != nil {
		return false, err
	}
	err = session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		blocked, err := w.deleteGate(ctx, tx, scope, tenant)
		if err != nil {
			return err
		}
		if blocked {
			return output.ErrPipelineConflict
		}
		return p.effects.CompleteAssemblyTx(ctx, tx, a, r)
	})
	return err == nil, err
}

func (w *hostedPipelineDelivery) prepareAssembly(ctx context.Context, scope lakeapi.HostedProjectScope, first output.PipelineFragment) (output.PipelineAssembly, error) {
	f := w.flusher
	p := f.publisher
	a := output.PipelineAssembly{Scope: pipelineScope(scope), TenantID: first.Claim.TenantID, Generation: eventapi.NewID(), Attempt: eventapi.NewID(), ArtifactLimit: 512 << 10}
	var err error
	a.Cut, _, err = p.checkpoint(ctx, scope, a.TenantID)
	if err != nil {
		return a, err
	}
	_, a.Prefix, err = p.objects.DescribeImmutableEffectObject(scope, a.Attempt, []byte("{}"))
	if err != nil {
		return a, err
	}
	fragments, err := p.effects.AssemblyFragments(ctx, p.control, first)
	if err != nil {
		return a, err
	}
	for _, fragment := range fragments {
		in := output.PipelineAssemblyInput{Fragment: fragment}
		for i := fragment.Next; i < len(fragment.Members); i++ {
			head, err := f.native.Load(ctx, scope.Key.ProjectID, fragment.Members[i].UUID)
			if err != nil {
				return a, err
			}
			if !head.IsDeleted {
				in.Eligible = append(in.Eligible, i)
			}
		}
		a.Inputs = append(a.Inputs, in)
	}
	var upperBound int64
	for _, input := range a.Inputs {
		for _, i := range input.Eligible {
			upperBound += input.Fragment.Members[i].Bytes + 1024
		}
	}
	a.ArtifactLimit += min(lakeapi.MaxImmutableEffectObjectBytes, upperBound)

	return a, nil
}
