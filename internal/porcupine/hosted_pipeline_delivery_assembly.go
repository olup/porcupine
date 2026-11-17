package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/olup/porcupine/internal/delivery"
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
	err = session.RunNative(ctx, func(ctx context.Context) error {
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
		if err := w.reconcile(ctx, session, scope, fence, first.Claim.TenantID); err != nil {
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
	r, err := w.stageAssembly(ctx, scope, a)
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
	fragments, err := p.effects.AssemblyFragments(ctx, p.control, first, hostedAssemblyMaxReadBytes)
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

func (w *hostedPipelineDelivery) stageAssembly(ctx context.Context, scope lakeapi.HostedProjectScope, a output.PipelineAssembly) (output.PipelineAssemblyResult, error) {
	f := w.flusher
	anchor := a.Inputs[0].Fragment
	if anchor.Recompose {
		return w.recomposeAssembly(ctx, scope, a)
	}
	target := frozenWebhookTarget{PipelineID: anchor.Target.ID, PipelineIncarnation: anchor.Target.Incarnation, RuleRevision: anchor.Target.RuleRevision, DestinationRevision: anchor.Target.DestinationRevision, Rule: anchor.Spec.Rule}
	claim := frozenWebhookAssemblyClaim{
		Scope: scope, TenantID: a.TenantID, Target: target, WindowStartMS: anchor.Spec.WindowStartMS, WindowEndMS: anchor.Spec.WindowEndMS,
		Generation: a.Generation, AttemptID: a.Attempt, MaxFragments: hostedAssemblyMaxFragments, MaxReadBytes: hostedAssemblyMaxReadBytes, MaxBodyBytes: lakeapi.MaxImmutableEffectObjectBytes,
	}
	for _, in := range a.Inputs {
		if len(in.Eligible) == 0 {
			continue
		}
		fragment := in.Fragment
		converted := hostedAssemblyFragment{
			Position: hostedFragmentPosition{Scope: scope, CommitSequence: fragment.Sequence, FragmentID: fragment.Claim.ID, NextMember: fragment.Next},
			Fragment: immutableWebhookFragment{FragmentID: fragment.Claim.ID, Target: target, WindowStartMS: fragment.Spec.WindowStartMS, WindowEndMS: fragment.Spec.WindowEndMS, Body: nativeRef(fragment.Body)},
			Members:  in.Eligible,
		}
		for _, member := range fragment.Members {
			converted.Fragment.EventUUIDs = append(converted.Fragment.EventUUIDs, member.UUID)
			converted.Fragment.EventBytes = append(converted.Fragment.EventBytes, member.Bytes)
		}
		claim.Fragments = append(claim.Fragments, converted)
	}
	var r output.PipelineAssemblyResult
	if len(claim.Fragments) > 0 {
		var err error
		claim.MembershipFingerprint, err = hostedAssemblyFingerprint(claim.Fragments)
		if err != nil {
			return r, err
		}
		staged, err := assembleHostedWebhook(ctx, f.publisher.objects, claim)
		if err != nil {
			return r, err
		}
		for _, ref := range []lakeapi.ImmutableObjectRef{staged.Body, staged.Manifest} {
			if _, err := f.publisher.objects.ReadImmutableEffectObject(ctx, scope, ref); err != nil {
				return r, err
			}
		}
		r.ID, r.Body, r.Manifest, r.Count = staged.DeliveryID, pipelineRef(staged.Body), pipelineRef(staged.Manifest), staged.EventCount
	}
	remaining := r.Count
	for _, in := range a.Inputs {
		take := min(remaining, len(in.Eligible))
		remaining -= take
		next := len(in.Fragment.Members)
		if take < len(in.Eligible) {
			next = in.Eligible[take]
		}
		r.Next = append(r.Next, next)
	}
	return r, nil
}

func (w *hostedPipelineDelivery) recomposeAssembly(ctx context.Context, scope lakeapi.HostedProjectScope, a output.PipelineAssembly) (output.PipelineAssemblyResult, error) {
	in := a.Inputs[0]
	f := in.Fragment
	r := output.PipelineAssemblyResult{Next: []int{f.Count}}
	if len(in.Eligible) == 0 {
		return r, nil
	}
	objects := w.flusher.publisher.objects
	body, err := objects.ReadImmutableEffectObject(ctx, scope, nativeRef(f.Body))
	if err != nil {
		return r, err
	}
	var original delivery.WebhookPayload
	if err := decodeHostedJSON(body, &original); err != nil {
		return r, err
	}
	if original.Event != nil || original.DeliveryID != f.Claim.ID || original.PipelineID != f.Target.ID || original.ProjectID != scope.Key.ProjectID ||
		original.WindowStartMS == nil || original.WindowEndMS == nil || *original.WindowStartMS != f.Spec.WindowStartMS || *original.WindowEndMS != f.Spec.WindowEndMS || len(original.Events) != f.Count {
		return r, output.ErrPipelineConflict
	}
	for i, event := range original.Events {
		if event.UUID != f.Members[i].UUID || event.ProjectID != scope.Key.ProjectID {
			return r, output.ErrPipelineConflict
		}
	}
	identity, err := json.Marshal(struct {
		Scope             lakeapi.HostedProjectScope
		Prior, Generation string
		Positions         []int
	}{scope, f.Claim.ID, a.Generation, in.Eligible})
	if err != nil {
		return r, err
	}
	r.ID = f.Claim.ID
	if f.Frozen {
		r.ID = fmt.Sprintf("pc_hook_%x", sha256.Sum256(identity))
	}
	prefix, err := delivery.WebhookBatchPrefix(r.ID, f.Target.ID, scope.Key.ProjectID, *original.WindowStartMS, *original.WindowEndMS)
	if err != nil {
		return r, err
	}
	var events [][]byte
	for _, i := range in.Eligible {
		b, err := delivery.EncodeJSON(original.Events[i])
		if err != nil {
			return r, err
		}
		if int64(len(b)) != f.Members[i].Bytes {
			return r, output.ErrPipelineConflict
		}
		events = append(events, b)
	}
	recomposed, err := delivery.EncodeWebhookBatch(prefix, events, lakeapi.MaxImmutableEffectObjectBytes)
	if err != nil {
		return r, err
	}
	newBody, err := objects.PutImmutableEffectObject(ctx, scope, a.Attempt, recomposed)
	if err != nil {
		return r, err
	}
	manifest, err := json.Marshal(struct {
		ID          string
		Prior, Body lakeapi.ImmutableObjectRef
		Positions   []int
	}{r.ID, nativeRef(f.Body), newBody, in.Eligible})
	if err != nil {
		return r, err
	}
	newManifest, err := objects.PutImmutableEffectObject(ctx, scope, a.Attempt, manifest)
	if err != nil {
		return r, err
	}
	for _, ref := range []lakeapi.ImmutableObjectRef{newBody, newManifest} {
		if _, err := objects.ReadImmutableEffectObject(ctx, scope, ref); err != nil {
			return r, err
		}
	}
	r.Body, r.Manifest, r.Count = pipelineRef(newBody), pipelineRef(newManifest), len(events)
	return r, nil
}
