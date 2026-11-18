package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

// Publication uses persisted pipeline authority and the native commit proof.
type hostedEffectPublisher struct {
	control               *sql.DB
	effects               *output.PGPipelineEffects
	usage                 *usage.Store
	objects               *lakeapi.DuckLakeObjectStore
	cancelMutationWebhook func(context.Context, *sql.Tx, output.PipelineScope, string, string) error
	checkpointImport      func(context.Context, *sql.Tx, lakeapi.NativeCommitEvidence) error
	ensureCapacity        func(context.Context, *sql.Tx, string, string) error
	loadRule              func(context.Context, string, string, output.PipelineTarget) ([]byte, error)
}
type hostedPublicationManifest struct {
	Version                                                  int
	Scope                                                    lakeapi.HostedProjectScope
	TenantID, BatchID, PlanID, PreparedSHA256, AcquisitionID string
	Epoch, Sequence                                          int64
	Outputs                                                  []output.PipelineOutput
	PlanRef                                                  output.PipelineRef
}

const maxHostedPreparedObjectBytes int64 = 64 << 20

type hostedPreparedObject struct {
	ref  lakeapi.ImmutableObjectRef
	body []byte
}

func pipelineScope(s lakeapi.HostedProjectScope) output.PipelineScope {
	return output.PipelineScope{SourceID: s.Key.SourceID, ProjectID: s.Key.ProjectID, CatalogID: s.CatalogID, StreamID: s.StreamID}
}
func pipelineRef(r lakeapi.ImmutableObjectRef) output.PipelineRef {
	return output.PipelineRef{Key: r.Key, VersionID: r.VersionID, SHA256: r.SHA256, Bytes: r.Bytes}
}
func nativeRef(r output.PipelineRef) lakeapi.ImmutableObjectRef {
	return lakeapi.ImmutableObjectRef{Key: r.Key, VersionID: r.VersionID, SHA256: r.SHA256, Bytes: r.Bytes}
}
func decodeHostedJSON(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("trailing immutable JSON")
	}
	return nil
}
func (p *hostedEffectPublisher) transaction(ctx context.Context, f func(*sql.Tx) error) error {
	tx, err := p.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = f(tx); err != nil {
		return err
	}
	return tx.Commit()
}
func (p *hostedEffectPublisher) plan(ctx context.Context, tenant string, scope lakeapi.HostedProjectScope) (output.PipelinePlan, frozenLiveOutputPlan, error) {
	var plan output.PipelinePlan
	err := p.transaction(ctx, func(tx *sql.Tx) error {
		if p.ensureCapacity != nil {
			if err := p.ensureCapacity(ctx, tx, tenant, scope.Key.ProjectID); err != nil {
				return err
			}
		}
		var e error
		plan, e = p.effects.FreezeLivePlanTx(ctx, tx, tenant, pipelineScope(scope))
		return e
	})
	routing := frozenLiveOutputPlan{Scope: scope, TenantID: tenant, MaxEventReferences: 10000}
	if err != nil {
		return plan, routing, err
	}
	for _, t := range plan.Targets {
		var raw []byte
		var e error
		if p.loadRule != nil {
			raw, e = p.loadRule(ctx, tenant, scope.Key.ProjectID, t)
		} else {
			raw, e = p.objects.ReadImmutableEffectObject(ctx, scope, nativeRef(t.Rule))
		}
		if e != nil {
			return plan, routing, e
		}
		var rule delivery.WebhookRule
		if e = decodeHostedJSON(raw, &rule); e != nil {
			return plan, routing, e
		}
		routing.Targets = append(routing.Targets, frozenWebhookTarget{PipelineID: t.ID, PipelineIncarnation: t.Incarnation, RuleRevision: t.RuleRevision, DestinationRevision: t.DestinationRevision, Rule: rule})
	}
	return plan, routing, nil
}

type liveEffectStageFunc func(context.Context, lakeapi.SelectedLiveEffects) (lakeapi.StagedLiveEffects, error)

func (f liveEffectStageFunc) StageLiveEffects(ctx context.Context, selected lakeapi.SelectedLiveEffects) (lakeapi.StagedLiveEffects, error) {
	return f(ctx, selected)
}

type reservingHostedStager struct {
	publisher        *hostedEffectPublisher
	session          *lakeapi.ProjectWriteSession
	fence            lakeapi.ProjectWriteFence
	plan             output.PipelinePlan
	routing          frozenLiveOutputPlan
	uploadedManifest lakeapi.ImmutableObjectRef
	binding          output.PipelineBinding
}

func (s *reservingHostedStager) StageLiveEffects(ctx context.Context, selected lakeapi.SelectedLiveEffects) (lakeapi.StagedLiveEffects, error) {
	p := s.publisher
	s.uploadedManifest = lakeapi.ImmutableObjectRef{}
	s.binding = output.PipelineBinding{}
	stager, err := newHostedImmutableEffects(p.objects, s.fence, s.routing)
	if err != nil {
		return lakeapi.StagedLiveEffects{}, err
	}
	stager.planID = s.plan.ID
	objects := []hostedPreparedObject{}
	var preparedBytes int64
	stager.put = func(_ context.Context, raw []byte) (lakeapi.ImmutableObjectRef, error) {
		if int64(len(raw)) > lakeapi.MaxImmutableEffectObjectBytes || int64(len(raw)) > maxHostedPreparedObjectBytes-preparedBytes {
			return lakeapi.ImmutableObjectRef{}, errors.Join(errHostedLiveBatchTooLarge, output.ErrPipelineCapacity)
		}
		ref, _, e := p.objects.DescribeImmutableEffectObject(selected.Scope, s.fence.AcquisitionID, raw)
		if e == nil {
			preparedBytes += int64(len(raw))
			objects = append(objects, hostedPreparedObject{ref: ref, body: raw})
		}
		return ref, e
	}
	staged, err := stager.StageLiveEffects(ctx, selected)
	if errors.Is(err, delivery.ErrWebhookCapacity) {
		return staged, errors.Join(errHostedLiveBatchTooLarge, err)
	}
	if err != nil {
		return staged, err
	}
	var manifest immutableLiveEffectManifest
	if staged.OutputCount > 0 {
		if err = decodeHostedJSON(objects[len(objects)-1].body, &manifest); err != nil {
			return staged, err
		}
		objects = objects[:len(objects)-1] // Replace the stager-only manifest with the digest-bound operational metadata.
	}
	prepared := []output.PipelineOutput{}
	for _, o := range manifest.Outputs {
		var body []byte
		for _, obj := range objects {
			if obj.ref == o.Body {
				body = obj.body
				break
			}
		}
		var payload delivery.WebhookPayload
		if err = decodeHostedJSON(body, &payload); err != nil {
			return staged, err
		}
		raw, e := delivery.EncodeJSON(payload.Event)
		if e != nil {
			return staged, e
		}
		prepared = append(prepared, output.PipelineOutput{ID: o.DeliveryID, PipelineID: o.Target.PipelineID, Incarnation: o.Target.PipelineIncarnation, RuleRevision: o.Target.RuleRevision, DestinationRevision: o.Target.DestinationRevision, Generation: s.fence.AcquisitionID, Kind: "delivery", Body: pipelineRef(o.Body), Members: []output.PipelineMember{{UUID: o.EventUUIDs[0], Bytes: int64(len(raw))}}, Charge: output.PipelineDemand{References: 1, QueuedBytes: o.Body.Bytes, MetadataRows: 2, ArtifactBytes: o.Body.Bytes}})
	}
	for _, o := range manifest.BatchFragments {
		members := make([]output.PipelineMember, len(o.EventUUIDs))
		var queued int64
		for i, id := range o.EventUUIDs {
			members[i] = output.PipelineMember{UUID: id, Bytes: o.EventBytes[i]}
			queued += o.EventBytes[i] + 1024
		}
		prepared = append(prepared, output.PipelineOutput{ID: o.FragmentID, PipelineID: o.Target.PipelineID, Incarnation: o.Target.PipelineIncarnation, RuleRevision: o.Target.RuleRevision, DestinationRevision: o.Target.DestinationRevision, Generation: s.fence.AcquisitionID, Kind: "fragment", Fragment: &output.PipelineFragmentSpec{WindowStartMS: o.WindowStartMS, WindowEndMS: o.WindowEndMS, Rule: o.Target.Rule}, Body: pipelineRef(o.Body), Members: members, Charge: output.PipelineDemand{References: int64(len(members)), QueuedBytes: max(queued, o.Body.Bytes), MetadataRows: int64(1 + len(members)), ArtifactBytes: o.Body.Bytes}})
	}
	planRaw, err := json.Marshal(s.plan)
	if err != nil {
		return staged, err
	}
	planRef, prefix, err := p.objects.DescribeImmutableEffectObject(selected.Scope, s.fence.AcquisitionID, planRaw)
	if err != nil {
		return staged, err
	}
	binding := output.PipelineBinding{Scope: pipelineScope(selected.Scope), TenantID: selected.TenantID, BatchID: selected.BatchID, PlanID: s.plan.ID, PreparedSHA256: output.PipelinePreparedSHA256(prepared), AcquisitionID: s.fence.AcquisitionID, ArtifactPrefix: prefix, Epoch: s.fence.Epoch, PlanRef: pipelineRef(planRef), Demand: output.PipelineDemand{MetadataRows: int64(3 + len(s.plan.Targets)), ArtifactBytes: planRef.Bytes, ProofSlots: 1}}
	if len(prepared) == 0 {
		binding.PlanRef = output.PipelineRef{}
		binding.Demand.ArtifactBytes = 0
	}

	staged.Manifest = lakeapi.ImmutableObjectRef{}
	if len(prepared) > 0 {
		binding.ManifestBytesLimit = lakeapi.MaxImmutableEffectObjectBytes
	}
	for _, o := range prepared {
		binding.Demand.References += o.Charge.References
		binding.Demand.QueuedBytes += o.Charge.QueuedBytes
		binding.Demand.MetadataRows += o.Charge.MetadataRows
		binding.Demand.ArtifactBytes += o.Charge.ArtifactBytes
	}
	binding.Demand.ArtifactBytes += binding.ManifestBytesLimit
	// Bound the largest version-bearing manifest before uploading any object.
	// Each optional version pin is capped at 1024 bytes by the metadata contract.
	if len(prepared) > 0 {
		preview, _ := json.Marshal(hostedPublicationManifest{Version: 1, Scope: selected.Scope, TenantID: selected.TenantID, BatchID: selected.BatchID, PlanID: s.plan.ID, PreparedSHA256: binding.PreparedSHA256, AcquisitionID: s.fence.AcquisitionID, Epoch: s.fence.Epoch, Sequence: selected.Sequence, Outputs: prepared, PlanRef: pipelineRef(planRef)})
		if int64(len(preview))+int64(len(prepared)+1)*6144 > binding.ManifestBytesLimit {
			return staged, errors.Join(errHostedLiveBatchTooLarge, output.ErrPipelineCapacity)
		}
	}

	// Recheck the saved creator, lifecycle and every revision in the transaction
	// which acquires capacity. Commit the reservation before any S3 PUT so a
	// successor can account for abandoned objects and settle a retained native
	// proof after a crash. Publication waits for that proof.
	err = s.session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return p.effects.ReserveCurrentEffectsTx(ctx, tx, binding, prepared)
	})
	if errors.Is(err, output.ErrPipelineDemandTooLarge) {
		return staged, errors.Join(errHostedLiveBatchTooLarge, err)
	}
	if err != nil {
		return staged, err
	}
	if len(prepared) > 0 {
		objects = append(objects, hostedPreparedObject{ref: planRef, body: planRaw})
	}
	for _, o := range objects {
		actual, e := p.objects.PutImmutableEffectObject(ctx, selected.Scope, s.fence.AcquisitionID, o.body)
		if e != nil {
			return staged, e
		}
		descriptor := actual
		descriptor.VersionID = ""
		if descriptor != o.ref {
			return staged, output.ErrPipelineConflict
		}
		for i := range prepared {
			if prepared[i].Body.Key == actual.Key {
				prepared[i].Body = pipelineRef(actual)
			}
		}
		if actual.Key == planRef.Key {
			planRef = actual
		}
	}
	if len(prepared) > 0 {
		m := hostedPublicationManifest{Version: 1, Scope: selected.Scope, TenantID: selected.TenantID, BatchID: selected.BatchID, PlanID: s.plan.ID, PreparedSHA256: binding.PreparedSHA256, AcquisitionID: s.fence.AcquisitionID, Epoch: s.fence.Epoch, Sequence: selected.Sequence, Outputs: prepared, PlanRef: pipelineRef(planRef)}
		raw, e := json.Marshal(m)
		if e != nil {
			return staged, e
		}
		if int64(len(raw)) > binding.ManifestBytesLimit {
			return staged, output.ErrPipelineCapacity
		}
		staged.Manifest, e = p.objects.PutImmutableEffectObject(ctx, selected.Scope, s.fence.AcquisitionID, raw)
		if e != nil {
			return staged, e
		}
	}

	s.binding = binding
	s.uploadedManifest = staged.Manifest
	return staged, nil
}

func (p *hostedEffectPublisher) verify(ctx context.Context, b output.PipelineBinding, e lakeapi.NativeCommitEvidence, uploadedManifest lakeapi.ImmutableObjectRef) (output.PipelinePublication, error) {
	fail := func() (output.PipelinePublication, error) {
		return output.PipelinePublication{}, output.ErrPipelineConflict
	}
	if e.Kind != lakeapi.NativeCommitLive || b.Scope != pipelineScope(e.Scope) || b.TenantID != e.TenantID || b.BatchID != e.OperationID || b.Epoch != e.FenceEpoch || b.PlanID != e.Effects.PlanID {
		return fail()
	}
	owned := func(r output.PipelineRef) bool {
		return strings.HasPrefix(r.Key, b.ArtifactPrefix) && r.Bytes > 0 && r.Bytes <= lakeapi.MaxImmutableEffectObjectBytes
	}
	if e.Effects.OutputCount > 0 && !owned(b.PlanRef) {
		return fail()
	}
	var raw []byte
	var err error
	if e.Effects.OutputCount == 0 && (b.PlanRef != (output.PipelineRef{}) || b.Demand.ArtifactBytes != 0) {
		return fail()
	}
	outputs := []output.PipelineOutput{}
	if e.Effects.OutputCount > 0 {
		ref := pipelineRef(e.Effects.Manifest)
		if !owned(ref) || ref.Bytes > b.ManifestBytesLimit {
			return fail()
		}
		raw, err = p.objects.ReadImmutableEffectObject(ctx, e.Scope, e.Effects.Manifest)
		if err != nil {
			return output.PipelinePublication{}, err
		}
		var m hostedPublicationManifest
		if err = decodeHostedJSON(raw, &m); err != nil {
			return output.PipelinePublication{}, err
		}
		if m.Version != 1 || m.Scope != e.Scope || m.TenantID != b.TenantID || m.BatchID != b.BatchID || m.PlanID != b.PlanID || m.Epoch != b.Epoch || m.AcquisitionID != b.AcquisitionID || m.Sequence != e.Sequence || m.PreparedSHA256 != b.PreparedSHA256 || int64(len(m.Outputs)) != e.Effects.OutputCount {
			return fail()
		}
		descriptor := m.PlanRef
		descriptor.VersionID = ""
		if descriptor != b.PlanRef {
			return fail()
		}
		raw, err = p.objects.ReadImmutableEffectObject(ctx, e.Scope, nativeRef(m.PlanRef))
		if err != nil {
			return output.PipelinePublication{}, err
		}
		var plan output.PipelinePlan
		if err = decodeHostedJSON(raw, &plan); err != nil {
			return output.PipelinePublication{}, err
		}
		if plan.ID != b.PlanID || plan.Scope != b.Scope || plan.TenantID != b.TenantID {
			return fail()
		}
		outputs = m.Outputs
	} else if e.Effects.Manifest != (lakeapi.ImmutableObjectRef{}) {
		return fail()
	}
	if output.PipelinePreparedSHA256(outputs) != b.PreparedSHA256 {
		return fail()
	}
	// The stager records this exact manifest only after uploading every referenced
	// body. Cold recovery has no such record and verifies each body in S3.
	bodiesUploaded := uploadedManifest == e.Effects.Manifest
	for _, o := range outputs {
		if !owned(o.Body) {
			return fail()
		}
		if !bodiesUploaded {
			if _, err = p.objects.ReadImmutableEffectObject(ctx, e.Scope, nativeRef(o.Body)); err != nil {
				return output.PipelinePublication{}, err
			}
		}
	}
	return output.PipelinePublication{Manifest: pipelineRef(e.Effects.Manifest), Sequence: e.Sequence, ProofSHA256: e.ProofFingerprint, Outputs: outputs}, nil
}
func nativeUsageReports(e lakeapi.NativeCommitEvidence) ([]usage.SourceTotals, error) {
	if e.AccountedAt.IsZero() || len(e.Totals) == 0 || len(e.Totals) > 1000 {
		return nil, output.ErrPipelineConflict
	}
	reports := make([]usage.SourceTotals, 0, len(e.Totals))
	for _, t := range e.Totals {
		if t.Day != usage.Day(e.AccountedAt) || t.Revision != e.Sequence || t.Live < e.AcceptedLive || t.Import < e.AcceptedImport {
			return nil, output.ErrPipelineConflict
		}
		reports = append(reports, usage.SourceTotals{SourceID: e.Scope.StreamID, TenantID: e.TenantID, ProjectID: e.Scope.Key.ProjectID, Day: t.Day, Revision: t.Revision, Counters: usage.Counters{Live: t.Live, Import: t.Import}})
	}
	return reports, nil
}
func (p *hostedEffectPublisher) publish(ctx context.Context, b output.PipelineBinding, e lakeapi.NativeCommitEvidence) error {
	return p.publishOn(ctx, b, e, nil)
}

func (p *hostedEffectPublisher) publishOn(ctx context.Context, b output.PipelineBinding, e lakeapi.NativeCommitEvidence, session *lakeapi.ProjectWriteSession) error {
	return p.publishEvidence(ctx, b, e, session, false, lakeapi.ImmutableObjectRef{})
}

// publishEvidence can settle existing durable proofs after a deletion fence.
// It does not admit new native work or add an output cursor.
func (p *hostedEffectPublisher) publishEvidence(ctx context.Context, b output.PipelineBinding, e lakeapi.NativeCommitEvidence, session *lakeapi.ProjectWriteSession, deleting bool, uploadedManifest lakeapi.ImmutableObjectRef) error {
	if err := lakeapi.ValidateNativeCommitEvidence(e); err != nil {
		return err
	}
	var v output.PipelinePublication
	var mutation hostedMutationIntent
	var mutationUUID string
	var err error
	switch e.Kind {
	case lakeapi.NativeCommitLive:
		v, err = p.verify(ctx, b, e, uploadedManifest)
	case lakeapi.NativeCommitImport:
		if b != (output.PipelineBinding{}) {
			return output.ErrPipelineConflict
		}
		_, err = p.objects.ReadImmutableImportCheckpoint(ctx, e.Scope, e.Import.Key.JobID, e.Import.Key.FileID, e.Import.Next.Parser)
		v = output.PipelinePublication{Sequence: e.Sequence, ProofSHA256: e.ProofFingerprint}
	case lakeapi.NativeCommitMutation:
		if b != (output.PipelineBinding{}) || session == nil {
			return output.ErrPipelineConflict
		}
		fence := session.Fence(e.Scope.StreamID)
		if fence.Key != e.Scope.Key || fence.Epoch < e.FenceEpoch {
			return output.ErrPipelineConflict
		}
		mutation, err = hostedMutationBy(ctx, p.control, e.Scope, e.TenantID, "mutation_id", e.OperationID)
		if err != nil {
			return err
		}
		if mutation.body.SHA256 != e.InputFingerprint || mutation.revision != int64(e.Mutation.Revision) || (mutation.operation == "delete") != e.Mutation.Deleted {
			return output.ErrPipelineConflict
		}
		if mutation.state == "ready" && e.Mutation.Deleted && e.Mutation.ErrorCode == "" {
			if p.cancelMutationWebhook == nil {
				return output.ErrPipelineConflict
			}
			body, err := p.readHostedMutation(ctx, mutation)
			if err != nil {
				return err
			}
			mutationUUID = body.Projection.UUID
		}
		v = output.PipelinePublication{Sequence: e.Sequence, ProofSHA256: e.ProofFingerprint}
	default:
		return output.ErrPipelineConflict
	}
	if err != nil {
		return err
	}
	reports, err := nativeUsageReports(e)
	if err != nil {
		return err
	}
	publishTx := func(ctx context.Context, tx *sql.Tx) error {
		var advanced bool
		switch e.Kind {
		case lakeapi.NativeCommitLive:
			var err error
			advanced, err = p.effects.PublishReservedEffectsTx(ctx, tx, b, v)
			if err != nil {
				return err
			}
		case lakeapi.NativeCommitImport, lakeapi.NativeCommitMutation:
			publishNoOutput := p.effects.PublishNoOutputEffectsTx
			if deleting {
				publishNoOutput = p.effects.PublishSealedProjectNoOutputEffectsTx
				if e.Kind == lakeapi.NativeCommitImport && p.checkpointImport == nil {
					return output.ErrPipelineConflict
				}
			}
			var err error
			advanced, err = publishNoOutput(ctx, tx, pipelineScope(e.Scope), e.TenantID, v)
			if err != nil {
				return err
			}
			if e.Kind == lakeapi.NativeCommitImport {
				if advanced && p.checkpointImport != nil {
					if err := p.checkpointImport(ctx, tx, e); err != nil {
						return err
					}
				}
				break
			}
			if !advanced {
				if !mutation.settledBy(e) {
					return output.ErrPipelineConflict
				}
				break
			}
			if mutationUUID != "" {
				if err := p.cancelMutationWebhook(ctx, tx, pipelineScope(e.Scope), e.TenantID, mutationUUID); err != nil {
					return err
				}
			}
			if err := finishHostedMutationTx(ctx, tx, mutation, e); err != nil {
				return err
			}
		default:
			return output.ErrPipelineConflict
		}
		// A replay of an already committed checkpoint has no new acceptance.
		count := e.AcceptedLive + e.AcceptedImport
		if advanced && count > 0 {
			start, _ := tenantMonth(e.AccountedAt)
			if err := addTenantEventsTx(ctx, tx, e.TenantID, start.Format(time.RFC3339), count); err != nil {
				return err
			}
		}
		return p.usage.PublishSourceTotalsTx(ctx, tx, reports)
	}
	if session != nil {
		err = session.RunControlTx(ctx, publishTx)
	} else {
		err = p.transaction(ctx, func(tx *sql.Tx) error { return publishTx(ctx, tx) })
	}
	if err == nil {
		return nil
	}
	// Commit may have succeeded despite a lost response. Only a new committed
	// checkpoint read can release the volatile batch in that case.
	seq, hash, readErr := p.checkpoint(ctx, e.Scope, e.TenantID)
	if readErr == nil && seq == e.Sequence && hash == e.ProofFingerprint {
		if e.Kind != lakeapi.NativeCommitMutation {
			return nil
		}
		receipt, readErr := hostedMutationBy(ctx, p.control, e.Scope, e.TenantID, "mutation_id", e.OperationID)
		if readErr == nil && receipt.settledBy(e) {
			return nil
		}
	}
	return fmt.Errorf("effect publication not confirmed: %w", err)
}
func (p *hostedEffectPublisher) checkpoint(ctx context.Context, scope lakeapi.HostedProjectScope, tenant string) (int64, string, error) {
	var seq int64
	var hash string
	err := p.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		seq, hash, e = p.effects.CheckpointTx(ctx, tx, pipelineScope(scope), tenant)
		return e
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, "", nil
	}
	return seq, hash, err
}
func (p *hostedEffectPublisher) reservation(ctx context.Context, scope lakeapi.HostedProjectScope) (output.PipelineBinding, error) {
	var b output.PipelineBinding
	err := p.transaction(ctx, func(tx *sql.Tx) error {
		var e error
		b, e = p.effects.ReservationTx(ctx, tx, pipelineScope(scope))
		return e
	})
	return b, err
}
