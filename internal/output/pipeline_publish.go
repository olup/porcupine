package output

import (
	"context"
	"database/sql"
	"errors"
)

type PipelineMember struct {
	UUID  string
	Bytes int64
}
type PipelineOutput struct {
	ID, PipelineID, Incarnation, RuleRevision, DestinationRevision, Generation, Kind string
	Body                                                                             PipelineRef
	Members                                                                          []PipelineMember
	Charge                                                                           PipelineDemand
}
type PipelinePublication struct {
	Manifest    PipelineRef
	Sequence    int64
	ProofSHA256 string
	Outputs     []PipelineOutput
}

// PipelinePreparedSHA256 binds content known before PUT. Version IDs returned by
// storage are retained in publication, but cannot be part of pre-upload capacity.
func PipelinePreparedSHA256(outputs []PipelineOutput) string {
	prepared := append([]PipelineOutput(nil), outputs...)
	if outputs != nil && prepared == nil {
		prepared = []PipelineOutput{}
	}
	for i := range prepared {
		prepared[i].Body.VersionID = ""
	}
	return pipelineDigest(prepared)
}

// PublishReservedEffectsTx does not commit. The publisher must call the real
// usage.PublishSourceTotalsTx on this same TX before committing and ACKing native.
// The adapter verifies S3 refs/manifests against native proof before calling here.
func (p *PGPipelineEffects) PublishReservedEffectsTx(ctx context.Context, tx *sql.Tx, b PipelineBinding, v PipelinePublication) (bool, error) {
	if tx == nil || !b.valid() || v.Sequence <= 0 || !pipelineHash(v.ProofSHA256) || len(v.Outputs) > 10000 || PipelinePreparedSHA256(v.Outputs) != b.PreparedSHA256 {
		return false, ErrPipelineConflict
	}
	if len(v.Outputs) > 0 && (!v.Manifest.valid() || v.Manifest.Bytes > b.ManifestBytesLimit) {
		return false, ErrPipelineCapacity
	}
	if len(v.Outputs) == 0 && v.Manifest != (PipelineRef{}) {
		return false, ErrPipelineConflict
	}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return false, e
	}
	seq, proof, e := p.lockStream(ctx, tx, b)
	if e != nil {
		return false, e
	}
	if seq == v.Sequence && proof == v.ProofSHA256 {
		return false, nil
	}
	if v.Sequence != seq+1 {
		return false, ErrPipelineConflict
	}
	if e = p.matchReservation(ctx, tx, b); e != nil {
		return false, e
	}
	spent := PipelineDemand{}
	seen := map[string]bool{}
	for _, o := range v.Outputs {
		if !pipelineID(o.ID) || seen[o.ID] || !pipelineID(o.Generation) || !o.Body.valid() || !(o.Kind == "delivery" || o.Kind == "fragment") || len(o.Members) == 0 || len(o.Members) > 100 || !o.Charge.valid() || o.Charge.ProofSlots != 0 || o.Charge.References != int64(len(o.Members)) || o.Charge.MetadataRows < int64(1+len(o.Members)) || o.Charge.ArtifactBytes < o.Body.Bytes || o.Charge.QueuedBytes < o.Body.Bytes {
			return false, ErrPipelineConflict
		}
		seen[o.ID] = true
		refs := map[string]bool{}
		var encoded int64
		for _, m := range o.Members {
			if !pipelineID(m.UUID) || refs[m.UUID] || m.Bytes <= 0 || m.Bytes > o.Body.Bytes-encoded {
				return false, ErrPipelineConflict
			}
			refs[m.UUID] = true
			encoded += m.Bytes
		}
		// Batch recomposition may emit singleton envelopes. Charge 1 KiB per
		// member above canonical event bytes, in addition to the current body.
		if o.Kind == "fragment" && (encoded > o.Charge.QueuedBytes-int64(len(o.Members))*1024) {
			return false, ErrPipelineCapacity
		}
		remaining := PipelineDemand{b.Demand.References - spent.References, b.Demand.QueuedBytes - spent.QueuedBytes, b.Demand.MetadataRows - spent.MetadataRows - 1, b.Demand.ArtifactBytes - spent.ArtifactBytes, 0}
		if !remaining.valid() || !o.Charge.fits(remaining) {
			return false, ErrPipelineCapacity
		}
		spent.References += o.Charge.References
		spent.QueuedBytes += o.Charge.QueuedBytes
		spent.MetadataRows += o.Charge.MetadataRows
		spent.ArtifactBytes += o.Charge.ArtifactBytes
		var target PipelineTarget
		e = tx.QueryRowContext(ctx, `SELECT id,incarnation,rule_revision,destination_revision,creator_id FROM pipeline_effect_targets WHERE plan_id=$1 AND id=$2`, b.PlanID, o.PipelineID).Scan(&target.ID, &target.Incarnation, &target.RuleRevision, &target.DestinationRevision, &target.CreatorID)
		if e != nil {
			return false, e
		}
		if o.Incarnation != target.Incarnation || o.RuleRevision != target.RuleRevision || o.DestinationRevision != target.DestinationRevision {
			return false, ErrPipelineConflict
		}
		state, e := p.currentTarget(ctx, tx, b, target)
		if e != nil {
			return false, e
		}
		a := append(pipelineScopeArgs(b.Scope), b.TenantID, o.ID, b.BatchID, o.PipelineID, o.Incarnation, o.RuleRevision, o.DestinationRevision, target.CreatorID, o.Generation, o.Kind, v.Sequence, o.Body.Key, o.Body.VersionID, o.Body.SHA256, o.Body.Bytes, state, o.Charge.References, o.Charge.QueuedBytes, o.Charge.MetadataRows, o.Charge.ArtifactBytes)
		_, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_jobs(source_id,project_id,catalog_id,stream_id,tenant_id,id,batch_id,pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,state,refs,bytes,rows,artifacts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24)`, a...)
		if e != nil {
			return false, e
		}
		for i, m := range o.Members {
			a := append(pipelineScopeArgs(b.Scope), o.ID, i, m.UUID, m.Bytes)
			_, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_members(source_id,project_id,catalog_id,stream_id,job_id,position,uuid,encoded_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, a...)
			if e != nil {
				return false, e
			}
		}
	}
	if spent.References != b.Demand.References {
		return false, ErrPipelineConflict
	}
	residual := PipelineDemand{b.Demand.References - spent.References, b.Demand.QueuedBytes - spent.QueuedBytes, b.Demand.MetadataRows - spent.MetadataRows, b.Demand.ArtifactBytes - spent.ArtifactBytes, 0}
	if residual.ArtifactBytes < b.PlanRef.Bytes+b.ManifestBytesLimit {
		return false, ErrPipelineCapacity
	}
	if e = p.transferCleanup(ctx, tx, b, residual, b.BatchID); e != nil {
		return false, e
	}
	a := append(pipelineScopeArgs(b.Scope), v.Sequence, v.ProofSHA256)
	r, e := tx.ExecContext(ctx, `UPDATE pipeline_effect_streams SET sequence=$5,proof_sha=$6 WHERE `+pipelineScopeWhere, a...)
	if e != nil {
		return false, e
	}
	if e = pipelineOne(r); e != nil {
		return false, e
	}
	return true, p.finishReservation(ctx, tx, b)
}
func (p *PGPipelineEffects) currentTarget(ctx context.Context, tx *sql.Tx, b PipelineBinding, t PipelineTarget) (string, error) {
	var inc, rule, dest, creator, state string
	e := tx.QueryRowContext(ctx, `SELECT incarnation,rule_revision,destination_revision,creator_id,state FROM pipeline_effect_definitions WHERE project_id=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE`, b.Scope.ProjectID, b.TenantID, t.ID).Scan(&inc, &rule, &dest, &creator, &state)
	if errors.Is(e, sql.ErrNoRows) {
		return "retired", nil
	}
	if e != nil {
		return "", e
	}
	if inc != t.Incarnation || dest != t.DestinationRevision || creator != t.CreatorID || state == "retired" {
		return "retired", nil
	}
	// Rule edits affect future selection; a frozen old rule remains valid.
	ok, e := p.authority.ResolveCreatorTx(ctx, tx, b.TenantID, b.Scope.ProjectID, t.CreatorID)
	if e != nil {
		return "", e
	}
	if !ok {
		return "retired", nil
	}
	return "pending", nil
}
func (p *PGPipelineEffects) transferCleanup(ctx context.Context, tx *sql.Tx, b PipelineBinding, d PipelineDemand, owner string) error {
	var checkpointRows int64
	if e := tx.QueryRowContext(ctx, `SELECT checkpoint_rows FROM pipeline_effect_reservations WHERE `+pipelineScopeWhere, pipelineScopeArgs(b.Scope)...).Scan(&checkpointRows); e != nil {
		return e
	}
	if d.MetadataRows < checkpointRows {
		return ErrPipelineCapacity
	}
	d.MetadataRows -= checkpointRows
	if d.ArtifactBytes == 0 && d.References == 0 && d.QueuedBytes == 0 {
		// Verified zero selected outputs own no external artifact. Release transient
		// plan/reservation metadata while the stream checkpoint stays charged.
		return p.charge(ctx, tx, b, d, -1)
	}
	a := append(pipelineScopeArgs(b.Scope), b.TenantID, owner, b.AcquisitionID, b.ArtifactPrefix, d.References, d.QueuedBytes, d.MetadataRows, d.ArtifactBytes)
	_, e := tx.ExecContext(ctx, `INSERT INTO pipeline_effect_cleanup(source_id,project_id,catalog_id,stream_id,tenant_id,owner_id,generation,prefix,refs,bytes,rows,artifacts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, a...)
	return e
}
func (p *PGPipelineEffects) finishReservation(ctx context.Context, tx *sql.Tx, b PipelineBinding) error {
	r, e := tx.ExecContext(ctx, `DELETE FROM pipeline_effect_reservations WHERE `+pipelineScopeWhere+` AND binding_sha=$5`, append(pipelineScopeArgs(b.Scope), pipelineDigest(b))...)
	if e != nil {
		return e
	}
	if e = pipelineOne(r); e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, `DELETE FROM pipeline_effect_plans WHERE id=$1`, b.PlanID)
	if e != nil {
		return e
	}
	return p.charge(ctx, tx, b, PipelineDemand{ProofSlots: 1}, -1)
}

// ReleaseAbsentReservationTx requires the adapter's successor-fence proven
// absence. Artifact and metadata capacity remains held by cleanup ownership.
func (p *PGPipelineEffects) ReleaseAbsentReservationTx(ctx context.Context, tx *sql.Tx, b PipelineBinding, evidence PipelineSettlement) error {
	if tx == nil || !b.valid() || PipelineSettlementDecision(b, evidence) != PipelineReleaseReservation {
		return ErrPipelineConflict
	}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return e
	}
	if e := p.matchReservation(ctx, tx, b); e != nil {
		return e
	}
	d := b.Demand
	d.ProofSlots = 0
	if e := p.transferCleanup(ctx, tx, b, d, b.AcquisitionID); e != nil {
		return e
	}
	return p.finishReservation(ctx, tx, b)
}
