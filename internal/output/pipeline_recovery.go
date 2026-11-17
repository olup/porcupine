package output

import (
	"context"
	"database/sql"
)

// ReservationTx permits cold recovery without the original request or producer
// credential. It does not acquire native fences or claim proof of absence.
func (p *PGPipelineEffects) ReservationTx(ctx context.Context, tx *sql.Tx, s PipelineScope) (PipelineBinding, error) {
	b := PipelineBinding{Scope: s}
	if tx == nil || !s.valid() {
		return b, ErrPipelineConflict
	}
	e := tx.QueryRowContext(ctx, `SELECT tenant_id,batch_id,artifact_prefix,epoch,acquisition_id,plan_id,prepared_sha,plan_key,plan_version,plan_sha,plan_bytes,manifest_bytes_limit,refs,bytes,rows,artifacts,proofs FROM pipeline_effect_reservations WHERE `+pipelineScopeWhere, pipelineScopeArgs(s)...).Scan(&b.TenantID, &b.BatchID, &b.ArtifactPrefix, &b.Epoch, &b.AcquisitionID, &b.PlanID, &b.PreparedSHA256, &b.PlanRef.Key, &b.PlanRef.VersionID, &b.PlanRef.SHA256, &b.PlanRef.Bytes, &b.ManifestBytesLimit, &b.Demand.References, &b.Demand.QueuedBytes, &b.Demand.MetadataRows, &b.Demand.ArtifactBytes, &b.Demand.ProofSlots)
	return b, e
}

// CheckpointTx returns only confirmed control publication coverage. The
// integration adapter ACKs native evidence after the enclosing TX commits.
func (p *PGPipelineEffects) CheckpointTx(ctx context.Context, tx *sql.Tx, s PipelineScope, tenant string) (int64, string, error) {
	return p.lockStream(ctx, tx, PipelineBinding{Scope: s, TenantID: tenant})
}
