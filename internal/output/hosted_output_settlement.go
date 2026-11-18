package output

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/exportstore"
)

// FinalizeHostedOutputsTx releases the selected project's output records and
// capacity after the caller has verified that the physical inventory is empty.
func FinalizeHostedOutputsTx(ctx context.Context, tx *sql.Tx, tenant, project string) error {
	if err := releaseHostedPipelineCapacity(ctx, tx, tenant, project); err != nil {
		return err
	}
	for _, statement := range []string{
		`DELETE FROM pipeline_effect_assemblies WHERE project_id=$1`,
		`DELETE FROM pipeline_effect_jobs WHERE project_id=$1`, // Cascades to members.
		`DELETE FROM pipeline_effect_cleanup WHERE project_id=$1`,
		`DELETE FROM pipeline_effect_reservations WHERE project_id=$1`,
		`DELETE FROM pipeline_effect_plans WHERE project_id=$1`, // Cascades to targets.
		`DELETE FROM pipeline_effect_streams WHERE project_id=$1`,
		`DELETE FROM pipeline_effect_definitions WHERE project_id=$1`, // Cascades to hosted configs.
		`DELETE FROM pipeline_effect_cooldowns WHERE project_id=$1`,
		`DELETE FROM pipeline_capacity WHERE scope_kind='project' AND scope_id=$1`,
		`DELETE FROM export_jobs WHERE project_id=$1`, // Cascades to files and external authority.
		`DELETE FROM project_deletion_files WHERE project_id=$1`,
		`DELETE FROM managed_artifact_ownership WHERE project_id=$1 AND kind='export'`,
	} {
		if _, err := tx.ExecContext(ctx, statement, project); err != nil {
			return err
		}
	}
	return nil
}

func releaseHostedPipelineCapacity(ctx context.Context, tx *sql.Tx, tenant, project string) error {
	// The project session fences its writers. Lock the shared counters in
	// admission order before applying the project's reconciled debit.
	var demand, observed PipelineDemand
	err := tx.QueryRowContext(ctx, `WITH charges AS (
		SELECT refs-transferred_refs AS refs,bytes-transferred_bytes AS bytes,rows,artifacts,0 AS proofs FROM pipeline_effect_jobs WHERE project_id=$1
		UNION ALL SELECT refs,bytes,rows,artifacts,0 FROM pipeline_effect_cleanup WHERE project_id=$1
		UNION ALL SELECT refs,bytes,rows-checkpoint_rows,artifacts,proofs FROM pipeline_effect_reservations WHERE project_id=$1
		UNION ALL SELECT 0,0,1,0,0 FROM pipeline_effect_streams WHERE project_id=$1
		UNION ALL SELECT 0,0,$2,reserved_bytes+$3,1 FROM hosted_s3_pipeline_runs WHERE project_id=$1 AND (state IN ('queued','running','failed') OR (state='canceled' AND attempt_id<>''))
	) SELECT coalesce(sum(refs),0),coalesce(sum(bytes),0),coalesce(sum(rows),0),coalesce(sum(artifacts),0),coalesce(sum(proofs),0) FROM charges`,
		project, hostedScheduledMaxFiles+1, exportstore.MaxManifestBytes).Scan(&demand.References, &demand.QueuedBytes, &demand.MetadataRows, &demand.ArtifactBytes, &demand.ProofSlots)
	if err != nil {
		return err
	}
	if !demand.valid() {
		return ErrPipelineConflict
	}
	if demand == (PipelineDemand{}) {
		err = tx.QueryRowContext(ctx, `SELECT used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity
			WHERE scope_kind='project' AND scope_id=$1 FOR UPDATE`, project).
			Scan(&observed.References, &observed.QueuedBytes, &observed.MetadataRows, &observed.ArtifactBytes, &observed.ProofSlots)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if observed != demand {
			return ErrPipelineConflict
		}
		return nil
	}

	effects := &PGPipelineEffects{}
	binding := PipelineBinding{Scope: PipelineScope{ProjectID: project}, TenantID: tenant}
	if err := effects.lockCapacity(ctx, tx, binding, false); err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity
		WHERE scope_kind='project' AND scope_id=$1`, project).
		Scan(&observed.References, &observed.QueuedBytes, &observed.MetadataRows, &observed.ArtifactBytes, &observed.ProofSlots)
	if err != nil {
		return err
	}
	if observed != demand {
		return ErrPipelineConflict
	}
	return effects.charge(ctx, tx, binding, demand, -1)
}
