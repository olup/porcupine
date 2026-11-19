package output

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ConfigureCapacityTx changes admission limits without invalidating capacity
// already owned by reservations or backlog. Call in tenant/project order.
func (p *PGPipelineEffects) ConfigureCapacityTx(ctx context.Context, tx *sql.Tx, kind, id string, d PipelineDemand) error {
	if tx == nil || !d.positive() || !pipelineID(id) || (kind != "tenant" && kind != "project") {
		return ErrPipelineConflict
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO pipeline_capacity(scope_kind,scope_id,limit_refs,limit_bytes,limit_rows,limit_artifacts,limit_proofs) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(scope_kind,scope_id) DO UPDATE SET limit_refs=$3,limit_bytes=$4,limit_rows=$5,limit_artifacts=$6,limit_proofs=$7`, kind, id, d.References, d.QueuedBytes, d.MetadataRows, d.ArtifactBytes, d.ProofSlots)
	return err
}
func pipelineScopeArgs(s PipelineScope) []any {
	return []any{s.SourceID, s.ProjectID, s.CatalogID, s.StreamID}
}

const pipelineScopeWhere = `source_id=$1 AND project_id=$2 AND catalog_id=$3 AND stream_id=$4`

// All mutating operations take capacity locks first, then stream, then targets.
// External code must acquire its common project session before opening the TX.
func (p *PGPipelineEffects) lockCapacity(ctx context.Context, tx *sql.Tx, b PipelineBinding, admit bool) error {
	var capacityErr error
	for _, scope := range pipelineCapacityScopes(b) {
		var limit, used PipelineDemand
		err := tx.QueryRowContext(ctx, `SELECT limit_refs,limit_bytes,limit_rows,limit_artifacts,limit_proofs,used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity WHERE scope_kind=$1 AND scope_id=$2 FOR UPDATE`, scope[0], scope[1]).Scan(&limit.References, &limit.QueuedBytes, &limit.MetadataRows, &limit.ArtifactBytes, &limit.ProofSlots, &used.References, &used.QueuedBytes, &used.MetadataRows, &used.ArtifactBytes, &used.ProofSlots)
		if err != nil {
			return fmt.Errorf("pipeline capacity %s unavailable: %w", scope[0], err)
		}
		if admit {
			a, l, u := b.Demand.values(), limit.values(), used.values()
			for i := range a {
				if a[i] > l[i] {
					return ErrPipelineDemandTooLarge
				}
				if u[i] > l[i]-a[i] {
					capacityErr = ErrPipelineCapacity
				}
			}
		}
	}
	return capacityErr
}

func pipelineCapacityScopes(b PipelineBinding) [2][2]string {
	return [2][2]string{{"tenant", b.TenantID}, {"project", b.Scope.ProjectID}}
}

func (p *PGPipelineEffects) charge(ctx context.Context, tx *sql.Tx, b PipelineBinding, d PipelineDemand, sign int64) error {
	for _, s := range pipelineCapacityScopes(b) {
		r, e := tx.ExecContext(ctx, `UPDATE pipeline_capacity SET used_refs=used_refs+$3,used_bytes=used_bytes+$4,used_rows=used_rows+$5,used_artifacts=used_artifacts+$6,used_proofs=used_proofs+$7 WHERE scope_kind=$1 AND scope_id=$2`, s[0], s[1], sign*d.References, sign*d.QueuedBytes, sign*d.MetadataRows, sign*d.ArtifactBytes, sign*d.ProofSlots)
		if e != nil {
			return e
		}
		if e = pipelineOne(r); e != nil {
			return e
		}
	}
	return nil
}
func pipelineOne(r sql.Result) error {
	n, e := r.RowsAffected()
	if e != nil {
		return e
	}
	if n != 1 {
		return ErrPipelineConflict
	}
	return nil
}
func (p *PGPipelineEffects) lockStream(ctx context.Context, tx *sql.Tx, b PipelineBinding) (int64, string, error) {
	var seq int64
	var tenant, proof string
	err := tx.QueryRowContext(ctx, `SELECT tenant_id,sequence,proof_sha FROM pipeline_effect_streams WHERE `+pipelineScopeWhere+` FOR UPDATE`, pipelineScopeArgs(b.Scope)...).Scan(&tenant, &seq, &proof)
	if err == nil && tenant != b.TenantID {
		err = ErrPipelineConflict
	}
	return seq, proof, err
}

// FreezeLivePlanTx reads only real definitions and rechecks their creator after
// locking. Empty plans are valid only when the real authority provider is present.
// No PG row persists until ReserveEffectsTx owns capacity.
func (p *PGPipelineEffects) FreezeLivePlanTx(ctx context.Context, tx *sql.Tx, tenant string, scope PipelineScope) (PipelinePlan, error) {
	plan := PipelinePlan{TenantID: tenant, Scope: scope, Targets: []PipelineTarget{}}
	if tx == nil || !scope.valid() || !pipelineID(tenant) {
		return plan, ErrPipelineConflict
	}
	var projectTenant string
	if e := tx.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1) AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=projects.tenant_id) FOR UPDATE`, scope.ProjectID).Scan(&projectTenant); e != nil {
		return plan, e
	}
	if projectTenant != tenant {
		return plan, ErrPipelineAuthority
	}
	// Scheduled rules share the authority registry, but only webhook rules
	// participate in live-event publication.
	rows, e := tx.QueryContext(ctx, `SELECT id,incarnation,rule_revision,destination_revision,creator_id,rule_key,rule_version,rule_sha,rule_bytes FROM pipeline_effect_definitions WHERE project_id=$1 AND tenant_id=$2 AND state='active' AND rule_key NOT LIKE '`+hostedScheduledRulePrefix+`%' ORDER BY id FOR UPDATE`, scope.ProjectID, tenant)
	if e != nil {
		return plan, e
	}
	for rows.Next() {
		var t PipelineTarget
		if e = rows.Scan(&t.ID, &t.Incarnation, &t.RuleRevision, &t.DestinationRevision, &t.CreatorID, &t.Rule.Key, &t.Rule.VersionID, &t.Rule.SHA256, &t.Rule.Bytes); e != nil {
			break
		}
		plan.Targets = append(plan.Targets, t)
		if len(plan.Targets) > 100 {
			e = ErrPipelineCapacity
			break
		}
	}
	e = errors.Join(e, rows.Err(), rows.Close())
	if e != nil {
		return plan, e
	}
	targets := plan.Targets
	plan.Targets = []PipelineTarget{}
	for _, t := range targets {
		ok, e := p.authority.ResolveCreatorTx(ctx, tx, tenant, scope.ProjectID, t.CreatorID)
		if e != nil {
			return plan, e
		}
		if ok {
			plan.Targets = append(plan.Targets, t)
		}
	}
	plan.ID = pipelineDigest(plan)
	return plan, nil
}
func pipelinePlanID(plan PipelinePlan) string { plan.ID = ""; return pipelineDigest(plan) }
func (p *PGPipelineEffects) ReserveEffectsTx(ctx context.Context, tx *sql.Tx, b PipelineBinding, plan PipelinePlan, prepared []PipelineOutput) error {
	if tx == nil || !b.valid() || plan.ID != pipelinePlanID(plan) || b.PlanID != plan.ID || plan.Scope != b.Scope || plan.TenantID != b.TenantID || len(plan.Targets) > 100 || b.Demand.MetadataRows < int64(3+len(plan.Targets)) {
		return ErrPipelineConflict
	}
	if e := validatePipelinePreparation(b, plan, prepared); e != nil {
		return e
	}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	a := append(pipelineScopeArgs(b.Scope), b.TenantID)
	streamInsert, e := tx.ExecContext(ctx, `INSERT INTO pipeline_effect_streams(source_id,project_id,catalog_id,stream_id,tenant_id) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, a...)
	if e != nil {
		return e
	}
	checkpointRows, e := streamInsert.RowsAffected()
	if e != nil {
		return e
	}
	if _, _, e = p.lockStream(ctx, tx, b); e != nil {
		return e
	}
	var oldCleanup bool
	if e = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pipeline_effect_cleanup WHERE `+pipelineScopeWhere+` AND owner_id=$5)`, append(pipelineScopeArgs(b.Scope), b.BatchID)...).Scan(&oldCleanup); e != nil {
		return e
	}
	if oldCleanup {
		return ErrPipelineConflict
	}
	var digest string
	e = tx.QueryRowContext(ctx, `SELECT binding_sha FROM pipeline_effect_reservations WHERE `+pipelineScopeWhere, pipelineScopeArgs(b.Scope)...).Scan(&digest)
	if e == nil {
		if digest == pipelineDigest(b) {
			return nil
		}
		return ErrPipelineConflict
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	reservationCharge := b.Demand
	reservationCharge.MetadataRows -= 1 - checkpointRows
	// An artifact-free reservation owns the in-flight native proof and usage
	// cursor. The project lock and stream key bound it to one per project.
	if b.PlanRef != (PipelineRef{}) {
		admission := b
		admission.Demand = reservationCharge
		if e = p.lockCapacity(ctx, tx, admission, true); e != nil {
			return e
		}
	}
	if _, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_plans(id,tenant_id,source_id,project_id,catalog_id,stream_id) VALUES($1,$2,$3,$4,$5,$6)`, plan.ID, b.TenantID, b.Scope.SourceID, b.Scope.ProjectID, b.Scope.CatalogID, b.Scope.StreamID); e != nil {
		return e
	}
	for _, t := range plan.Targets {
		if !pipelineID(t.ID) || !pipelineID(t.Incarnation) || !pipelineID(t.RuleRevision) || !pipelineID(t.DestinationRevision) || !pipelineID(t.CreatorID) || !t.Rule.valid() {
			return ErrPipelineConflict
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_targets(plan_id,id,incarnation,rule_revision,destination_revision,creator_id,rule_key,rule_version,rule_sha,rule_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, plan.ID, t.ID, t.Incarnation, t.RuleRevision, t.DestinationRevision, t.CreatorID, t.Rule.Key, t.Rule.VersionID, t.Rule.SHA256, t.Rule.Bytes)
		if e != nil {
			return e
		}
	}
	a = append(a, b.BatchID, b.ArtifactPrefix, pipelineDigest(b), b.Epoch, b.AcquisitionID, b.PlanID, b.PreparedSHA256, b.PlanRef.Key, b.PlanRef.VersionID, b.PlanRef.SHA256, b.PlanRef.Bytes, b.ManifestBytesLimit, b.Demand.References, b.Demand.QueuedBytes, b.Demand.MetadataRows, b.Demand.ArtifactBytes, b.Demand.ProofSlots, int64(1))
	_, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_reservations(source_id,project_id,catalog_id,stream_id,tenant_id,batch_id,artifact_prefix,binding_sha,epoch,acquisition_id,plan_id,prepared_sha,plan_key,plan_version,plan_sha,plan_bytes,manifest_bytes_limit,refs,bytes,rows,artifacts,proofs,checkpoint_rows) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`, a...)
	if e != nil {
		return e
	}
	return p.charge(ctx, tx, b, reservationCharge, 1)
}
func (p *PGPipelineEffects) matchReservation(ctx context.Context, tx *sql.Tx, b PipelineBinding) error {
	var digest string
	e := tx.QueryRowContext(ctx, `SELECT binding_sha FROM pipeline_effect_reservations WHERE `+pipelineScopeWhere+` FOR UPDATE`, pipelineScopeArgs(b.Scope)...).Scan(&digest)
	if e != nil {
		return e
	}
	if digest != pipelineDigest(b) {
		return ErrPipelineConflict
	}
	return nil
}
