package output

import (
	"context"
	"database/sql"
	"errors"
)

type PipelineDefinition struct {
	TenantID, ProjectID, State string
	Target                     PipelineTarget
}

// CompareAndSwapDefinitionTx is the metadata participant for the future common
// API adapter. expected=nil creates. Replacement of destination revision includes
// URL, authentication and account changes; callers must never reuse a revision
// for different configuration. Secrets remain in the existing encrypted domain.
// Call under the same project session as reservation/publication/claims.
func (p *PGPipelineEffects) CompareAndSwapDefinitionTx(ctx context.Context, tx *sql.Tx, expected *PipelineTarget, next PipelineDefinition) error {
	t := next.Target
	if tx == nil || !pipelineID(next.TenantID) || !pipelineID(next.ProjectID) || !pipelineID(t.ID) || !pipelineID(t.Incarnation) || !pipelineID(t.RuleRevision) || !pipelineID(t.DestinationRevision) || !pipelineID(t.CreatorID) || !t.Rule.valid() || !(next.State == "active" || next.State == "paused" || next.State == "retired") {
		return ErrPipelineConflict
	}
	b := PipelineBinding{Scope: PipelineScope{ProjectID: next.ProjectID}, TenantID: next.TenantID}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	var old PipelineTarget
	e := tx.QueryRowContext(ctx, `SELECT id,incarnation,rule_revision,destination_revision,creator_id,rule_key,rule_version,rule_sha,rule_bytes FROM pipeline_effect_definitions WHERE project_id=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE`, next.ProjectID, next.TenantID, t.ID).Scan(&old.ID, &old.Incarnation, &old.RuleRevision, &old.DestinationRevision, &old.CreatorID, &old.Rule.Key, &old.Rule.VersionID, &old.Rule.SHA256, &old.Rule.Bytes)
	if expected == nil {
		if !errors.Is(e, sql.ErrNoRows) {
			if e != nil {
				return e
			}
			return ErrPipelineConflict
		}
	} else {
		if e != nil {
			return e
		}
		if old != *expected {
			return ErrPipelineConflict
		}
	}
	// Lock first, then resolve current creator, including a create.
	if next.State != "retired" {
		ok, e := p.authority.ResolveCreatorTx(ctx, tx, next.TenantID, next.ProjectID, t.CreatorID)
		if e != nil {
			return e
		}
		if !ok {
			return ErrPipelineAuthority
		}
	}
	if expected != nil && t.RuleRevision == old.RuleRevision && t.Rule != old.Rule {
		return ErrPipelineConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_definitions(tenant_id,project_id,id,incarnation,rule_revision,destination_revision,creator_id,state,rule_key,rule_version,rule_sha,rule_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(project_id,id) DO UPDATE SET incarnation=$4,rule_revision=$5,destination_revision=$6,creator_id=$7,state=$8,rule_key=$9,rule_version=$10,rule_sha=$11,rule_bytes=$12 WHERE pipeline_effect_definitions.tenant_id=$1`, next.TenantID, next.ProjectID, t.ID, t.Incarnation, t.RuleRevision, t.DestinationRevision, t.CreatorID, next.State, t.Rule.Key, t.Rule.VersionID, t.Rule.SHA256, t.Rule.Bytes)
	if e != nil {
		return e
	}
	if expected != nil && (next.State == "retired" || t.Incarnation != old.Incarnation || t.DestinationRevision != old.DestinationRevision || t.CreatorID != old.CreatorID) {
		_, e = tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET state='retired',attempt='',version=version+1,lease_until=NULL WHERE project_id=$1 AND tenant_id=$2 AND pipeline_id=$3 AND state IN ('pending','running')`, next.ProjectID, next.TenantID, t.ID)
	}
	return e
}
