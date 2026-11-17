package output

import (
	"context"
	"database/sql"
	"time"
)

type PipelineClaim struct {
	Scope                                          PipelineScope
	TenantID, ID, Incarnation, Generation, Attempt string
	Version                                        int64
}

// ClaimJobTx is a storage claim only. It does not authorize an HTTP send. The
// eventual sender must fence native work and check durable DELETE intents before
// a separate sending/frozen transition in the integration adapter.
func (p *PGPipelineEffects) ClaimJobTx(ctx context.Context, tx *sql.Tx, scope PipelineScope, tenant, id, attempt string, lease time.Duration) (PipelineClaim, error) {
	c := PipelineClaim{Scope: scope, TenantID: tenant, ID: id, Attempt: attempt}
	if tx == nil || !scope.valid() || !pipelineID(tenant) || !pipelineID(id) || !pipelineID(attempt) || lease < time.Second || lease > time.Minute {
		return c, ErrPipelineConflict
	}
	b := PipelineBinding{Scope: scope, TenantID: tenant}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return c, e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return c, e
	}
	var target PipelineTarget
	var state string
	var available bool
	e := tx.QueryRowContext(ctx, `SELECT pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,state,version,(lease_until IS NULL OR lease_until<clock_timestamp()) FROM pipeline_effect_jobs WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6 FOR UPDATE`, append(pipelineScopeArgs(scope), tenant, id)...).Scan(&target.ID, &target.Incarnation, &target.RuleRevision, &target.DestinationRevision, &target.CreatorID, &c.Generation, &state, &c.Version, &available)
	if e != nil {
		return c, e
	}
	if !(state == "pending" || state == "running" && available) {
		return c, ErrPipelineConflict
	}
	var currentState string
	e = tx.QueryRowContext(ctx, `SELECT state FROM pipeline_effect_definitions WHERE project_id=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE`, scope.ProjectID, tenant, target.ID).Scan(&currentState)
	if e != nil {
		return c, e
	}
	s, e := p.currentTarget(ctx, tx, b, target)
	if e != nil {
		return c, e
	}
	if s == "retired" {
		return c, ErrPipelineAuthority
	}
	if currentState != "active" {
		return c, ErrPipelineAuthority
	}
	c.Incarnation = target.Incarnation
	c.Version++
	a := append(pipelineScopeArgs(scope), tenant, id, attempt, c.Version, int64(lease/time.Millisecond))
	r, e := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET state='running',attempt=$7,version=$8,lease_until=clock_timestamp()+$9*interval '1 millisecond' WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6`, a...)
	if e != nil {
		return c, e
	}
	return c, pipelineOne(r)
}
func pipelineClaimArgs(c PipelineClaim) []any {
	return append(pipelineScopeArgs(c.Scope), c.TenantID, c.ID, c.Incarnation, c.Generation, c.Attempt, c.Version)
}

const pipelineClaimWhere = pipelineScopeWhere + ` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND attempt=$9 AND version=$10 AND state='running' AND lease_until>clock_timestamp()`

func (p *PGPipelineEffects) RenewJobTx(ctx context.Context, tx *sql.Tx, c PipelineClaim, lease time.Duration) error {
	if lease < time.Second || lease > time.Minute {
		return ErrPipelineConflict
	}
	if e := p.authorizeClaim(ctx, tx, c); e != nil {
		return e
	}
	r, e := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET lease_until=clock_timestamp()+$11*interval '1 millisecond' WHERE `+pipelineClaimWhere, append(pipelineClaimArgs(c), int64(lease/time.Millisecond))...)
	if e != nil {
		return e
	}
	return pipelineOne(r)
}
func (p *PGPipelineEffects) CheckpointFragmentTx(ctx context.Context, tx *sql.Tx, c PipelineClaim, next int64) error {
	if next < 0 {
		return ErrPipelineConflict
	}
	if e := p.authorizeClaim(ctx, tx, c); e != nil {
		return e
	}
	r, e := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET next_member=$11 WHERE `+pipelineClaimWhere+` AND kind='fragment' AND next_member<=$11 AND $11<=refs`, append(pipelineClaimArgs(c), next)...)
	if e != nil {
		return e
	}
	return pipelineOne(r)
}

// FinishJobTx only records terminal state. Capacity and object ownership survive
// until verified cleanup. Output-success usage, when implemented, joins this TX.
func (p *PGPipelineEffects) FinishJobTx(ctx context.Context, tx *sql.Tx, c PipelineClaim, success bool) error {
	state := "retired"
	if success {
		if e := p.authorizeClaim(ctx, tx, c); e != nil {
			return e
		}
		state = "succeeded"
	}
	r, e := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET state=$11,version=version+1,lease_until=NULL WHERE `+pipelineClaimWhere, append(pipelineClaimArgs(c), state)...)
	if e != nil {
		return e
	}
	return pipelineOne(r)
}

// RemoveCleanedJobTx is called only after version-aware object cleanup has been
// verified by the adapter. It cannot infer cleanup from an expired worker lease.
func (p *PGPipelineEffects) RemoveCleanedJobTx(ctx context.Context, tx *sql.Tx, c PipelineClaim) error {
	b := PipelineBinding{Scope: c.Scope, TenantID: c.TenantID}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return e
	}
	var d PipelineDemand
	e := tx.QueryRowContext(ctx, `DELETE FROM pipeline_effect_jobs WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND attempt=$9 AND version=$10 AND state IN ('retired','succeeded') RETURNING refs,bytes,rows,artifacts`, pipelineClaimArgs(c)...).Scan(&d.References, &d.QueuedBytes, &d.MetadataRows, &d.ArtifactBytes)
	if e != nil {
		return e
	}
	return p.charge(ctx, tx, b, d, -1)
}

type PipelineCleanupClaim struct {
	Scope                                          PipelineScope
	TenantID, OwnerID, Generation, Prefix, Attempt string
	Version                                        int64
}

func (p *PGPipelineEffects) ClaimCleanupTx(ctx context.Context, tx *sql.Tx, s PipelineScope, tenant, owner, attempt string) (PipelineCleanupClaim, error) {
	c := PipelineCleanupClaim{Scope: s, TenantID: tenant, OwnerID: owner, Attempt: attempt}
	if !s.valid() || !pipelineID(tenant) || !pipelineID(owner) || !pipelineID(attempt) {
		return c, ErrPipelineConflict
	}
	b := PipelineBinding{Scope: s, TenantID: tenant}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return c, e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return c, e
	}
	a := append(pipelineScopeArgs(s), tenant, owner, attempt)
	e := tx.QueryRowContext(ctx, `UPDATE pipeline_effect_cleanup c SET attempt=$7,version=version+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND owner_id=$6 AND (lease_until IS NULL OR lease_until<clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM pipeline_effect_jobs j WHERE j.source_id=c.source_id AND j.project_id=c.project_id AND j.catalog_id=c.catalog_id AND j.stream_id=c.stream_id AND j.batch_id=c.owner_id) RETURNING generation,prefix,version`, a...).Scan(&c.Generation, &c.Prefix, &c.Version)
	return c, e
}

// CompleteCleanupTx requires namespace/version/multipart cleanup proof checked
// outside PG. Unknown namespaces or failed deletes must leave this row intact.
func (p *PGPipelineEffects) CompleteCleanupTx(ctx context.Context, tx *sql.Tx, c PipelineCleanupClaim) error {
	b := PipelineBinding{Scope: c.Scope, TenantID: c.TenantID}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return e
	}
	var d PipelineDemand
	a := append(pipelineScopeArgs(c.Scope), c.TenantID, c.OwnerID, c.Generation, c.Prefix, c.Attempt, c.Version)
	e := tx.QueryRowContext(ctx, `DELETE FROM pipeline_effect_cleanup WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND owner_id=$6 AND generation=$7 AND prefix=$8 AND attempt=$9 AND version=$10 AND lease_until>clock_timestamp() RETURNING refs,bytes,rows,artifacts`, a...).Scan(&d.References, &d.QueuedBytes, &d.MetadataRows, &d.ArtifactBytes)
	if e != nil {
		return e
	}
	return p.charge(ctx, tx, b, d, -1)
}

func (p *PGPipelineEffects) authorizeClaim(ctx context.Context, tx *sql.Tx, c PipelineClaim) error {
	b := PipelineBinding{Scope: c.Scope, TenantID: c.TenantID}
	if e := p.lockCapacity(ctx, tx, b, false); e != nil {
		return e
	}
	if _, _, e := p.lockStream(ctx, tx, b); e != nil {
		return e
	}
	var t PipelineTarget
	e := tx.QueryRowContext(ctx, `SELECT pipeline_id,incarnation,rule_revision,destination_revision,creator_id FROM pipeline_effect_jobs WHERE `+pipelineClaimWhere+` FOR UPDATE`, pipelineClaimArgs(c)...).Scan(&t.ID, &t.Incarnation, &t.RuleRevision, &t.DestinationRevision, &t.CreatorID)
	if e != nil {
		return ErrPipelineConflict
	}
	state, e := p.currentTarget(ctx, tx, b, t)
	if e != nil {
		return e
	}
	if state == "retired" {
		return ErrPipelineAuthority
	}
	return nil
}
