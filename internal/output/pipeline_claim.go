package output

import (
	"context"
	"database/sql"
)

type PipelineClaim struct {
	Scope                                          PipelineScope
	TenantID, ID, Incarnation, Generation, Attempt string
	Version                                        int64
}

func pipelineClaimArgs(c PipelineClaim) []any {
	return append(pipelineScopeArgs(c.Scope), c.TenantID, c.ID, c.Incarnation, c.Generation, c.Attempt, c.Version)
}

const pipelineClaimWhere = pipelineScopeWhere + ` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND attempt=$9 AND version=$10 AND state='running' AND lease_until>clock_timestamp()`

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
	e := tx.QueryRowContext(ctx, `UPDATE pipeline_effect_cleanup c SET attempt=$7,version=version+1,lease_until=clock_timestamp()+interval '30 seconds' WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND owner_id=$6 AND (lease_until IS NULL OR lease_until<clock_timestamp()) AND NOT EXISTS(SELECT 1 FROM pipeline_effect_assemblies a WHERE a.source_id=c.source_id AND a.project_id=c.project_id AND a.catalog_id=c.catalog_id AND a.stream_id=c.stream_id AND a.generation=c.owner_id) AND NOT EXISTS(SELECT 1 FROM pipeline_effect_jobs j WHERE j.source_id=c.source_id AND j.project_id=c.project_id AND j.catalog_id=c.catalog_id AND j.stream_id=c.stream_id AND j.batch_id=c.owner_id) RETURNING generation,prefix,version`, a...).Scan(&c.Generation, &c.Prefix, &c.Version)
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
