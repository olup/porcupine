package output

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type HostedWebhookLimits struct {
	MaxAttempts int
	Timeout     time.Duration
}

type PipelineDelivery struct {
	Claim    PipelineClaim
	Target   PipelineTarget
	Body     PipelineRef
	Sequence int64
	Attempts int
}

const pipelineDeliveryDue = `j.kind='delivery' AND j.next_attempt_at<=clock_timestamp() AND
 (j.state='pending' OR
  (j.state='running' AND j.lease_until<=clock_timestamp()))
 AND NOT EXISTS (SELECT 1 FROM pipeline_effect_cooldowns c
 WHERE c.project_id=j.project_id AND c.pipeline_id=j.pipeline_id AND c.destination_revision=j.destination_revision
 AND c.not_before>clock_timestamp())`

// NextDelivery selects an advisory candidate. Eligibility is checked again in
// the transaction that freezes the send.
func (p *PGPipelineEffects) NextDelivery(ctx context.Context, db *sql.DB, source string) (PipelineDelivery, error) {
	return p.NextDeliveryOutsideProjects(ctx, db, source, nil)
}

// NextDeliveryOutsideProjects keeps a blocked project from monopolizing a
// bounded scheduler pass while its durable DELETE is being settled.
func (p *PGPipelineEffects) NextDeliveryOutsideProjects(ctx context.Context, db *sql.DB, source string, excluded []string) (PipelineDelivery, error) {
	var d PipelineDelivery
	c := &d.Claim
	t := &d.Target
	r := &d.Body
	err := db.QueryRowContext(ctx, `SELECT j.source_id,j.project_id,j.catalog_id,j.stream_id,j.tenant_id,j.id,j.incarnation,j.generation,j.version,
 j.pipeline_id,j.rule_revision,j.destination_revision,j.creator_id,j.body_key,j.body_version,j.body_sha,j.body_bytes,j.sequence,j.attempts
 FROM pipeline_effect_jobs j JOIN pipeline_effect_definitions p ON
 (p.project_id,p.tenant_id,p.id,p.incarnation,p.destination_revision,p.creator_id)=
 (j.project_id,j.tenant_id,j.pipeline_id,j.incarnation,j.destination_revision,j.creator_id)
 WHERE j.source_id=$1 AND (cardinality($2::text[])=0 OR j.project_id<>ALL($2::text[])) AND p.state='active' AND `+pipelineDeliveryDue+`
 AND NOT EXISTS (SELECT 1 FROM project_deletions x WHERE x.project_id=j.project_id)
 AND NOT EXISTS (SELECT 1 FROM tenant_deletions x WHERE x.tenant_id=j.tenant_id)
 ORDER BY j.next_attempt_at,j.project_id,j.id LIMIT 1`, source, append([]string{}, excluded...)).Scan(
		&c.Scope.SourceID, &c.Scope.ProjectID, &c.Scope.CatalogID, &c.Scope.StreamID, &c.TenantID, &c.ID, &c.Incarnation, &c.Generation, &c.Version,
		&t.ID, &t.RuleRevision, &t.DestinationRevision, &t.CreatorID, &r.Key, &r.VersionID, &r.SHA256, &r.Bytes, &d.Sequence, &d.Attempts)
	t.Incarnation = c.Incarnation
	return d, err
}

// FreezeDeliveryTx records the first externally visible attempt, or reserves
// one retry of the exact body. The caller must hold the project coordinator
// session, verify the immutable body, reconcile native through cut, and check
// deletion intents in this transaction before allowing HTTP.
func (p *PGPipelineEffects) FreezeDeliveryTx(ctx context.Context, tx *sql.Tx, d PipelineDelivery, attempt string, cut int64, limits HostedWebhookLimits) (PipelineClaim, bool, error) {
	c := d.Claim
	if tx == nil || !pipelineID(attempt) || !c.Scope.valid() || !d.Body.valid() || cut < d.Sequence || limits.MaxAttempts < 1 || limits.MaxAttempts > 100 || limits.Timeout < time.Second || limits.Timeout > 2*time.Minute {
		return c, false, ErrPipelineConflict
	}
	b := PipelineBinding{Scope: c.Scope, TenantID: c.TenantID}
	if err := p.lockCapacity(ctx, tx, b, false); err != nil {
		return c, false, err
	}
	seq, _, err := p.lockStream(ctx, tx, b)
	if err != nil {
		return c, false, err
	}
	if seq != cut {
		return c, false, ErrPipelineConflict
	}

	a := append(pipelineScopeArgs(c.Scope), c.TenantID, c.ID, c.Incarnation, c.Generation, c.Version)
	var attempts int
	var body PipelineRef
	var target PipelineTarget
	err = tx.QueryRowContext(ctx, `SELECT j.attempts,j.body_key,j.body_version,j.body_sha,j.body_bytes,j.pipeline_id,j.incarnation,j.rule_revision,j.destination_revision,j.creator_id FROM pipeline_effect_jobs j WHERE `+pipelineScopeWhere+`
 AND j.tenant_id=$5 AND j.id=$6 AND j.incarnation=$7 AND j.generation=$8 AND j.version=$9 AND `+pipelineDeliveryDue+` FOR UPDATE OF j`, a...).Scan(&attempts, &body.Key, &body.VersionID, &body.SHA256, &body.Bytes, &target.ID, &target.Incarnation, &target.RuleRevision, &target.DestinationRevision, &target.CreatorID)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}

	if body != d.Body || target != d.Target || attempts != d.Attempts {
		return c, false, ErrPipelineConflict
	}

	var state string
	err = tx.QueryRowContext(ctx, `SELECT state FROM pipeline_effect_definitions
 WHERE project_id=$1 AND tenant_id=$2 AND id=$3 AND incarnation=$4 AND destination_revision=$5 AND creator_id=$6 FOR UPDATE`,
		c.Scope.ProjectID, c.TenantID, d.Target.ID, c.Incarnation, d.Target.DestinationRevision, d.Target.CreatorID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) || err == nil && state != "active" {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	// Serialize the deletion check with the transaction that creates a fence.
	var project string
	err = tx.QueryRowContext(ctx, `SELECT id FROM projects WHERE id=$1 AND tenant_id=$2 FOR UPDATE`, c.Scope.ProjectID, c.TenantID).Scan(&project)
	if errors.Is(err, sql.ErrNoRows) {
		return c, false, nil
	}
	if err != nil {
		return c, false, err
	}
	var deleting bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)
 OR EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=$2)`, project, c.TenantID).Scan(&deleting)
	if err != nil || deleting {
		return c, false, err
	}
	authorized, err := p.authority.ResolveCreatorTx(ctx, tx, c.TenantID, project, d.Target.CreatorID)
	if err != nil {
		return c, false, err
	}
	if !authorized || attempts >= limits.MaxAttempts {
		code := "attempts_exhausted"
		if !authorized {
			code = "creator_unauthorized"
		}
		r, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET state='retired',version=version+1,lease_until=NULL,
 completed_at=clock_timestamp(),error_code=$10 WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND version=$9`, append(a, code)...)
		if err != nil {
			return c, false, err
		}
		return c, false, pipelineOne(r)
	}
	lease := limits.Timeout + 15*time.Second
	err = tx.QueryRowContext(ctx, `UPDATE pipeline_effect_jobs SET state='running',attempt=$10,version=version+1,
 lease_until=clock_timestamp()+$11*interval '1 millisecond',send_cut=coalesce(send_cut,$12),attempts=attempts+1
 WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND version=$9
 RETURNING version`, append(a, attempt, lease.Milliseconds(), cut)...).Scan(&c.Version)
	c.Attempt = attempt
	return c, err == nil, err
}

// DeferPipelineWorkTx only shifts advisory scheduling. Attempts, leases,
// immutable refs and capacity stay owned by the existing version.
func (p *PGPipelineEffects) DeferPipelineWorkTx(ctx context.Context, tx *sql.Tx, c PipelineClaim) error {
	_, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET next_attempt_at=GREATEST(next_attempt_at,clock_timestamp()+interval '2 seconds')
 WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6 AND incarnation=$7 AND generation=$8 AND version=$9 AND
 (state='pending' OR state='running' AND lease_until<=clock_timestamp() OR state='retired' AND needs_recompose)`,
		append(pipelineScopeArgs(c.Scope), c.TenantID, c.ID, c.Incarnation, c.Generation, c.Version)...)
	return err
}
