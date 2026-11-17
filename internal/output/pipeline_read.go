package output

import (
	"context"
	"database/sql"
)

type PipelineStoredJob struct {
	Claim                PipelineClaim
	Output               PipelineOutput
	State, CreatorID     string
	Sequence, NextMember int64
}

// JobTx returns bounded metadata for cold processing and terminal cleanup. It
// does not confer claim authority. Read it in the same transaction as a claim.
func (p *PGPipelineEffects) JobTx(ctx context.Context, tx *sql.Tx, s PipelineScope, tenant, id string) (PipelineStoredJob, error) {
	j := PipelineStoredJob{Claim: PipelineClaim{Scope: s, TenantID: tenant, ID: id}}
	if tx == nil || !s.valid() || !pipelineID(tenant) || !pipelineID(id) {
		return j, ErrPipelineConflict
	}
	o := &j.Output
	o.ID = id
	e := tx.QueryRowContext(ctx, `SELECT pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,state,attempt,version,next_member,refs,bytes,rows,artifacts FROM pipeline_effect_jobs WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND id=$6`, append(pipelineScopeArgs(s), tenant, id)...).Scan(&o.PipelineID, &o.Incarnation, &o.RuleRevision, &o.DestinationRevision, &j.CreatorID, &o.Generation, &o.Kind, &j.Sequence, &o.Body.Key, &o.Body.VersionID, &o.Body.SHA256, &o.Body.Bytes, &j.State, &j.Claim.Attempt, &j.Claim.Version, &j.NextMember, &o.Charge.References, &o.Charge.QueuedBytes, &o.Charge.MetadataRows, &o.Charge.ArtifactBytes)
	if e != nil {
		return j, e
	}
	j.Claim.Incarnation = o.Incarnation
	j.Claim.Generation = o.Generation
	rows, e := tx.QueryContext(ctx, `SELECT uuid,encoded_bytes FROM pipeline_effect_members WHERE `+pipelineScopeWhere+` AND job_id=$5 ORDER BY position LIMIT 101`, append(pipelineScopeArgs(s), id)...)
	if e != nil {
		return j, e
	}
	defer rows.Close()
	for rows.Next() {
		var m PipelineMember
		if e = rows.Scan(&m.UUID, &m.Bytes); e != nil {
			return j, e
		}
		o.Members = append(o.Members, m)
	}
	if e = rows.Err(); e != nil {
		return j, e
	}
	if len(o.Members) > 100 || int64(len(o.Members)) != o.Charge.References {
		return j, ErrPipelineConflict
	}
	return j, nil
}
