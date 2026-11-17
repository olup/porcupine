package output

import (
	"context"
	"database/sql"
	"time"

	"github.com/olup/porcupine/internal/usage"
)

func (p *PGPipelineEffects) DeliveryMembers(ctx context.Context, db *sql.DB, c PipelineClaim) ([]PipelineMember, error) {
	rows, err := db.QueryContext(ctx, `SELECT uuid,encoded_bytes FROM pipeline_effect_members WHERE `+pipelineScopeWhere+`
 AND job_id=$5 ORDER BY position LIMIT 101`, append(pipelineScopeArgs(c.Scope), c.ID)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []PipelineMember
	for rows.Next() {
		var member PipelineMember
		if err := rows.Scan(&member.UUID, &member.Bytes); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(members) == 0 || len(members) > 100 {
		return nil, ErrPipelineConflict
	}
	return members, nil
}

// CancelHostedWebhookEventTx suppresses outstanding deliveries for the deleted
// UUID and retains composed bodies until their surviving members are rescheduled.
// It is safe to replay in the mutation publication transaction.
func (p *PGPipelineEffects) CancelHostedWebhookEventTx(ctx context.Context, tx *sql.Tx, scope PipelineScope, tenant, uuid string) error {
	if tx == nil || !scope.valid() || !pipelineID(tenant) || !pipelineID(uuid) {
		return ErrPipelineConflict
	}
	_, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs AS j SET state='retired',version=version+1,lease_until=NULL,
 completed_at=clock_timestamp(),error_code='event_deleted',needs_recompose=(window_end_ms>0 AND refs>1) WHERE `+pipelineScopeWhere+`
 AND tenant_id=$5 AND kind='delivery' AND state IN ('pending','running') AND EXISTS (
 SELECT 1 FROM pipeline_effect_members m
 WHERE (m.source_id,m.project_id,m.catalog_id,m.stream_id,m.job_id)=(j.source_id,j.project_id,j.catalog_id,j.stream_id,j.id)
 AND m.uuid=$6)`, append(pipelineScopeArgs(scope), tenant, uuid)...)
	return err
}

// CompleteDeliveryTx joins success accounting to the same attempt CAS. An
// uncertain completion response can be retried without incrementing usage.
func (p *PGPipelineEffects) CompleteDeliveryTx(ctx context.Context, tx *sql.Tx, d PipelineDelivery, c PipelineClaim, status int, code string, retry bool, maxAttempts int) error {
	if tx == nil || status < 0 || status > 599 || code == "" && (status < 200 || status >= 300) || c.Attempt == "" || c.Scope != d.Claim.Scope || c.ID != d.Claim.ID || c.Version != d.Claim.Version+1 {
		return ErrPipelineConflict
	}
	state := "retired"
	var delay time.Duration
	if code == "" {
		state = "succeeded"
	} else if retry && d.Attempts+1 < maxAttempts {
		state = "pending"
		delay = webhookBackoff(d.Attempts + 1)
	}
	var completedAt sql.NullTime
	err := tx.QueryRowContext(ctx, `UPDATE pipeline_effect_jobs SET state=$11,version=version+1,lease_until=NULL,
 next_attempt_at=clock_timestamp()+$12*interval '1 millisecond',last_status=$13,error_code=$14,
 completed_at=CASE WHEN $11='pending' THEN NULL ELSE clock_timestamp() END
 WHERE `+pipelineClaimWhere+` AND kind='delivery' AND send_cut IS NOT NULL RETURNING completed_at`,
		append(pipelineClaimArgs(c), state, delay.Milliseconds(), status, code)...).Scan(&completedAt)
	if err != nil {
		return err
	}
	if retry {
		var failures int
		err = tx.QueryRowContext(ctx, `INSERT INTO pipeline_effect_cooldowns(project_id,pipeline_id,destination_revision,not_before,failures)
 VALUES($1,$2,$3,clock_timestamp(),1) ON CONFLICT(project_id,pipeline_id) DO UPDATE SET
 destination_revision=excluded.destination_revision,failures=CASE WHEN pipeline_effect_cooldowns.destination_revision=excluded.destination_revision
 THEN LEAST(pipeline_effect_cooldowns.failures+1,16) ELSE 1 END RETURNING failures`, c.Scope.ProjectID, d.Target.ID, d.Target.DestinationRevision).Scan(&failures)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE pipeline_effect_cooldowns SET not_before=clock_timestamp()+$4*interval '1 millisecond'
 WHERE project_id=$1 AND pipeline_id=$2 AND destination_revision=$3`, c.Scope.ProjectID, d.Target.ID, d.Target.DestinationRevision,
			webhookBackoff(max(failures, d.Attempts+1)).Milliseconds())
		return err
	}
	if code != "" {
		return nil
	}
	_, err = tx.ExecContext(ctx, `DELETE FROM pipeline_effect_cooldowns WHERE project_id=$1 AND pipeline_id=$2 AND destination_revision=$3`,
		c.Scope.ProjectID, d.Target.ID, d.Target.DestinationRevision)
	if err != nil {
		return err
	}
	// Distinguish these PG-owned counters from native cumulative live/import rollups.
	source := "webhooks_" + pipelineDigest(c.Scope)
	return usage.NewRecorder(nil, source, time.Now).AddTx(ctx, tx, c.Scope.ProjectID, completedAt.Time, usage.Counters{Webhooks: 1}, 0)
}

// DeliveryLeaseRemaining uses the database attempt nonce and clock so workers
// waking after a pause cannot dispatch an already expired attempt.
func (p *PGPipelineEffects) DeliveryLeaseRemaining(ctx context.Context, db *sql.DB, c PipelineClaim) (time.Duration, error) {
	var milliseconds int64
	err := db.QueryRowContext(ctx, `SELECT floor(extract(epoch FROM lease_until-clock_timestamp())*1000)::bigint
 FROM pipeline_effect_jobs WHERE `+pipelineClaimWhere+` AND send_cut IS NOT NULL`, pipelineClaimArgs(c)...).Scan(&milliseconds)
	return time.Duration(milliseconds) * time.Millisecond, err
}
