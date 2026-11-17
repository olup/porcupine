package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/olup/porcupine/internal/usage"
	"time"
)

type WebhookAttempt struct {
	ID, PipelineID, ProjectID, ActorID, Payload string
	Secret                                      []byte
	Attempts                                    int
}

func (s *WebhookQueue) claimWebhook(ctx context.Context) (*WebhookAttempt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	var d WebhookAttempt
	err = tx.QueryRowContext(ctx, `SELECT d.id,d.pipeline_id,d.project_id,p.actor_id,d.payload,p.secret,d.attempts FROM webhook_deliveries d JOIN pipelines p ON p.id=d.pipeline_id AND p.project_id=d.project_id WHERE d.state='queued' AND d.next_attempt_ms<=$1 AND p.kind='webhook' AND p.state='active' AND NOT EXISTS(SELECT 1 FROM webhook_cooldowns c WHERE c.pipeline_id=d.pipeline_id AND c.not_before_ms>$2) ORDER BY d.next_attempt_ms,d.created_at_ms,d.id LIMIT 1`, s.clock().UnixMilli(), s.clock().UnixMilli()).Scan(&d.ID, &d.PipelineID, &d.ProjectID, &d.ActorID, &d.Payload, &d.Secret, &d.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	allowed, err := s.host.AuthorizeWebhook(ctx, d.ActorID, d.ProjectID)
	if err != nil {
		return nil, err
	}
	var deletedUUID string
	err = tx.QueryRowContext(ctx, `SELECT m.event_uuid FROM webhook_members m JOIN event_heads h ON h.project_id=m.project_id AND h.uuid=m.event_uuid WHERE m.project_id=$1 AND m.delivery_id=$2 AND h.is_deleted=1 LIMIT 1`, d.ProjectID, d.ID).Scan(&deletedUUID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	deleted := deletedUUID != ""
	if deleted {
		if err = CancelWebhookEvent(ctx, tx, d.ProjectID, deletedUUID, s.clock); err != nil {
			return nil, err
		}
	} else if !allowed || d.Attempts >= s.limits.MaxAttempts {
		code := "creator_unauthorized"
		if allowed {
			code = "attempts_exhausted"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='failed',error_code=$1,completed_at_ms=$2 WHERE id=$3`, code, s.clock().UnixMilli(), d.ID); err != nil {
			return nil, err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='sending',frozen=1,attempts=attempts+1 WHERE id=$1`, d.ID); err != nil {
			return nil, err
		}
		d.Attempts++
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if deleted || !allowed || d.Attempts > s.limits.MaxAttempts {
		return &WebhookAttempt{}, nil
	}
	// A previously exhausted row is now terminal rather than a newly claimed send.
	if d.Payload == "" {
		return &WebhookAttempt{}, nil
	}
	var state string
	if err = s.db.QueryRowContext(ctx, `SELECT state FROM webhook_deliveries WHERE id=$1`, d.ID).Scan(&state); err != nil {
		return nil, err
	}
	if state != "sending" {
		return &WebhookAttempt{}, nil
	}
	return &d, nil
}

func (s *WebhookQueue) Step(ctx context.Context) (progress bool, resultErr error) {
	if err := s.pruneWebhookHistory(ctx); err != nil {
		return false, err
	}
	d, err := s.claimWebhook(ctx)
	if err != nil || d == nil {
		return false, err
	}
	if d.ID == "" {
		return true, nil
	}
	defer func() {
		if resultErr != nil {
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			s.db.ExecContext(cleanup, `UPDATE webhook_deliveries SET state='queued',next_attempt_ms=$1 WHERE id=$2 AND state='sending'`, s.clock().Add(time.Second).UnixMilli(), d.ID)
		}
	}()

	ctx, finish, active := s.host.BeginWebhookActivity(ctx, d.ProjectID)
	if !active {
		return true, nil
	}
	defer finish()
	secret, err := s.host.OpenWebhookSecret(d.ProjectID, d.PipelineID, d.Secret)
	var destination WebhookDestination
	if err != nil || json.Unmarshal(secret, &destination) != nil || ValidateWebhookEndpoint(destination.Endpoint) != nil || ValidateWebhookHeaders(destination.Headers) != nil {
		return true, s.Complete(ctx, d, 0, "invalid_destination", false)
	}
	allowed, err := s.host.AuthorizeWebhook(ctx, d.ActorID, d.ProjectID)
	if err != nil {
		return true, err
	}
	if !allowed {
		return true, s.Complete(ctx, d, 0, "creator_unauthorized", false)
	}
	// Pauses and deletes committed after claim suppress the send. Work already in
	// flight cannot be recalled from the receiver.
	var state, pipelineState string
	if err = s.db.QueryRowContext(ctx, `SELECT d.state,p.state FROM webhook_deliveries d JOIN pipelines p ON p.id=d.pipeline_id WHERE d.id=$1`, d.ID).Scan(&state, &pipelineState); errors.Is(err, sql.ErrNoRows) {
		return true, nil
	} else if err != nil {
		return true, err
	}
	if state != "sending" {
		return true, nil
	}
	if pipelineState != "active" {
		_, err = s.db.ExecContext(ctx, `UPDATE webhook_deliveries SET state='queued' WHERE id=$1 AND state='sending'`, d.ID)
		return true, err
	}
	attempt, cancel := context.WithTimeout(ctx, time.Duration(s.limits.TimeoutSeconds)*time.Second)
	defer cancel()
	status, code := SendWebhook(attempt, s.Client, WebhookMessage{Endpoint: destination.Endpoint, Headers: destination.Headers, DeliveryID: d.ID, Payload: d.Payload})
	if ctx.Err() != nil {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_, err = s.db.ExecContext(cleanup, `UPDATE webhook_deliveries SET state='queued',next_attempt_ms=$1 WHERE id=$2 AND state='sending'`, s.clock().UnixMilli(), d.ID)
		return true, err
	}
	retry := code != "" && (status == 0 || status == 408 || status == 425 || status == 429 || status >= 500)
	return true, s.Complete(ctx, d, status, code, retry)
}

func (s *WebhookQueue) Complete(ctx context.Context, d *WebhookAttempt, status int, code string, retry bool) error {
	state := "failed"
	completed := s.clock().UnixMilli()
	next := int64(0)
	if code == "" {
		state = "delivered"
	} else if retry && d.Attempts < s.limits.MaxAttempts {
		state = "queued"
		completed = 0
		next = s.clock().Add(webhookBackoff(d.Attempts)).UnixMilli()
	}
	payload := d.Payload
	size := len(payload)
	if state == "delivered" {
		payload = ""
		size = 0
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state=$1,payload=$2,bytes=$3,next_attempt_ms=$4,last_status=$5,error_code=$6,completed_at_ms=$7 WHERE id=$8 AND state='sending'`, state, payload, size, next, status, code, completed, d.ID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 0 {
		return nil
	}
	if retry {
		var failures int
		err = tx.QueryRowContext(ctx, `SELECT failures FROM webhook_cooldowns WHERE pipeline_id=$1`, d.PipelineID).Scan(&failures)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		failures = min(max(failures, 0), 15) + 1
		until := s.clock().Add(webhookBackoff(max(failures, d.Attempts))).UnixMilli()
		if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_cooldowns(pipeline_id,not_before_ms,failures) VALUES($1,$2,$3) ON CONFLICT(pipeline_id) DO UPDATE SET not_before_ms=excluded.not_before_ms,failures=excluded.failures`, d.PipelineID, until, failures); err != nil {
			return err
		}
	} else if code == "" {
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_cooldowns WHERE pipeline_id=$1`, d.PipelineID); err != nil {
			return err
		}
	}
	if state == "delivered" && s.Usage != nil {
		if err = s.Usage.AddTx(ctx, tx, d.ProjectID, s.clock(), usage.Counters{Webhooks: 1}, 0); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func webhookBackoff(attempt int) time.Duration {
	return min(time.Second*time.Duration(1<<min(max(attempt-1, 0), 12)), time.Hour)
}

func (s *WebhookQueue) Retry(ctx context.Context, project, pipeline, id string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var state, payload string
	err = tx.QueryRowContext(ctx, `SELECT state,payload FROM webhook_deliveries WHERE project_id=$1 AND pipeline_id=$2 AND id=$3`, project, pipeline, id).Scan(&state, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, sql.ErrNoRows
	}
	if err != nil {
		return false, err
	}
	if state != "failed" || payload == "" {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='queued',attempts=0,next_attempt_ms=$1,error_code='',completed_at_ms=0 WHERE id=$2 AND state='failed'`, s.clock().UnixMilli(), id)
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_cooldowns WHERE pipeline_id=$1`, pipeline); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *WebhookQueue) pruneWebhookHistory(ctx context.Context) error {
	var changes, last int64
	if err := s.db.QueryRowContext(ctx, `SELECT CAST((SELECT value FROM metadata WHERE key='webhook_history_changes') AS INTEGER),CAST((SELECT value FROM metadata WHERE key='webhook_history_pruned_ms') AS INTEGER)`).Scan(&changes, &last); err != nil {
		return err
	}
	if changes < 256 && s.clock().UnixMilli()-last < 60000 {
		return nil
	}
	// Bound metadata independently of retained failed payloads. Amortize scans.
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE id IN (SELECT id FROM webhook_deliveries WHERE payload='' AND completed_at_ms>0 AND (completed_at_ms<$1 OR id NOT IN (SELECT id FROM webhook_deliveries WHERE payload='' ORDER BY completed_at_ms DESC,id DESC LIMIT 10000) OR id IN (SELECT id FROM (SELECT id,row_number() OVER(PARTITION BY project_id ORDER BY completed_at_ms DESC,id DESC) AS position FROM webhook_deliveries WHERE payload='') WHERE position>1000)) LIMIT 256)`, s.clock().Add(-7*24*time.Hour).UnixMilli())
	if err == nil {
		_, err = s.db.ExecContext(ctx, `UPDATE metadata SET value=CASE key WHEN 'webhook_history_changes' THEN '0' ELSE $1 END WHERE key IN ('webhook_history_changes','webhook_history_pruned_ms')`, s.clock().UnixMilli())
	}

	return err
}
