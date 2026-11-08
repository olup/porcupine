package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"
)

var errWebhookCapacity = errors.New("webhook queue is full")
var errWebhookDeliveryMissing = errors.New("webhook delivery unavailable")

// Payload-bearing failures remain charged until retried or canceled. Successful
// audit records contain no event payload and have bounded retention.
func (s *Service) initializeWebhook(ctx context.Context) error {
	if s.pipelines != nil && s.pipelines.webhookClient == nil {
		s.pipelines.webhookClient = newWebhookHTTPClient()
	}
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS webhook_deliveries (
 id TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
 project_id TEXT NOT NULL,event_uuid TEXT NOT NULL,payload TEXT NOT NULL,bytes INTEGER NOT NULL,
 frozen INTEGER NOT NULL DEFAULT 0,event_count INTEGER NOT NULL DEFAULT 1,window_start_ms INTEGER NOT NULL DEFAULT 0,window_end_ms INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('queued','sending','delivered','failed','canceled')),
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL,last_status INTEGER NOT NULL DEFAULT 0,
 error_code TEXT NOT NULL DEFAULT '',created_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL DEFAULT 0,
 UNIQUE(pipeline_id,event_uuid));
 CREATE TABLE IF NOT EXISTS webhook_cooldowns(pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,not_before_ms INTEGER NOT NULL,failures INTEGER NOT NULL);
 CREATE TABLE IF NOT EXISTS webhook_members(project_id TEXT NOT NULL,pipeline_id TEXT NOT NULL,event_uuid TEXT NOT NULL,delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(id) ON DELETE CASCADE,PRIMARY KEY(pipeline_id,event_uuid)) WITHOUT ROWID;
 CREATE INDEX IF NOT EXISTS webhook_chunk ON webhook_deliveries(pipeline_id,state,window_end_ms,attempts,created_at_ms);
 CREATE INDEX IF NOT EXISTS webhook_appendable ON webhook_deliveries(pipeline_id,window_end_ms,created_at_ms DESC,id DESC) WHERE state='queued' AND frozen=0 AND event_count<100 AND bytes<1048576;
 CREATE INDEX IF NOT EXISTS webhook_due ON webhook_deliveries(state,next_attempt_ms,created_at_ms);
 CREATE INDEX IF NOT EXISTS webhook_project_event ON webhook_deliveries(project_id,event_uuid);
 CREATE INDEX IF NOT EXISTS webhook_completed ON webhook_deliveries(completed_at_ms);
 CREATE INDEX IF NOT EXISTS webhook_project_completed ON webhook_deliveries(project_id,completed_at_ms,id);
 INSERT OR IGNORE INTO metadata(key,value) VALUES('webhook_history_changes','0'),('webhook_history_pruned_ms','0');
 CREATE TRIGGER IF NOT EXISTS webhook_history_change AFTER UPDATE OF payload ON webhook_deliveries WHEN OLD.payload<>'' AND NEW.payload='' BEGIN UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='webhook_history_changes'; END;
 CREATE TABLE IF NOT EXISTS webhook_queue_usage(scope TEXT PRIMARY KEY,count INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID;
 INSERT OR IGNORE INTO webhook_queue_usage VALUES('instance',0,0);
 CREATE TRIGGER IF NOT EXISTS webhook_usage_insert AFTER INSERT ON webhook_deliveries WHEN NEW.payload<>'' BEGIN
 INSERT INTO webhook_queue_usage(scope,count,bytes) VALUES('project:'||NEW.project_id,NEW.event_count,NEW.bytes) ON CONFLICT(scope) DO UPDATE SET count=count+NEW.event_count,bytes=bytes+NEW.bytes;
 UPDATE webhook_queue_usage SET count=count+NEW.event_count,bytes=bytes+NEW.bytes WHERE scope='instance'; END;
 CREATE TRIGGER IF NOT EXISTS webhook_usage_update AFTER UPDATE OF payload,bytes,event_count ON webhook_deliveries BEGIN
 INSERT OR IGNORE INTO webhook_queue_usage(scope,count,bytes) VALUES('project:'||NEW.project_id,0,0);
 UPDATE webhook_queue_usage SET count=count+CASE WHEN NEW.payload<>'' THEN NEW.event_count ELSE 0 END-CASE WHEN OLD.payload<>'' THEN OLD.event_count ELSE 0 END,bytes=bytes+NEW.bytes-OLD.bytes WHERE scope IN ('instance','project:'||NEW.project_id); END;
 CREATE TRIGGER IF NOT EXISTS webhook_usage_delete AFTER DELETE ON webhook_deliveries WHEN OLD.payload<>'' BEGIN
 UPDATE webhook_queue_usage SET count=count-OLD.event_count,bytes=bytes-OLD.bytes WHERE scope IN ('instance','project:'||OLD.project_id); END;
 UPDATE webhook_deliveries SET state='queued',next_attempt_ms=0 WHERE state='sending';`)
	return err
}

func webhookMatches(spec WebhookPipelineSpec, event string) bool {
	for _, name := range spec.ExcludeEvents {
		if name == event {
			return false
		}
	}
	if len(spec.IncludeEvents) == 0 {
		return true
	}
	for _, name := range spec.IncludeEvents {
		if name == event {
			return true
		}
	}
	return false
}

func (s *Service) enqueueWebhook(ctx context.Context, tx *sql.Tx, event Event) error {
	return s.enqueueWebhooks(ctx, tx, []Event{event})
}
func (s *Service) enqueueWebhooks(ctx context.Context, tx *sql.Tx, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	projectID := events[0].ProjectID
	if s.pipelines == nil {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,config FROM pipelines WHERE project_id=? AND kind='webhook' AND state='active' ORDER BY id`, projectID)
	if err != nil {
		return err
	}
	type destination struct {
		id   string
		spec WebhookPipelineSpec
	}
	destinations := []destination{}
	for rows.Next() {
		var d destination
		var config string
		if err = rows.Scan(&d.id, &config); err != nil {
			rows.Close()
			return err
		}
		if json.Unmarshal([]byte(config), &d.spec) != nil {
			rows.Close()
			return errors.New("invalid stored webhook configuration")
		}
		destinations = append(destinations, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(destinations) == 0 {
		return nil
	}
	var globalCount, globalBytes, projectCount, projectBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT count,bytes FROM webhook_queue_usage WHERE scope='instance'`).Scan(&globalCount, &globalBytes); err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT count,bytes FROM webhook_queue_usage WHERE scope=?`, "project:"+projectID).Scan(&projectCount, &projectBytes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	cfg := s.pipelines.config
	for _, d := range destinations {
		matching := make([]Event, 0, len(events))
		for _, event := range events {
			if event.ProjectID != projectID {
				return errors.New("mixed webhook projects")
			}
			if webhookMatches(d.spec, event.Event) {
				matching = append(matching, event)
			}
		}
		count, delta, err := enqueueWebhookDestinationGroup(ctx, tx, matching, d.id, d.spec, s.clock().UnixMilli(), globalCount, globalBytes, projectCount, projectBytes, cfg)
		if err != nil {
			return err
		}
		globalCount += count
		projectCount += count
		globalBytes += delta
		projectBytes += delta
	}
	return nil
}

type webhookDelivery struct {
	id, pipelineID, projectID, actorID, payload string
	secret                                      []byte
	attempts                                    int
}

func pipelineCreatorAuthorized(ctx context.Context, q oauthQueryer, id, project string) (bool, error) {
	var a actor
	var permissions, projects string
	err := q.QueryRowContext(ctx, `SELECT id,permissions,projects FROM access_keys WHERE id=? AND revoked_at IS NULL`, id).Scan(&a.ID, &permissions, &projects)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return false, errors.New("invalid pipeline creator grants")
	}
	return authorize(a, "pipelines:write", project) && authorize(a, "events:read", project), nil
}

func (s *Service) claimWebhook(ctx context.Context) (*webhookDelivery, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	var d webhookDelivery
	err = tx.QueryRowContext(ctx, `SELECT d.id,d.pipeline_id,d.project_id,p.actor_id,d.payload,p.secret,d.attempts FROM webhook_deliveries d JOIN pipelines p ON p.id=d.pipeline_id AND p.project_id=d.project_id WHERE d.state='queued' AND d.next_attempt_ms<=? AND p.kind='webhook' AND p.state='active' AND NOT EXISTS(SELECT 1 FROM webhook_cooldowns c WHERE c.pipeline_id=d.pipeline_id AND c.not_before_ms>?) ORDER BY d.next_attempt_ms,d.created_at_ms,d.id LIMIT 1`, s.clock().UnixMilli(), s.clock().UnixMilli()).Scan(&d.id, &d.pipelineID, &d.projectID, &d.actorID, &d.payload, &d.secret, &d.attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	allowed, err := pipelineCreatorAuthorized(ctx, tx, d.actorID, d.projectID)
	if err != nil {
		return nil, err
	}
	var deletedUUID string
	err = tx.QueryRowContext(ctx, `SELECT m.event_uuid FROM webhook_members m JOIN event_heads h ON h.project_id=m.project_id AND h.uuid=m.event_uuid WHERE m.project_id=? AND m.delivery_id=? AND h.is_deleted=1 LIMIT 1`, d.projectID, d.id).Scan(&deletedUUID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	deleted := deletedUUID != ""
	if deleted {
		if err = s.cancelWebhookEvent(ctx, tx, d.projectID, deletedUUID); err != nil {
			return nil, err
		}
	} else if !allowed || d.attempts >= s.pipelines.config.WebhookMaxAttempts {
		code := "creator_unauthorized"
		if allowed {
			code = "attempts_exhausted"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='failed',error_code=?,completed_at_ms=? WHERE id=?`, code, s.clock().UnixMilli(), d.id); err != nil {
			return nil, err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='sending',frozen=1,attempts=attempts+1 WHERE id=?`, d.id); err != nil {
			return nil, err
		}
		d.attempts++
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if deleted || !allowed || d.attempts > s.pipelines.config.WebhookMaxAttempts {
		return &webhookDelivery{}, nil
	}
	// A previously exhausted row is now terminal rather than a newly claimed send.
	if d.payload == "" {
		return &webhookDelivery{}, nil
	}
	var state string
	if err = s.db.QueryRowContext(ctx, `SELECT state FROM webhook_deliveries WHERE id=?`, d.id).Scan(&state); err != nil {
		return nil, err
	}
	if state != "sending" {
		return &webhookDelivery{}, nil
	}
	return &d, nil
}

func (s *Service) stepWebhook(ctx context.Context) (progress bool, resultErr error) {
	if s.pipelines == nil {
		return false, nil
	}
	if err := s.pruneWebhookHistory(ctx); err != nil {
		return false, err
	}
	d, err := s.claimWebhook(ctx)
	if err != nil || d == nil {
		return false, err
	}
	if d.id == "" {
		return true, nil
	}
	defer func() {
		if resultErr != nil {
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			s.db.ExecContext(cleanup, `UPDATE webhook_deliveries SET state='queued',next_attempt_ms=? WHERE id=? AND state='sending'`, s.clock().Add(time.Second).UnixMilli(), d.id)
		}
	}()

	secret, err := s.openPipelineSecret(d.projectID, d.pipelineID, d.secret)
	var destination WebhookPipelineSecret
	if err != nil || json.Unmarshal(secret, &destination) != nil || validateWebhookEndpoint(destination.Endpoint) != nil || validateWebhookHeaders(destination.Headers) != nil {
		return true, s.finishWebhook(ctx, d, 0, "invalid_destination", false)
	}
	allowed, err := pipelineCreatorAuthorized(ctx, s.db, d.actorID, d.projectID)
	if err != nil {
		return true, err
	}
	if !allowed {
		return true, s.finishWebhook(ctx, d, 0, "creator_unauthorized", false)
	}
	// Pauses and deletes committed after claim suppress the send. Work already in
	// flight cannot be recalled from the receiver.
	var state, pipelineState string
	if err = s.db.QueryRowContext(ctx, `SELECT d.state,p.state FROM webhook_deliveries d JOIN pipelines p ON p.id=d.pipeline_id WHERE d.id=?`, d.id).Scan(&state, &pipelineState); errors.Is(err, sql.ErrNoRows) {
		return true, nil
	} else if err != nil {
		return true, err
	}
	if state != "sending" {
		return true, nil
	}
	if pipelineState != "active" {
		_, err = s.db.ExecContext(ctx, `UPDATE webhook_deliveries SET state='queued' WHERE id=? AND state='sending'`, d.id)
		return true, err
	}
	attempt, cancel := context.WithTimeout(ctx, time.Duration(s.pipelines.config.WebhookTimeoutSeconds)*time.Second)
	defer cancel()
	status, code := s.sendWebhook(attempt, d, destination)
	if ctx.Err() != nil {
		cleanup, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		_, err = s.db.ExecContext(cleanup, `UPDATE webhook_deliveries SET state='queued',next_attempt_ms=? WHERE id=? AND state='sending'`, s.clock().UnixMilli(), d.id)
		return true, err
	}
	retry := code != "" && (status == 0 || status == 408 || status == 425 || status == 429 || status >= 500)
	return true, s.finishWebhook(ctx, d, status, code, retry)
}

func (s *Service) sendWebhook(ctx context.Context, d *webhookDelivery, destination WebhookPipelineSecret) (int, string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, destination.Endpoint, strings.NewReader(d.payload))
	if err != nil {
		return 0, "invalid_destination"
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("User-Agent", "Porcupine-Webhooks/1")
	request.Header.Set("X-Porcupine-Delivery-ID", d.id)
	request.Header.Set("Idempotency-Key", d.id)
	for key, value := range destination.Headers {
		request.Header.Set(key, value)
	}
	client := s.pipelines.webhookClient
	if client == nil {
		client = newWebhookHTTPClient()
		defer client.CloseIdleConnections()
	}
	response, err := client.Do(request)
	if err != nil {
		return 0, "request_failed"
	}
	defer response.Body.Close()
	n, err := io.Copy(io.Discard, io.LimitReader(response.Body, (64<<10)+1))
	if err != nil || n > 64<<10 {
		return response.StatusCode, "invalid_response"
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, "http_rejected"
	}
	return response.StatusCode, ""
}

func (s *Service) finishWebhook(ctx context.Context, d *webhookDelivery, status int, code string, retry bool) error {
	state := "failed"
	completed := s.clock().UnixMilli()
	next := int64(0)
	if code == "" {
		state = "delivered"
	} else if retry && d.attempts < s.pipelines.config.WebhookMaxAttempts {
		state = "queued"
		completed = 0
		next = s.clock().Add(webhookBackoff(d.attempts)).UnixMilli()
	}
	payload := d.payload
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
	result, err := tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state=?,payload=?,bytes=?,next_attempt_ms=?,last_status=?,error_code=?,completed_at_ms=? WHERE id=? AND state='sending'`, state, payload, size, next, status, code, completed, d.id)
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
		err = tx.QueryRowContext(ctx, `SELECT failures FROM webhook_cooldowns WHERE pipeline_id=?`, d.pipelineID).Scan(&failures)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		failures = min(max(failures, 0), 15) + 1
		until := s.clock().Add(webhookBackoff(max(failures, d.attempts))).UnixMilli()
		if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_cooldowns(pipeline_id,not_before_ms,failures) VALUES(?,?,?) ON CONFLICT(pipeline_id) DO UPDATE SET not_before_ms=excluded.not_before_ms,failures=excluded.failures`, d.pipelineID, until, failures); err != nil {
			return err
		}
	} else if code == "" {
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_cooldowns WHERE pipeline_id=?`, d.pipelineID); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func webhookBackoff(attempt int) time.Duration {
	return min(time.Second*time.Duration(1<<min(max(attempt-1, 0), 12)), time.Hour)
}

func (s *Service) retryWebhookDelivery(ctx context.Context, project, pipeline, id string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var state, payload string
	err = tx.QueryRowContext(ctx, `SELECT state,payload FROM webhook_deliveries WHERE project_id=? AND pipeline_id=? AND id=?`, project, pipeline, id).Scan(&state, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return false, sql.ErrNoRows
	}
	if err != nil {
		return false, err
	}
	if state != "failed" || payload == "" {
		return false, nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='queued',attempts=0,next_attempt_ms=?,error_code='',completed_at_ms=0 WHERE id=? AND state='failed'`, s.clock().UnixMilli(), id)
	if err != nil {
		return false, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_cooldowns WHERE pipeline_id=?`, pipeline); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	s.pipelineWake()
	return true, nil
}

func (s *Service) pruneWebhookHistory(ctx context.Context) error {
	var changes, last int64
	if err := s.db.QueryRowContext(ctx, `SELECT CAST((SELECT value FROM metadata WHERE key='webhook_history_changes') AS INTEGER),CAST((SELECT value FROM metadata WHERE key='webhook_history_pruned_ms') AS INTEGER)`).Scan(&changes, &last); err != nil {
		return err
	}
	if changes < 256 && s.clock().UnixMilli()-last < 60000 {
		return nil
	}
	// Bound metadata independently of retained failed payloads. Amortize scans.
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_deliveries WHERE id IN (SELECT id FROM webhook_deliveries WHERE payload='' AND completed_at_ms>0 AND (completed_at_ms<? OR id NOT IN (SELECT id FROM webhook_deliveries WHERE payload='' ORDER BY completed_at_ms DESC,id DESC LIMIT 10000) OR id IN (SELECT id FROM (SELECT id,row_number() OVER(PARTITION BY project_id ORDER BY completed_at_ms DESC,id DESC) AS position FROM webhook_deliveries WHERE payload='') WHERE position>1000)) LIMIT 256)`, s.clock().Add(-7*24*time.Hour).UnixMilli())
	if err == nil {
		_, err = s.db.ExecContext(ctx, `UPDATE metadata SET value=CASE key WHEN 'webhook_history_changes' THEN '0' ELSE ? END WHERE key IN ('webhook_history_changes','webhook_history_pruned_ms')`, s.clock().UnixMilli())
	}

	return err
}
