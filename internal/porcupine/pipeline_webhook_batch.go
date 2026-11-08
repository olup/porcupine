package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

const webhookChunkEvents = 100
const webhookChunkBytes = 1 << 20

type webhookPayload struct {
	DeliveryID    string  `json:"delivery_id"`
	PipelineID    string  `json:"pipeline_id"`
	ProjectID     string  `json:"project_id"`
	Event         *Event  `json:"event,omitempty"`
	WindowStartMS *int64  `json:"window_start_ms,omitempty"`
	WindowEndMS   *int64  `json:"window_end_ms,omitempty"`
	Events        []Event `json:"events,omitempty"`
}

func encodeWebhookPayload(payload webhookPayload) ([]byte, error) { return encodeWebhookJSON(payload) }
func encodeWebhookJSON(payload any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buffer.Bytes(), []byte{'\n'}), nil
}

type webhookOpenChunk struct {
	id         string
	start, end int64
	count      int
	size       int64
	existing   bool
	events     []Event
	encoded    [][]byte
}

func enqueueWebhookDestinationGroup(ctx context.Context, tx *sql.Tx, events []Event, pipelineID string, spec WebhookPipelineSpec, now int64, globalCount, globalBytes, projectCount, projectBytes int64, cfg PipelineConfig) (int64, int64, error) {
	added, bytesAdded := int64(0), int64(0)
	if len(events) == 0 {
		return 0, 0, nil
	}
	project := events[0].ProjectID
	var chunk *webhookOpenChunk
	prefix := func(id string, start, end int64) string {
		base, _ := encodeWebhookPayload(webhookPayload{DeliveryID: id, PipelineID: pipelineID, ProjectID: project, WindowStartMS: &start, WindowEndMS: &end})
		return string(base[:len(base)-1]) + `,"events":[`
	}
	flush := func() error {
		if chunk == nil || len(chunk.events) == 0 {
			return nil
		}
		var body strings.Builder
		for i, encoded := range chunk.encoded {
			if i > 0 {
				body.WriteByte(',')
			}
			body.Write(encoded)
		}
		if chunk.existing {
			result, err := tx.ExecContext(ctx, `UPDATE webhook_deliveries SET payload=substr(payload,1,length(payload)-2)||','||?||']}',bytes=?,event_count=? WHERE id=? AND state='queued' AND frozen=0 AND substr(payload,-2)=']}'`, body.String(), chunk.size, chunk.count, chunk.id)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil || n != 1 {
				return errors.New("invalid stored webhook chunk")
			}
		} else {
			raw := prefix(chunk.id, chunk.start, chunk.end) + body.String() + "]}"
			if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,window_start_ms,window_end_ms,state,next_attempt_ms,created_at_ms) VALUES(?,?,?,?,?,?,?,?,?,'queued',?,?)`, chunk.id, pipelineID, project, chunk.events[0].UUID, raw, len(raw), chunk.count, chunk.start, chunk.end, max(now, chunk.end), now); err != nil {
				return err
			}
		}
		for _, event := range chunk.events {
			if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_members(project_id,pipeline_id,event_uuid,delivery_id) VALUES(?,?,?,?)`, project, pipelineID, event.UUID, chunk.id); err != nil {
				return err
			}
		}
		return nil
	}
	for _, event := range events {
		if spec.DeliveryMode != "batch" {
			id := newID()
			raw, err := encodeWebhookPayload(webhookPayload{DeliveryID: id, PipelineID: pipelineID, ProjectID: project, Event: &event})
			if err != nil {
				return 0, 0, err
			}
			delta := int64(len(raw))
			if webhookCapacityExceeded(globalCount+added, globalBytes+bytesAdded, projectCount+added, projectBytes+bytesAdded, delta, cfg) {
				return 0, 0, errWebhookCapacity
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,state,next_attempt_ms,created_at_ms) VALUES(?,?,?,?,?,?,1,'queued',?,?)`, id, pipelineID, project, event.UUID, string(raw), len(raw), now, now); err != nil {
				return 0, 0, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_members(project_id,pipeline_id,event_uuid,delivery_id) VALUES(?,?,?,?)`, project, pipelineID, event.UUID, id); err != nil {
				return 0, 0, err
			}
			added++
			bytesAdded += delta
			continue
		}
		if spec.IntervalSeconds < 60 || spec.IntervalSeconds > 86400 {
			return 0, 0, errors.New("invalid stored webhook interval")
		}
		received, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
		if err != nil {
			return 0, 0, err
		}
		if spec.StartAt != "" {
			cutoff, err := time.Parse(time.RFC3339Nano, spec.StartAt)
			if err != nil {
				return 0, 0, err
			}
			if received.Before(cutoff) {
				continue
			}
		}
		interval := spec.IntervalSeconds * 1000
		start := received.UnixMilli() / interval * interval
		end := start + interval
		if chunk == nil || chunk.end != end {
			if err := flush(); err != nil {
				return 0, 0, err
			}
			chunk = &webhookOpenChunk{id: newID(), start: start, end: end}
			err = tx.QueryRowContext(ctx, `SELECT id,bytes,event_count FROM webhook_deliveries WHERE pipeline_id=? AND state='queued' AND frozen=0 AND window_end_ms=? AND event_count<100 AND bytes<1048576 ORDER BY created_at_ms DESC,id DESC LIMIT 1`, pipelineID, end).Scan(&chunk.id, &chunk.size, &chunk.count)
			if err == nil {
				chunk.existing = true
			} else if !errors.Is(err, sql.ErrNoRows) {
				return 0, 0, err
			}
		}
		encoded, err := encodeWebhookJSON(event)
		if err != nil {
			return 0, 0, err
		}
		delta := int64(len(encoded) + 1)
		if chunk.count == 0 {
			delta = int64(len(prefix(chunk.id, chunk.start, chunk.end)) + len(encoded) + 2)
		}
		if chunk.count >= webhookChunkEvents || (chunk.count > 0 && chunk.size+delta > webhookChunkBytes) {
			if err := flush(); err != nil {
				return 0, 0, err
			}
			chunk = &webhookOpenChunk{id: newID(), start: start, end: end}
			delta = int64(len(prefix(chunk.id, chunk.start, chunk.end)) + len(encoded) + 2)
		}
		if webhookCapacityExceeded(globalCount+added, globalBytes+bytesAdded, projectCount+added, projectBytes+bytesAdded, delta, cfg) {
			return 0, 0, errWebhookCapacity
		}
		chunk.count++
		chunk.size += delta
		chunk.events = append(chunk.events, event)
		chunk.encoded = append(chunk.encoded, encoded)
		added++
		bytesAdded += delta
	}
	if err := flush(); err != nil {
		return 0, 0, err
	}
	return added, bytesAdded, nil
}
func webhookCapacityExceeded(globalCount, globalBytes, projectCount, projectBytes, delta int64, cfg PipelineConfig) bool {
	return globalCount >= int64(cfg.WebhookMaxQueuedEvents) || projectCount >= int64(cfg.WebhookProjectMaxQueuedEvents) || delta > cfg.WebhookMaxQueuedBytes-globalBytes || delta > cfg.WebhookProjectMaxQueuedBytes-projectBytes
}

func (s *Service) webhookPipelineRunPage(ctx context.Context, project, pipeline string, limit, offset int) ([]PipelineRun, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,pipeline_id,state,attempts,created_at_ms,completed_at_ms,error_code,last_status,window_start_ms,window_end_ms,event_count,bytes FROM webhook_deliveries WHERE project_id=? AND pipeline_id=? ORDER BY created_at_ms DESC,id DESC LIMIT ? OFFSET ?`, project, pipeline, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []PipelineRun{}
	for rows.Next() {
		var run PipelineRun
		if err = rows.Scan(&run.ID, &run.PipelineID, &run.State, &run.Attempts, &run.CreatedAtMS, &run.CompletedAtMS, &run.Error, &run.LastStatus, &run.WindowStartMS, &run.WindowEndMS, &run.EventCount, &run.Bytes); err != nil {
			return nil, err
		}
		result = append(result, run)
	}
	return result, rows.Err()
}
func (s *Service) getWebhookPipelineRun(ctx context.Context, project, pipeline, id string) (PipelineRun, error) {
	var run PipelineRun
	err := s.db.QueryRowContext(ctx, `SELECT id,pipeline_id,state,attempts,created_at_ms,completed_at_ms,error_code,last_status,window_start_ms,window_end_ms,event_count,bytes FROM webhook_deliveries WHERE project_id=? AND pipeline_id=? AND id=?`, project, pipeline, id).Scan(&run.ID, &run.PipelineID, &run.State, &run.Attempts, &run.CreatedAtMS, &run.CompletedAtMS, &run.Error, &run.LastStatus, &run.WindowStartMS, &run.WindowEndMS, &run.EventCount, &run.Bytes)
	return run, err
}
func (s *Service) retryWebhookPipelineRun(ctx context.Context, project, pipeline, id string) (bool, error) {
	return s.retryWebhookDelivery(ctx, project, pipeline, id)
}

// A changed body can keep its delivery ID only before any attempt. Once an
// attempt may have reached the receiver, surviving members get a fresh ID.
func (s *Service) cancelWebhookEvent(ctx context.Context, tx *sql.Tx, project, uuid string) error {
	rows, err := tx.QueryContext(ctx, `SELECT d.id,d.pipeline_id,d.payload,d.state,d.attempts,d.frozen,d.window_start_ms,d.window_end_ms FROM webhook_deliveries d JOIN webhook_members m ON m.delivery_id=d.id WHERE m.project_id=? AND m.event_uuid=? AND d.project_id=? AND d.payload<>''`, project, uuid, project)
	if err != nil {
		return err
	}
	type chunk struct {
		id, pipeline, payload, state string
		attempts                     int
		frozen                       bool
		start, end                   int64
	}
	chunks := []chunk{}
	for rows.Next() {
		var c chunk
		if err = rows.Scan(&c.id, &c.pipeline, &c.payload, &c.state, &c.attempts, &c.frozen, &c.start, &c.end); err != nil {
			rows.Close()
			return err
		}
		chunks = append(chunks, c)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, c := range chunks {
		var body webhookPayload
		if json.Unmarshal([]byte(c.payload), &body) != nil {
			return errors.New("invalid stored webhook chunk")
		}
		surviving := []Event{}
		for _, event := range body.Events {
			if event.UUID != uuid {
				surviving = append(surviving, event)
			}
		}
		if body.Event != nil && body.Event.UUID != uuid {
			surviving = append(surviving, *body.Event)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_members WHERE delivery_id=? AND event_uuid=?`, c.id, uuid); err != nil {
			return err
		}
		if len(surviving) > 0 && c.state == "queued" && !c.frozen {
			body.Event = nil
			body.Events = surviving
			encoded, err := encodeWebhookPayload(body)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET payload=?,bytes=?,event_count=? WHERE id=?`, string(encoded), len(encoded), len(surviving), c.id); err != nil {
				return err
			}
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='canceled',payload='',bytes=0,error_code='event_deleted',completed_at_ms=? WHERE id=?`, s.clock().UnixMilli(), c.id); err != nil {
			return err
		}
		if len(surviving) == 0 {
			continue
		}
		nextID := newID()
		body.DeliveryID = nextID
		body.Event = nil
		body.Events = surviving
		encoded, err := encodeWebhookPayload(body)
		if err != nil {
			return err
		}
		// event_uuid is a private representative, while webhook_members preserves
		// each true identity. A fresh representative avoids legacy unique-key overlap.
		if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,window_start_ms,window_end_ms,state,next_attempt_ms,created_at_ms) VALUES(?,?,?,?,?,?,?,?,?,'queued',?,?)`, nextID, c.pipeline, project, nextID, string(encoded), len(encoded), len(surviving), c.start, c.end, max(s.clock().UnixMilli(), c.end), s.clock().UnixMilli()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_members SET delivery_id=? WHERE delivery_id=?`, nextID, c.id); err != nil {
			return err
		}
	}
	return nil
}
