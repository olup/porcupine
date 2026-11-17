package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
)

const WebhookChunkEvents = 100
const WebhookChunkBytes = 1 << 20

type WebhookPayload struct {
	DeliveryID    string           `json:"delivery_id"`
	PipelineID    string           `json:"pipeline_id"`
	ProjectID     string           `json:"project_id"`
	Event         *eventapi.Event  `json:"event,omitempty"`
	WindowStartMS *int64           `json:"window_start_ms,omitempty"`
	WindowEndMS   *int64           `json:"window_end_ms,omitempty"`
	Events        []eventapi.Event `json:"events,omitempty"`
}

type webhookOpenChunk struct {
	id         string
	start, end int64
	count      int
	size       int64
	existing   bool
	events     []eventapi.Event
	encoded    [][]byte
}

func enqueueWebhookDestinationGroup(ctx context.Context, tx *sql.Tx, events []eventapi.Event, pipelineID string, spec WebhookRule, now int64, globalCount, globalBytes, projectCount, projectBytes int64, cfg WebhookLimits) (int64, int64, error) {
	added, bytesAdded := int64(0), int64(0)
	if len(events) == 0 {
		return 0, 0, nil
	}
	project := events[0].ProjectID
	var chunk *webhookOpenChunk
	prefix := func(id string, start, end int64) string {
		base, _ := WebhookBatchPrefix(id, pipelineID, project, start, end)
		return string(base)
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
			result, err := tx.ExecContext(ctx, `UPDATE webhook_deliveries SET payload=substr(payload,1,length(payload)-2)||','||$1||']}',bytes=$2,event_count=$3 WHERE id=$4 AND state='queued' AND frozen=0 AND substr(payload,-2)=']}'`, body.String(), chunk.size, chunk.count, chunk.id)
			if err != nil {
				return err
			}
			n, err := result.RowsAffected()
			if err != nil || n != 1 {
				return errors.New("invalid stored webhook chunk")
			}
		} else {
			raw := prefix(chunk.id, chunk.start, chunk.end) + body.String() + "]}"
			if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,window_start_ms,window_end_ms,state,next_attempt_ms,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'queued',$10,$11)`, chunk.id, pipelineID, project, chunk.events[0].UUID, raw, len(raw), chunk.count, chunk.start, chunk.end, max(now, chunk.end), now); err != nil {
				return err
			}
		}
		for _, event := range chunk.events {
			if _, err := tx.ExecContext(ctx, `INSERT INTO webhook_members(project_id,pipeline_id,event_uuid,delivery_id) VALUES($1,$2,$3,$4)`, project, pipelineID, event.UUID, chunk.id); err != nil {
				return err
			}
		}
		return nil
	}
	for _, event := range events {
		if spec.DeliveryMode != "batch" {
			id := eventapi.NewID()
			raw, err := EncodeJSON(WebhookPayload{DeliveryID: id, PipelineID: pipelineID, ProjectID: project, Event: &event})
			if err != nil {
				return 0, 0, err
			}
			delta := int64(len(raw))
			if webhookCapacityExceeded(globalCount+added, globalBytes+bytesAdded, projectCount+added, projectBytes+bytesAdded, delta, cfg) {
				return 0, 0, ErrWebhookCapacity
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,state,next_attempt_ms,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,1,'queued',$7,$8)`, id, pipelineID, project, event.UUID, string(raw), len(raw), now, now); err != nil {
				return 0, 0, err
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_members(project_id,pipeline_id,event_uuid,delivery_id) VALUES($1,$2,$3,$4)`, project, pipelineID, event.UUID, id); err != nil {
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
			chunk = &webhookOpenChunk{id: eventapi.NewID(), start: start, end: end}
			err = tx.QueryRowContext(ctx, `SELECT id,bytes,event_count FROM webhook_deliveries WHERE pipeline_id=$1 AND state='queued' AND frozen=0 AND window_end_ms=$2 AND event_count<100 AND bytes<1048576 ORDER BY created_at_ms DESC,id DESC LIMIT 1`, pipelineID, end).Scan(&chunk.id, &chunk.size, &chunk.count)
			if err == nil {
				chunk.existing = true
			} else if !errors.Is(err, sql.ErrNoRows) {
				return 0, 0, err
			}
		}
		encoded, err := EncodeJSON(event)
		if err != nil {
			return 0, 0, err
		}
		delta := int64(len(encoded) + 1)
		if chunk.count == 0 {
			delta = int64(len(prefix(chunk.id, chunk.start, chunk.end)) + len(encoded) + 2)
		}
		if chunk.count >= WebhookChunkEvents || (chunk.count > 0 && chunk.size+delta > WebhookChunkBytes) {
			if err := flush(); err != nil {
				return 0, 0, err
			}
			chunk = &webhookOpenChunk{id: eventapi.NewID(), start: start, end: end}
			delta = int64(len(prefix(chunk.id, chunk.start, chunk.end)) + len(encoded) + 2)
		}
		if webhookCapacityExceeded(globalCount+added, globalBytes+bytesAdded, projectCount+added, projectBytes+bytesAdded, delta, cfg) {
			return 0, 0, ErrWebhookCapacity
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
func webhookCapacityExceeded(globalCount, globalBytes, projectCount, projectBytes, delta int64, cfg WebhookLimits) bool {
	return globalCount >= int64(cfg.MaxQueuedEvents) || projectCount >= int64(cfg.ProjectMaxQueuedEvents) || delta > cfg.MaxQueuedBytes-globalBytes || delta > cfg.ProjectMaxQueuedBytes-projectBytes
}

// A changed body can keep its delivery ID only before any attempt. Once an
// attempt may have reached the receiver, surviving members get a fresh ID.
func CancelWebhookEvent(ctx context.Context, tx *sql.Tx, project, uuid string, clock func() time.Time) error {
	rows, err := tx.QueryContext(ctx, `SELECT d.id,d.pipeline_id,d.payload,d.state,d.attempts,d.frozen,d.window_start_ms,d.window_end_ms FROM webhook_deliveries d JOIN webhook_members m ON m.delivery_id=d.id WHERE m.project_id=$1 AND m.event_uuid=$2 AND d.project_id=$3 AND d.payload<>''`, project, uuid, project)
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
		var body WebhookPayload
		if json.Unmarshal([]byte(c.payload), &body) != nil {
			return errors.New("invalid stored webhook chunk")
		}
		surviving := []eventapi.Event{}
		for _, event := range body.Events {
			if event.UUID != uuid {
				surviving = append(surviving, event)
			}
		}
		if body.Event != nil && body.Event.UUID != uuid {
			surviving = append(surviving, *body.Event)
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_members WHERE delivery_id=$1 AND event_uuid=$2`, c.id, uuid); err != nil {
			return err
		}
		if len(surviving) > 0 && c.state == "queued" && !c.frozen {
			body.Event = nil
			body.Events = surviving
			encoded, err := EncodeJSON(body)
			if err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET payload=$1,bytes=$2,event_count=$3 WHERE id=$4`, string(encoded), len(encoded), len(surviving), c.id); err != nil {
				return err
			}
			continue
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='canceled',payload='',bytes=0,error_code='event_deleted',completed_at_ms=$1 WHERE id=$2`, clock().UnixMilli(), c.id); err != nil {
			return err
		}
		if len(surviving) == 0 {
			continue
		}
		nextID := eventapi.NewID()
		body.DeliveryID = nextID
		body.Event = nil
		body.Events = surviving
		encoded, err := EncodeJSON(body)
		if err != nil {
			return err
		}
		// event_uuid is a private representative, while webhook_members preserves
		// each true identity. A fresh representative avoids legacy unique-key overlap.
		if _, err = tx.ExecContext(ctx, `INSERT INTO webhook_deliveries(id,pipeline_id,project_id,event_uuid,payload,bytes,event_count,window_start_ms,window_end_ms,state,next_attempt_ms,created_at_ms) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'queued',$10,$11)`, nextID, c.pipeline, project, nextID, string(encoded), len(encoded), len(surviving), c.start, c.end, max(clock().UnixMilli(), c.end), clock().UnixMilli()); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_members SET delivery_id=$1 WHERE delivery_id=$2`, nextID, c.id); err != nil {
			return err
		}
	}
	return nil
}
