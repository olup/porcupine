package delivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
)

func webhookMatches(spec WebhookRule, event string) bool {
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

// WebhookEnqueuer joins the producer's transaction without owning the journal
// connection, the destination worker or the rest of the application.
type WebhookEnqueuer struct {
	limits *WebhookLimits
	clock  func() time.Time
}

func (p *WebhookEnqueuer) Enqueue(ctx context.Context, tx *sql.Tx, events []eventapi.Event) error {
	if len(events) == 0 {
		return nil
	}
	projectID := events[0].ProjectID
	rows, err := tx.QueryContext(ctx, `SELECT id,config FROM pipelines WHERE project_id=$1 AND kind='webhook' AND state='active' ORDER BY id`, projectID)
	if err != nil {
		return err
	}
	type destination struct {
		id   string
		spec WebhookRule
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
	err = tx.QueryRowContext(ctx, `SELECT count,bytes FROM webhook_queue_usage WHERE scope=$1`, "project:"+projectID).Scan(&projectCount, &projectBytes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	cfg := *p.limits
	for _, d := range destinations {
		matching := make([]eventapi.Event, 0, len(events))
		for _, event := range events {
			if event.ProjectID != projectID {
				return errors.New("mixed webhook projects")
			}
			if webhookMatches(d.spec, event.Event) {
				matching = append(matching, event)
			}
		}
		count, delta, err := enqueueWebhookDestinationGroup(ctx, tx, matching, d.id, d.spec, p.clock().UnixMilli(), globalCount, globalBytes, projectCount, projectBytes, cfg)
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
