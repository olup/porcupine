package porcupine

import (
	"context"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
)

type QuarantinedEvent struct {
	EventID       string `json:"event_id" doc:"Durable event ID as a decimal string; used for retry and keyset pagination"`
	UUID          string `json:"uuid" doc:"Original SDK event identifier"`
	ErrorCode     int    `json:"error_code" doc:"Confirmed row-specific ClickHouse rejection code; raw error details are never returned"`
	QuarantinedAt string `json:"quarantined_at"`
	ReceivedAt    string `json:"received_at"`
}

type QuarantinedEventsInput struct {
	ID           string `path:"id"`
	Limit        int    `query:"limit" default:"100" minimum:"1" maximum:"1000"`
	AfterEventID string `query:"after_event_id" default:"0" maxLength:"19" pattern:"^(0|[1-9][0-9]{0,18})$" doc:"Return quarantined originals with event_id greater than this decimal cursor"`
}

type QuarantinedEvents struct {
	Events           []QuarantinedEvent `json:"events"`
	Limit            int                `json:"limit"`
	AfterEventID     string             `json:"after_event_id"`
	NextAfterEventID *string            `json:"next_after_event_id,omitempty" doc:"Use this cursor to request the next page"`
}

type QuarantinedEventsOutput struct{ Body QuarantinedEvents }

type QuarantinedEventRetryInput struct {
	ID      string `path:"id"`
	EventID string `path:"event_id" maxLength:"19" pattern:"^[1-9][0-9]{0,18}$"`
}

type QuarantinedEventRetry struct {
	EventID  string `json:"event_id"`
	Requeued bool   `json:"requeued" doc:"True when this request restored delivery eligibility; false if the retained original was already eligible. Neither value confirms ClickHouse delivery."`
}

type QuarantinedEventRetryOutput struct{ Body QuarantinedEventRetry }

func (s *Service) registerQuarantine(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	base := "/api/v1/projects/{id}/quarantined-events"
	huma.Register(api, op("list-quarantined-events", "GET", base, "List retained quarantined delivery metadata using an event ID cursor", operationAccess{Permission: "events:read", ProjectParameter: "id"}, 200), s.listQuarantinedEvents)
	huma.Register(api, op("retry-quarantined-event", "POST", base+"/{event_id}/retry", "Restore an accepted original's delivery eligibility without claiming downstream acknowledgement", operationAccess{Permission: "events:edit", ProjectParameter: "id"}, 202), s.retryQuarantinedEvent)
}

func (s *Service) listQuarantinedEvents(ctx context.Context, input *QuarantinedEventsInput) (*QuarantinedEventsOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if input.Limit < 1 || input.Limit > 1000 {
		return nil, apiError(400, "limit must be between 1 and 1000")
	}
	cursor := input.AfterEventID
	if cursor == "" {
		cursor = "0"
	}
	after, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil || after < 0 {
		return nil, apiError(400, "invalid event ID cursor")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT e.id,e.uuid,q.error_code,q.quarantined_at,e.received_at FROM events e JOIN delivery_quarantine q ON q.event_id=e.id WHERE e.project_id=? AND e.id>? ORDER BY e.id LIMIT ?`, input.ID, after, input.Limit+1)
	if err != nil {
		return nil, apiError(503, "quarantined event metadata unavailable")
	}
	defer rows.Close()
	result := QuarantinedEvents{Events: []QuarantinedEvent{}, Limit: input.Limit, AfterEventID: strconv.FormatInt(after, 10)}
	for rows.Next() {
		var item QuarantinedEvent
		var id int64
		if rows.Scan(&id, &item.UUID, &item.ErrorCode, &item.QuarantinedAt, &item.ReceivedAt) != nil {
			return nil, apiError(503, "quarantined event metadata unavailable")
		}
		item.EventID = strconv.FormatInt(id, 10)
		result.Events = append(result.Events, item)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "quarantined event metadata unavailable")
	}
	if len(result.Events) > input.Limit {
		result.Events = result.Events[:input.Limit]
		last := result.Events[len(result.Events)-1].EventID
		result.NextAfterEventID = &last
	}
	return &QuarantinedEventsOutput{Body: result}, nil
}

func (s *Service) retryQuarantinedEvent(ctx context.Context, input *QuarantinedEventRetryInput) (*QuarantinedEventRetryOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	id, err := strconv.ParseInt(input.EventID, 10, 64)
	if err != nil || id < 1 {
		return nil, apiError(400, "invalid event ID")
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE id=? AND project_id=?)`, id, input.ID).Scan(&exists); err != nil {
		return nil, apiError(503, "quarantined event storage unavailable")
	}
	if exists == 0 {
		return nil, apiError(404, "retained event not found")
	}
	changed, err := s.retryQuarantinedDelivery(ctx, input.ID, id)
	if err != nil {
		return nil, apiError(503, "cannot retry quarantined event")
	}
	return &QuarantinedEventRetryOutput{Body: QuarantinedEventRetry{EventID: strconv.FormatInt(id, 10), Requeued: changed}}, nil
}
