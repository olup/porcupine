package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/apierr"
)

const maxEventDeletionMatches = 1000
const eventDeletionSampleSize = 5

type EventsDeleteInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body EventsDeleteBody
}

type EventsDeleteBody struct {
	Where          string                  `json:"where" minLength:"1" maxLength:"16384" doc:"One DuckDB WHERE predicate over the project's projected events. Use only read-only expressions allowed by query-events."`
	Parameters     map[string]string       `json:"parameters,omitempty" doc:"Bound DuckDB scalar parameters such as $event. Same limits as query-events."`
	TimeRange      AnalyticsQueryTimeRange `json:"time_range,omitzero" required:"false" doc:"Inclusive from and exclusive to in RFC3339. Bounds every events reference by occurrence time."`
	DryRun         bool                    `json:"dry_run,omitempty" doc:"Preview the match count and up to five UUIDs without queuing a deletion or reserving the key."`
	ExpectedCount  *int                    `json:"expected_count,omitempty" minimum:"0" doc:"Required when dry_run is false, including for zero matches. A different match count returns 409."`
	IdempotencyKey string                  `json:"idempotency_key" minLength:"1" maxLength:"200" doc:"Project-scoped key for retrying a confirmed request. A preview can use the same key as its confirmation."`
}

type EventDeletionResponse struct {
	ProjectID    string   `json:"project_id"`
	MutationID   string   `json:"mutation_id,omitempty" doc:"Durable deletion ID. Present after confirmation."`
	State        string   `json:"state" enum:"preview,pending,applied,failed"`
	MatchedCount int      `json:"matched_count"`
	AppliedCount int      `json:"applied_count"`
	FailedCount  int      `json:"failed_count"`
	SampleUUIDs  []string `json:"sample_uuids,omitempty" doc:"Up to five matching UUIDs from a preview."`
	CreatedAt    string   `json:"created_at,omitempty"`
	AppliedAt    string   `json:"applied_at,omitempty"`
	ErrorCode    string   `json:"error_code,omitempty" doc:"Stable code for a terminal deletion failure."`
}

type EventsDeleteOutput struct {
	Status int
	Body   EventDeletionResponse
}

type EventDeletionStatusOutput struct{ Body EventDeletionResponse }

type eventDeletionTarget struct {
	UUID       string `json:"uuid"`
	Revision   uint64 `json:"revision,string"`
	ID         int64  `json:"id,string"`
	ReceivedAt string `json:"received_at"`
	Imported   bool   `json:"-"`
}

func (s *Service) deleteEvents(ctx context.Context, input *EventsDeleteInput) (*EventsDeleteOutput, error) {
	body := input.Body
	if strings.TrimSpace(body.IdempotencyKey) == "" || len(body.IdempotencyKey) > 200 {
		return nil, apierr.New(400, "idempotency_key must contain 1 to 200 bytes and cannot be blank")
	}
	if (!body.DryRun && body.ExpectedCount == nil) || (body.ExpectedCount != nil && *body.ExpectedCount < 0) {
		return nil, apierr.New(400, "expected_count must be a nonnegative integer for a confirmed deletion")
	}
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if err := validateQueryParameters(body.Parameters); err != nil {
		return nil, err
	}
	timeRange, err := body.TimeRange.queryRange()
	if err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(struct {
		Actor     string
		Operation string
		Body      EventsDeleteBody
	}{principal(ctx).ID, "delete-events", body})
	if err != nil {
		return nil, apierr.New(400, "invalid deletion request")
	}
	selectEvents := func(ctx context.Context) ([]eventDeletionTarget, error) {
		reader := s.interactiveReader()
		if reader == nil {
			return nil, apierr.New(503, "analytics storage is not configured")
		}
		result, err := s.queries.EventDeletionSelection(ctx, reader, input.ID, body.Where, body.Parameters, timeRange, maxEventDeletionMatches)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(result.Data)
		if err != nil {
			return nil, apierr.New(503, "invalid event deletion selection")
		}
		var rows []struct {
			eventDeletionTarget
			IsImported uint8 `json:"is_imported"`
		}
		if err := json.Unmarshal(raw, &rows); err != nil {
			return nil, apierr.New(503, "invalid event deletion selection")
		}
		targets := make([]eventDeletionTarget, 0, len(rows))
		seen := make(map[string]bool, len(rows))
		for _, row := range rows {
			target := row.eventDeletionTarget
			if target.UUID == "" || target.ID <= 0 || target.Revision > math.MaxInt64 || row.IsImported > 1 || seen[target.UUID] {
				return nil, apierr.New(503, "invalid event deletion selection")
			}
			if _, err := time.Parse(time.RFC3339Nano, target.ReceivedAt); err != nil {
				return nil, apierr.New(503, "invalid event deletion selection")
			}
			seen[target.UUID] = true
			target.Imported = row.IsImported == 1
			targets = append(targets, target)
		}
		return targets, nil
	}
	keyHash, requestHash := hash(body.IdempotencyKey), hash(string(canonical))
	var result EventDeletionResponse
	if s.hostedMutations != nil {
		result, err = s.hostedMutations.deleteEventsHosted(ctx, input, keyHash, requestHash, selectEvents, s.authorizeHostedMutation)
	} else {
		result, err = s.deleteEventsStandalone(ctx, input, keyHash, requestHash, selectEvents)
	}
	if err != nil {
		return nil, err
	}
	status := http.StatusAccepted
	if body.DryRun {
		status = http.StatusOK
	}
	return &EventsDeleteOutput{Status: status, Body: result}, nil
}

func checkEventDeletionSelection(body EventsDeleteBody, project string, selected []eventDeletionTarget) (EventDeletionResponse, error) {
	count := len(selected)
	if count > maxEventDeletionMatches {
		return EventDeletionResponse{}, apierr.Detailed(422, "event_deletion_match_limit", "Too many events match the deletion query", "Narrow where or time_range and retry.", "where")
	}
	result := EventDeletionResponse{ProjectID: project, MatchedCount: count}
	if body.DryRun {
		result.State = "preview"
		for _, target := range selected[:min(count, eventDeletionSampleSize)] {
			result.SampleUUIDs = append(result.SampleUUIDs, target.UUID)
		}
		return result, nil
	}
	if count != *body.ExpectedCount {
		return EventDeletionResponse{}, apierr.Detailed(409, "event_deletion_count_mismatch", fmt.Sprintf("expected_count does not match the %d events selected", count), "Preview again before confirming the deletion.", "expected_count")
	}
	return result, nil
}

func (s *Service) getEventDeletion(ctx context.Context, input *EventMutationInput) (*EventDeletionStatusOutput, error) {
	var result EventDeletionResponse
	var err error
	if s.hostedMutations != nil {
		result, err = s.hostedMutations.hostedEventDeletionStatus(ctx, input.ID, input.MutationID)
	} else {
		result, err = s.standaloneEventDeletionStatus(ctx, input.ID, input.MutationID)
	}
	if err != nil {
		return nil, err
	}
	return &EventDeletionStatusOutput{Body: result}, nil
}
