package porcupine

import (
	"context"
	"encoding/json"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

type EventProfileOutput struct{ Body lakeapi.EventProfile }

func (s *Service) eventProfile(ctx context.Context, input *lakeapi.EventProfileInput) (*EventProfileOutput, error) {
	options, err := lakeapi.NormalizeEventProfileInput(*input)
	if err != nil {
		return nil, err
	}
	if err = s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.lake == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	ctx, limits, release, err := s.beginInteractiveQuery(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	defer release()
	asOf := s.clock().UTC().Truncate(time.Millisecond)
	read, err := s.lake.Profile(ctx, input.ID, options, asOf)
	if err != nil {
		return nil, err
	}
	result, err := lakeapi.SummarizeEventProfile(read, options, asOf)
	if err != nil {
		return nil, err
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > limits.MaxResultBytes {
		return nil, apiError(429, "event profile result byte limit reached")
	}
	return &EventProfileOutput{Body: result}, nil
}
