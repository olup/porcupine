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
	native, limits, finish, err := s.beginUsageQuery(ctx, input.ID, "profile")
	if err != nil {
		return nil, err
	}
	defer finish()
	deadline, _ := native.Deadline()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	asOf := s.clock().UTC().Truncate(time.Millisecond)
	read, err := s.lake.Profile(native, input.ID, options, asOf)
	err = interactiveQueryResultError(native, finish(), err)
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
