package porcupine

import (
	"context"
	"encoding/json"

	"github.com/olup/porcupine/internal/apierr"
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
		return nil, apierr.New(503, "analytics storage is not configured")
	}
	result, bounds, err := s.queries.Profile(ctx, s.interactiveReader(), input.ID, options)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, bounds.Deadline)
	defer cancel()
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apierr.New(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > bounds.MaxBytes {
		return nil, apierr.New(429, "event profile result byte limit reached")
	}
	return &EventProfileOutput{Body: result}, nil
}
