package porcupine

import (
	"context"
	"encoding/json"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

type EventCatalogOutput struct{ Body lakeapi.EventCatalog }

func (s *Service) eventCatalog(ctx context.Context, input *lakeapi.EventCatalogInput) (*EventCatalogOutput, error) {
	options, err := lakeapi.NormalizeCatalogInput(*input)
	if err != nil {
		return nil, err
	}
	if err = s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.lake == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	result, bounds, err := s.queries.Catalog(ctx, s.interactiveReader(), options)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, bounds.Deadline)
	defer cancel()
	if err := s.describeCatalogPage(ctx, options.ID, options.Event, &result); err != nil {
		return nil, apiError(503, "catalog descriptions unavailable")
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > bounds.MaxBytes {
		return nil, apiError(429, "event catalog result byte limit reached")
	}
	return &EventCatalogOutput{Body: result}, nil
}
