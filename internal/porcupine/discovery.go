package porcupine

import (
	"context"
	"encoding/json"

	"github.com/olup/porcupine/internal/apierr"
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
	reader := s.interactiveReader()
	if reader == nil {
		return nil, apierr.New(503, "analytics storage is not configured")
	}
	result, bounds, err := s.queries.Catalog(ctx, reader, options)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, bounds.Deadline)
	defer cancel()
	if err := s.describeCatalogPage(ctx, options.ID, options.Event, &result); err != nil {
		return nil, apierr.New(503, "catalog descriptions unavailable")
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apierr.New(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > bounds.MaxBytes {
		return nil, apierr.New(429, "event catalog result byte limit reached")
	}
	return &EventCatalogOutput{Body: result}, nil
}
