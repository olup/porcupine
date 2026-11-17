package porcupine

import (
	"context"
)

type EventCatalogOutput struct{ Body EventCatalog }

func (s *Service) eventCatalog(ctx context.Context, input *EventCatalogInput) (*EventCatalogOutput, error) {
	options, err := normalizeCatalogInput(*input)
	if err != nil {
		return nil, err
	}
	if err = s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.lake == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	return s.lakeEventCatalog(ctx, options)
}
