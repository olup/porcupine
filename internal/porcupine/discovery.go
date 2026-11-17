package porcupine

import (
	"context"

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
	return s.lakeEventCatalog(ctx, options)
}
