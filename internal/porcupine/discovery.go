package porcupine

import (
	"context"
	"encoding/json"

	"github.com/olup/porcupine/internal/apierr"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type EventCatalogOutput struct {
	Body struct {
		lakeapi.EventCatalog
		Refresh *CatalogRefreshStatus `json:"refresh,omitempty" doc:"Materialized event-name catalog status. When present, counts and recency may lag SQL. Omitted for property samples."`
	}
}

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
	out := &EventCatalogOutput{}
	bounds, err := s.queries.Catalog(ctx, options.ID, func(ctx context.Context) error {
		if refresh := s.eventCatalogRefresh; refresh != nil && options.Event == "" {
			result, status, usable := refresh.page(ctx, options)
			out.Body.Refresh = status
			if usable {
				out.Body.EventCatalog = result
				return nil
			}
		}
		result, err := reader.Catalog(ctx, options)
		out.Body.EventCatalog = result
		return err
	})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, bounds.Deadline)
	defer cancel()
	if err := s.describeCatalogPage(ctx, options.ID, options.Event, &out.Body.EventCatalog); err != nil {
		return nil, apierr.New(503, "catalog descriptions unavailable")
	}
	out.Body.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apierr.New(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(out.Body)
	if err != nil || len(encoded) > bounds.MaxBytes {
		return nil, apierr.New(429, "event catalog result byte limit reached")
	}
	return out, nil
}
