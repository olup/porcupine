package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/catalog"
)

type CatalogRefreshStatus struct {
	Status    string     `json:"status" enum:"queued,running,ready,failed" doc:"A failed refresh can be retried or requested again."`
	UpdatedAt *time.Time `json:"updated_at,omitempty" doc:"Last successfully checked or refreshed time for the stored catalog."`
}

type catalogRefreshOutput struct{ Body CatalogRefreshStatus }

func (s *Service) startCatalogRefresh() {
	if s.lake == nil {
		return
	}
	var lock func(context.Context, *sql.Tx, string) error
	if s.hosted != nil {
		lock = lockHostedCatalogProject
	}
	s.eventCatalogRefresh = catalog.Start(s.control, s.lake, s.queries, s.clock, lock)
}

func lockHostedCatalogProject(ctx context.Context, tx *sql.Tx, project string) error {
	_, err := lockActiveHostedProject(ctx, tx, project)
	return err
}

func (s *Service) refreshEventCatalog(ctx context.Context, input *ProjectIDInput) (*catalogRefreshOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	refresh := s.eventCatalogRefresh
	if refresh == nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	status, err := refresh.Request(ctx, input.ID)
	if errors.Is(err, acceptance.ErrProjectFenced) {
		return nil, apierr.New(404, "project not found")
	}
	if err != nil || status == nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	return &catalogRefreshOutput{Body: CatalogRefreshStatus(*status)}, nil
}

func (s *Service) eventCatalogRefreshStatus(ctx context.Context, input *ProjectIDInput) (*catalogRefreshOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	refresh := s.eventCatalogRefresh
	if refresh == nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	status, err := refresh.Status(ctx, input.ID)
	if err != nil {
		return nil, apierr.New(503, "event catalog refresh unavailable")
	}
	return &catalogRefreshOutput{Body: CatalogRefreshStatus(status)}, nil
}
