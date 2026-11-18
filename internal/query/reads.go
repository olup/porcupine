package query

import (
	"context"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/lake"
)

type Reader interface {
	Query(context.Context, string, string, map[string]string, lake.QueryExecutionLimits) (lake.AnalyticsResult, error)
	Catalog(context.Context, lake.EventCatalogInput) (lake.EventCatalog, error)
	Profile(context.Context, string, lake.EventProfileOptions, time.Time) (lake.EventProfileRead, error)
}

type HostedReader interface {
	Reader
	ListEvents(context.Context, string, int, int) ([]event.Event, error)
}

func (r *Runtime) SQL(ctx context.Context, reader Reader, project, sql string, params map[string]string) (result lake.AnalyticsResult, bounds ResponseBounds, err error) {
	bounds, err = r.run(ctx, project, opSQL, func(ctx context.Context, limits InteractiveQueryConfig) error {
		result, err = reader.Query(ctx, project, sql, params, limits.executionLimits())
		return err
	})
	return
}

func (r *Runtime) Catalog(ctx context.Context, reader Reader, input lake.EventCatalogInput) (result lake.EventCatalog, bounds ResponseBounds, err error) {
	bounds, err = r.run(ctx, input.ID, opCatalog, func(ctx context.Context, _ InteractiveQueryConfig) error {
		result, err = reader.Catalog(ctx, input)
		return err
	})
	return
}

func (r *Runtime) Profile(ctx context.Context, reader Reader, project string, options lake.EventProfileOptions) (lake.EventProfile, ResponseBounds, error) {
	var read lake.EventProfileRead
	var asOf time.Time
	bounds, err := r.run(ctx, project, opProfile, func(ctx context.Context, _ InteractiveQueryConfig) error {
		asOf = r.clock().UTC().Truncate(time.Millisecond)
		var err error
		read, err = reader.Profile(ctx, project, options, asOf)
		return err
	})
	if err != nil {
		return lake.EventProfile{}, bounds, err
	}
	result, err := lake.SummarizeEventProfile(read, options, asOf)
	return result, bounds, err
}

func (r *Runtime) ListEvents(ctx context.Context, reader HostedReader, project string, limit int) (events []event.Event, err error) {
	_, err = r.run(ctx, project, opCatalog, func(ctx context.Context, limits InteractiveQueryConfig) error {
		events, err = reader.ListEvents(ctx, project, min(limit, limits.MaxRows), limits.MaxResultBytes)
		return err
	})
	return
}
