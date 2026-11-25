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

func (r *Runtime) SQL(ctx context.Context, reader Reader, project, sql string, params map[string]string, timeRange *lake.QueryTimeRange) (result lake.AnalyticsResult, bounds ResponseBounds, err error) {
	return r.sql(ctx, reader, project, sql, params, timeRange, 0, "")
}

// SQLWithInternalParameter uses the same read budget with one larger server-supplied value.
func (r *Runtime) SQLWithInternalParameter(ctx context.Context, reader Reader, project, sql string, params map[string]string, timeRange *lake.QueryTimeRange, internalParameter string) (lake.AnalyticsResult, ResponseBounds, error) {
	return r.sql(ctx, reader, project, sql, params, timeRange, 0, internalParameter)
}

func (r *Runtime) EventDeletionSelection(ctx context.Context, reader Reader, project, predicate string, params map[string]string, timeRange *lake.QueryTimeRange, maxMatches int) (lake.AnalyticsResult, error) {
	result, _, err := r.sql(ctx, reader, project, predicate, params, timeRange, maxMatches, "")
	return result, err
}

func (r *Runtime) sql(ctx context.Context, reader Reader, project, sql string, params map[string]string, timeRange *lake.QueryTimeRange, maxMatches int, internalParameter string) (result lake.AnalyticsResult, bounds ResponseBounds, err error) {
	bounds, err = r.run(ctx, project, opSQL, func(ctx context.Context, limits InteractiveQueryConfig) error {
		execution := limits.executionLimits()
		execution.TimeRange = timeRange
		execution.EventSelectionMax = maxMatches
		execution.InternalParameter = internalParameter
		result, err = reader.Query(ctx, project, sql, params, execution)
		return err
	})
	return
}

func (r *Runtime) Catalog(ctx context.Context, project string, read func(context.Context) error) (ResponseBounds, error) {
	return r.run(ctx, project, opCatalog, func(ctx context.Context, _ InteractiveQueryConfig) error {
		return read(ctx)
	})
}

// RefreshCatalog shares analytical capacity and shutdown tracking without
// recording background maintenance as an interactive customer query.
func (r *Runtime) RefreshCatalog(ctx context.Context, project string, read func(context.Context) error) (err error) {
	native, _, reserved, err := r.admit(ctx, project)
	if err != nil {
		return err
	}
	defer func() { err = resultError(native, reserved.finish(), err) }()
	return read(native)
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
