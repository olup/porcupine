package lake

import (
	"context"
	"errors"
	"time"
)

type analyticalProjectLease func(context.Context, string) (*ProjectLake, func(), error)

// HostedReader attaches only existing, initialized hosted catalogs. It never
// creates a project, migrates its schema, or falls back to the legacy opener.
type HostedReader struct {
	store *DuckLakeStore
}

func NewHostedReader(store *DuckLakeStore) (*HostedReader, error) {
	if store == nil || store.SharedCatalog == nil || store.Config.SharedSourceID == "" {
		return nil, errors.New("hosted reads require a shared native catalog")
	}
	return &HostedReader{store: store}, nil
}

func (r *HostedReader) Query(ctx context.Context, project, rawSQL string, parameters map[string]string, limits QueryExecutionLimits) (AnalyticsResult, error) {
	return queryProject(ctx, r.lease, project, rawSQL, parameters, limits)
}

func (r *HostedReader) Catalog(ctx context.Context, input EventCatalogInput) (EventCatalog, error) {
	return catalogProject(ctx, r.lease, input)
}

func (r *HostedReader) Profile(ctx context.Context, project string, options EventProfileOptions, asOf time.Time) (EventProfileRead, error) {
	return profileProject(ctx, r.lease, project, options, asOf)
}

func (r *HostedReader) lease(ctx context.Context, project string) (*ProjectLake, func(), error) {
	p, release, err := r.store.leaseHostedProjectExisting(ctx, project)
	if err != nil {
		return nil, nil, err
	}
	key := ProjectWriteKey{SourceID: r.store.Config.SharedSourceID, ProjectID: project}
	if _, err := readHostedProjectScope(ctx, p.DB, key); err != nil {
		release()
		return nil, nil, err
	}
	return p, release, nil
}
