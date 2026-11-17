package lake

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/event"
)

const hostedLakeSchemaVersion = 3

// Version 3 is an explicit empty-catalog cutover, never an inferred legacy
// identity backfill. Published native steps remain unchanged.
var hostedCatalogSteps = []struct {
	version int
	sql     string
}{
	{3, `ALTER TABLE lake.identity_profiles ADD COLUMN is_identified BOOLEAN DEFAULT false;
CREATE TABLE lake.hosted_write_guard (
 id INTEGER,source_id VARCHAR,project_id VARCHAR,catalog_id VARCHAR,stream_id VARCHAR,
 fence_epoch BIGINT,fence_nonce VARCHAR,generation UBIGINT,state VARCHAR,
 next_effect_seq UBIGINT,ack_effect_seq UBIGINT,sealed_effect_seq UBIGINT,identity_ready BOOLEAN
)`},
}

type HostedProjectScope struct {
	Key                 ProjectWriteKey
	CatalogID, StreamID string
}

// InitializeHostedProject prepares an empty shared catalog for fenced writers.
// All legacy binaries, journal/import writers and already-open legacy handles
// must be drained before this explicit cutover. No production mode calls it yet.
// Populated legacy catalogs require a verified rebuild from their real authority.
func (s *DuckLakeStore) InitializeHostedProject(ctx context.Context, key ProjectWriteKey) (HostedProjectScope, error) {
	if s.SharedCatalog == nil || key.SourceID != s.Config.SharedSourceID || key.ProjectID == "" {
		return HostedProjectScope{}, ErrProjectWriteFenceStale
	}
	p, release, err := s.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		return HostedProjectScope{}, err
	}
	defer release()
	if err := p.write.Lock(ctx); err != nil {
		return HostedProjectScope{}, err
	}
	defer p.write.Unlock()
	version, err := lakeCatalogVersion(ctx, p.DB, false)
	if err != nil {
		return HostedProjectScope{}, err
	}
	if version == hostedLakeSchemaVersion {
		return readHostedProjectScope(ctx, p.DB, key)
	}
	if version != lakeSchemaVersion {
		return HostedProjectScope{}, errors.New("hosted cutover requires a supported empty native catalog")
	}
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return HostedProjectScope{}, err
	}
	defer tx.Rollback()
	if err := p.acquireNativeWrite(ctx, tx); err != nil {
		return HostedProjectScope{}, err
	}
	var populated bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM lake.heads UNION ALL SELECT 1 FROM lake.current_events UNION ALL SELECT 1 FROM lake.identity_profiles UNION ALL SELECT 1 FROM lake.identity_links)`).Scan(&populated); err != nil {
		return HostedProjectScope{}, err
	}
	if populated {
		return HostedProjectScope{}, errors.New("hosted identity authority is unavailable for this legacy catalog; rebuild from the authoritative journal before cutover")
	}
	scope := HostedProjectScope{Key: key, CatalogID: SharedNativeCatalogID(key.SourceID, key.ProjectID), StreamID: event.NewID()}
	for _, step := range hostedCatalogSteps {
		if step.version <= version {
			continue
		}
		if _, err := tx.ExecContext(ctx, step.sql); err != nil {
			return HostedProjectScope{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO lake.hosted_write_guard VALUES(1,?,?,?,?,0,'',0,'open',0,0,NULL,true)`, key.SourceID, key.ProjectID, scope.CatalogID, scope.StreamID); err != nil {
			return HostedProjectScope{}, err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM lake.schema_version; INSERT INTO lake.schema_version VALUES(?)", step.version); err != nil {
			return HostedProjectScope{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return HostedProjectScope{}, err
	}
	return scope, nil
}
func readHostedProjectScope(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProjectWriteKey) (HostedProjectScope, error) {
	scope := HostedProjectScope{Key: key}
	var count int
	if err := query.QueryRowContext(ctx, "SELECT count(*) FROM lake.hosted_write_guard").Scan(&count); err != nil {
		return HostedProjectScope{}, err
	}
	if count != 1 {
		return HostedProjectScope{}, errors.New("invalid hosted native write guard")
	}
	err := query.QueryRowContext(ctx, `SELECT catalog_id,stream_id FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND identity_ready`, key.SourceID, key.ProjectID).Scan(&scope.CatalogID, &scope.StreamID)
	if err != nil || scope.CatalogID != SharedNativeCatalogID(key.SourceID, key.ProjectID) || scope.StreamID == "" {
		return HostedProjectScope{}, ErrProjectWriteFenceStale
	}
	return scope, nil
}
