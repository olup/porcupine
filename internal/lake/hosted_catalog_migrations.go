package lake

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/event"
)

const hostedFenceSchemaVersion = 3
const hostedLakeSchemaVersion = 5

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
	{4, `ALTER TABLE lake.hosted_write_guard ADD COLUMN tenant_id VARCHAR;
CREATE TABLE lake.live_commit_proofs (
 proof_version INTEGER,effect_seq BIGINT,batch_id VARCHAR,batch_fingerprint VARCHAR,proof_fingerprint VARCHAR,
 tenant_id VARCHAR,fence_epoch BIGINT,accounted_at VARCHAR,accepted_live BIGINT,
 plan_id VARCHAR,manifest_key VARCHAR,manifest_version VARCHAR,manifest_sha256 VARCHAR,
 manifest_bytes BIGINT,output_count BIGINT,totals_json VARCHAR
);
CREATE TABLE lake.live_usage_totals (day VARCHAR,revision BIGINT,accepted_live BIGINT,accepted_import BIGINT)
`},
	{5, `DROP TABLE lake.live_commit_proofs;
ALTER TABLE lake.live_usage_totals RENAME TO native_usage_totals;
CREATE TABLE lake.native_commit_proofs (proof_version INTEGER,effect_seq BIGINT,operation_id VARCHAR,input_fingerprint VARCHAR,proof_fingerprint VARCHAR,kind VARCHAR,evidence_json VARCHAR);
CREATE TABLE lake.import_file_heads (job_id VARCHAR,file_id VARCHAR,manifest_sha256 VARCHAR,source_id VARCHAR,format VARCHAR,effect_seq BIGINT,checkpoint_json VARCHAR);
CREATE TABLE lake.import_person_state (distinct_id VARCHAR,person_id VARCHAR,person_version UBIGINT,mapping_version UBIGINT,profile_version UBIGINT,deleted BOOLEAN,source_id VARCHAR)
`},
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
	if version != lakeSchemaVersion && version != hostedFenceSchemaVersion && version != 4 {
		return HostedProjectScope{}, errors.New("hosted cutover requires a supported empty native catalog")
	}
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return HostedProjectScope{}, err
	}
	defer tx.Rollback()
	if err := p.acquireNativeProjectGuard(ctx, tx); err != nil {
		return HostedProjectScope{}, err
	}
	var populated bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM lake.heads UNION ALL SELECT 1 FROM lake.current_events UNION ALL SELECT 1 FROM lake.identity_profiles UNION ALL SELECT 1 FROM lake.identity_links)`).Scan(&populated); err != nil {
		return HostedProjectScope{}, err
	}
	if version == 4 {
		var prototype bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM lake.live_commit_proofs UNION ALL SELECT 1 FROM lake.live_usage_totals UNION ALL SELECT 1 FROM lake.hosted_write_guard WHERE next_effect_seq<>0 OR ack_effect_seq<>0)`).Scan(&prototype); err != nil {
			return HostedProjectScope{}, err
		}
		populated = populated || prototype
	}
	if populated {
		return HostedProjectScope{}, errors.New("populated native prototype requires rebuild from authoritative data before hosted schema5; existing data preserved")
	}
	scope := HostedProjectScope{Key: key, CatalogID: SharedNativeCatalogID(key.SourceID, key.ProjectID), StreamID: event.NewID()}
	if version >= hostedFenceSchemaVersion {
		scope, err = readHostedProjectScope(ctx, tx, key)
		if err != nil {
			return HostedProjectScope{}, err
		}
	}
	for _, step := range hostedCatalogSteps {
		if step.version <= version {
			continue
		}
		if _, err := tx.ExecContext(ctx, step.sql); err != nil {
			return HostedProjectScope{}, err
		}
		if step.version == 3 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO lake.hosted_write_guard (id,source_id,project_id,catalog_id,stream_id,fence_epoch,fence_nonce,generation,state,next_effect_seq,ack_effect_seq,sealed_effect_seq,identity_ready) VALUES(1,?,?,?,?,0,'',0,'open',0,0,NULL,true)`, key.SourceID, key.ProjectID, scope.CatalogID, scope.StreamID); err != nil {
				return HostedProjectScope{}, err
			}
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
	return readHostedProjectScopeState(ctx, query, key, false)
}

func readHostedProjectScopeState(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, key ProjectWriteKey, sealedAllowed bool) (HostedProjectScope, error) {
	scope := HostedProjectScope{Key: key}
	var count int
	if err := query.QueryRowContext(ctx, "SELECT count(*) FROM lake.hosted_write_guard").Scan(&count); err != nil {
		return HostedProjectScope{}, err
	}
	if count != 1 {
		return HostedProjectScope{}, errors.New("invalid hosted native write guard")
	}
	var state string
	var ready bool
	err := query.QueryRowContext(ctx, `SELECT catalog_id,stream_id,state,identity_ready FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=?`, key.SourceID, key.ProjectID).Scan(&scope.CatalogID, &scope.StreamID, &state, &ready)
	if err != nil || scope.CatalogID != SharedNativeCatalogID(key.SourceID, key.ProjectID) || scope.StreamID == "" {
		return HostedProjectScope{}, ErrProjectWriteFenceStale
	}
	if state != "open" && state != "sealed" {
		return HostedProjectScope{}, errors.New("invalid hosted native stream state")
	}
	if state == "sealed" && !sealedAllowed {
		return HostedProjectScope{}, ErrProjectWriteSealed
	}
	if !ready {
		return HostedProjectScope{}, errors.New("hosted identity authority is not ready")
	}
	return scope, nil
}
