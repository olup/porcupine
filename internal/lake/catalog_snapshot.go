package lake

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

const (
	// Detect overflow before saving any partial summary or checkpoint.
	maxCatalogSummaryNames = 20_000
	maxCatalogSnapshotSpan = 100_000
	catalogDuckDBVersion   = "v2.0.0"
	catalogDuckLakeVersion = "417a9117f5"
)

var ErrCatalogSummaryTooLarge = errors.New("DuckLake event catalog exceeds summary capacity")

type CatalogAggregate struct {
	Event           EventCatalogEvent
	FirstID, LastID int64
}

type CatalogSnapshotRead struct {
	Snapshot ExportSnapshot
	// Replace means Rows represents the complete catalog at Snapshot.
	Replace bool
	Rows    []CatalogAggregate
}

type catalogSnapshotChange uint8

const (
	catalogRebuild catalogSnapshotChange = iota
	catalogUnchanged
	catalogAppend
)

// ReadCatalogSnapshot builds or advances a catalog summary at one pinned
// DuckLake snapshot. Pure maintenance can advance the checkpoint with no rows.
func (s *DuckLakeStore) ReadCatalogSnapshot(ctx context.Context, project string, previous *ExportSnapshot) (result CatalogSnapshotRead, err error) {
	err = s.withCatalogSnapshot(ctx, project, func(p *ProjectLake, tx *sql.Tx, current ExportSnapshot) error {
		result, err = p.readCatalogSnapshotAt(ctx, tx, current, previous)
		return err
	})
	return result, err
}

// CatalogSnapshotValid reports whether a summary can still be served after
// accounting for mutations since its checkpoint. Appends may be absent from
// the summary until the next refresh. This check reads no event data files.
func (s *DuckLakeStore) CatalogSnapshotValid(ctx context.Context, project string, previous ExportSnapshot) (valid bool, err error) {
	err = s.withCatalogSnapshot(ctx, project, func(p *ProjectLake, tx *sql.Tx, current ExportSnapshot) error {
		change, err := p.catalogChanges(ctx, tx, current, &previous)
		valid = change != catalogRebuild
		return err
	})
	return valid, err
}

func (s *DuckLakeStore) withCatalogSnapshot(ctx context.Context, project string, read func(*ProjectLake, *sql.Tx, ExportSnapshot) error) error {
	lease := s.LeaseProject
	if s.SharedCatalog != nil {
		lease = s.leaseHostedProjectExisting
	}
	p, release, err := lease(ctx, project)
	if err != nil {
		return err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var snapshot ExportSnapshot
	// DuckLake acquires its analytical snapshot lazily on first use.
	if err := tx.QueryRowContext(ctx, "SELECT id FROM ducklake_current_snapshot('lake')").Scan(&snapshot.Version); err != nil {
		return err
	}
	if p.shared {
		key := ProjectWriteKey{SourceID: s.Config.SharedSourceID, ProjectID: project}
		scope, err := readHostedProjectScope(ctx, tx, key)
		if err != nil {
			return err
		}
		snapshot.CatalogID = scope.CatalogID
	} else if err := tx.QueryRowContext(ctx, "SELECT (SELECT id FROM lake.catalog_identity)").Scan(&snapshot.CatalogID); err != nil {
		return err
	}
	return read(p, tx, snapshot)
}

func (p *ProjectLake) readCatalogSnapshotAt(ctx context.Context, tx *sql.Tx, current ExportSnapshot, previous *ExportSnapshot) (CatalogSnapshotRead, error) {
	change, proofErr := p.catalogChanges(ctx, tx, current, previous)
	switch change {
	case catalogUnchanged:
		return CatalogSnapshotRead{Snapshot: current}, nil
	case catalogAppend:
		rows, err := readCatalogAggregates(ctx, tx, "ducklake_table_insertions('lake','main','current_events',?,?)", previous.Version+1, current.Version)
		if err == nil {
			return CatalogSnapshotRead{Snapshot: current, Rows: rows}, nil
		}
		if errors.Is(err, ErrCatalogSummaryTooLarge) {
			return CatalogSnapshotRead{}, err
		}
		proofErr = err
	}
	if err := ctx.Err(); err != nil {
		return CatalogSnapshotRead{}, err
	}
	rows, err := readCatalogAggregates(ctx, tx, "lake.current_events")
	if err != nil {
		return CatalogSnapshotRead{}, errors.Join(proofErr, err)
	}
	return CatalogSnapshotRead{Snapshot: current, Replace: true, Rows: rows}, nil
}

const catalogSnapshotChangesSQL = `
WITH catalog_tables AS (
 SELECT t.table_name,t.table_id
 FROM metadata.ducklake_table t JOIN metadata.ducklake_schema sc USING(schema_id)
 WHERE t.table_name IN ('current_events','catalog_identity') AND sc.schema_name='main'
 AND t.begin_snapshot<=$1 AND (t.end_snapshot IS NULL OR t.end_snapshot>$2)
 AND sc.begin_snapshot<=$1 AND (sc.end_snapshot IS NULL OR sc.end_snapshot>$2)
)
SELECT s.snapshot_id, events.table_id, identity.table_id, c.changes_made
FROM metadata.ducklake_snapshot s
CROSS JOIN (SELECT table_id FROM catalog_tables WHERE table_name='current_events') events
CROSS JOIN (SELECT table_id FROM catalog_tables WHERE table_name='catalog_identity') identity
LEFT JOIN metadata.ducklake_snapshot_changes c USING(snapshot_id)
WHERE s.snapshot_id BETWEEN $1 AND $2 ORDER BY s.snapshot_id`

func (p *ProjectLake) catalogChanges(ctx context.Context, tx *sql.Tx, current ExportSnapshot, previous *ExportSnapshot) (catalogSnapshotChange, error) {
	if previous == nil || previous.CatalogID == "" || previous.CatalogID != current.CatalogID ||
		previous.Version < 0 || previous.Version > current.Version || current.Version-previous.Version > maxCatalogSnapshotSpan {
		return catalogRebuild, nil
	}
	if *previous == current {
		return catalogUnchanged, nil
	}
	// This extension's metadata and change feed preserve logical insertion
	// versions across file merges and inlined-data flushes.
	var engine, extension string
	err := tx.QueryRowContext(ctx, "SELECT version(),extension_version FROM ducklake_settings('lake')").Scan(&engine, &extension)
	if err != nil {
		return catalogRebuild, err
	}
	if engine != catalogDuckDBVersion || extension != catalogDuckLakeVersion {
		return catalogRebuild, nil
	}

	var query interface {
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	} = tx
	metadata := "__ducklake_metadata_lake.main."
	if p.shared {
		registry, ok := p.sharedStore.registry.(*postgresNativeRegistry)
		if !ok {
			return catalogRebuild, nil
		}
		var cancel context.CancelFunc
		ctx, cancel = registry.metadataContext(ctx)
		defer cancel()
		query = registry.db
		metadata = `"` + SharedNativeSchema(current.CatalogID) + `".`
	}
	// PostgreSQL metadata can already see a newer commit. Only classify the
	// bounded version range covered by this DuckLake transaction.
	rows, err := query.QueryContext(ctx, strings.ReplaceAll(catalogSnapshotChangesSQL, "metadata.", metadata), previous.Version, current.Version)
	if err != nil {
		return catalogRebuild, err
	}
	defer rows.Close()
	change := catalogUnchanged
	lastVersion := previous.Version - 1
	for rows.Next() {
		var version, eventsID, identityID int64
		var changes sql.NullString
		if err := rows.Scan(&version, &eventsID, &identityID, &changes); err != nil {
			return catalogRebuild, err
		}
		if version != lastVersion+1 || eventsID < 0 || identityID < 0 || eventsID == identityID {
			return catalogRebuild, nil
		}
		lastVersion = version
		if version == previous.Version {
			continue
		}
		switch classifyCatalogChanges(changes, eventsID, identityID) {
		case catalogRebuild:
			return catalogRebuild, nil
		case catalogAppend:
			change = catalogAppend
		}
	}
	if err := rows.Err(); err != nil {
		return catalogRebuild, err
	}
	if lastVersion != current.Version {
		return catalogRebuild, nil
	}
	return change, nil
}

func classifyCatalogChanges(changes sql.NullString, eventsID, identityID int64) catalogSnapshotChange {
	if !changes.Valid || changes.String == "" {
		return catalogRebuild
	}
	result := catalogUnchanged
	for change := range strings.SplitSeq(changes.String, ",") {
		kind, rawID, found := strings.Cut(change, ":")
		id, err := strconv.ParseInt(rawID, 10, 64)
		if !found || err != nil || id < 0 {
			return catalogRebuild
		}
		switch kind {
		case "inserted_into_table", "inlined_insert":
			if id == identityID {
				return catalogRebuild
			}
			if id == eventsID {
				result = catalogAppend
			}
		case "deleted_from_table", "inlined_delete":
			if id == eventsID || id == identityID {
				return catalogRebuild
			}
		case "merge_adjacent", "rewrite_delete", "compacted_table", "flushed_inlined", "inline_flush":
			// These changes rewrite storage while preserving visible rows.
		default:
			return catalogRebuild
		}
	}
	return result
}

func readCatalogAggregates(ctx context.Context, tx *sql.Tx, source string, args ...any) ([]CatalogAggregate, error) {
	query := duckLakeCatalogAggregateColumns + ", " + duckLakeCatalogPointerColumns +
		" FROM " + source + duckLakeCatalogVisibility + " GROUP BY event ORDER BY max(id) DESC LIMIT ?"
	rows, err := tx.QueryContext(ctx, query, append(args, maxCatalogSummaryNames+1)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []CatalogAggregate
	for rows.Next() {
		if len(result) == maxCatalogSummaryNames {
			return nil, ErrCatalogSummaryTooLarge
		}
		var aggregate CatalogAggregate
		item := &aggregate.Event
		if err := rows.Scan(&item.Event, &item.Count, &aggregate.FirstID, &aggregate.LastID,
			&item.FirstReceivedAt, &item.LastReceivedAt, &item.LatestEventUUID, &item.LatestEventTimestamp); err != nil {
			return nil, err
		}
		result = append(result, aggregate)
	}
	return result, rows.Err()
}
