package lake

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

// ExportSnapshot identifies a catalog incarnation and a committed DuckLake
// version. Both are persisted on admission and reused across attempts.
type ExportSnapshot struct {
	CatalogID string
	Version   int64
}

var ErrExportSnapshotUnavailable = errors.New("export analytical snapshot unavailable")

func (s *DuckLakeStore) CaptureExportSnapshot(ctx context.Context, project string) (ExportSnapshot, error) {
	var snapshot ExportSnapshot
	lease := s.LeaseProject
	if s.SharedCatalog != nil {
		lease = s.leaseHostedProjectExisting
	}
	p, release, err := lease(ctx, project)
	if err != nil {
		return snapshot, err
	}
	defer release()
	err = p.DB.QueryRowContext(ctx, `SELECT (SELECT id FROM lake.catalog_identity), id FROM ducklake_current_snapshot('lake')`).Scan(&snapshot.CatalogID, &snapshot.Version)
	return snapshot, err
}

func (s *DuckLakeStore) ExportAtSnapshot(ctx context.Context, project, raw string, parameters map[string]string, snapshot ExportSnapshot, maxRowBytes int64, callbacks ExportStreamCallbacks) (ExportStreamResult, error) {
	return s.export(ctx, project, raw, parameters, nil, &snapshot, maxRowBytes, callbacks)
}

func createExportSnapshotView(ctx context.Context, conn *sql.Conn, snapshot ExportSnapshot) error {
	var identity string
	if snapshot.CatalogID == "" || snapshot.Version < 0 {
		return ErrExportSnapshotUnavailable
	}
	if err := conn.QueryRowContext(ctx, `SELECT id FROM lake.catalog_identity`).Scan(&identity); err != nil {
		return err
	}
	if identity != snapshot.CatalogID {
		return ErrExportSnapshotUnavailable
	}
	// Version every base table, including the person projection, so a later
	// identity mutation cannot change an otherwise historical export.
	version := " AT (VERSION => " + strconv.FormatInt(snapshot.Version, 10) + ")"
	view := duckLakeEventsSelect
	for _, table := range []string{"lake.current_events", "lake.identity_links", "lake.identity_profiles"} {
		view = strings.Replace(view, table+" ", "(SELECT * FROM "+table+version+") ", 1)
	}
	_, err := conn.ExecContext(ctx, "CREATE TEMP VIEW events AS "+view)
	return err
}
