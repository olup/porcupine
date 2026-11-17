package lake

import (
	"context"
	"database/sql"
	"errors"
)

type nativeTableReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// Enumerate the actual managed base tables, not the public analytical views.
// Catalogue version, write guards and new commit-evidence tables can own current
// Parquet too. Callers may pass their existing read transaction for one snapshot.
func nativeDataTables(ctx context.Context, reader nativeTableReader, catalog string) ([]string, error) {
	rows, err := reader.QueryContext(ctx, `SELECT table_name FROM information_schema.tables WHERE table_catalog=? AND table_schema='main' AND table_type='BASE TABLE' ORDER BY table_name`, catalog)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tables := []string{}
	for rows.Next() {
		var table string
		if err = rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, errors.New("managed native catalog has no base tables")
	}
	return tables, nil
}
