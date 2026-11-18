package lake

import (
	"context"
	"database/sql"
	"errors"
	"math"
)

// CurrentParquetBytes measures only current referenced data/delete files. It
// neither lists the bucket nor counts obsolete snapshots, backups or outputs.
func (s *DuckLakeStore) CurrentParquetBytes(ctx context.Context, project string) (int64, error) {
	if err := s.Mu.Lock(ctx); err != nil {
		return 0, err
	}
	var ready bool
	err := s.Registry.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM catalogs WHERE project_id=? AND ready=1)`, project).Scan(&ready)
	if err != nil || !ready {
		s.Mu.Unlock()
		if err != nil {
			return 0, err
		}
		return 0, sql.ErrNoRows
	}
	p, release, err := s.leaseProjectLocked(ctx, project)
	s.Mu.Unlock()
	if err != nil {
		return 0, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	files := map[string]int64{}
	tables, err := nativeDataTables(ctx, tx, "lake")
	if err != nil {
		return 0, err
	}
	for _, table := range tables {
		rows, e := tx.QueryContext(ctx, `SELECT data_file,data_file_size_bytes,delete_file,delete_file_size_bytes FROM ducklake_list_files('lake',?)`, table)
		if e != nil {
			return 0, e
		}
		for rows.Next() {
			var data string
			var bytes int64
			var deleted sql.NullString
			var deletedBytes sql.NullInt64
			if e = rows.Scan(&data, &bytes, &deleted, &deletedBytes); e != nil {
				rows.Close()
				return 0, e
			}
			if bytes < 0 {
				rows.Close()
				return 0, errors.New("invalid current Parquet size")
			}
			if old, exists := files[data]; exists && old != bytes {
				rows.Close()
				return 0, errors.New("conflicting current Parquet size")
			}
			files[data] = bytes
			if deleted.Valid {
				if !deletedBytes.Valid || deletedBytes.Int64 < 0 {
					rows.Close()
					return 0, errors.New("invalid current delete-file size")
				}
				if old, exists := files[deleted.String]; exists && old != deletedBytes.Int64 {
					rows.Close()
					return 0, errors.New("conflicting current delete-file size")
				}
				files[deleted.String] = deletedBytes.Int64
			}
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return 0, e
		}
	}
	var total int64
	for _, bytes := range files {
		if total > math.MaxInt64-bytes {
			return 0, errors.New("current Parquet size overflow")
		}
		total += bytes
	}
	return total, tx.Commit()
}

// HostedCurrentParquetBytes measures the same current data/delete files from
// a hosted project's DuckLake metadata in PostgreSQL. It reads one statement
// snapshot, opens no analytical engine and does not list S3. A project without
// a ready, undeleted catalog is not measured.
func (s *DuckLakeStore) HostedCurrentParquetBytes(ctx context.Context, project string) (int64, error) {
	registry, ok := s.registry.(*postgresNativeRegistry)
	if !ok || s.SharedCatalog == nil {
		return 0, ErrHostedProjectNotReady
	}
	p, err := registry.lookup(ctx, project)
	if err != nil {
		return 0, err
	}
	if !p.Ready || p.Deleted {
		return 0, sql.ErrNoRows
	}
	ctx, cancel := registry.metadataContext(ctx)
	defer cancel()
	// SharedNativeSchema derives a fixed-format identifier from the checked
	// catalog ID. Like ducklake_list_files, count current files of live main
	// tables and the current delete files attached to them, once per path.
	schema := SharedNativeSchema(p.CatalogID)
	var exists bool
	if err = registry.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, schema+".ducklake_data_file").Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, sql.ErrNoRows
	}
	schema = `"` + schema + `"`
	var total, smallest, paths, files int64
	err = registry.db.QueryRowContext(ctx, `WITH tables AS (
	SELECT t.table_id FROM `+schema+`.ducklake_table t JOIN `+schema+`.ducklake_schema s ON s.schema_id=t.schema_id
	WHERE t.end_snapshot IS NULL AND s.end_snapshot IS NULL AND s.schema_name='main'
), data AS (
	SELECT d.data_file_id,d.path,d.file_size_bytes FROM `+schema+`.ducklake_data_file d JOIN tables USING(table_id)
	WHERE d.end_snapshot IS NULL
), current AS (
	SELECT path,file_size_bytes FROM data
	UNION SELECT f.path,f.file_size_bytes FROM `+schema+`.ducklake_delete_file f JOIN data USING(data_file_id)
	WHERE f.end_snapshot IS NULL
)
SELECT coalesce(sum(file_size_bytes),0)::bigint,count(DISTINCT path),count(*),coalesce(min(file_size_bytes),0) FROM current`).Scan(&total, &paths, &files, &smallest)
	if err != nil {
		return 0, err
	}
	if paths != files {
		return 0, errors.New("conflicting current Parquet size")
	}
	if smallest < 0 {
		return 0, errors.New("invalid current Parquet size")
	}
	return total, nil
}
