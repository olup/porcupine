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
	for _, table := range []string{"catalog_identity", "heads", "current_events", "identity_profiles", "identity_links"} {
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
