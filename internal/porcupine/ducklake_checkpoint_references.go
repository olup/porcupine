//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// Offline restore reconstructs current application state. Check the current
// snapshot's data/delete references through DuckLake's public API, independent
// of the filesystem walk or S3 LIST that produced the inventory. Older physical
// snapshots are copied when present, but are not part of the restore contract.
func verifyCheckpointReferences(ctx context.Context, checkpoint *nativeCheckpoint) error {
	objects := make(map[string]nativeCheckpointObject, len(checkpoint.Objects))
	for _, object := range checkpoint.Objects {
		objects[object.RelativePath] = object
	}
	for _, catalog := range checkpoint.Catalogs {
		if err := verifyCheckpointCatalogReferences(ctx, checkpoint, catalog, objects); err != nil {
			return err
		}
	}
	return nil
}

func verifyCheckpointCatalogReferences(ctx context.Context, checkpoint *nativeCheckpoint, catalog nativeBackupCatalog, objects map[string]nativeCheckpointObject) (result error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	defer func() { result = errors.Join(result, db.Close()) }()
	for _, statement := range []string{
		"SET memory_limit='64MiB'", "SET threads=1", "LOAD ducklake",
		"ATTACH " + lakeLiteral("ducklake:"+filepath.Join(checkpoint.Directory, filepath.FromSlash(catalog.Path))) + " AS archive (DATA_PATH " + lakeLiteral(filepath.Join(checkpoint.Directory, filepath.FromSlash(catalog.DataRoot))) + ", OVERRIDE_DATA_PATH true, READ_ONLY)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return checkpointReferenceError(ctx)
		}
	}
	for _, table := range []string{"catalog_identity", "heads", "current_events", "identity_profiles", "identity_links"} {
		rows, err := db.QueryContext(ctx, "SELECT data_file,data_file_size_bytes,delete_file,delete_file_size_bytes FROM ducklake_list_files('archive',?)", table)
		if err != nil {
			return checkpointReferenceError(ctx)
		}
		for rows.Next() {
			var dataPath string
			var dataBytes int64
			var deletePath sql.NullString
			var deleteBytes sql.NullInt64
			if err := rows.Scan(&dataPath, &dataBytes, &deletePath, &deleteBytes); err != nil {
				rows.Close()
				return checkpointReferenceError(ctx)
			}
			if err := verifyCheckpointReference(checkpoint, catalog, objects, dataPath, dataBytes); err != nil {
				rows.Close()
				return err
			}
			if deletePath.Valid {
				if !deleteBytes.Valid {
					rows.Close()
					return checkpointReferenceError(ctx)
				}
				if err := verifyCheckpointReference(checkpoint, catalog, objects, deletePath.String, deleteBytes.Int64); err != nil {
					rows.Close()
					return err
				}
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return checkpointReferenceError(ctx)
		}
	}
	return nil
}

func verifyCheckpointReference(checkpoint *nativeCheckpoint, catalog nativeBackupCatalog, objects map[string]nativeCheckpointObject, path string, bytes int64) error {
	relative, err := filepath.Rel(checkpoint.Directory, path)
	archive := filepath.ToSlash(relative)
	if err != nil || !validCheckpointRelative(archive) || !strings.HasPrefix(archive, catalog.DataRoot+"/") || bytes < 0 {
		return errors.New("native checkpoint catalog references an unsafe analytical file")
	}
	if checkpoint.Storage.Mode == "s3" {
		object, exists := objects[archive]
		if !exists || object.ProjectID != catalog.ProjectID || object.Bytes != bytes {
			return errors.New("native checkpoint is missing a current analytical data or delete file")
		}
		return nil
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != bytes {
		return errors.New("native checkpoint is missing a current analytical data or delete file")
	}
	return nil
}

func checkpointReferenceError(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("native checkpoint current analytical file references could not be verified")
}
