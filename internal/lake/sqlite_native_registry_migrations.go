package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const (
	sqliteSourceDDL   = "source (id TEXT PRIMARY KEY)"
	sqliteCatalogsDDL = "catalogs (project_id TEXT PRIMARY KEY, id TEXT NOT NULL, ready INTEGER NOT NULL DEFAULT 0)"
	sqliteDeletingDDL = "deleting_projects (project_id TEXT PRIMARY KEY)"
	sqliteCleanupDDL  = "cleanup_progress (project_id TEXT PRIMARY KEY, next_attempt INTEGER NOT NULL, last_attempt INTEGER NOT NULL, last_success INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, failed INTEGER NOT NULL DEFAULT 0 CHECK(failed IN (0,1)))"
	sqliteStorageDDL  = "storage_configuration (id INTEGER PRIMARY KEY CHECK(id=1),fingerprint TEXT NOT NULL)"
	sqliteCreateTable = "CREATE TABLE IF NOT EXISTS "

	duckLakeEncryptionModeChanged = "DuckLake encryption mode changed. Use the original mode or an explicit migration"
)

const sqliteNativeRegistryInitialized = `EXISTS(SELECT 1 FROM storage_configuration) OR EXISTS(SELECT 1 FROM catalogs)
OR EXISTS(SELECT 1 FROM deleting_projects) OR EXISTS(SELECT 1 FROM cleanup_progress)`

var sqliteNativeRegistrySteps = []string{
	// Version 1 adopts the released registry layout and fills in missing tables
	// for empty databases, legacy registries, and source-only restore seeds.
	sqliteCreateTable + sqliteSourceDDL + ";\n" + sqliteCreateTable + sqliteCatalogsDDL + ";\n" +
		sqliteCreateTable + sqliteDeletingDDL + ";\n" + sqliteCreateTable + sqliteCleanupDDL + ";\n" +
		sqliteCreateTable + sqliteStorageDDL,
	// Version 2 pins existing stores to plaintext. A source-only seed or an
	// interrupted initialization without a storage binding can still choose.
	`CREATE TABLE encryption_configuration (id INTEGER PRIMARY KEY CHECK(id=1),encrypted INTEGER NOT NULL CHECK(encrypted IN (0,1)));
INSERT INTO encryption_configuration(id,encrypted) SELECT 1,0 WHERE ` + sqliteNativeRegistryInitialized,
}

// The caller holds the writer lock for the DuckLake directory.
func migrateSQLiteNativeRegistry(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	switch {
	case version == 0:
		if err := validateUnversionedSQLiteNativeRegistry(ctx, tx); err != nil {
			return err
		}
	case version < 0:
		return errors.New("invalid SQLite native registry schema version")
	case version > len(sqliteNativeRegistrySteps):
		return fmt.Errorf("SQLite native registry schema version %d is newer than supported version %d", version, len(sqliteNativeRegistrySteps))
	}
	for ; version < len(sqliteNativeRegistrySteps); version++ {
		if _, err := tx.ExecContext(ctx, sqliteNativeRegistrySteps[version]); err != nil {
			return fmt.Errorf("apply SQLite native registry schema version %d: %w", version+1, err)
		}
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", version+1)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateUnversionedSQLiteNativeRegistry(ctx context.Context, tx *sql.Tx) error {
	// SQLite retains the table definitions without IF NOT EXISTS. The restore
	// seed differs only in its whitespace before the source column list.
	expected := map[string]string{
		"source":                "CREATE TABLE " + sqliteSourceDDL,
		"catalogs":              "CREATE TABLE " + sqliteCatalogsDDL,
		"deleting_projects":     "CREATE TABLE " + sqliteDeletingDDL,
		"cleanup_progress":      "CREATE TABLE " + sqliteCleanupDDL,
		"storage_configuration": "CREATE TABLE " + sqliteStorageDDL,
	}
	rows, err := tx.QueryContext(ctx, "SELECT name,sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'")
	if err != nil {
		return err
	}
	defer rows.Close()
	found := make(map[string]bool)
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			return err
		}
		if definition != expected[name] && !(name == "source" && definition == "CREATE TABLE source(id TEXT PRIMARY KEY)") {
			return errors.New("unsupported unversioned SQLite native registry requires explicit offline migration")
		}
		found[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(found) == 0 || len(found) == 1 && found["source"] ||
		found["source"] && found["catalogs"] && found["deleting_projects"] && found["cleanup_progress"] {
		return nil
	}
	return errors.New("incomplete unversioned SQLite native registry requires explicit offline migration")
}

func pinSQLiteNativeEncryption(ctx context.Context, db *sql.DB, encrypted bool) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO encryption_configuration(id,encrypted)
SELECT 1,? WHERE NOT (`+sqliteNativeRegistryInitialized+`) ON CONFLICT(id) DO NOTHING`, encrypted); err != nil {
		return err
	}
	var actual bool
	err = tx.QueryRowContext(ctx, "SELECT encrypted FROM encryption_configuration WHERE id=1").Scan(&actual)
	if errors.Is(err, sql.ErrNoRows) {
		return errors.New("DuckLake encryption mode is missing for an initialized registry")
	}
	if err != nil {
		return err
	}
	if actual != encrypted {
		return errors.New(duckLakeEncryptionModeChanged)
	}
	return tx.Commit()
}
