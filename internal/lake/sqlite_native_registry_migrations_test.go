package lake

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

const legacySQLiteRegistry = `CREATE TABLE IF NOT EXISTS source (id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS catalogs (project_id TEXT PRIMARY KEY, id TEXT NOT NULL, ready INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS deleting_projects (project_id TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS cleanup_progress (project_id TEXT PRIMARY KEY, next_attempt INTEGER NOT NULL, last_attempt INTEGER NOT NULL, last_success INTEGER NOT NULL DEFAULT 0, state TEXT NOT NULL, failed INTEGER NOT NULL DEFAULT 0 CHECK(failed IN (0,1)));`

const legacySQLiteStorage = `CREATE TABLE IF NOT EXISTS storage_configuration (id INTEGER PRIMARY KEY CHECK(id=1),fingerprint TEXT NOT NULL);`

func openSQLiteRegistryTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func sqliteRegistrySchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	var definition string
	if err := db.QueryRow(`SELECT coalesce(group_concat(sql,';'),'') FROM (SELECT sql FROM sqlite_schema ORDER BY name)`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	return definition
}

func TestSQLiteNativeRegistryNumberedMigrations(t *testing.T) {
	t.Parallel()
	fresh, err := NewDuckLake(DuckLakeConfig{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	expected := sqliteRegistrySchema(t, fresh.Registry)
	for start := range len(sqliteNativeRegistrySteps) + 1 {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			t.Parallel()
			db := openSQLiteRegistryTestDB(t, filepath.Join(t.TempDir(), "registry.sqlite"))
			for i := range start {
				if _, err := db.Exec(sqliteNativeRegistrySteps[i]); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", i+1)); err != nil {
					t.Fatal(err)
				}
			}
			if err := migrateSQLiteNativeRegistry(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			if schema := sqliteRegistrySchema(t, db); schema != expected {
				t.Fatal("numbered schema differs from initialization", schema, expected)
			}
			future := len(sqliteNativeRegistrySteps) + 1
			if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", future)); err != nil {
				t.Fatal(err)
			}
			if err := migrateSQLiteNativeRegistry(t.Context(), db); err == nil || !strings.Contains(err.Error(), "newer") {
				t.Fatal("future schema accepted", err)
			}
			var version int
			if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != future || sqliteRegistrySchema(t, db) != expected {
				t.Fatal("future version refusal modified the registry", version, err)
			}
		})
	}
}

func TestSQLiteNativeRegistryLegacyEncryptionPin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		seed            string
		legacyPlaintext bool
	}{
		{"registered-catalog", legacySQLiteRegistry + legacySQLiteStorage + `INSERT INTO source VALUES('source'); INSERT INTO catalogs VALUES('project','catalog',1)`, true},
		{"pre-storage-configuration", legacySQLiteRegistry + `INSERT INTO source VALUES('source'); INSERT INTO catalogs VALUES('project','catalog',0)`, true},
		{"bound-storage", legacySQLiteRegistry + legacySQLiteStorage + `INSERT INTO source VALUES('source'); INSERT INTO storage_configuration VALUES(1,'binding')`, true},
		{"restore-seed", `CREATE TABLE source(id TEXT PRIMARY KEY); INSERT INTO source VALUES('source')`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "registry.sqlite")
			db := openSQLiteRegistryTestDB(t, path)
			if _, err := db.Exec(tc.seed); err != nil {
				t.Fatal(err)
			}
			if err := migrateSQLiteNativeRegistry(t.Context(), db); err != nil {
				t.Fatal(err)
			}
			var source string
			if err := db.QueryRow("SELECT id FROM source").Scan(&source); err != nil || source != "source" {
				t.Fatal("migration changed the source", source, err)
			}
			if tc.legacyPlaintext {
				if err := pinSQLiteNativeEncryption(t.Context(), db, true); err == nil || !strings.Contains(err.Error(), duckLakeEncryptionModeChanged) {
					t.Fatal("migration did not pin the legacy registry to plaintext", err)
				}
			}
			encrypted := !tc.legacyPlaintext
			if err := pinSQLiteNativeEncryption(t.Context(), db, encrypted); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db = openSQLiteRegistryTestDB(t, path)
			if err := pinSQLiteNativeEncryption(t.Context(), db, !encrypted); err == nil || !strings.Contains(err.Error(), duckLakeEncryptionModeChanged) {
				t.Fatal("a different encryption mode was accepted", err)
			}
			if err := pinSQLiteNativeEncryption(t.Context(), db, encrypted); err != nil {
				t.Fatal("restart with the original mode failed", err)
			}
		})
	}
}

func TestSQLiteNativeRegistryRefusesUnknownLegacySchema(t *testing.T) {
	t.Parallel()
	for _, seed := range []string{
		`CREATE TABLE unrelated (id TEXT)`,
		`CREATE TABLE source (id TEXT)`,
		`CREATE TABLE source (id TEXT PRIMARY KEY); CREATE TABLE catalogs (project_id TEXT PRIMARY KEY, id TEXT NOT NULL, ready INTEGER NOT NULL DEFAULT 0)`,
	} {
		db := openSQLiteRegistryTestDB(t, filepath.Join(t.TempDir(), "registry.sqlite"))
		if _, err := db.Exec(seed); err != nil {
			t.Fatal(err)
		}
		before := sqliteRegistrySchema(t, db)
		if err := migrateSQLiteNativeRegistry(t.Context(), db); err == nil || !strings.Contains(err.Error(), "unversioned") {
			t.Fatal("unknown unversioned schema accepted", err)
		}
		var version int
		if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 0 || sqliteRegistrySchema(t, db) != before {
			t.Fatal("unknown schema refusal modified the registry", version, err)
		}
	}
}

func TestSQLiteNativeRegistryRefusesMissingEncryptionMode(t *testing.T) {
	t.Parallel()
	config := DuckLakeConfig{Directory: t.TempDir()}
	store, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	_, deleteErr := store.Registry.Exec("DELETE FROM encryption_configuration")
	closeErr := store.Close()
	if deleteErr != nil || closeErr != nil {
		t.Fatal(deleteErr, closeErr)
	}
	for _, encrypted := range []bool{false, true} {
		config.Encrypted = encrypted
		store, err := NewDuckLake(config)
		if err == nil {
			store.Close()
			t.Fatal("startup adopted an encryption mode for an initialized registry")
		}
		if !strings.Contains(err.Error(), "encryption mode is missing") {
			t.Fatal(err)
		}
	}
}
