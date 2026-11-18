package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Version 1 is the registry layout written before numbered migrations existed.
// Keep its released definitions unchanged when appending later versions.
const nativeRegistrySchemaV1 = `CREATE TABLE IF NOT EXISTS pc_native_sources(source_id text PRIMARY KEY,fingerprint text NOT NULL);
 CREATE TABLE IF NOT EXISTS pc_native_projects(source_id text NOT NULL REFERENCES pc_native_sources(source_id),project_id text NOT NULL,catalog_id text NOT NULL,ready integer NOT NULL DEFAULT 0,deleted integer NOT NULL DEFAULT 0,PRIMARY KEY(source_id,project_id));`

var nativeRegistrySteps = []string{
	nativeRegistrySchemaV1 + `
CREATE TABLE pc_native_registry_version(id integer PRIMARY KEY CHECK(id=1),version integer NOT NULL CHECK(version>0));`,
	`ALTER TABLE pc_native_projects ADD CONSTRAINT pc_native_projects_ready_valid CHECK(ready IN (0,1));
ALTER TABLE pc_native_projects ADD CONSTRAINT pc_native_projects_deleted_valid CHECK(deleted IN (0,1));
CREATE TABLE pc_native_cleanup (
source_id text NOT NULL,project_id text NOT NULL,attempt_id text NOT NULL,
next_attempt bigint NOT NULL,last_attempt bigint NOT NULL,last_success bigint NOT NULL DEFAULT 0,
state text NOT NULL CHECK(state IN ('running','waiting','failed','ok','interrupted')),
failed integer NOT NULL DEFAULT 0 CHECK(failed IN (0,1)),
PRIMARY KEY(source_id,project_id),FOREIGN KEY(source_id,project_id) REFERENCES pc_native_projects(source_id,project_id));`,
	`CREATE TABLE pc_native_hosted_deletion (
source_id text NOT NULL,project_id text NOT NULL,stream_id text NOT NULL,
final_sequence bigint NOT NULL CHECK(final_sequence>=0),
PRIMARY KEY(source_id,project_id),FOREIGN KEY(source_id,project_id) REFERENCES pc_native_projects(source_id,project_id));`,
	`CREATE TABLE pc_native_compaction (
source_id text NOT NULL,project_id text NOT NULL,attempt_id text NOT NULL,
next_attempt bigint NOT NULL,last_attempt bigint NOT NULL,
state text NOT NULL CHECK(state IN ('running','waiting','failed','ok','interrupted')),
checked_layout_snapshot bigint NOT NULL DEFAULT 0,
PRIMARY KEY(source_id,project_id),FOREIGN KEY(source_id,project_id) REFERENCES pc_native_projects(source_id,project_id));`,
	`ALTER TABLE pc_native_cleanup ADD COLUMN snapshots_expired bigint NOT NULL DEFAULT 0 CHECK(snapshots_expired>=0),
ADD COLUMN files_deleted bigint NOT NULL DEFAULT 0 CHECK(files_deleted>=0),
ADD COLUMN last_error text NOT NULL DEFAULT '' CHECK(last_error IN ('','restore_guard','failed','interrupted'));`,
}

// Caller holds the domain initialization advisory lock. Catalog metadata can
// use a different database from application control, so lake owns this runner.
func migrateNativeRegistry(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var versioned bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass('pc_native_registry_version') IS NOT NULL`).Scan(&versioned); err != nil {
		return err
	}
	version := 0
	if versioned {
		var rows int
		if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(max(version),0) FROM pc_native_registry_version WHERE id=1`).Scan(&rows, &version); err != nil {
			return err
		}
		if rows != 1 || version < 1 {
			return errors.New("invalid native registry schema version")
		}
		if version > len(nativeRegistrySteps) {
			return fmt.Errorf("native registry schema version %d is newer than supported version %d", version, len(nativeRegistrySteps))
		}
	} else if err := validateUnversionedNativeRegistry(ctx, tx); err != nil {
		return err
	}
	for ; version < len(nativeRegistrySteps); version++ {
		if _, err := tx.ExecContext(ctx, nativeRegistrySteps[version]); err != nil {
			return fmt.Errorf("apply native registry schema version %d: %w", version+1, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO pc_native_registry_version(id,version) VALUES(1,$1) ON CONFLICT(id) DO UPDATE SET version=excluded.version`, version+1); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateUnversionedNativeRegistry(ctx context.Context, tx *sql.Tx) error {
	// Exact column/type/nullability order prevents adoption of an unrelated or
	// partially upgraded format. Other application/DuckLake schemas may coexist.
	expected := map[string]string{
		"pc_native_sources":  "source_id:text:NO:,fingerprint:text:NO:",
		"pc_native_projects": "source_id:text:NO:,project_id:text:NO:,catalog_id:text:NO:,ready:integer:NO:0,deleted:integer:NO:0",
	}
	found := 0
	for table, shape := range expected {
		var actual sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT string_agg(column_name||':'||data_type||':'||is_nullable||':'||coalesce(column_default,''),',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1`, table).Scan(&actual); err != nil {
			return err
		}
		if actual.Valid {
			found++
			if actual.String != shape {
				return errors.New("unsupported unversioned native registry; explicit offline migration is required")
			}
		}
	}
	if found == 1 {
		return errors.New("incomplete unversioned native registry; explicit offline migration is required")
	}
	var tables int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema=current_schema() AND table_name LIKE 'pc\_native\_%' ESCAPE '\'`).Scan(&tables); err != nil {
		return err
	}
	if tables != found {
		return errors.New("unknown unversioned native registry tables; explicit offline migration is required")
	}
	if found == 0 {
		return nil
	}
	var constraints int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM pg_constraint WHERE conrelid IN ('pc_native_sources'::regclass,'pc_native_projects'::regclass)`).Scan(&constraints); err != nil {
		return err
	}
	if constraints != 3 {
		return errors.New("unsupported unversioned native registry constraints")
	}
	for table, columns := range map[string][]string{"pc_native_sources": {"source_id"}, "pc_native_projects": {"source_id", "project_id"}} {
		var key sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT string_agg(a.attname,',' ORDER BY k.ordinality)
FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid JOIN pg_namespace n ON n.oid=t.relnamespace
CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY k(attnum,ordinality)
JOIN pg_attribute a ON a.attrelid=t.oid AND a.attnum=k.attnum
WHERE n.nspname=current_schema() AND t.relname=$1 AND c.contype='p'`, table).Scan(&key); err != nil {
			return err
		}
		if !key.Valid || key.String != strings.Join(columns, ",") {
			return errors.New("unsupported unversioned native registry constraints")
		}
	}
	var validForeignKey bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_constraint
WHERE conrelid='pc_native_projects'::regclass AND confrelid='pc_native_sources'::regclass AND contype='f'
AND conkey=ARRAY[1]::smallint[] AND confkey=ARRAY[1]::smallint[] AND confdeltype='a' AND confupdtype='a')`).Scan(&validForeignKey); err != nil {
		return err
	}
	if !validForeignKey {
		return errors.New("unsupported unversioned native registry foreign key")
	}
	return nil
}
