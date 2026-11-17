package porcupine

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
)

// Each operational store has an ordered list of schema steps. steps[0] is the
// complete schema of version 1; steps[n] upgrades version n to n+1. Never edit
// a released step: append a new one. The server refuses a database whose
// version is newer than the steps it knows, and refuses unknown unversioned
// databases before changing them.

//go:embed journal_schema.sql
var journalSchemaV1 string

//go:embed local_control_schema.sql
var localControlSchemaV1 string

type schemaStore struct {
	name  string
	steps []string
	// adopt reports whether a non-empty database without a version was
	// created by the pre-versioning build whose schema equals version 1.
	adopt func(context.Context, *sql.Tx) (bool, error)
}

var journalSchema = schemaStore{name: "journal", steps: []string{
	journalSchemaV1,
	// 2: export objects of a deleted project, created lazily before versioning.
	`CREATE TABLE IF NOT EXISTS project_deletion_files (project_id TEXT NOT NULL, object_key TEXT NOT NULL, upload_id TEXT NOT NULL, PRIMARY KEY(project_id, object_key, upload_id)) WITHOUT ROWID`,
	// 3: receipt age for bounded retention. Existing receipts count from the upgrade.
	`ALTER TABLE event_receipts ADD COLUMN recorded_at_ms INTEGER NOT NULL DEFAULT 0;
UPDATE event_receipts SET recorded_at_ms=CAST(strftime('%s','now') AS INTEGER)*1000;
CREATE INDEX event_receipts_recorded ON event_receipts(recorded_at_ms)`,
	// 4: bind cleanup to the namespace used before the first external upload.
	`ALTER TABLE export_files ADD COLUMN storage_id TEXT NOT NULL DEFAULT '';
ALTER TABLE project_deletion_files ADD COLUMN storage_id TEXT NOT NULL DEFAULT '';`,
}, adopt: func(ctx context.Context, tx *sql.Tx) (bool, error) {
	return sqliteMetadataHas(ctx, tx, "journal_layout", "separate-control-v1")
}}

const tenantDeletionSchema = `CREATE TABLE tenant_deletions (
tenant_id TEXT PRIMARY KEY,state TEXT NOT NULL CHECK(state IN ('deleting','logically_deleted','purged')),
started_at TEXT NOT NULL,updated_at TEXT NOT NULL,error_code TEXT NOT NULL DEFAULT '');
CREATE TABLE tenant_deletion_usage (
tenant_id TEXT NOT NULL,project_id TEXT NOT NULL DEFAULT '',period_start TEXT NOT NULL,
accepted_events BIGINT NOT NULL CHECK(accepted_events>=0),PRIMARY KEY(tenant_id,project_id,period_start));`

const managedArtifactOwnershipSchema = `CREATE TABLE managed_artifact_ownership (
tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('export','pipeline_unpublished')),
storage_id TEXT NOT NULL,object_key TEXT NOT NULL,upload_id TEXT NOT NULL,
PRIMARY KEY(tenant_id,project_id,kind,storage_id,object_key,upload_id));`

var localControlSchema = schemaStore{name: "control", steps: []string{controlSchema + localControlSchemaV1, tenantDeletionSchema, managedArtifactOwnershipSchema}, adopt: func(ctx context.Context, tx *sql.Tx) (bool, error) {
	return sqliteMetadataHas(ctx, tx, "local_control_source_id", "")
}}

// sharedControlSteps follow the same rules for shared PostgreSQL control.
// Version 1 is idempotent because workers of that build reapplied it on start.
var sharedControlSteps = []string{controlSchema + sharedControlSchema, tenantDeletionSchema, managedArtifactOwnershipSchema}

// sqliteMetadataHas reports whether metadata holds key, and value when one is given.
func sqliteMetadataHas(ctx context.Context, tx *sql.Tx, key, value string) (bool, error) {
	var table bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='metadata')`).Scan(&table); err != nil || !table {
		return false, err
	}
	var found bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM metadata WHERE key=? AND (?='' OR value=?))`, key, value, value).Scan(&found)
	return found, err
}

func schemaTooNew(store string, version, latest int) error {
	return fmt.Errorf("%s database schema version %d is newer than this server supports (%d); run a newer release", store, version, latest)
}

// migrateSQLite brings one SQLite database to the latest version in a single
// transaction. PRAGMA user_version is part of that transaction.
func migrateSQLite(ctx context.Context, db *sql.DB, store schemaStore) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	latest := len(store.steps)
	if version > latest {
		return schemaTooNew(store.name, version, latest)
	}
	if version == latest {
		return nil
	}
	if version == 0 {
		var tables int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&tables); err != nil {
			return err
		}
		if tables > 0 {
			adopted, err := store.adopt(ctx, tx)
			if err != nil {
				return err
			}
			if !adopted {
				return fmt.Errorf("unversioned %s database is unsupported; start with an empty data directory", store.name)
			}
			version = 1
		}
	}
	for ; version < latest; version++ {
		if _, err := tx.ExecContext(ctx, store.steps[version]); err != nil {
			return fmt.Errorf("apply %s schema version %d: %w", store.name, version+1, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=`+strconv.Itoa(latest)); err != nil {
		return err
	}
	return tx.Commit()
}

// migrateSharedControl runs inside the caller's transaction, which already
// holds the shared initialization advisory lock.
func migrateSharedControl(ctx context.Context, tx *sql.Tx) error {
	latest := len(sharedControlSteps)
	version := 0
	var hasMetadata bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='metadata')`).Scan(&hasMetadata); err != nil {
		return err
	}
	if hasMetadata {
		var raw string
		err := tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='schema_version'`).Scan(&raw)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return errors.New("unversioned shared control schema is unsupported; use an empty dedicated schema")
		case err != nil:
			return err
		default:
			if version, err = strconv.Atoi(raw); err != nil || version < 1 {
				return errors.New("invalid shared control schema version")
			}
		}
	}
	if version > latest {
		return schemaTooNew("shared control", version, latest)
	}
	if version == 0 {
		var occupied bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=current_schema() AND c.relkind IN ('r','p','v','m','f','S')
)`).Scan(&occupied); err != nil {
			return err
		}
		if occupied {
			return errors.New("unversioned shared control schema is unsupported; use an empty dedicated schema")
		}
		if _, err := tx.ExecContext(ctx, sharedControlSteps[0]); err != nil {
			return fmt.Errorf("apply shared control schema version 1: %w", err)
		}
		version = 1
	}
	for ; version < latest; version++ {
		if _, err := tx.ExecContext(ctx, sharedControlSteps[version]); err != nil {
			return fmt.Errorf("apply shared control schema version %d: %w", version+1, err)
		}
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('schema_version',$1) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, strconv.Itoa(latest))
	return err
}
