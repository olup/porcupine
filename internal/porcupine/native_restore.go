package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

// NativeRestoreRequest is an offline operator contract. It is read from private
// stdin, never from command-line arguments or a running management endpoint.
type NativeRestoreRequest struct {
	BackupID                     string `json:"backup_id"`
	SourceID                     string `json:"source_id"`
	LocalBundlePath              string `json:"local_bundle_path,omitempty"`
	TargetDBPath                 string `json:"target_db_path"`
	TargetControlDBPath          string `json:"target_control_db_path,omitempty"`
	TargetDuckLakeDirectory      string `json:"target_ducklake_directory"`
	ResetCredentialsAcknowledged bool   `json:"reset_credentials_acknowledged"`
	AuthMode                     string `json:"auth_mode,omitempty"`
	BootstrapToken               string `json:"bootstrap_token,omitempty"`
	ChownToNonroot               bool   `json:"chown_to_nonroot,omitempty"`
}
type NativeRestoreConfig struct {
	Request  NativeRestoreRequest
	Backup   lakeapi.NativeBackupConfig
	DuckLake lakeapi.DuckLakeConfig
	Auth     *AuthConfig
}
type NativeRestoreIngestionKey struct {
	ProjectID string `json:"project_id"`
	SourceID  string `json:"source_id,omitempty"`
	Token     string `json:"token"`
}
type NativeRestoreResult struct {
	BackupID        string                      `json:"backup_id"`
	SourceID        string                      `json:"source_id"`
	RecoveryAdminID string                      `json:"recovery_admin_id,omitempty"`
	IngestionKeys   []NativeRestoreIngestionKey `json:"ingestion_keys"`
	Warning         string                      `json:"warning"`
}

func (r NativeRestoreRequest) String() string   { return "NativeRestoreRequest{credentials:redacted}" }
func (r NativeRestoreRequest) GoString() string { return r.String() }

func nativeRestoreAuthMode(request NativeRestoreRequest, destination *AuthConfig) (string, error) {
	mode := request.AuthMode
	if destination != nil {
		if mode != "" && mode != destination.Mode {
			return "", errors.New("restore auth_mode differs from destination authentication configuration")
		}
		mode = destination.Mode
	}
	if mode == "" && destination == nil {
		mode = "builtin"
	}
	if mode != "builtin" && mode != "external" {
		return "", errors.New("restore auth_mode must be builtin or external")
	}
	return mode, nil
}

func normalizeNativeRestorePaths(r *NativeRestoreRequest) error {
	if r.TargetControlDBPath == "" {
		r.TargetControlDBPath = localControlPath(r.TargetDBPath)
	}
	for _, p := range []*string{&r.TargetDBPath, &r.TargetControlDBPath, &r.TargetDuckLakeDirectory, &r.LocalBundlePath} {
		if *p == "" {
			continue
		}
		if !filepath.IsAbs(*p) {
			return errors.New("restore paths must be absolute")
		}
		current := filepath.Clean(*p)
		var missing []string
		for {
			_, err := os.Lstat(current)
			if err == nil {
				break
			}
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			missing = append(missing, filepath.Base(current))
			current = filepath.Dir(current)
		}
		resolved, err := filepath.EvalSymlinks(current)
		if err != nil {
			return err
		}
		for i := len(missing) - 1; i >= 0; i-- {
			resolved = filepath.Join(resolved, missing[i])
		}
		*p = resolved
	}
	return nil
}

func validateNativeRestoreRequest(r NativeRestoreRequest) error {
	if err := normalizeNativeRestorePaths(&r); err != nil {
		return err
	}
	if !lakeapi.ValidNativeBackupID(r.BackupID) {
		return errors.New("backup_id is required")
	}
	if !r.ResetCredentialsAcknowledged {
		return errors.New("restore requires explicit acknowledgment that all credentials and outbound work will be reset")
	}
	mode, err := nativeRestoreAuthMode(r, nil)
	if err != nil {
		return err
	}
	if mode == "external" && r.BootstrapToken != "" {
		return errors.New("external restore recovers through an external JWT operator and cannot accept a bootstrap_token")
	}
	if mode == "builtin" && (len(r.BootstrapToken) < 32 || strings.TrimSpace(r.BootstrapToken) != r.BootstrapToken) {
		return errors.New("a fresh private recovery bootstrap token of at least 32 characters is required")
	}
	if !filepath.IsAbs(r.TargetDBPath) || !filepath.IsAbs(r.TargetControlDBPath) || !filepath.IsAbs(r.TargetDuckLakeDirectory) {
		return errors.New("fresh absolute SQLite and DuckLake target paths are required")
	}
	if nativeRestorePathsOverlap(r.TargetDBPath, r.TargetControlDBPath) || nativeRestorePathsOverlap(r.TargetDBPath, r.TargetDuckLakeDirectory) || nativeRestorePathsOverlap(r.TargetControlDBPath, r.TargetDuckLakeDirectory) {
		return errors.New("SQLite target must be outside the DuckLake target directory")
	}
	for _, path := range []string{r.TargetDBPath, r.TargetDBPath + "-wal", r.TargetDBPath + "-shm", r.TargetDBPath + ".native-restore-incomplete", r.TargetControlDBPath, r.TargetControlDBPath + "-wal", r.TargetControlDBPath + "-shm", r.TargetControlDBPath + ".native-restore-incomplete", r.TargetDuckLakeDirectory} {
		if _, err := os.Lstat(path); err == nil {
			return errors.New("restore target already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if r.LocalBundlePath != "" {
		if !filepath.IsAbs(r.LocalBundlePath) {
			return errors.New("local bundle path must be absolute")
		}
		if nativeRestorePathsOverlap(r.LocalBundlePath, r.TargetDBPath) || nativeRestorePathsOverlap(r.LocalBundlePath, r.TargetControlDBPath) || nativeRestorePathsOverlap(r.LocalBundlePath, r.TargetDuckLakeDirectory) {
			return errors.New("restore source and target paths overlap")
		}
	}

	return nil
}
func nativeRestorePathsOverlap(a, b string) bool {
	a, b = filepath.Clean(a), filepath.Clean(b)
	rel, err := filepath.Rel(a, b)
	if err == nil && (rel == "." || filepath.IsLocal(rel)) {
		return true
	}
	rel, err = filepath.Rel(b, a)
	return err == nil && (rel == "." || filepath.IsLocal(rel))
}

// verifyRestoreInventory streams every object and only stages small catalog and
// SQLite metadata. Analytical S3 data stays in the archive until queried.
func verifyRestoreInventory(ctx context.Context, r lakeapi.NativeBackupReader, m lakeapi.NativeBackupManifest, stage string) error {
	if remote, ok := r.(*lakeapi.NativeBackupS3); ok {
		prefix := remote.Namespace + m.ID + "/"
		keys, err := remote.List(ctx, prefix)
		if err != nil {
			return err
		}
		expected := map[string]bool{"manifest.json": true}
		for _, f := range m.Files {
			expected[f.Path] = true
		}
		for _, key := range keys {
			path := strings.TrimPrefix(key, prefix)
			if !expected[path] {
				return errors.New("backup S3 bundle contains an unlisted object")
			}
			delete(expected, path)
		}
		if len(expected) != 0 {
			return errors.New("backup S3 bundle is missing an inventory object")
		}
	}
	catalogs := map[string]bool{"acceptance.sqlite": true, "control.sqlite": true, "ducklake/registry.sqlite": true}
	for _, c := range m.Catalogs {
		catalogs[c.Path] = true
	}
	for _, f := range m.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		body, err := r.Open(ctx, m.ID, f)
		if err != nil {
			return err
		}
		hash := sha256.New()
		var out *os.File
		writer := io.Writer(hash)
		if catalogs[f.Path] {
			path := filepath.Join(stage, filepath.FromSlash(f.Path))
			if err = os.MkdirAll(filepath.Dir(path), 0700); err == nil {
				out, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			}
			if err != nil {
				body.Close()
				return err
			}
			writer = io.MultiWriter(hash, out)
		}
		n, copyErr := io.CopyBuffer(writer, io.LimitReader(body, f.Bytes+1), make([]byte, 128<<10))
		err = errors.Join(copyErr, body.Close())
		if out != nil {
			err = errors.Join(err, out.Sync(), out.Close())
		}
		if err != nil {
			return err
		}
		if n != f.Bytes || hex.EncodeToString(hash.Sum(nil)) != f.SHA256 {
			return errors.New("backup file checksum mismatch")
		}
	}
	return nil
}

// Compare each restored database with the versioned schema it claims, without
// opening or migrating it. The next server start applies any newer steps.
func verifyRestoreSQLiteSchema(ctx context.Context, journal, control *sql.DB) error {
	if err := verifyRestoreDatabaseSchema(ctx, journalSchema, journal); err != nil {
		return fmt.Errorf("journal: %w", err)
	}
	if err := verifyRestoreDatabaseSchema(ctx, localControlSchema, control); err != nil {
		return fmt.Errorf("control: %w", err)
	}
	return nil
}

func verifyRestoreDatabaseSchema(ctx context.Context, store schemaStore, source *sql.DB) error {
	var version int
	if err := source.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version > len(store.steps) {
		return schemaTooNew("backup "+store.name, version, len(store.steps))
	}
	actual, err := nativeRestoreTableSchemas(ctx, source)
	if err != nil {
		return err
	}
	if version == 0 {
		// Pre-versioning backups equal version 1, except for a table that
		// was created on first project deletion.
		version = 1
		delete(actual, "project_deletion_files")
	}
	reference, err := openLocalSQLite(":memory:")
	if err != nil {
		return err
	}
	defer reference.Close()
	at := store
	at.steps = store.steps[:version]
	if err := migrateSQLite(ctx, reference, at); err != nil {
		return err
	}
	expected, err := nativeRestoreTableSchemas(ctx, reference)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, actual) {
		for name, wanted := range expected {
			if actual[name] != wanted {
				return fmt.Errorf("backup SQLite schema differs from version %d for table %s", version, name)
			}
		}
		for name := range actual {
			if _, ok := expected[name]; !ok {
				return fmt.Errorf("backup SQLite schema has unknown table %s for version %d", name, version)
			}
		}
		return fmt.Errorf("backup SQLite schema differs from version %d", version)
	}
	return nil
}
func nativeRestoreTableSchemas(ctx context.Context, db *sql.DB) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var names []string
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		names = append(names, name)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, name := range names {
		var schema string
		err = db.QueryRowContext(ctx, `SELECT coalesce(json_group_array(json_array(name,type,"notnull",dflt_value,pk)),'[]') FROM (SELECT * FROM pragma_table_info(?) ORDER BY name)`, name).Scan(&schema)
		if err != nil {
			return nil, err
		}
		result[name] = schema
	}
	return result, nil
}

func resetNativeRestoreCredentials(ctx context.Context, journal, control *sql.DB, bootstrap, mode string) (NativeRestoreResult, error) {
	result := NativeRestoreResult{IngestionKeys: []NativeRestoreIngestionKey{}, Warning: "Physical DuckLake snapshot history is rebuilt. Data deleted after the checkpoint cannot be inferred or reconciled by this restore."}
	mode, err := nativeRestoreAuthMode(NativeRestoreRequest{AuthMode: mode}, nil)
	if err != nil {
		return result, err
	}
	if mode == "external" && bootstrap != "" {
		return result, errors.New("external restore cannot accept a bootstrap_token")
	}
	if err := checkControlAuthMode(ctx, control, mode == "external"); err != nil {
		return result, err
	}
	for _, store := range []struct {
		db     *sql.DB
		schema schemaStore
	}{{journal, journalSchema}, {control, localControlSchema}} {
		if err := migrateSQLite(ctx, store.db, store.schema); err != nil {
			return result, err
		}
	}
	tx, err := control.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	if mode == "builtin" {
		for _, credential := range []struct{ table, column string }{
			{"access_keys", "token_hash"}, {"oauth_clients", "secret_hash"}, {"oauth_tokens", "token_hash"},
			{"oauth_refresh_tokens", "token_hash"}, {"oauth_authorization_codes", "code_hash"}, {"oauth_authorization_requests", "request_hash"},
		} {

			var duplicate bool
			if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+credential.table+" WHERE "+credential.column+"=?)", hash(bootstrap)).Scan(&duplicate); err != nil {
				return result, err
			}
			if duplicate {
				return result, errors.New("recovery bootstrap token must differ from every archived credential")
			}
		}
	}
	if err := preserveNativeRestoreArtifactHolds(ctx, journal, tx); err != nil {
		return result, err
	}
	// Commit the safety inventory before any reset can discard the journal's
	// last object reference. A crash between these transactions is retry-safe.
	if err := tx.Commit(); err != nil {
		return result, err
	}
	tx, err = control.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, `SELECT k.name,k.projects,k.source_id FROM access_keys k WHERE k.revoked_at IS NULL AND json_array_length(k.permissions)=1 AND json_extract(k.permissions,'$[0]')='events:write' AND json_array_length(k.projects)=1 AND NOT EXISTS(SELECT 1 FROM oauth_clients c WHERE c.client_id=k.id) AND NOT EXISTS(SELECT 1 FROM service_api_keys s WHERE s.principal_id=k.id)`)
	if err != nil {
		return result, err
	}
	type ingestion struct{ name, project, source string }
	var keys []ingestion
	for rows.Next() {
		var name, projects, source string
		if err = rows.Scan(&name, &projects, &source); err != nil {
			rows.Close()
			return result, err
		}
		var scope []string
		if json.Unmarshal([]byte(projects), &scope) != nil || len(scope) != 1 {
			rows.Close()
			return result, errors.New("invalid archived ingestion key scope")
		}
		keys = append(keys, ingestion{name, scope[0], source})
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,?),token_hash='restored-revoked-'||id`, now()); err != nil {
		return result, err
	}
	tables := []string{"service_api_keys", "tenant_credentials", "oauth_clients"}
	if mode == "builtin" {
		tables = []string{"oauth_interactive_access", "oauth_authorization_codes", "oauth_refresh_tokens", "oauth_authorization_requests", "oauth_tokens", "oauth_interactive_grants", "service_api_keys", "tenant_credentials", "oauth_clients", "oidc_identities", "email_humans"}
	}
	for _, table := range tables {

		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return result, fmt.Errorf("reset restored %s: %w", table, err)
		}
	}

	if mode == "builtin" {
		result.RecoveryAdminID = eventapi.NewID()
		if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'recovery bootstrap admin',?,'["instance:admin"]','["*"]',?)`, result.RecoveryAdminID, hash(bootstrap), now()); err != nil {
			return result, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('oauth_bootstrap_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, result.RecoveryAdminID); err != nil {
			return result, err
		}
	} else {
		result.Warning += " Recover administration with an external JWT operator; no local recovery administrator was created."
	}
	for _, key := range keys {
		var mixed bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id=? AND ingestion_mode='mixed')`, key.project).Scan(&mixed); err != nil {
			return result, err
		}
		if !mixed {
			continue
		}
		raw := token()
		scope, _ := json.Marshal([]string{key.project})
		keyID := eventapi.NewID()
		if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,source_id) VALUES(?,?,?,'["events:write"]',?,?,?)`, keyID, key.name, hash(raw), string(scope), now(), key.source); err != nil {
			return result, err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO tenant_credentials(principal_id,tenant_id) SELECT ?,tenant_id FROM projects WHERE id=?`, keyID, key.project); err != nil {
			return result, err
		}
		result.IngestionKeys = append(result.IngestionKeys, NativeRestoreIngestionKey{key.project, key.source, raw})
	}
	if err := resetNativeRestoreJournal(ctx, journal); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('usage_epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, usage.NewID()); err != nil {
		return result, err
	}
	return result, tx.Commit()
}

func resetNativeRestoreJournal(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"webhook_members", "webhook_deliveries", "webhook_cooldowns", "pipeline_s3_files", "pipeline_s3_runs", "export_files", "export_external_authority", "project_deletion_files", "import_upload_samples", "import_upload_files"} {
		if table == "project_deletion_files" {
			var exists bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)`, table).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return fmt.Errorf("reset restored %s: %w", table, err)
		}
	}
	for _, statement := range []string{
		`UPDATE pipelines SET state='paused',secret=X''`,
		`UPDATE export_jobs SET state='canceled',cancel_requested=1,attempt_id='',cleanup_pending=0,file_count=0,error='cancelled by offline credential reset'`,
		`UPDATE import_uploads SET state='cancelled',error='cancelled by offline credential reset' WHERE state NOT IN ('completed','failed','cancelled')`,
		`DELETE FROM webhook_queue_usage`,
		`DELETE FROM metadata WHERE key='native_backup_writer_id'`,
		`INSERT INTO metadata(key,value) VALUES('native_restore_credentials_reset','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('usage_source_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, usage.NewID()); err != nil {
		return err
	}
	return tx.Commit()
}
