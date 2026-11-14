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
)

// NativeRestoreRequest is an offline operator contract. It is read from private
// stdin, never from command-line arguments or a running management endpoint.
type NativeRestoreRequest struct {
	BackupID                     string `json:"backup_id"`
	SourceID                     string `json:"source_id"`
	LocalBundlePath              string `json:"local_bundle_path,omitempty"`
	TargetDBPath                 string `json:"target_db_path"`
	TargetDuckLakeDirectory      string `json:"target_ducklake_directory"`
	ResetCredentialsAcknowledged bool   `json:"reset_credentials_acknowledged"`
	BootstrapToken               string `json:"bootstrap_token"`
	ChownToNonroot               bool   `json:"chown_to_nonroot,omitempty"`
}
type NativeRestoreConfig struct {
	Request  NativeRestoreRequest
	Backup   NativeBackupConfig
	DuckLake DuckLakeConfig
}
type NativeRestoreIngestionKey struct {
	ProjectID string `json:"project_id"`
	SourceID  string `json:"source_id,omitempty"`
	Token     string `json:"token"`
}
type NativeRestoreResult struct {
	BackupID        string                      `json:"backup_id"`
	SourceID        string                      `json:"source_id"`
	RecoveryAdminID string                      `json:"recovery_admin_id"`
	IngestionKeys   []NativeRestoreIngestionKey `json:"ingestion_keys"`
	Warning         string                      `json:"warning"`
}

func (r NativeRestoreRequest) String() string   { return "NativeRestoreRequest{credentials:redacted}" }
func (r NativeRestoreRequest) GoString() string { return r.String() }

func normalizeNativeRestorePaths(r *NativeRestoreRequest) error {
	for _, p := range []*string{&r.TargetDBPath, &r.TargetDuckLakeDirectory, &r.LocalBundlePath} {
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
	if !validNativeBackupID(r.BackupID) {
		return errors.New("backup_id is required")
	}
	if !r.ResetCredentialsAcknowledged {
		return errors.New("restore requires explicit acknowledgment that all credentials and outbound work will be reset")
	}
	if len(r.BootstrapToken) < 32 || strings.TrimSpace(r.BootstrapToken) != r.BootstrapToken {
		return errors.New("a fresh private recovery bootstrap token of at least 32 characters is required")
	}
	if !filepath.IsAbs(r.TargetDBPath) || !filepath.IsAbs(r.TargetDuckLakeDirectory) || r.TargetDBPath == r.TargetDuckLakeDirectory {
		return errors.New("fresh absolute SQLite and DuckLake target paths are required")
	}
	if nativeRestorePathsOverlap(r.TargetDBPath, r.TargetDuckLakeDirectory) {
		return errors.New("SQLite target must be outside the DuckLake target directory")
	}
	for _, path := range []string{r.TargetDBPath, r.TargetDBPath + "-wal", r.TargetDBPath + "-shm", r.TargetDBPath + ".native-restore-incomplete", r.TargetDuckLakeDirectory} {
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
		if nativeRestorePathsOverlap(r.LocalBundlePath, r.TargetDBPath) || nativeRestorePathsOverlap(r.LocalBundlePath, r.TargetDuckLakeDirectory) {
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
func verifyRestoreInventory(ctx context.Context, r nativeBackupReader, m nativeBackupManifest, stage string) error {
	if remote, ok := r.(*nativeBackupS3); ok {
		prefix := remote.namespace + m.ID + "/"
		keys, err := remote.list(ctx, prefix)
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
	catalogs := map[string]bool{"acceptance.sqlite": true, "ducklake/registry.sqlite": true}
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

// Compare the current application schema without opening or migrating the
// restored database. Equal release versions alone do not establish compatibility.
func verifyRestoreSQLiteSchema(ctx context.Context, source *sql.DB) error {
	reference, err := Open(Config{DBPath: ":memory:", BootstrapToken: strings.Repeat("r", 32)})
	if err != nil {
		return err
	}
	defer reference.Close()
	expected, err := nativeRestoreTableSchemas(ctx, reference.db)
	if err != nil {
		return err
	}
	actual, err := nativeRestoreTableSchemas(ctx, source)
	if err != nil {
		return err
	}
	// Retired invitations contain no analytical data and are erased in the reset.
	delete(actual, "local_invitations")
	if _, ok := actual["project_deletion_files"]; ok {
		if _, err = reference.db.ExecContext(ctx, `CREATE TABLE project_deletion_files(project_id TEXT NOT NULL, object_key TEXT NOT NULL, upload_id TEXT NOT NULL,PRIMARY KEY(project_id, object_key, upload_id)) WITHOUT ROWID`); err != nil {
			return err
		}
		expected, err = nativeRestoreTableSchemas(ctx, reference.db)
		if err != nil {
			return err
		}
	}
	if !reflect.DeepEqual(expected, actual) {
		for name, wanted := range expected {
			if actual[name] != wanted {
				return fmt.Errorf("backup SQLite application schema differs for table %s; migrations are not supported by native restore", name)
			}
		}
		for name := range actual {
			if _, ok := expected[name]; !ok {
				return fmt.Errorf("backup SQLite application schema has unknown table %s; migrations are not supported by native restore", name)
			}
		}
		return errors.New("backup SQLite application schema differs from this release")
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

func resetNativeRestoreCredentials(ctx context.Context, db *sql.DB, bootstrap string) (NativeRestoreResult, error) {
	result := NativeRestoreResult{IngestionKeys: []NativeRestoreIngestionKey{}, Warning: "Physical DuckLake snapshot history is rebuilt. Data deleted after the checkpoint cannot be inferred or reconciled by this restore."}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()

	for _, credential := range []struct{ table, column string }{
		{"access_keys", "token_hash"}, {"oauth_clients", "secret_hash"}, {"oauth_tokens", "token_hash"},
		{"oauth_refresh_tokens", "token_hash"}, {"oauth_authorization_codes", "code_hash"}, {"oauth_authorization_requests", "request_hash"},
		{"local_invitations", "code_hash"},
	} {
		if credential.table == "local_invitations" {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_invitations')`).Scan(&exists); err != nil {
				return result, err
			}
			if !exists {
				continue
			}
		}
		var duplicate bool
		if err = tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM "+credential.table+" WHERE "+credential.column+"=?)", hash(bootstrap)).Scan(&duplicate); err != nil {
			return result, err
		}
		if duplicate {
			return result, errors.New("recovery bootstrap token must differ from every archived credential")
		}
	}

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
	for _, table := range []string{"oauth_interactive_access", "oauth_authorization_codes", "oauth_refresh_tokens", "oauth_authorization_requests", "oauth_tokens", "oauth_interactive_grants", "service_api_keys", "oauth_clients", "email_humans", "oidc_humans", "webhook_members", "webhook_deliveries", "webhook_cooldowns", "pipeline_s3_files", "pipeline_s3_runs", "export_files", "project_deletion_files", "import_upload_samples", "import_upload_files"} {
		if table == "project_deletion_files" {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)`, table).Scan(&exists); err != nil {
				return result, err
			}
			if !exists {
				continue
			}
		}
		if _, err = tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			return result, fmt.Errorf("reset restored %s: %w", table, err)
		}
	}
	var invitations bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='local_invitations')`).Scan(&invitations); err != nil {
		return result, err
	}
	if invitations {
		if _, err = tx.ExecContext(ctx, "DROP TABLE local_invitations"); err != nil {
			return result, err
		}
	}
	for _, statement := range []string{
		`UPDATE pipelines SET state='paused',secret=X''`,
		`UPDATE export_jobs SET state='canceled',cancel_requested=1,attempt_id='',query_id='',cleanup_pending=0,file_count=0,error='cancelled by offline credential reset'`,
		`UPDATE import_uploads SET state='cancelled',error='cancelled by offline credential reset' WHERE state NOT IN ('completed','failed','cancelled')`,
		`DELETE FROM webhook_queue_usage`,
		`DELETE FROM metadata WHERE key='native_backup_writer_id'`,
		`INSERT INTO metadata(key,value) VALUES('native_restore_credentials_reset','1') ON CONFLICT(key) DO UPDATE SET value=excluded.value`,
	} {
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return result, err
		}
	}
	result.RecoveryAdminID = newID()
	if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'recovery bootstrap admin',?,'["instance:admin"]','["*"]',?)`, result.RecoveryAdminID, hash(bootstrap), now()); err != nil {
		return result, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('oauth_bootstrap_id',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`, result.RecoveryAdminID); err != nil {
		return result, err
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
		if _, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at,source_id) VALUES(?,?,?,'["events:write"]',?,?,?)`, newID(), key.name, hash(raw), string(scope), now(), key.source); err != nil {
			return result, err
		}
		result.IngestionKeys = append(result.IngestionKeys, NativeRestoreIngestionKey{key.project, key.source, raw})
	}
	return result, tx.Commit()
}
