//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// RestoreNativeBackup reconstructs current native state in a fresh namespace.
// The marker remains after any failure, so normal startup cannot use a partial
// recovery. The source is only attached READ_ONLY; target DATA_PATH is persisted
// by normal store creation, never by changing private DuckLake catalog metadata.
func RestoreNativeBackup(ctx context.Context, c NativeRestoreConfig) (result NativeRestoreResult, err error) {
	r := c.Request
	if err = normalizeNativeRestorePaths(&r); err != nil {
		return result, err
	}
	if err = validateNativeRestoreRequest(r); err != nil {
		return result, err
	}
	if r.ChownToNonroot && os.Geteuid() != 0 {
		return result, errors.New("chown_to_nonroot requires an offline restore process running as root")
	}
	if r.LocalBundlePath != "" {
		if filepath.Base(filepath.Clean(r.LocalBundlePath)) != r.BackupID {
			return result, errors.New("local bundle directory must match backup_id")
		}
		c.Backup = NativeBackupConfig{Directory: filepath.Dir(filepath.Clean(r.LocalBundlePath))}
	}
	if c.Backup.S3 == nil && r.LocalBundlePath == "" {
		return result, errors.New("local_bundle_path is required for a local backup")
	}
	reader, err := newNativeBackupReader(ctx, c.Backup, r.SourceID)
	if err != nil {
		return result, err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}
	manifest, err := reader.Manifest(ctx, r.BackupID)
	if err != nil {
		return result, err
	}
	if manifest.Version != Version {
		return result, errors.New("backup release version differs from this server; native restore does not migrate formats")
	}
	if manifest.ID != r.BackupID || (r.SourceID != "" && manifest.SourceID != r.SourceID) {
		return result, errors.New("backup source identity mismatch")
	}
	if err = validateNativeBackupManifest(manifest); err != nil {
		return result, err
	}
	_, remoteArchive := reader.(*nativeBackupS3)
	if err = validateNativeRestoreSourceStorage(c.DuckLake.S3, manifest.SourceStorage, remoteArchive); err != nil {
		return result, err
	}
	if err = validateNativeRestoreS3Target(ctx, c.DuckLake.S3, c.Backup.S3); err != nil {
		return result, err
	}
	if err = os.MkdirAll(filepath.Dir(r.TargetDBPath), 0700); err != nil {
		return result, err
	}
	marker := r.TargetDBPath + ".native-restore-incomplete"
	incomplete, err := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return result, err
	}
	_, err = incomplete.WriteString("Offline native restore has not completed. Do not start this target.\n")
	err = errors.Join(err, incomplete.Sync(), incomplete.Close())
	if err != nil {
		return result, err
	}
	if err = syncBackupDirectory(filepath.Dir(marker)); err != nil {
		return result, err
	}
	stage, err := os.MkdirTemp(filepath.Dir(r.TargetDBPath), ".native-restore-stage-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(stage)
	if err = verifyRestoreInventory(ctx, reader, manifest, stage); err != nil {
		return result, err
	}
	sqlitePath := filepath.Join(stage, "acceptance.sqlite")
	if err = validateBackupSQLite(ctx, sqlitePath); err != nil {
		return result, err
	}
	db, err := sql.Open("sqlite", sqlitePath+"?_pragma=foreign_keys(1)")
	if err != nil {
		return result, err
	}
	defer func() {
		if db != nil {
			err = errors.Join(err, db.Close())
		}
	}()
	db.SetMaxOpenConns(1)
	var source string
	if err = db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='ducklake_source_id'`).Scan(&source); err != nil || source != manifest.SourceID {
		return result, errors.New("backup acceptance source identity mismatch")
	}
	if err = verifyRestoreSQLiteSchema(ctx, db); err != nil {
		return result, err
	}
	if err = verifyNativeRestoreRegistry(ctx, stage, manifest); err != nil {
		return result, err
	}
	result, err = resetNativeRestoreCredentials(ctx, db, r.BootstrapToken)
	if err != nil {
		return result, err
	}
	if err = db.Close(); err != nil {
		return result, err
	}
	db = nil
	c.DuckLake.Directory = r.TargetDuckLakeDirectory
	if err = os.MkdirAll(c.DuckLake.Directory, 0700); err != nil {
		return result, err
	}
	registry, err := sql.Open("sqlite", filepath.Join(c.DuckLake.Directory, "registry.sqlite"))
	if err != nil {
		return result, err
	}
	_, err = registry.ExecContext(ctx, `CREATE TABLE source(id TEXT PRIMARY KEY);INSERT INTO source(id) VALUES(?)`, manifest.SourceID)
	err = errors.Join(err, registry.Close())
	if err != nil {
		return result, err
	}
	store, err := newDuckLake(c.DuckLake)
	if err != nil {
		return result, err
	}
	lake := store.(*duckLakeStore)
	for _, catalog := range manifest.Catalogs {
		if err = restoreNativeProject(ctx, lake, reader, manifest, catalog, stage); err != nil {
			lake.Close()
			return result, err
		}
	}
	if err = lake.Close(); err != nil {
		return result, err
	}
	if err = validateBackupSQLite(ctx, sqlitePath); err != nil {
		return result, err
	}
	// SQLite stays unpublished until the reconstructed catalogs have all closed.
	if err = os.Rename(sqlitePath, r.TargetDBPath); err != nil {
		return result, err
	}
	if err = os.Chmod(r.TargetDBPath, 0600); err != nil {
		return result, err
	}
	if r.ChownToNonroot {
		for _, root := range []string{r.TargetDBPath, r.TargetDuckLakeDirectory} {
			if err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
				if walkErr != nil {
					return walkErr
				}
				if entry.Type()&os.ModeSymlink != 0 {
					return errors.New("restore created a symbolic link")
				}
				return os.Chown(path, 65532, 65532)
			}); err != nil {
				return result, err
			}
		}
		// The volume mount root is created by Docker as root. The service needs to
		// create SQLite WAL and shared-memory files beside the installed database.
		if err = os.Chown(filepath.Dir(r.TargetDBPath), 65532, 65532); err != nil {
			return result, err
		}
	}
	if err = syncBackupDirectory(r.TargetDuckLakeDirectory); err != nil {
		return result, err
	}
	if err = syncBackupDirectory(filepath.Dir(r.TargetDBPath)); err != nil {
		return result, err
	}
	if err = os.Remove(marker); err != nil {
		return result, err
	}
	if err = syncBackupDirectory(filepath.Dir(marker)); err != nil {
		return result, err
	}
	result.BackupID, result.SourceID = manifest.ID, manifest.SourceID
	return result, nil
}

func verifyNativeRestoreRegistry(ctx context.Context, stage string, m nativeBackupManifest) error {
	path := filepath.Join(stage, "ducklake", "registry.sqlite")
	if err := validateBackupSQLite(ctx, path); err != nil {
		return err
	}
	registry, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path}).String()+"?mode=ro")
	if err != nil {
		return err
	}
	defer registry.Close()
	var source string
	var sourceCount int
	if err = registry.QueryRowContext(ctx, "SELECT count(*),min(id) FROM source").Scan(&sourceCount, &source); err != nil || sourceCount != 1 || source != m.SourceID {
		return errors.New("backup DuckLake registry source mismatch")
	}
	rows, err := registry.QueryContext(ctx, "SELECT project_id FROM catalogs WHERE ready=1 ORDER BY project_id")
	if err != nil {
		return err
	}
	defer rows.Close()
	projects := map[string]bool{}
	for rows.Next() {
		var project string
		if err = rows.Scan(&project); err != nil {
			return err
		}
		projects[project] = true
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if len(projects) != len(m.Catalogs) {
		return errors.New("backup catalog inventory differs from registry")
	}
	for _, catalog := range m.Catalogs {
		if !projects[catalog.ProjectID] {
			return errors.New("backup catalog inventory differs from registry")
		}
	}
	return nil
}

func validateNativeRestoreS3Target(ctx context.Context, target, backup *DuckLakeS3Config) error {
	if target == nil {
		return nil
	}
	normalized, err := normalizeDuckLakeS3(target)
	if err != nil {
		return err
	}
	if backup != nil {
		backup, err = normalizeDuckLakeS3(backup)
		if err != nil {
			return err
		}
	}
	if backup != nil && normalized.Bucket == backup.Bucket {
		if nativeRestorePrefixesOverlap(normalized.Prefix, backup.Prefix) {
			return errors.New("backup and restore S3 prefixes overlap")
		}
	}
	client, transport, err := newNativeS3Client(ctx, *normalized)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	out, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(normalized.Bucket), Prefix: aws.String(normalized.Prefix + "/"), MaxKeys: aws.Int32(1)})
	if err != nil {
		return errors.New("cannot verify fresh analytical S3 restore prefix")
	}
	if len(out.Contents) != 0 || aws.ToBool(out.IsTruncated) {
		return errors.New("analytical S3 restore prefix already contains objects")
	}
	return nil
}

func validateNativeRestoreSourceStorage(target *DuckLakeS3Config, source nativeBackupStorageIdentity, remoteArchive bool) error {
	if source.Mode == "" {
		if target != nil || remoteArchive {
			return errors.New("remote native restore requires the backup's original analytical storage identity")
		}
		return nil
	}
	if source.Mode == "local" {
		if source.Bucket != "" || source.Prefix != "" || source.Region != "" || source.Endpoint != "" {
			return errors.New("invalid local backup source storage identity")
		}
		return nil
	}
	if source.Mode != "s3" {
		return errors.New("unsupported backup source storage mode")
	}
	original, err := normalizeDuckLakeS3(&DuckLakeS3Config{Bucket: source.Bucket, Prefix: source.Prefix, Region: source.Region, Endpoint: source.Endpoint})
	if err != nil {
		return errors.New("invalid backup original analytical storage identity")
	}
	if target == nil {
		return nil
	}
	normalized, err := normalizeDuckLakeS3(target)
	if err != nil {
		return err
	}
	if original.Bucket == normalized.Bucket && nativeRestorePrefixesOverlap(original.Prefix, normalized.Prefix) {
		return errors.New("restore target overlaps the original analytical S3 namespace")
	}
	return nil
}
func nativeRestorePrefixesOverlap(a, b string) bool {
	a, b = strings.Trim(a, "/"), strings.Trim(b, "/")
	return a == b || strings.HasPrefix(a, b+"/") || strings.HasPrefix(b, a+"/")
}

func restoreNativeProject(ctx context.Context, target *duckLakeStore, reader nativeBackupReader, m nativeBackupManifest, c nativeBackupCatalog, stage string) error {
	project, err := target.openProjectInitialized(ctx, c.ProjectID, func(ctx context.Context, db *sql.DB) error {
		dataPath := ""
		archiveCA := ""
		remoteArchive := false
		var targetCA string
		var targetTimeout int64
		if target.remote != nil {
			if err := db.QueryRowContext(ctx, "SELECT current_setting('ca_cert_file'),current_setting('http_timeout')").Scan(&targetCA, &targetTimeout); err != nil {
				return err
			}
		}
		switch archive := reader.(type) {
		case *localNativeBackupReader:
			dataPath = filepath.Join(archive.directory, m.ID, filepath.FromSlash(c.DataRoot))
		case *nativeBackupS3:
			archiveConfig := archive.config
			if target.remote != nil {
				combined, err := nativeRestoreCombinedTrust(stage, archive.config.CACertFile, target.remote.config.CACertFile)
				if err != nil {
					return err
				}
				archiveConfig.CACertFile = combined
			}
			helper := &duckLakeObjectStore{config: archiveConfig, namespace: archive.namespace + m.ID + "/ducklake/", client: archive.client, transport: archive.transport}
			if configuredCA, err := helper.configureNamed(ctx, db, c.ProjectID, "pc_restore_archive"); err != nil {
				return err
			} else {
				archiveCA = configuredCA
			}
			dataPath = "s3://" + archive.config.Bucket + "/" + archive.namespace + m.ID + "/" + c.DataRoot
			remoteArchive = true
		default:
			return errors.New("unsupported native backup reader")
		}
		if target.remote != nil {
			var currentTimeout int64
			if err := db.QueryRowContext(ctx, "SELECT current_setting('http_timeout')").Scan(&currentTimeout); err != nil {
				return err
			}
			if targetTimeout > currentTimeout {
				if _, err := db.ExecContext(ctx, fmt.Sprintf("SET GLOBAL http_timeout=%d", targetTimeout)); err != nil {
					return err
				}
			}
		}
		directories := []string{stage + string(os.PathSeparator), target.projectDirectory(c.ProjectID) + string(os.PathSeparator), strings.TrimRight(dataPath, "/") + "/"}
		if target.remote != nil {
			directories = append(directories, target.remote.projectPath(c.ProjectID))
		}
		quoted := make([]string, len(directories))
		for i, path := range directories {
			quoted[i] = lakeLiteral(path)
		}
		var paths []string
		for _, path := range []string{targetCA, archiveCA} {
			if path != "" {
				paths = append(paths, lakeLiteral(path))
			}
		}
		for _, statement := range []string{"SET allowed_directories=[" + strings.Join(quoted, ",") + "]", "SET allowed_paths=[" + strings.Join(paths, ",") + "]", "SET enable_external_access=false"} {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		statement := "ATTACH " + lakeLiteral("ducklake:"+filepath.Join(stage, filepath.FromSlash(c.Path))) + " AS archive (DATA_PATH " + lakeLiteral(dataPath) + ", OVERRIDE_DATA_PATH true, READ_ONLY)"
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("attach readonly backup catalog: %w", err)
		}
		var identity string
		if err := db.QueryRowContext(ctx, "SELECT id FROM archive.catalog_identity").Scan(&identity); err != nil || identity == "" {
			return errors.New("backup project catalog identity is invalid")
		}
		registry, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: filepath.Join(stage, "ducklake", "registry.sqlite")}).String()+"?mode=ro")
		if err != nil {
			return err
		}
		var expectedIdentity string
		err = registry.QueryRowContext(ctx, "SELECT id FROM catalogs WHERE project_id=? AND ready=1", c.ProjectID).Scan(&expectedIdentity)
		registry.Close()
		if err != nil || identity != expectedIdentity {
			return errors.New("backup project catalog identity differs from registry")
		}
		var sourceVersion, targetVersion string
		if err = db.QueryRowContext(ctx, "SELECT value FROM archive.options() WHERE option_name='version'").Scan(&sourceVersion); err != nil {
			return err
		}
		if err = db.QueryRowContext(ctx, "SELECT value FROM lake.options() WHERE option_name='version'").Scan(&targetVersion); err != nil {
			return err
		}
		if sourceVersion != targetVersion {
			return errors.New("backup DuckLake format version differs from the new target; native restore does not migrate formats")
		}
		for _, table := range []string{"heads", "current_events", "identity_profiles", "identity_links"} {
			if err = copyNativeRestoreTable(ctx, db, table); err != nil {
				return fmt.Errorf("restore project %q table %s: %w", c.ProjectID, table, err)
			}
		}
		if _, err = db.ExecContext(ctx, "DETACH archive"); err != nil {
			return err
		}
		if remoteArchive {
			if _, err = db.ExecContext(ctx, "DROP SECRET pc_restore_archive"); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	// These catalogs are never registered as resident projects in the offline
	// store. Close each one before opening the next to keep resource use bounded.
	if err = project.db.Close(); err != nil {
		return err
	}
	// A new engine establishes the final target-only allowlist. This also proves
	// the catalog opens normally without the archive attachment or its secret.
	verified, err := target.openProject(ctx, c.ProjectID)
	if err != nil {
		return err
	}
	return verified.db.Close()
}

// DuckDB scans the archive and writes the new persistent target in one engine.
// Exec exposes only the affected-row count, avoiding driver result materialization
// and repeated remote scans from paginating an unordered analytical table.
func copyNativeRestoreTable(ctx context.Context, db *sql.DB, table string) error {
	sourceColumns, err := nativeRestoreLakeColumns(ctx, db, "archive", table)
	if err != nil {
		return err
	}
	targetColumns, err := nativeRestoreLakeColumns(ctx, db, "lake", table)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sourceColumns, targetColumns) {
		return errors.New("backup analytical schema differs from this release")
	}
	result, err := db.ExecContext(ctx, "INSERT INTO lake."+table+" SELECT * FROM archive."+table)
	if err != nil {
		return err
	}
	copied, err := result.RowsAffected()
	if err != nil {
		return err
	}
	var count int64
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM lake."+table).Scan(&count); err != nil {
		return err
	}
	if copied != count {
		return errors.New("restored table row count mismatch")
	}
	return nil
}

func nativeRestoreCombinedTrust(stage string, archives, targets string) (string, error) {
	if archives == targets {
		return archives, nil
	}
	var pem []byte
	seen := map[string]bool{}
	for _, path := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem", "/etc/pki/tls/certs/ca-bundle.crt", archives, targets} {
		if path == "" || seen[path] {
			continue
		}
		seen[path] = true
		data, err := os.ReadFile(path)
		if err != nil {
			if path == archives || path == targets {
				return "", errors.New("restore trusted certificate bundle is unavailable")
			}
			continue
		}
		pem = append(pem, data...)
		pem = append(pem, '\n')
	}
	if len(pem) == 0 {
		return "", errors.New("restore trusted certificate bundle is unavailable")
	}
	file, err := os.CreateTemp(stage, "restore-trust-*.pem")
	if err != nil {
		return "", err
	}
	_, err = file.Write(pem)
	err = errors.Join(err, file.Close())
	if err != nil {
		return "", err
	}
	return file.Name(), nil
}

func nativeRestoreLakeColumns(ctx context.Context, db *sql.DB, catalog, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT column_name,data_type FROM information_schema.columns WHERE table_catalog=? AND table_name=? ORDER BY ordinal_position", catalog, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name, kind string
		if err = rows.Scan(&name, &kind); err != nil {
			return nil, err
		}
		columns = append(columns, name+":"+kind)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, errors.New("backup is missing an application analytical table")
	}
	return columns, nil
}
