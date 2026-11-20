package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// RestoreNativeBackup reconstructs current native state in a fresh namespace.
// The marker remains after any failure, so normal startup cannot use a partial
// recovery. The source is only attached READ_ONLY; target DATA_PATH is persisted
// by normal store creation, never by changing private DuckLake catalog metadata.
func RestoreNativeBackup(ctx context.Context, c NativeRestoreConfig) (result NativeRestoreResult, err error) {
	r := c.Request
	r.AuthMode, err = nativeRestoreAuthMode(r, c.Auth)
	if err != nil {
		return result, err
	}
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
		c.Backup = lakeapi.NativeBackupConfig{Directory: filepath.Dir(filepath.Clean(r.LocalBundlePath))}
	}
	if c.Backup.S3 == nil && r.LocalBundlePath == "" {
		return result, errors.New("local_bundle_path is required for a local backup")
	}
	reader, err := lakeapi.NewNativeBackupReader(ctx, c.Backup, r.SourceID)
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
	if err = lakeapi.ValidateNativeBackupManifest(manifest); err != nil {
		return result, err
	}
	_, remoteArchive := reader.(*lakeapi.NativeBackupS3)
	if err = validateNativeRestoreSourceStorage(c.DuckLake.S3, manifest.SourceStorage, remoteArchive); err != nil {
		return result, err
	}
	if err = validateNativeRestoreS3Target(ctx, c.DuckLake.S3, c.Backup.S3); err != nil {
		return result, err
	}
	markers := []string{r.TargetDBPath + ".native-restore-incomplete", r.TargetControlDBPath + ".native-restore-incomplete"}
	for _, marker := range markers {
		if err = os.MkdirAll(filepath.Dir(marker), 0700); err != nil {
			return result, err
		}
		incomplete, openErr := os.OpenFile(marker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if openErr != nil {
			return result, openErr
		}
		_, err = incomplete.WriteString("Offline native restore has not completed. Do not start this target.\n")
		err = errors.Join(err, incomplete.Sync(), incomplete.Close())
		if err != nil {
			return result, err
		}
		if err = lakeapi.SyncBackupDirectory(filepath.Dir(marker)); err != nil {
			return result, err
		}
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
	if err = lakeapi.ValidateBackupSQLite(ctx, sqlitePath); err != nil {
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
	controlPath := filepath.Join(stage, "control.sqlite")
	if err = lakeapi.ValidateBackupSQLite(ctx, controlPath); err != nil {
		return result, err
	}
	if err = lakeapi.VerifyNativeBackupPair(ctx, sqlitePath, controlPath); err != nil {
		return result, err
	}
	control, err := sql.Open("sqlite", controlPath+"?_pragma=foreign_keys(1)")
	if err != nil {
		return result, err
	}
	defer func() {
		if control != nil {
			err = errors.Join(err, control.Close())
		}
	}()
	control.SetMaxOpenConns(1)
	if err = verifyRestoreSQLiteSchema(ctx, db, control); err != nil {
		return result, err
	}
	if err = verifyNativeRestoreRegistry(ctx, stage, manifest); err != nil {
		return result, err
	}
	result, err = resetNativeRestoreCredentials(ctx, db, control, r.BootstrapToken, r.AuthMode)
	if err != nil {
		return result, err
	}
	if err = db.Close(); err != nil {
		return result, err
	}
	db = nil
	if err = control.Close(); err != nil {
		return result, err
	}
	control = nil
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
	lake, err := lakeapi.NewDuckLake(c.DuckLake)
	if err != nil {
		return result, err
	}
	for _, catalog := range manifest.Catalogs {
		if err = lakeapi.RestoreNativeProject(ctx, lake, reader, manifest, catalog, stage); err != nil {
			lake.Close()
			return result, err
		}
	}
	if err = lake.Close(); err != nil {
		return result, err
	}
	if err = lakeapi.ValidateBackupSQLite(ctx, sqlitePath); err != nil {
		return result, err
	}
	// Both SQLite files stay unpublished until the reconstructed catalogs close.
	if err = lakeapi.ValidateBackupSQLite(ctx, controlPath); err != nil {
		return result, err
	}
	if err = lakeapi.CopyCheckpointFile(ctx, controlPath, r.TargetControlDBPath); err != nil {
		return result, err
	}
	if err = os.Rename(sqlitePath, r.TargetDBPath); err != nil {
		return result, err
	}
	if err = os.Chmod(r.TargetDBPath, 0600); err != nil {
		return result, err
	}
	if r.ChownToNonroot {
		for _, root := range []string{r.TargetDBPath, r.TargetControlDBPath, r.TargetDuckLakeDirectory} {
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
		for _, path := range []string{r.TargetDBPath, r.TargetControlDBPath} {
			if err = os.Chown(filepath.Dir(path), 65532, 65532); err != nil {
				return result, err
			}
		}
	}
	if err = lakeapi.SyncBackupDirectory(r.TargetDuckLakeDirectory); err != nil {
		return result, err
	}
	if err = lakeapi.SyncBackupDirectory(filepath.Dir(r.TargetDBPath)); err != nil {
		return result, err
	}
	if err = lakeapi.SyncBackupDirectory(filepath.Dir(r.TargetControlDBPath)); err != nil {
		return result, err
	}
	for i := len(markers) - 1; i >= 0; i-- {
		if err = os.Remove(markers[i]); err != nil {
			return result, err
		}
		if err = lakeapi.SyncBackupDirectory(filepath.Dir(markers[i])); err != nil {
			return result, err
		}
	}
	result.BackupID, result.SourceID = manifest.ID, manifest.SourceID
	return result, nil
}

func verifyNativeRestoreRegistry(ctx context.Context, stage string, m lakeapi.NativeBackupManifest) error {
	path := filepath.Join(stage, "ducklake", "registry.sqlite")
	if err := lakeapi.ValidateBackupSQLite(ctx, path); err != nil {
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

func validateNativeRestoreS3Target(ctx context.Context, target, backup *lakeapi.DuckLakeS3Config) error {
	if target == nil {
		return nil
	}
	normalized, err := lakeapi.NormalizeDuckLakeS3(target)
	if err != nil {
		return err
	}
	if backup != nil {
		backup, err = lakeapi.NormalizeDuckLakeS3(backup)
		if err != nil {
			return err
		}
	}
	if backup != nil && normalized.Bucket == backup.Bucket {
		if nativeRestorePrefixesOverlap(normalized.Prefix, backup.Prefix) {
			return errors.New("backup and restore S3 prefixes overlap")
		}
	}
	client, transport, err := lakeapi.NewNativeS3Client(ctx, *normalized)
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

func validateNativeRestoreSourceStorage(target *lakeapi.DuckLakeS3Config, source lakeapi.NativeBackupStorageIdentity, remoteArchive bool) error {
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
	original, err := lakeapi.NormalizeDuckLakeS3(&lakeapi.DuckLakeS3Config{Bucket: source.Bucket, Prefix: source.Prefix, Region: source.Region, Endpoint: source.Endpoint})
	if err != nil {
		return errors.New("invalid backup original analytical storage identity")
	}
	if target == nil {
		return nil
	}
	normalized, err := lakeapi.NormalizeDuckLakeS3(target)
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
