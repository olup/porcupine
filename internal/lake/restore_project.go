package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

func RestoreNativeProject(ctx context.Context, target *DuckLakeStore, reader NativeBackupReader, m NativeBackupManifest, c NativeBackupCatalog, stage string) error {
	legacy, err := validateNativeRestoreCatalog(ctx, reader, m, c, stage, target.Config.Encrypted)
	if err != nil {
		return err
	}
	project, err := target.openProjectInitialized(ctx, c.ProjectID, func(ctx context.Context, db *sql.DB) error {
		dataPath := ""
		archiveCA := ""
		remoteArchive := false
		var targetCA string
		var targetTimeout int64
		if target.Remote != nil {
			if err := db.QueryRowContext(ctx, "SELECT current_setting('ca_cert_file'),current_setting('http_timeout')").Scan(&targetCA, &targetTimeout); err != nil {
				return err
			}
		}
		switch archive := reader.(type) {
		case *LocalNativeBackupReader:
			dataPath = filepath.Join(archive.directory, m.ID, filepath.FromSlash(c.DataRoot))
		case *NativeBackupS3:
			archiveConfig := archive.config
			if target.Remote != nil {
				combined, err := nativeRestoreCombinedTrust(stage, archive.config.CACertFile, target.Remote.Config.CACertFile)
				if err != nil {
					return err
				}
				archiveConfig.CACertFile = combined
			}
			helper := &DuckLakeObjectStore{Config: archiveConfig, Namespace: archive.Namespace + m.ID + "/ducklake/", client: archive.client, transport: archive.transport}
			if configuredCA, err := helper.configureNamed(ctx, db, c.ProjectID, "pc_restore_archive"); err != nil {
				return err
			} else {
				archiveCA = configuredCA
			}
			dataPath = "s3://" + archive.config.Bucket + "/" + archive.Namespace + m.ID + "/" + c.DataRoot
			remoteArchive = true
		default:
			return errors.New("unsupported native backup reader")
		}
		if target.Remote != nil {
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
		directories := []string{stage + string(os.PathSeparator), target.ProjectDirectory(c.ProjectID) + string(os.PathSeparator), strings.TrimRight(dataPath, "/") + "/"}
		if target.Remote != nil {
			directories = append(directories, target.Remote.projectPath(c.ProjectID))
		}
		quoted := make([]string, len(directories))
		for i, path := range directories {
			quoted[i] = LakeLiteral(path)
		}
		var paths []string
		for _, path := range []string{targetCA, archiveCA} {
			if path != "" {
				paths = append(paths, LakeLiteral(path))
			}
		}
		for _, statement := range []string{"SET allowed_directories=[" + strings.Join(quoted, ",") + "]", "SET allowed_paths=[" + strings.Join(paths, ",") + "]", "SET enable_external_access=false"} {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		statement := "ATTACH " + LakeLiteral("ducklake:"+filepath.Join(stage, filepath.FromSlash(c.Path))) + " AS archive (DATA_PATH " + LakeLiteral(dataPath) + ", OVERRIDE_DATA_PATH true, READ_ONLY)"
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
			if err = copyNativeRestoreTable(ctx, db, table, legacy); err != nil {
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
	if err = project.DB.Close(); err != nil {
		return err
	}
	// A new engine establishes the final target-only allowlist. This also proves
	// the catalog opens normally without the archive attachment or its secret.
	verified, err := target.openProject(ctx, c.ProjectID)
	if err != nil {
		return err
	}
	return verified.DB.Close()
}

// Validate the source before creating a target catalog. A dormant project can
// still contain the previous application layout when a new binary captures it.
// Attach its catalog read-only and translate rows while copying, leaving the
// verified archive unchanged.
func validateNativeRestoreCatalog(ctx context.Context, reader NativeBackupReader, m NativeBackupManifest, c NativeBackupCatalog, stage string, targetEncrypted bool) (legacy bool, result error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return false, err
	}
	db.SetMaxOpenConns(1)
	defer func() { result = errors.Join(result, db.Close()) }()
	for _, statement := range []string{"SET memory_limit='64MiB'", "SET threads=1", "SET allow_persistent_secrets=false"} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return false, err
		}
	}
	if _, err := db.ExecContext(ctx, "LOAD ducklake"); err != nil {
		if _, err := db.ExecContext(ctx, "INSTALL ducklake; LOAD ducklake"); err != nil {
			return false, errors.New("DuckLake extension is unavailable")
		}
	}
	var dataPath, caFile string
	switch archive := reader.(type) {
	case *LocalNativeBackupReader:
		dataPath = filepath.Join(archive.directory, m.ID, filepath.FromSlash(c.DataRoot))
	case *NativeBackupS3:
		helper := &DuckLakeObjectStore{Config: archive.config, Namespace: archive.Namespace + m.ID + "/ducklake/", client: archive.client, transport: archive.transport}
		caFile, err = helper.configureNamed(ctx, db, c.ProjectID, "pc_restore_archive")
		if err != nil {
			return false, err
		}
		dataPath = "s3://" + archive.config.Bucket + "/" + archive.Namespace + m.ID + "/" + c.DataRoot
	default:
		return false, errors.New("unsupported native backup reader")
	}
	directories := []string{LakeLiteral(stage + string(os.PathSeparator)), LakeLiteral(strings.TrimRight(dataPath, "/") + "/")}
	paths := "[]"
	if caFile != "" {
		paths = "[" + LakeLiteral(caFile) + "]"
	}
	for _, statement := range []string{
		"SET allowed_directories=[" + strings.Join(directories, ",") + "]", "SET allowed_paths=" + paths, "SET enable_external_access=false",
		"ATTACH " + LakeLiteral("ducklake:"+filepath.Join(stage, filepath.FromSlash(c.Path))) + " AS archive (DATA_PATH " + LakeLiteral(dataPath) + ", OVERRIDE_DATA_PATH true, READ_ONLY)",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return false, err
		}
	}
	var encrypted bool
	if err := db.QueryRowContext(ctx, "SELECT encrypted FROM duckdb_databases() WHERE database_name='archive'").Scan(&encrypted); err != nil {
		return false, err
	}
	if encrypted && !targetEncrypted {
		return false, errors.New("encrypted DuckLake backup requires encryption on the restore target")
	}
	version, err := applicationCatalogVersion(ctx, db, "archive", false)
	if err != nil {
		return false, err
	}
	if version >= hostedFenceSchemaVersion {
		return false, errors.New("hosted catalog restore requires the hosted identity and commit-proof recovery protocol; standalone restore cannot discard that authority")
	}
	if err := db.QueryRowContext(ctx, "SELECT count(*)>0 FROM duckdb_columns() WHERE database_name='archive' AND table_name='current_events' AND column_name='payload'").Scan(&legacy); err != nil {
		return false, err
	}
	return legacy, nil
}

// DuckDB scans the archive and writes the new persistent target in one engine.
// Exec exposes only the affected-row count, avoiding driver result materialization
// and repeated remote scans from paginating an unordered analytical table.
func copyNativeRestoreTable(ctx context.Context, db *sql.DB, table string, legacy bool) error {
	sourceColumns, err := nativeRestoreLakeColumns(ctx, db, "archive", table)
	if err != nil {
		return err
	}
	targetColumns, err := nativeRestoreLakeColumns(ctx, db, "lake", table)
	if err != nil {
		return err
	}
	source := make(map[string]string, len(sourceColumns))
	for _, column := range sourceColumns {
		name, kind, _ := strings.Cut(column, ":")
		source[name] = kind
	}
	if legacy && table == "current_events" {
		delete(source, "payload")
		source["original_snapshot"] = "VARCHAR"
	}
	if len(source) != len(targetColumns) {
		return errors.New("backup analytical schema differs from this release")
	}
	names, expressions := make([]string, len(targetColumns)), make([]string, len(targetColumns))
	for i, column := range targetColumns {
		name, kind, _ := strings.Cut(column, ":")
		if source[name] != kind {
			return errors.New("backup analytical schema differs from this release")
		}
		names[i] = `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		expressions[i] = names[i]
		if legacy && table == "current_events" && name == "original_snapshot" {
			expressions[i] = "CASE WHEN revision>0 THEN coalesce(json_extract_string(payload,'$.OriginalSnapshot'),'') ELSE '' END"
		}
	}
	result, err := db.ExecContext(ctx, "INSERT INTO lake."+table+" ("+strings.Join(names, ",")+") SELECT "+strings.Join(expressions, ",")+" FROM archive."+table)
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
