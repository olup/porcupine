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
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const nativeBackupManifestLimit = 16 << 20
const nativeBackupFileLimit = 50000

type backupFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type nativeBackupManifest struct {
	Format            string       `json:"format"`
	ID                string       `json:"id"`
	Version           string       `json:"version"`
	SourceID          string       `json:"source_id"`
	CreatedAt         string       `json:"created_at"`
	CapturedAt        string       `json:"captured_at"`
	SQLitePath        string       `json:"sqlite_path"`
	DuckLakeDirectory string       `json:"ducklake_directory"`
	Files             []backupFile `json:"files"`
}

func hashBackupFile(ctx context.Context, path, relative string) (backupFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return backupFile{}, err
	}
	defer file.Close()
	digest := sha256.New()
	buffer := make([]byte, 128<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return backupFile{}, err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			digest.Write(buffer[:n])
			size += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return backupFile{}, err
		}
	}
	return backupFile{Path: relative, Bytes: size, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func validateBackupSQLite(ctx context.Context, path string) error {
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path}).String()+"?mode=ro")
	if err != nil {
		return err
	}
	defer db.Close()
	var result string
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil || result != "ok" {
		return errors.New("backup SQLite integrity check failed")
	}
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return err
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("backup SQLite foreign key check failed")
	}
	return rows.Err()
}

func writeBackupManifest(directory string, manifest nativeBackupManifest) (result error) {
	file, err := os.OpenFile(filepath.Join(directory, "manifest.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, file.Close()) }()
	if err := json.NewEncoder(file).Encode(manifest); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	// Flush child directory entries before publishing the completed parent.
	return filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return syncBackupDirectory(path)
		}
		return nil
	})
}

func syncBackupDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func pruneNativeBackups(directory, source string, retain int) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	type candidate struct {
		id      string
		created time.Time
	}
	var completed []candidate
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "pc_backup_") {
			continue
		}
		file, err := os.Open(filepath.Join(directory, entry.Name(), "manifest.json"))
		if err != nil {
			continue
		}
		var manifest nativeBackupManifest
		err = json.NewDecoder(io.LimitReader(file, nativeBackupManifestLimit)).Decode(&manifest)
		file.Close()
		if err != nil || manifest.Format != "porcupine-native-v1" || manifest.ID != entry.Name() || manifest.SourceID != source {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
		if err != nil {
			continue
		}
		completed = append(completed, candidate{entry.Name(), created})
	}
	sort.Slice(completed, func(i, j int) bool { return completed[i].created.After(completed[j].created) })
	for _, old := range completed[min(retain, len(completed)):] {
		if err := os.RemoveAll(filepath.Join(directory, old.id)); err != nil {
			return fmt.Errorf("remove expired backup: %w", err)
		}
	}
	return syncBackupDirectory(directory)
}

// verifyNativeBackup checks the complete recovery inventory before publication
// and can be rerun by an offline recovery tool before installing a bundle.
func verifyNativeBackup(ctx context.Context, directory string) (nativeBackupManifest, error) {
	var manifest nativeBackupManifest
	path := filepath.Join(directory, "manifest.json")
	info, err := os.Lstat(path)
	if err != nil {
		return manifest, err
	}
	if !info.Mode().IsRegular() || info.Size() > nativeBackupManifestLimit {
		return manifest, errors.New("backup manifest is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return manifest, err
	}
	decoder := json.NewDecoder(io.LimitReader(file, nativeBackupManifestLimit))
	err = decoder.Decode(&manifest)
	if err == nil {
		var extra any
		if decoder.Decode(&extra) != io.EOF {
			err = errors.New("invalid backup manifest trailer")
		}
	}
	file.Close()
	if err != nil {
		return manifest, err
	}
	if manifest.Format != "porcupine-native-v1" || manifest.SourceID == "" || manifest.ID == "" {
		return manifest, errors.New("unsupported native backup manifest")
	}
	expected := map[string]backupFile{}
	for _, item := range manifest.Files {
		relative := filepath.FromSlash(item.Path)
		if !filepath.IsLocal(relative) || relative == "." || item.Path != filepath.ToSlash(filepath.Clean(relative)) || item.Path == "manifest.json" || item.Bytes < 0 || len(item.SHA256) != 64 {
			return manifest, errors.New("unsafe backup inventory")
		}
		if _, exists := expected[item.Path]; exists {
			return manifest, errors.New("duplicate backup inventory entry")
		}
		expected[item.Path] = item
	}
	for _, required := range []string{"acceptance.sqlite", "ducklake/registry.sqlite"} {
		if _, ok := expected[required]; !ok {
			return manifest, errors.New("incomplete backup inventory")
		}
	}
	err = filepath.WalkDir(directory, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return errors.New("backup contains a non-regular file")
		}
		relative, err := filepath.Rel(directory, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "manifest.json" {
			return nil
		}
		wanted, ok := expected[relative]
		if !ok {
			return fmt.Errorf("backup contains an unlisted file %q", relative)
		}
		actual, err := hashBackupFile(ctx, path, relative)
		if err != nil {
			return err
		}
		if actual.Bytes != wanted.Bytes || actual.SHA256 != wanted.SHA256 {
			return errors.New("backup file checksum mismatch")
		}
		delete(expected, relative)
		return nil
	})
	if err != nil {
		return manifest, err
	}
	if len(expected) > 0 {
		return manifest, errors.New("backup file is missing")
	}
	for _, item := range []struct{ path, query string }{{"acceptance.sqlite", "SELECT value FROM metadata WHERE key='ducklake_source_id'"}, {"ducklake/registry.sqlite", "SELECT id FROM source"}} {
		path := filepath.Join(directory, filepath.FromSlash(item.path))
		if err := validateBackupSQLite(ctx, path); err != nil {
			return manifest, err
		}
		database, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path}).String()+"?mode=ro")
		if err != nil {
			return manifest, err
		}
		var source string
		err = database.QueryRowContext(ctx, item.query).Scan(&source)
		database.Close()
		if err != nil || source != manifest.SourceID {
			return manifest, errors.New("backup SQLite and DuckLake sources do not match")
		}
	}
	// Every ready registry entry needs its catalog, even if an inventory was
	// accidentally generated without that project directory.
	registry, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: filepath.Join(directory, "ducklake", "registry.sqlite")}).String()+"?mode=ro")
	if err != nil {
		return manifest, err
	}
	defer registry.Close()
	rows, err := registry.QueryContext(ctx, "SELECT project_id FROM catalogs WHERE ready=1")
	if err != nil {
		return manifest, err
	}
	defer rows.Close()
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return manifest, err
		}
		digest := sha256.Sum256([]byte(project))
		catalog := filepath.Join(directory, "ducklake", hex.EncodeToString(digest[:]), "catalog.ducklake")
		if _, err := os.Stat(catalog); err != nil {
			return manifest, errors.New("backup is missing a registered project catalog")
		}
	}
	if err := rows.Err(); err != nil {
		return manifest, err
	}
	return manifest, nil
}

// The destination lock excludes any other active writer. Only the reserved
// incomplete namespace is disposable; completed bundles follow retention.
func removeIncompleteNativeBackups(directory string) error {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".pc_backup_") && strings.HasSuffix(entry.Name(), ".incomplete") {
			if err := os.RemoveAll(filepath.Join(directory, entry.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
