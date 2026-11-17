package lake

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
const NativeBackupFileLimit = 50000

type BackupFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type NativeBackupManifest struct {
	Format            string                      `json:"format"`
	ID                string                      `json:"id"`
	Version           string                      `json:"version"`
	SourceID          string                      `json:"source_id"`
	SourceStorage     NativeBackupStorageIdentity `json:"source_storage,omitempty"`
	CreatedAt         string                      `json:"created_at"`
	CapturedAt        string                      `json:"captured_at"`
	ControlSQLitePath string                      `json:"control_sqlite_path"`
	SQLitePath        string                      `json:"sqlite_path"`
	DuckLakeDirectory string                      `json:"ducklake_directory"`
	Files             []BackupFile                `json:"files"`
	Catalogs          []NativeBackupCatalog       `json:"catalogs,omitempty"`
}

func HashBackupFile(ctx context.Context, path, relative string) (BackupFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return BackupFile{}, err
	}
	defer file.Close()
	digest := sha256.New()
	buffer := make([]byte, 128<<10)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			return BackupFile{}, err
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
			return BackupFile{}, err
		}
	}
	return BackupFile{Path: relative, Bytes: size, SHA256: hex.EncodeToString(digest.Sum(nil))}, nil
}

func ValidateBackupSQLite(ctx context.Context, path string) error {
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

func WriteBackupManifest(directory string, manifest NativeBackupManifest) (result error) {
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
			return SyncBackupDirectory(path)
		}
		return nil
	})
}

func SyncBackupDirectory(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func PruneNativeBackups(directory, source string, retain int, protectedID string) error {
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
		var manifest NativeBackupManifest
		err = json.NewDecoder(io.LimitReader(file, nativeBackupManifestLimit)).Decode(&manifest)
		file.Close()
		if err != nil || manifest.Format != "porcupine-native-v2" || manifest.ID != entry.Name() || manifest.SourceID != source {
			continue
		}
		created, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
		if err != nil {
			continue
		}
		completed = append(completed, candidate{entry.Name(), created})
	}
	if protectedID != "" {
		found := false
		for _, candidate := range completed {
			if candidate.id == protectedID {
				found = true
				break
			}
		}
		if !found {
			return errors.New("newly verified backup is missing from the retention inventory")
		}
	}
	sort.Slice(completed, func(i, j int) bool {
		if completed[i].id == protectedID {
			return completed[j].id != protectedID
		}
		if completed[j].id == protectedID {
			return false
		}
		return completed[i].created.After(completed[j].created)
	})
	for _, old := range completed[min(retain, len(completed)):] {
		if err := os.RemoveAll(filepath.Join(directory, old.id)); err != nil {
			return fmt.Errorf("remove expired backup: %w", err)
		}
	}
	return SyncBackupDirectory(directory)
}

// verifyNativeBackup checks the complete recovery inventory before publication
// and can be rerun by an offline recovery tool before installing a bundle.
func VerifyNativeBackup(ctx context.Context, directory string) (NativeBackupManifest, error) {
	var manifest NativeBackupManifest
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
	if manifest.Format != "porcupine-native-v2" || manifest.SourceID == "" || manifest.ID == "" {
		return manifest, errors.New("unsupported native backup manifest")
	}
	if err := ValidateNativeBackupManifest(manifest); err != nil {
		return manifest, err
	}
	expected := map[string]BackupFile{}
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
	for _, required := range []string{"acceptance.sqlite", "control.sqlite", "ducklake/registry.sqlite"} {
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
		actual, err := HashBackupFile(ctx, path, relative)
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
	if err := ValidateBackupSQLite(ctx, filepath.Join(directory, "control.sqlite")); err != nil {
		return manifest, err
	}
	if err := VerifyNativeBackupPair(ctx, filepath.Join(directory, "acceptance.sqlite"), filepath.Join(directory, "control.sqlite")); err != nil {
		return manifest, err
	}
	for _, item := range []struct{ path, query string }{{"acceptance.sqlite", "SELECT value FROM metadata WHERE key='ducklake_source_id'"}, {"ducklake/registry.sqlite", "SELECT id FROM source"}} {
		path := filepath.Join(directory, filepath.FromSlash(item.path))
		if err := ValidateBackupSQLite(ctx, path); err != nil {
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

func VerifyNativeBackupPair(ctx context.Context, journalPath, controlPath string) error {
	paths := []string{journalPath, controlPath}
	metadata := make([]map[string]string, len(paths))
	for i, path := range paths {
		db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path}).String()+"?mode=ro")
		if err != nil {
			return err
		}
		rows, err := db.QueryContext(ctx, `SELECT key,value FROM metadata WHERE key IN ('journal_source_id','local_control_source_id','local_journal_source_id','journal_layout')`)
		if err != nil {
			db.Close()
			return err
		}
		metadata[i] = map[string]string{}
		for rows.Next() {
			var key, value string
			if err = rows.Scan(&key, &value); err != nil {
				break
			}
			metadata[i][key] = value
		}
		err = errors.Join(err, rows.Err(), rows.Close(), db.Close())
		if err != nil {
			return err
		}
	}
	journal, control := metadata[0], metadata[1]
	if journal["journal_layout"] != "separate-control-v1" || journal["journal_source_id"] == "" || control["local_control_source_id"] == "" ||
		journal["journal_source_id"] != control["local_journal_source_id"] || journal["local_control_source_id"] != control["local_control_source_id"] {
		return errors.New("backup control and journal identities do not match")
	}
	return nil
}

// The destination lock excludes any other active writer. Only the reserved
// incomplete namespace is disposable; completed bundles follow retention.
func RemoveIncompleteNativeBackups(directory string) error {
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

func ValidateNativeBackupManifest(m NativeBackupManifest) error {
	if m.Format != "porcupine-native-v2" || !ValidNativeBackupID(m.ID) || m.SourceID == "" || !LakePrefixSegmentPattern.MatchString(m.SourceID) || len(m.Files) > NativeBackupFileLimit {
		return errors.New("unsupported native backup manifest")
	}
	if err := ValidateNativeBackupStorageIdentity(m.SourceStorage, false); err != nil {
		return err
	}
	files := map[string]bool{}
	for _, f := range m.Files {
		if !validBackupFile(f) || files[f.Path] {
			return errors.New("unsafe backup inventory")
		}
		files[f.Path] = true
	}
	if m.SQLitePath != "acceptance.sqlite" || m.ControlSQLitePath != "control.sqlite" || m.DuckLakeDirectory != "ducklake" || !files["acceptance.sqlite"] || !files["control.sqlite"] || !files["ducklake/registry.sqlite"] {
		return errors.New("incomplete backup inventory")
	}
	projects := map[string]bool{}
	for _, c := range m.Catalogs {
		digest := sha256.Sum256([]byte(c.ProjectID))
		root := "ducklake/" + hex.EncodeToString(digest[:])
		if c.ProjectID == "" || projects[c.ProjectID] || c.Path != root+"/catalog.ducklake" || c.DataRoot != root+"/files" || !files[c.Path] {
			return errors.New("invalid backup catalog descriptor")
		}
		projects[c.ProjectID] = true
	}
	return nil
}

func ValidateNativeBackupStorageIdentity(storage NativeBackupStorageIdentity, allowLegacy bool) error {
	switch storage.Mode {
	case "":
		if allowLegacy && storage == (NativeBackupStorageIdentity{}) {
			return nil
		}
		return errors.New("native backup is missing its original analytical storage identity")
	case "local":
		if storage == (NativeBackupStorageIdentity{Mode: "local"}) {
			return nil
		}
	case "s3":
		normalized, err := NormalizeDuckLakeS3(&DuckLakeS3Config{Bucket: storage.Bucket, Prefix: storage.Prefix, Region: storage.Region, Endpoint: storage.Endpoint})
		if err == nil && normalized.Bucket == storage.Bucket && normalized.Prefix == storage.Prefix && normalized.Region == storage.Region && normalized.Endpoint == storage.Endpoint {
			return nil
		}
	}
	return errors.New("invalid native backup original analytical storage identity")
}
