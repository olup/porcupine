package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNativeBackupConfigurationRequiresNativeFiles(t *testing.T) {
	for _, config := range []Config{
		{NativeBackup: &NativeBackupConfig{Directory: t.TempDir()}},
		{DBPath: ":memory:", DuckLake: &DuckLakeConfig{Directory: t.TempDir()}, NativeBackup: &NativeBackupConfig{Directory: t.TempDir()}},
		{DuckLake: &DuckLakeConfig{Directory: t.TempDir()}, NativeBackup: &NativeBackupConfig{Directory: t.TempDir(), Timeout: -time.Second}},
	} {
		if s, err := Open(config); err == nil {
			if s != nil {
				s.Close()
			}
			t.Fatal("unsupported native backup configuration accepted")
		}
	}
	root := t.TempDir()
	if _, err := prepareNativeBackup(&NativeBackupConfig{Directory: filepath.Join(root, "lake", "backup")}, filepath.Join(root, "db"), &DuckLakeConfig{Directory: filepath.Join(root, "lake")}); err == nil {
		t.Fatal("backup destination inside analytical store accepted")
	}
}

func TestSQLiteCheckpointIntegrityCancellationAndImmutableDestination(t *testing.T) {
	root := t.TempDir()
	source, err := sql.Open("sqlite", filepath.Join(root, "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Exec("CREATE TABLE event(id INTEGER PRIMARY KEY,value TEXT);INSERT INTO event VALUES(1,'kept')"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "snapshot?quoted.db")
	if err := snapshotSQLite(context.Background(), source, path); err != nil {
		t.Fatal(err)
	}
	if err := validateBackupSQLite(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec("DELETE FROM event"); err != nil {
		t.Fatal(err)
	}
	if err := snapshotSQLite(context.Background(), source, path); err == nil {
		t.Fatal("checkpoint overwrote an existing destination")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := snapshotSQLite(ctx, source, filepath.Join(root, "cancelled.db")); err == nil {
		t.Fatal("checkpoint ignored cancellation")
	}
}

func TestNativeBackupRetentionExcludesIncompleteAndForeignBundles(t *testing.T) {
	root := t.TempDir()
	for _, item := range []struct{ id, source, created string }{{"pc_backup_old", "source", "2026-11-14T00:00:00.1Z"}, {"pc_backup_new", "source", "2026-11-14T00:00:00.11Z"}, {"pc_backup_foreign", "other", "2026-11-14T00:00:00.01Z"}} {
		path := filepath.Join(root, item.id)
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeBackupManifest(path, nativeBackupManifest{Format: "porcupine-native-v1", ID: item.id, SourceID: item.source, CreatedAt: item.created}); err != nil {
			t.Fatal(err)
		}
	}
	incomplete := filepath.Join(root, ".pc_backup_partial.incomplete")
	os.Mkdir(incomplete, 0700)
	invalid := filepath.Join(root, "pc_backup_invalid")
	os.Mkdir(invalid, 0700)
	os.WriteFile(filepath.Join(invalid, "manifest.json"), []byte(`{"invalid":true}`), 0600)
	if err := pruneNativeBackups(root, "source", 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "pc_backup_old")); !os.IsNotExist(err) {
		t.Fatal("old completed backup retained")
	}
	for _, id := range []string{"pc_backup_new", "pc_backup_foreign", "pc_backup_invalid", ".pc_backup_partial.incomplete"} {
		if _, err := os.Stat(filepath.Join(root, id)); err != nil {
			t.Fatal("retention removed an unrelated/incomplete/current bundle", id, err)
		}
	}
}

func readNativeManifest(t *testing.T, directory string) nativeBackupManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest nativeBackupManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func minimalNativeBackupBundle(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "ducklake"), 0700); err != nil {
		t.Fatal(err)
	}
	manifest := nativeBackupManifest{Format: "porcupine-native-v1", ID: "pc_backup_test", SourceID: "source", SQLitePath: "acceptance.sqlite", DuckLakeDirectory: "ducklake", CreatedAt: now(), CapturedAt: now()}
	for _, item := range []struct{ path, statement string }{
		{"acceptance.sqlite", "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT); INSERT INTO metadata VALUES('ducklake_source_id','source')"},
		{"ducklake/registry.sqlite", "CREATE TABLE source(id TEXT PRIMARY KEY);INSERT INTO source VALUES('source');CREATE TABLE catalogs(project_id TEXT PRIMARY KEY,ready INTEGER)"},
	} {
		path := filepath.Join(directory, filepath.FromSlash(item.path))
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(item.statement)
		db.Close()
		if err != nil {
			t.Fatal(err)
		}
		file, err := hashBackupFile(context.Background(), path, item.path)
		if err != nil {
			t.Fatal(err)
		}
		manifest.Files = append(manifest.Files, file)
	}
	if err := writeBackupManifest(directory, manifest); err != nil {
		t.Fatal(err)
	}
	return directory
}

func TestNativeBackupVerificationRejectsUnsafeOrIncompleteInventory(t *testing.T) {
	for _, scenario := range []string{"valid", "valid-legacy-paths", "escape", "absolute", "duplicate", "missing", "extra", "symlink", "source-mismatch", "missing-catalog"} {
		t.Run(scenario, func(t *testing.T) {
			directory := minimalNativeBackupBundle(t)
			manifest := readNativeManifest(t, directory)
			target := filepath.Join(directory, "acceptance.sqlite")
			switch scenario {
			case "valid-legacy-paths":
				manifest.SQLitePath = "/original/source/queue.db"
				manifest.DuckLakeDirectory = "/original/source/lake"
			case "escape":
				manifest.Files[0].Path = "../outside"
			case "absolute":
				manifest.Files[0].Path = filepath.Join(directory, "absolute")
			case "duplicate":
				manifest.Files = append(manifest.Files, manifest.Files[0])
			case "missing":
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(directory, "extra"), []byte("unexpected"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				outside := filepath.Join(t.TempDir(), "outside")
				os.WriteFile(outside, []byte("private"), 0600)
				if err := os.Remove(target); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, target); err != nil {
					t.Skip("symlinks unavailable")
				}
			case "source-mismatch", "missing-catalog":
				path := target
				statement := "UPDATE metadata SET value='other-source' WHERE key='ducklake_source_id'"
				if scenario == "missing-catalog" {
					path = filepath.Join(directory, "ducklake", "registry.sqlite")
					statement = "INSERT INTO catalogs VALUES('missing-project',1)"
				}
				db, err := sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				_, err = db.Exec(statement)
				db.Close()
				if err != nil {
					t.Fatal(err)
				}
				for i, file := range manifest.Files {
					if filepath.Join(directory, filepath.FromSlash(file.Path)) == path {
						manifest.Files[i], err = hashBackupFile(context.Background(), path, file.Path)
						if err != nil {
							t.Fatal(err)
						}
					}
				}
			}
			raw, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(directory, "manifest.json"), raw, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = verifyNativeBackup(context.Background(), directory)
			if strings.HasPrefix(scenario, "valid") && err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(scenario, "valid") && err == nil {
				t.Fatal("unsafe or inconsistent recovery bundle accepted")
			}
		})
	}
}

func TestNativeBackupRetentionProtectsPublishedBundleAfterClockRollback(t *testing.T) {
	root := t.TempDir()
	for _, item := range []struct{ id, created string }{{"pc_backup_previous", "2030-01-01T00:00:00Z"}, {"pc_backup_published", "2026-11-14T00:00:00Z"}} {
		directory := filepath.Join(root, item.id)
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		if err := writeBackupManifest(directory, nativeBackupManifest{Format: "porcupine-native-v1", ID: item.id, SourceID: "source", CreatedAt: item.created}); err != nil {
			t.Fatal(err)
		}
	}
	if err := pruneNativeBackups(root, "source", 1, "pc_backup_published"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "pc_backup_published")); err != nil {
		t.Fatal("retention deleted the newly verified backup after clock rollback", err)
	}
	if _, err := os.Stat(filepath.Join(root, "pc_backup_previous")); !os.IsNotExist(err) {
		t.Fatal("retention did not respect its limit", err)
	}
}

func TestNativeBackupIndependentS3Configuration(t *testing.T) {
	localRoot := t.TempDir()
	if _, err := prepareNativeBackup(&NativeBackupConfig{Directory: filepath.Join(localRoot, "backups")}, filepath.Join(localRoot, "backups", "queue.db"), &DuckLakeConfig{Directory: filepath.Join(localRoot, "lake")}); err == nil {
		t.Fatal("backup destination containing live SQLite accepted")
	}
	remote := &DuckLakeS3Config{Bucket: "backup-test", Prefix: "independent", Region: "us-east-1"}
	root := t.TempDir()
	lake := &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &DuckLakeS3Config{Bucket: "backup-test", Prefix: "analytics", Region: "us-east-1"}}
	if _, err := prepareNativeBackup(&NativeBackupConfig{S3: remote}, filepath.Join(root, "queue.db"), lake); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"analytics", "analytics/backups"} {
		copy := *remote
		copy.Prefix = prefix
		if _, err := prepareNativeBackup(&NativeBackupConfig{S3: &copy}, filepath.Join(root, "queue.db"), lake); err == nil {
			t.Fatal("overlapping S3 destination accepted")
		}
	}
	copy := *remote
	copy.Prefix = "analytics-parent"
	lake.S3.Prefix = "analytics-parent/child"
	if _, err := prepareNativeBackup(&NativeBackupConfig{S3: &copy}, filepath.Join(root, "queue.db"), lake); err == nil {
		t.Fatal("ancestor S3 destination accepted")
	}
	if _, err := prepareNativeBackup(&NativeBackupConfig{Directory: t.TempDir(), S3: remote}, filepath.Join(root, "queue.db"), lake); err == nil {
		t.Fatal("ambiguous destination accepted")
	}
	if _, err := prepareNativeBackup(&NativeBackupConfig{Directory: t.TempDir()}, filepath.Join(root, "queue.db"), lake); err == nil {
		t.Fatal("analytical S3 metadata-only local backup accepted")
	}
}

func TestNativeBackupLocalOwnerExcludesRestoredWriter(t *testing.T) {
	directory := t.TempDir()
	source := "00000000-0000-0000-0000-000000000000"
	if err := claimLocalNativeBackupOwner(directory, source, "original-writer"); err != nil {
		t.Fatal(err)
	}
	if err := claimLocalNativeBackupOwner(directory, source, "original-writer"); err != nil {
		t.Fatal(err)
	}
	if err := claimLocalNativeBackupOwner(directory, source, "restored-writer"); err == nil {
		t.Fatal("restored writer claimed original local backup directory")
	}
}

func TestNativeBackupNormalizesSourceBeforeS3OverlapCheck(t *testing.T) {
	root := t.TempDir()
	for _, prefix := range []string{"analytics/", "/analytics/"} {
		lake := &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &DuckLakeS3Config{Bucket: "backup-test", Prefix: prefix, Region: "us-east-1"}}
		destination := &NativeBackupConfig{S3: &DuckLakeS3Config{Bucket: "backup-test", Prefix: "analytics/nested", Region: "us-east-1"}}
		if _, err := prepareNativeBackup(destination, filepath.Join(root, "queue.db"), lake); err == nil {
			t.Fatal("raw source prefix bypassed overlap validation", prefix)
		}
	}
	lake := &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &DuckLakeS3Config{Bucket: "source-test", Prefix: "../invalid", Region: "us-east-1"}}
	if _, err := prepareNativeBackup(&NativeBackupConfig{S3: &DuckLakeS3Config{Bucket: "backup-test", Prefix: "separate", Region: "us-east-1"}}, filepath.Join(root, "queue.db"), lake); err == nil {
		t.Fatal("invalid source config bypassed validation")
	}
}
func TestNativeBackupStorageIdentityValidation(t *testing.T) {
	valid := []nativeBackupStorageIdentity{{Mode: "local"}, {Mode: "s3", Bucket: "backup-test", Prefix: "analytics", Region: "us-east-1", Endpoint: "http://127.0.0.1:9000"}}
	for _, identity := range valid {
		if err := validateNativeBackupStorageIdentity(identity, false); err != nil {
			t.Fatal(err)
		}
	}
	for _, identity := range []nativeBackupStorageIdentity{{}, {Mode: "local", Bucket: "backup-test"}, {Mode: "s3", Bucket: "backup-test", Prefix: "analytics/", Region: "us-east-1"}, {Mode: "s3", Bucket: "backup-test", Prefix: "analytics", Region: "us-east-1", Endpoint: "https://secret:password@example.test"}, {Mode: "unexpected"}} {
		if err := validateNativeBackupStorageIdentity(identity, false); err == nil {
			t.Fatalf("invalid storage identity accepted: %+v", identity)
		}
	}
	if err := validateNativeBackupStorageIdentity(nativeBackupStorageIdentity{}, true); err != nil {
		t.Fatal("legacy local v1 identity rejected", err)
	}
}
