package lake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestSharedPostgresDuckLakeParquetEncryption(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	dsn := sharedPostgresDSN(t)
	metadata, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { metadata.Close() })
	for _, encrypted := range []bool{false, true} {
		directory := t.TempDir()
		catalogID := SharedNativeCatalogID("encryption-fixture", fmt.Sprint(encrypted))
		store := &DuckLakeStore{Config: DuckLakeConfig{PostgresURL: dsn, MetadataTimeoutSeconds: 10, Encrypted: encrypted}}
		db, err := sql.Open("duckdb", "?async_threads=0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, stmt := range []string{"SET threads=1", "LOAD ducklake; LOAD postgres; LOAD httpfs"} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		if err := store.ConfigureSharedPostgres(ctx, db); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{store.sharedNativeAttach(directory, catalogID, true), "CREATE TABLE lake.events(id INTEGER)", "INSERT INTO lake.events VALUES (1),(2)"} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		var snapshot int64
		if err := db.QueryRowContext(ctx, "SELECT id FROM ducklake_current_snapshot('lake')").Scan(&snapshot); err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{"DELETE FROM lake.events WHERE id=2", "DETACH lake", store.sharedNativeAttach(directory, catalogID, false)} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatal(err)
			}
		}
		var current, historical int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT (SELECT sum(id) FROM lake.events), (SELECT sum(id) FROM lake.events AT (VERSION => %d))", snapshot)).Scan(&current, &historical); err != nil || current != 1 || historical != 3 {
			t.Fatalf("encrypted=%t: current=%d historical=%d err=%v", encrypted, current, historical, err)
		}
		var dataFile, deleteFile string
		if err := db.QueryRowContext(ctx, "SELECT data_file,delete_file FROM ducklake_list_files('lake','events')").Scan(&dataFile, &deleteFile); err != nil {
			t.Fatal(err)
		}
		for _, table := range []string{"ducklake_data_file", "ducklake_delete_file"} {
			var total, missingKey int
			query := fmt.Sprintf(`SELECT count(*), count(*) FILTER (WHERE encryption_key IS NULL OR encryption_key = '') FROM %s.%s WHERE end_snapshot IS NULL`, SharedNativeSchema(catalogID), table)
			if err := metadata.QueryRowContext(ctx, query).Scan(&total, &missingKey); err != nil || total != 1 || encrypted != (missingKey == 0) {
				t.Fatalf("encrypted=%t: %s total=%d missing keys=%d err=%v", encrypted, table, total, missingKey, err)
			}
		}
		// Both files exist even for a tiny write. Hosted never inlines rows.
		wantMagic := "PAR1"
		if encrypted {
			wantMagic = "PARE"
		}
		for _, path := range []string{dataFile, deleteFile} {
			file, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			var magic [4]byte
			_, err = file.ReadAt(magic[:], 0)
			if err := errors.Join(err, file.Close()); err != nil {
				t.Fatal(err)
			}
			if string(magic[:]) != wantMagic {
				t.Fatalf("encrypted=%t: %s has header %q", encrypted, filepath.Base(path), magic)
			}
		}
	}
}

func TestDuckLakeEncryptedFilesAndLifecycle(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	config := DuckLakeConfig{Directory: t.TempDir(), MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 2, Encrypted: true}
	encrypted := openEncryptionTestStore(t, config)
	plaintext := openLakeStorageTest(t, t.TempDir(), 2)

	a := lakeStorageProjection(t, "project-a", "same-uuid", 1)
	a.Event.Event = "purchase"
	b := lakeStorageProjection(t, "project-b", "same-uuid", 2)
	b.Event.Event = "signup"
	survivor := lakeStorageProjection(t, a.ProjectID, "survivor", 3)
	batch := []EventProjection{a, b, survivor}
	revision := a
	revision.Revision = 1
	revision.Event.Event = "corrected"
	for _, store := range []*DuckLakeStore{plaintext, encrypted} {
		if err := store.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if err := store.Apply(ctx, []EventProjection{revision}); err != nil {
			t.Fatal(err)
		}
	}
	flushCleanupProject(t, plaintext, b.ProjectID)

	// A separate DuckDB connection can read the plaintext control file with
	// no catalog attached. The same scan must fail on encrypted Parquet.
	raw, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var uuid string
	if err := raw.QueryRowContext(ctx, "SELECT uuid FROM read_parquet(?)", cleanupEventFiles(t, plaintext, b.ProjectID)[0]).Scan(&uuid); err != nil || uuid != b.UUID {
		t.Fatalf("direct plaintext read returned %q, %v", uuid, err)
	}
	for _, project := range []string{a.ProjectID, b.ProjectID} {
		assertRawParquetIsEncrypted(t, raw, encrypted, project)
	}

	query := func(store *DuckLakeStore, project string) AnalyticsResult {
		t.Helper()
		result, err := store.Query(ctx, project, `SELECT id, uuid, event, json_extract_string(properties,'$.page') AS page FROM events ORDER BY id`, nil, DefaultQueryExecutionLimits())
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	assertQueryParity := func(store *DuckLakeStore) {
		t.Helper()
		for _, check := range []struct {
			seed EventProjection
			rows int
		}{{revision, 2}, {b, 1}} {
			project := check.seed.ProjectID
			want := query(plaintext, project)
			got := query(store, project)
			if !reflect.DeepEqual(got.Meta, want.Meta) || !reflect.DeepEqual(got.Data, want.Data) {
				t.Fatalf("query parity for %s: got %s, want %s", project, got.Data, want.Data)
			}
			if len(got.Data) != check.rows || string(got.Data[0]["event"]) != `"`+check.seed.Event.Event+`"` {
				t.Fatalf("project %s has unexpected rows: %s", project, got.Data)
			}
		}
	}
	assertQueryParity(encrypted)
	if err := encrypted.Close(); err != nil {
		t.Fatal(err)
	}
	encrypted = openEncryptionTestStore(t, config)
	if err := encrypted.Apply(ctx, batch); err != nil {
		t.Fatal(err)
	}
	fresh := lakeStorageProjection(t, a.ProjectID, "uncommitted", 4)
	conflict := revision
	conflict.ID++
	if err := encrypted.Apply(ctx, []EventProjection{fresh, conflict}); err == nil {
		t.Fatal("retry with a changed stable ID succeeded")
	}
	if _, err := encrypted.Load(ctx, a.ProjectID, fresh.UUID); !errors.Is(err, ErrEventNotFound) {
		t.Fatalf("failed batch left a partial event: %v", err)
	}
	assertQueryParity(encrypted)

	checkpoint, err := encrypted.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoint.Close()
	if len(checkpoint.Catalogs) != 2 {
		t.Fatalf("checkpoint captured %d project catalogs, want 2", len(checkpoint.Catalogs))
	}
	reader := &LocalNativeBackupReader{directory: filepath.Dir(checkpoint.Directory)}
	manifest := NativeBackupManifest{ID: filepath.Base(checkpoint.Directory), SourceStorage: checkpoint.Storage}
	unprotectedTarget := openLakeStorageTest(t, t.TempDir(), 2)
	if err := RestoreNativeProject(ctx, unprotectedTarget, reader, manifest, checkpoint.Catalogs[0], checkpoint.Directory); err == nil || !strings.Contains(strings.ToLower(err.Error()), "encrypt") {
		t.Fatalf("restoring an encrypted backup into a plaintext target returned %v", err)
	}
	if _, err := unprotectedTarget.registry.lookup(ctx, a.ProjectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("rejected restore wrote target catalog metadata: %v", err)
	}
	config.Directory = t.TempDir()
	restored := openEncryptionTestStore(t, config)
	for _, catalog := range checkpoint.Catalogs {
		if err := RestoreNativeProject(ctx, restored, reader, manifest, catalog, checkpoint.Directory); err != nil {
			t.Fatalf("restore encrypted project %s: %v", catalog.ProjectID, err)
		}
		assertRawParquetIsEncrypted(t, raw, restored, catalog.ProjectID)
	}
	if err := checkpoint.Close(); err != nil {
		t.Fatal(err)
	}
	assertQueryParity(restored)
	for range 2 {
		if err := restored.DeleteProject(ctx, a.ProjectID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := restored.Load(ctx, a.ProjectID, a.UUID); err == nil {
		t.Fatal("deleted encrypted project reopened")
	}
	if _, err := os.Stat(restored.ProjectDirectory(a.ProjectID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("deleted project's catalog with its keys and data files remain: %v", err)
	}
	if _, err := restored.registry.lookup(ctx, a.ProjectID); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("deleted project remains registered: %v", err)
	}
	if got, want := query(restored, b.ProjectID), query(plaintext, b.ProjectID); !reflect.DeepEqual(got.Data, want.Data) {
		t.Fatalf("deleting project A changed project B: got %s, want %s", got.Data, want.Data)
	}
}

func TestDuckLakeEncryptsPlaintextBackupOnRestore(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	source := openLakeStorageTest(t, t.TempDir(), 1)
	want := lakeStorageProjection(t, "project", "from-plaintext-backup", 1)
	if err := source.Apply(ctx, []EventProjection{want}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, source, want.ProjectID)
	checkpoint, err := source.StageCheckpoint(ctx, func(context.Context, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer checkpoint.Close()
	reader := &LocalNativeBackupReader{directory: filepath.Dir(checkpoint.Directory)}
	manifest := NativeBackupManifest{ID: filepath.Base(checkpoint.Directory), SourceStorage: checkpoint.Storage}
	target := openEncryptionTestStore(t, DuckLakeConfig{Directory: t.TempDir(), MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, Encrypted: true})
	if err := RestoreNativeProject(ctx, target, reader, manifest, checkpoint.Catalogs[0], checkpoint.Directory); err != nil {
		t.Fatal(err)
	}
	got, err := target.Load(ctx, want.ProjectID, want.UUID)
	if err != nil || got.ID != want.ID || got.OriginalSnapshot != want.OriginalSnapshot {
		t.Fatalf("encrypted restore changed the backed-up event: event ID %d, %v", got.ID, err)
	}
	raw, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	assertRawParquetIsEncrypted(t, raw, target, want.ProjectID)
}

func TestDuckLakeEncryptionModeCannotChangeOnReopen(t *testing.T) {
	t.Parallel()
	for name, encrypted := range map[string]bool{"plaintext": false, "encrypted": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			config := DuckLakeConfig{Directory: t.TempDir(), MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, Encrypted: encrypted}
			store := openEncryptionTestStore(t, config)
			event := lakeStorageProjection(t, "a", "original", 1)
			if err := store.Apply(ctx, []EventProjection{event}); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			wrong := config
			wrong.Encrypted = !encrypted
			if invalid, err := NewDuckLake(wrong); err == nil {
				invalid.Close()
				t.Fatal("store opened in the opposite encryption mode")
			} else if !strings.Contains(strings.ToLower(err.Error()), "encrypt") {
				t.Fatalf("wrong encryption mode failed for a different reason: %v", err)
			}
			store = openEncryptionTestStore(t, config)
			if got, err := store.Load(ctx, event.ProjectID, event.UUID); err != nil || got.ID != event.ID {
				t.Fatalf("original mode failed after rejected reopen: event ID %d, %v", got.ID, err)
			}
		})
	}
}

func TestDuckLakeEncryptedStoreRejectsCatalogWithEncryptionDisabled(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	config := DuckLakeConfig{Directory: t.TempDir(), MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, Encrypted: true}
	store := openEncryptionTestStore(t, config)
	event := lakeStorageProjection(t, "a", "original", 1)
	if err := store.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	catalogPath := filepath.Join(store.ProjectDirectory(event.ProjectID), "catalog.ducklake")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	metadata, err := sql.Open("duckdb", catalogPath)
	if err != nil {
		t.Fatal(err)
	}
	// Change the native catalog mode while leaving its application identity
	// and the store registry intact.
	var flag string
	err = metadata.QueryRowContext(ctx, "UPDATE ducklake_metadata SET value='false' WHERE key='encrypted' RETURNING value").Scan(&flag)
	if err := errors.Join(err, metadata.Close()); err != nil || flag != "false" {
		t.Fatalf("change persisted catalog encryption flag: %v", err)
	}
	store = openEncryptionTestStore(t, config)
	if _, release, err := store.LeaseProject(ctx, event.ProjectID); err == nil {
		release()
		t.Fatal("encrypted store accepted a project catalog with encryption disabled")
	} else if !strings.Contains(strings.ToLower(err.Error()), "encrypt") {
		t.Fatalf("project catalog reopen failed for a different reason: %v", err)
	}
}

func openEncryptionTestStore(t *testing.T, config DuckLakeConfig) *DuckLakeStore {
	t.Helper()
	store, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func assertRawParquetIsEncrypted(t *testing.T, raw *sql.DB, store *DuckLakeStore, project string) {
	t.Helper()
	for _, path := range cleanupEventFiles(t, store, project) {
		var uuid string
		err := raw.QueryRowContext(t.Context(), "SELECT uuid FROM read_parquet(?) LIMIT 1", path).Scan(&uuid)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "encrypt") {
			t.Fatalf("direct read of %s for %s returned %q, %v", filepath.Base(path), project, uuid, err)
		}
	}
}
