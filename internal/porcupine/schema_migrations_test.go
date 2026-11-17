package porcupine

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// assertSQLiteUnchanged checks that a rejected database kept its version and tables.
func assertSQLiteUnchanged(t *testing.T, path string, tables int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version, count int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != 0 {
		t.Fatalf("rejected database version changed: %d %v", version, err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`).Scan(&count); err != nil || count != tables {
		t.Fatalf("rejected database tables changed: %d %v", count, err)
	}
}

func sqliteSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			t.Fatal(err)
		}
		b.WriteString(text + ";\n")
	}
	return b.String()
}

// The versioned schema files are the whole schema: startup code must not add
// tables, columns, indexes or triggers outside a numbered step.
func TestStartupSchemaMatchesVersionedSteps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, store := range []schemaStore{journalSchema, localControlSchema} {
		t.Run(store.name, func(t *testing.T) {
			db, err := openLocalSQLite(filepath.Join(t.TempDir(), "reference.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if err := migrateSQLite(ctx, db, store); err != nil {
				t.Fatal(err)
			}
			s := duckLakeJourneyService(t)
			live := s.db
			if store.name == "control" {
				live = s.control
			}
			if got, want := sqliteSchema(t, live), sqliteSchema(t, db); got != want {
				t.Fatalf("startup schema differs from versioned steps\n--- startup\n%s\n--- steps\n%s", got, want)
			}
			var version int
			if err := live.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != len(store.steps) {
				t.Fatalf("schema version %d, want %d (%v)", version, len(store.steps), err)
			}
		})
	}
}

func TestNewerSchemaVersionIsRefusedBeforeChanges(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "events.db")
	s, err := Open(Config{DBPath: path, BootstrapToken: adminToken})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version=99`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if s, err = Open(Config{DBPath: path, BootstrapToken: adminToken}); err == nil {
		s.Close()
		t.Fatal("opened a journal written by a newer release")
	}
	if !strings.Contains(err.Error(), "newer than this server supports") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPreVersioningDatabaseIsAdoptedAsVersionOne(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := openLocalSQLite(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// The pre-versioning build created exactly version 1 and marked its layout.
	v1 := journalSchema
	v1.steps = journalSchema.steps[:1]
	if err := migrateSQLite(ctx, db, v1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO metadata(key,value) VALUES('journal_layout','separate-control-v1'); PRAGMA user_version=0`); err != nil {
		t.Fatal(err)
	}
	if err := migrateSQLite(ctx, db, journalSchema); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != len(journalSchema.steps) {
		t.Fatalf("adopted journal version %d (%v)", version, err)
	}
	var column bool
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM pragma_table_info('event_receipts') WHERE name='recorded_at_ms')`).Scan(&column); err != nil || !column {
		t.Fatal("later steps were not applied after adoption", err)
	}
}

// A backup taken at version 1 restores, and the next start applies step 2.
func TestRestoreAcceptsOlderSchemaVersionAndStartMigratesIt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	journal, err := openLocalSQLite(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	v1 := journalSchema
	v1.steps = journalSchema.steps[:1]
	if err := migrateSQLite(ctx, journal, v1); err != nil {
		t.Fatal(err)
	}
	if err := verifyRestoreDatabaseSchema(ctx, journalSchema, journal); err != nil {
		t.Fatal("version 1 backup rejected:", err)
	}
	if err := migrateSQLite(ctx, journal, journalSchema); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err := journal.QueryRow(`SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='project_deletion_files')`).Scan(&exists); err != nil || !exists {
		t.Fatal("step 2 was not applied", err)
	}
	if _, err := journal.Exec(`PRAGMA user_version=1`); err != nil {
		t.Fatal(err)
	}
	if err := verifyRestoreDatabaseSchema(ctx, journalSchema, journal); err == nil {
		t.Fatal("restore accepted a table that its schema version does not define")
	}
}

func TestSharedControlRecordsAndEnforcesSchemaVersion(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	queries, _ := normalizeInteractiveQueries(nil)
	ctx := context.Background()
	if err := initializeSharedControl(ctx, db, adminToken, queries); err != nil {
		t.Fatal(err)
	}
	var version string
	if err := db.QueryRow(`SELECT value FROM metadata WHERE key='schema_version'`).Scan(&version); err != nil || version != fmt.Sprint(len(sharedControlSteps)) {
		t.Fatalf("shared schema version %q (%v)", version, err)
	}
	if err := initializeSharedControl(ctx, db, adminToken, queries); err != nil {
		t.Fatal("restart at the current version failed:", err)
	}
	if _, err := db.Exec(`UPDATE metadata SET value='99' WHERE key='schema_version'`); err != nil {
		t.Fatal(err)
	}
	if err := initializeSharedControl(ctx, db, adminToken, queries); err == nil || !strings.Contains(err.Error(), "newer than this server supports") {
		t.Fatalf("newer shared schema accepted: %v", err)
	}
}
