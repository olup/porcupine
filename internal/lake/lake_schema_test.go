package lake

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// A catalog written with the duplicated payload column is upgraded in place:
// the original snapshot survives and the payload column is dropped.
func TestLakeCatalogMigratesPayloadToOriginalSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 2)
	want := lakeStorageProjection(t, "a", "legacy", 1)
	want.Revision = 3
	want.Event.Event = "corrected"
	if err := store.Apply(ctx, []EventProjection{want}); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(want)
	_, err = p.DB.ExecContext(ctx, `ALTER TABLE lake.current_events ADD COLUMN payload VARCHAR`)
	if err == nil {
		_, err = p.DB.ExecContext(ctx, `UPDATE lake.current_events SET payload=?`, string(payload))
	}
	if err == nil {
		_, err = p.DB.ExecContext(ctx, `ALTER TABLE lake.current_events DROP COLUMN original_snapshot; DELETE FROM lake.schema_version`)
	}
	release()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLakeStorageTest(t, directory, 2)
	got, err := reopened.Load(ctx, "a", "legacy")
	if err != nil {
		t.Fatal(err)
	}
	if got.OriginalSnapshot != want.OriginalSnapshot || string(got.Properties) != string(want.Properties) || got.ID != 1 {
		t.Fatalf("migration lost event data: %+v", got)
	}
	p, release, err = reopened.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var payloadColumns, version int
	if err := p.DB.QueryRowContext(ctx, "SELECT count(*) FROM duckdb_columns() WHERE database_name='lake' AND table_name='current_events' AND column_name='payload'").Scan(&payloadColumns); err != nil || payloadColumns != 0 {
		t.Fatal("payload column kept", err)
	}
	if err := p.DB.QueryRowContext(ctx, "SELECT max(version) FROM lake.schema_version").Scan(&version); err != nil || version != lakeSchemaVersion {
		t.Fatal("schema version not recorded", version, err)
	}
}

func TestLakeCatalogRefusesNewerSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 2)
	if err := store.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "x", 1)}); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.DB.ExecContext(ctx, "UPDATE lake.schema_version SET version=99")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.ProjectDirectory("a"), "catalog.ducklake")
	before := catalogDigest(t, path)
	reopened := openLakeStorageTest(t, directory, 2)
	if _, err := reopened.Load(ctx, "a", "x"); err == nil {
		t.Fatal("opened a catalog written by a newer release")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if catalogDigest(t, path) != before {
		t.Fatal("refusing a newer catalog changed its persistent metadata")
	}
}

func catalogDigest(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(data)
}

func TestLakeCatalogRefusesUnknownLayoutWithoutWrites(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 1)
	if err := store.Apply(ctx, []EventProjection{lakeStorageProjection(t, "a", "x", 1)}); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.DB.ExecContext(ctx, "ALTER TABLE lake.current_events ADD COLUMN unknown_future_column VARCHAR")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.ProjectDirectory("a"), "catalog.ducklake")
	before := catalogDigest(t, path)
	reopened := openLakeStorageTest(t, directory, 1)
	if _, err := reopened.Load(ctx, "a", "x"); err == nil {
		t.Fatal("opened unknown event schema")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if catalogDigest(t, path) != before {
		t.Fatal("refusing an unknown schema changed persistent metadata")
	}
}

func TestLakeCatalogResumesAfterColumnRewriteBeforeVersionCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 1)
	want := lakeStorageProjection(t, "a", "x", 1)
	want.OriginalSnapshot = ""
	if err := store.Apply(ctx, []EventProjection{want}); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.DB.ExecContext(ctx, "DELETE FROM lake.schema_version; INSERT INTO lake.schema_version VALUES(1)")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		reopened := openLakeStorageTest(t, directory, 1)
		got, err := reopened.Load(ctx, "a", "x")
		if err != nil || got.ID != want.ID || got.Timestamp != want.Timestamp {
			t.Fatalf("interrupted migration lost event: %+v %v", got, err)
		}
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLakeCatalogRejectsMalformedOrUnfamiliarTablesBeforeWrites(t *testing.T) {
	t.Parallel()
	for name, statement := range map[string]string{
		"unversioned-heads":          "DROP TABLE lake.schema_version; DROP TABLE lake.current_events; DROP TABLE lake.heads; CREATE TABLE lake.heads(uuid VARCHAR)",
		"unversioned-unrelated-only": "DROP TABLE lake.schema_version; DROP TABLE lake.current_events; DROP TABLE lake.heads; DROP TABLE lake.identity_profiles; DROP TABLE lake.identity_links; CREATE TABLE lake.unrelated(value INTEGER)",
		"identity-type":              "DROP TABLE lake.catalog_identity; CREATE TABLE lake.catalog_identity(id BIGINT); INSERT INTO lake.catalog_identity VALUES(1)",
		"duplicate-identities":       "INSERT INTO lake.catalog_identity VALUES('unexpected-second-identity')",
		"profile-layout":             "ALTER TABLE lake.identity_profiles ADD COLUMN unknown_column INTEGER",
		"missing-table":              "DROP TABLE lake.identity_links",
		"version-type":               "DROP TABLE lake.schema_version; CREATE TABLE lake.schema_version(version VARCHAR); INSERT INTO lake.schema_version VALUES('2')",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			directory := t.TempDir()
			store := openLakeStorageTest(t, directory, 1)
			p, release, err := store.LeaseProject(ctx, "a")
			if err != nil {
				t.Fatal(err)
			}
			_, err = p.DB.ExecContext(ctx, statement)
			release()
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(store.ProjectDirectory("a"), "catalog.ducklake")
			before := catalogDigest(t, path)
			reopened := openLakeStorageTest(t, directory, 1)
			if _, _, err := reopened.LeaseProject(ctx, "a"); err == nil {
				t.Fatal("opened invalid catalog")
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if catalogDigest(t, path) != before {
				t.Fatal("refusal modified catalog metadata")
			}
		})
	}
}

func TestLakeCatalogCompletesKnownUnfinishedInitialization(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 1)
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	_, err = p.DB.ExecContext(ctx, "DROP TABLE lake.schema_version; DROP TABLE lake.current_events; DROP TABLE lake.identity_profiles; DROP TABLE lake.identity_links")
	release()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Registry.ExecContext(ctx, "UPDATE catalogs SET ready=0 WHERE project_id='a'"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLakeStorageTest(t, directory, 1)
	want := lakeStorageProjection(t, "a", "first", 1)
	if err := reopened.Apply(ctx, []EventProjection{want}); err != nil {
		t.Fatal("known partial bootstrap did not resume", err)
	}
	if got, err := reopened.Load(ctx, "a", "first"); err != nil || got.Event.Event != want.Event.Event {
		t.Fatal(got, err)
	}
}

func TestLakeCatalogWritesUseColumnNamesAfterPhysicalReordering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	directory := t.TempDir()
	store := openLakeStorageTest(t, directory, 1)
	initial := lakeStorageProjection(t, "a", "initial", 1)
	if err := store.Apply(ctx, []EventProjection{initial}); err != nil {
		t.Fatal(err)
	}
	p, release, err := store.LeaseProject(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	for table, columns := range map[string]string{
		"current_events":    "id,project_id,uuid,distinct_id,event,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot",
		"heads":             "received_at,event_id,revision,is_deleted,uuid",
		"identity_profiles": "traits,version,identified_id",
		"identity_links":    "identified_id,anonymous_id,version",
	} {
		_, err = p.DB.ExecContext(ctx, "CREATE TABLE lake.reordered AS SELECT "+columns+" FROM lake."+table+"; DROP TABLE lake."+table+"; ALTER TABLE lake.reordered RENAME TO "+table)
		if err != nil {
			release()
			t.Fatal(err)
		}
	}
	release()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openLakeStorageTest(t, directory, 1)
	want := lakeStorageProjection(t, "a", "after-reorder", 2)
	want.Event.Event = "signup_completed"
	want.DistinctID = "browser"
	control := lakeStorageProjection(t, "a", "identity", 3)
	control.Event.Event = "$identify"
	control.IdentitySnapshot = `{"version":"1","identified_id":"person","traits":{"plan":"team"},"links":[{"anonymous_id":"browser","identified_id":"person","version":"1"}]}`
	if err := reopened.Apply(ctx, []EventProjection{want, control}); err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Load(ctx, "a", want.UUID)
	if err != nil || got.ID != want.ID || got.Event.Event != want.Event.Event || got.DistinctID != want.DistinctID {
		t.Fatal("column order changed event values", got, err)
	}
	result, err := reopened.Query(ctx, "a", "SELECT person_id,json_extract_string(person_properties,'$.plan') AS plan FROM events WHERE uuid='after-reorder'", nil, DefaultQueryExecutionLimits())
	if err != nil || len(result.Data) != 1 || string(result.Data[0]["person_id"]) != `"person"` || string(result.Data[0]["plan"]) != `"team"` {
		t.Fatal("column order changed identity values", result, err)
	}
}
