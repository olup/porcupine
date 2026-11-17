package lake

import (
	"context"
	"encoding/json"
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
	reopened := openLakeStorageTest(t, directory, 2)
	if _, err := reopened.Load(ctx, "a", "x"); err == nil {
		t.Fatal("opened a catalog written by a newer release")
	}
}
