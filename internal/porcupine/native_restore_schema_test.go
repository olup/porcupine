package porcupine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestNativeBackupRestoresDormantPreviousLakeSchema(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := context.Background()
	s, config := nativeBackupService(t)
	projectID, ingest := project(t, s)
	state := lakeMaintenanceLifecycle(t, s, projectID, ingest, "dormant-schema", true)
	duckLakeJourneyDrain(t, s)
	lake := s.lake.(*lakeapi.DuckLakeStore)
	p, release, err := lake.LeaseProject(ctx, projectID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := p.DB.QueryContext(ctx, "SELECT uuid FROM lake.current_events")
	if err != nil {
		release()
		t.Fatal(err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	originals := make(map[string]lakeapi.EventProjection, len(ids))
	for _, id := range ids {
		value, err := lake.Load(ctx, projectID, id)
		if err != nil {
			release()
			t.Fatal(err)
		}
		originals[id] = value
	}
	if _, err = p.DB.ExecContext(ctx, "ALTER TABLE lake.current_events ADD COLUMN payload VARCHAR"); err != nil {
		release()
		t.Fatal(err)
	}
	for id, value := range originals {
		payload, err := json.Marshal(value)
		if err != nil {
			release()
			t.Fatal(err)
		}
		if _, err = p.DB.ExecContext(ctx, "UPDATE lake.current_events SET payload=? WHERE uuid=?", string(payload), id); err != nil {
			release()
			t.Fatal(err)
		}
	}
	if _, err = p.DB.ExecContext(ctx, "ALTER TABLE lake.current_events DROP COLUMN original_snapshot; DROP TABLE lake.schema_version"); err != nil {
		release()
		t.Fatal(err)
	}
	release()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Startup does not lease dormant catalogs. The new binary therefore backs up
	// the previous layout before lazy migration has ever opened this project.
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.stopNativeBackups()
	if len(s.lake.(*lakeapi.DuckLakeStore).Projects) != 0 {
		t.Fatal("fixture unexpectedly migrated dormant project")
	}
	if err := s.createNativeBackup(ctx); err != nil {
		t.Fatal(err)
	}
	id := s.nativeBackupStatus().LatestBundle
	bundle := filepath.Join(config.NativeBackup.Directory, id)
	manifest, err := lakeapi.VerifyNativeBackup(ctx, bundle)
	if err != nil {
		t.Fatal(err)
	}
	r := nativeRestoreTarget(t, id, bundle, manifest.SourceID)
	result, err := RestoreNativeBackup(ctx, NativeRestoreConfig{Request: r, DuckLake: lakeapi.DuckLakeConfig{MemoryLimitMB: 256, Threads: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if result.BackupID != id {
		t.Fatal("restored wrong backup")
	}
	if _, err := lakeapi.VerifyNativeBackup(ctx, bundle); err != nil {
		t.Fatal("restoration changed verified archive", err)
	}
	restored, err := Open(Config{DBPath: r.TargetDBPath, BootstrapToken: r.BootstrapToken, DuckLake: &lakeapi.DuckLakeConfig{Directory: r.TargetDuckLakeDirectory, MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for uuid, want := range originals {
		got, err := restored.lake.Load(ctx, projectID, uuid)
		if err != nil || got.ID != want.ID || got.Revision != want.Revision || got.Event.Event != want.Event.Event || got.OriginalSnapshot != want.OriginalSnapshot || got.Timestamp != want.Timestamp || string(got.Properties) != string(want.Properties) {
			t.Fatalf("dormant restore changed %s: %+v %v", uuid, got, err)
		}
	}
	deleted, err := restored.lake.Load(ctx, projectID, state.receipts[1]["uuid"].(string))
	if err != nil || !deleted.IsDeleted {
		t.Fatal("dormant restore lost tombstone", err)
	}
	rowsOut := duckLakeJourneyQuery(t, restored, projectID, r.BootstrapToken, "SELECT event,person_id,person_properties FROM events", nil)
	if len(rowsOut) != 1 || rowsOut[0].(map[string]any)["person_id"] != "dormant-schema-person" {
		t.Fatal("dormant restore lost identity", rowsOut)
	}
}
