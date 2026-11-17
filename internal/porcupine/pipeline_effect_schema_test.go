package porcupine

import (
	"context"
	"testing"
)

func TestSharedPipelineEffectMigrationAppendsAfterNativeEpoch(t *testing.T) {
	t.Parallel()
	db, _ := sharedControlFixture(t)
	ctx := context.Background()
	for _, step := range sharedControlSteps[:7] {
		if _, e := db.ExecContext(ctx, step); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('schema_version','7');SELECT nextval('shared_native_write_epoch')`); e != nil {
		t.Fatal(e)
	}
	migrate := func() {
		tx, e := db.BeginTx(ctx, nil)
		if e != nil {
			t.Fatal(e)
		}
		defer tx.Rollback()
		if e = migrateSharedControl(ctx, tx); e != nil {
			t.Fatal(e)
		}
		if e = tx.Commit(); e != nil {
			t.Fatal(e)
		}
	}
	migrate()
	migrate()
	var epoch int64
	if e := db.QueryRow(`SELECT nextval('shared_native_write_epoch')`).Scan(&epoch); e != nil || epoch != 2 {
		t.Fatal("pipeline migration reset native epochs", epoch, e)
	}
	for _, table := range []string{"pipeline_capacity", "pipeline_effect_reservations", "pipeline_effect_streams", "pipeline_effect_jobs", "pipeline_effect_members", "pipeline_effect_cleanup"} {
		var exists bool
		if e := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=$1)`, table).Scan(&exists); e != nil || !exists {
			t.Fatal("pipeline table unavailable", table, e)
		}
	}
}
