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

func TestSharedPipelineTenantCapacityMigrationPreservesCounters(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, _ := sharedControlFixture(t)
	const beforeTenantCapacity = 18
	for _, step := range sharedControlSteps[:beforeTenantCapacity] {
		if _, err := db.ExecContext(ctx, step); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `
INSERT INTO metadata(key,value) VALUES('schema_version','18');
INSERT INTO pipeline_capacity VALUES
 ('instance','',20,21,22,23,24,10,11,12,13,14),
 ('tenant','tenant-a',10,11,12,13,14,5,6,7,8,9),
 ('project','project-a',5,6,7,8,9,5,6,7,8,9),
 ('tenant','tenant-b',10,11,12,13,14,0,0,0,0,0);
`); err != nil {
		t.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := migrateSharedControl(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		kind, id string
		want     [10]int64
	}{
		{"tenant", "tenant-a", [10]int64{10, 11, 12, 13, 14, 5, 6, 7, 8, 9}},
		{"project", "project-a", [10]int64{5, 6, 7, 8, 9, 5, 6, 7, 8, 9}},
		{"tenant", "tenant-b", [10]int64{10, 11, 12, 13, 14}},
	} {
		var got [10]int64
		err := db.QueryRowContext(ctx, `SELECT limit_refs,limit_bytes,limit_rows,limit_artifacts,limit_proofs,
used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity WHERE scope_kind=$1 AND scope_id=$2`, row.kind, row.id).
			Scan(&got[0], &got[1], &got[2], &got[3], &got[4], &got[5], &got[6], &got[7], &got[8], &got[9])
		if err != nil || got != row.want {
			t.Fatalf("%s/%s counters after migration = %v, want %v (error %v)", row.kind, row.id, got, row.want, err)
		}
	}
	var instances int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pipeline_capacity WHERE scope_kind='instance'`).Scan(&instances); err != nil || instances != 0 {
		t.Fatalf("legacy instance rows after migration = %d (error %v)", instances, err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO pipeline_capacity VALUES ('instance','',1,1,1,1,1)`); err == nil {
		t.Fatal("migration still allows an instance capacity row")
	}
}
