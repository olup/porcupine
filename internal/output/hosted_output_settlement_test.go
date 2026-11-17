package output

import (
	"database/sql"
	"errors"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestSharedPipelineHostedProjectOutputSettlement(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, effects := pipelineFixture(t)
	_, err := db.ExecContext(ctx, ExportPostgresSchemaV1+PipelineJobsPostgresSchemaV1+`
		CREATE TABLE managed_artifact_ownership(
			tenant_id TEXT, project_id TEXT, kind TEXT, storage_id TEXT, object_key TEXT, upload_id TEXT
		);
		UPDATE pipeline_effect_definitions SET rule_key='control-rule/pipeline/rule';
		INSERT INTO hosted_pipeline_configs VALUES('project','pipeline','tenant','name','{}','secret',0);
	`)
	if err != nil {
		t.Fatal(err)
	}

	scope := lakeapi.HostedProjectScope{Key: lakeapi.ProjectWriteKey{ProjectID: "project"}}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		items, err := HostedPipelineDeletionInventory(ctx, tx, "tenant", scope, nil)
		if err == nil && len(items) != 0 {
			t.Errorf("control-only definition produced provider objects: %+v", items)
		}
		return err
	})
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var exports *ExportJobs
		items, err := exports.HostedDeletionInventory(ctx, tx, "tenant", "project")
		if err == nil && len(items) != 0 {
			t.Errorf("empty exports produced provider objects: %+v", items)
		}
		return err
	})

	binding, plan, publication := pipelinePrepare(t, db, effects)
	pipelineTx(t, db, func(tx *sql.Tx) error {
		if err := effects.ReserveEffectsTx(ctx, tx, binding, plan, publication.Outputs); err != nil {
			return err
		}
		_, err := effects.PublishReservedEffectsTx(ctx, tx, binding, publication)
		return err
	})
	other := PipelineScope{"source", "other", "another-catalog", "another-stream"}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return effects.PrepareNoOutputStreamTx(ctx, tx, other, "other")
	})

	for range 2 {
		pipelineTx(t, db, func(tx *sql.Tx) error {
			return FinalizeHostedOutputsTx(ctx, tx, "tenant", "project")
		})
	}
	if got := pipelineCount(t, db, `
		SELECT (SELECT count(*) FROM hosted_pipeline_configs)
			+ (SELECT count(*) FROM pipeline_effect_jobs)
			+ (SELECT count(*) FROM pipeline_effect_members)
			+ (SELECT count(*) FROM pipeline_effect_cleanup)
			+ (SELECT count(*) FROM pipeline_effect_streams WHERE project_id='project')
			+ (SELECT count(*) FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project')`); got != 0 {
		t.Fatalf("selected project still has %d output records", got)
	}
	if got := pipelineCount(t, db, `SELECT used_rows FROM pipeline_capacity WHERE scope_kind='instance'`); got != 1 {
		t.Fatalf("other project's charged stream has %d rows in instance capacity", got)
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, err := HostedPipelineDeletionInventory(ctx, tx, "wrong-tenant", lakeapi.HostedProjectScope{
			Key:       lakeapi.ProjectWriteKey{ProjectID: other.ProjectID, SourceID: other.SourceID},
			CatalogID: other.CatalogID, StreamID: other.StreamID,
		}, nil)
		if !errors.Is(err, errHostedOutputOwnership) {
			t.Errorf("inventory accepted inconsistent ownership: %v", err)
		}
		return nil
	})
}
