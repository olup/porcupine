package output

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/usage"
)

func TestSharedPipelineDeliveryAttemptCAS(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, p := pipelineFixture(t)
	if _, err := db.Exec(usage.ControlSchema); err != nil {
		t.Fatal(err)
	}
	b, plan, v := pipelinePrepare(t, db, p)
	pipelineTx(t, db, func(tx *sql.Tx) error {
		if err := p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs); err != nil {
			return err
		}
		_, err := p.PublishReservedEffectsTx(ctx, tx, b, v)
		return err
	})
	limits := HostedWebhookLimits{MaxAttempts: 3, Timeout: time.Second}
	freeze := func(d PipelineDelivery, nonce string) PipelineClaim {
		t.Helper()
		var c PipelineClaim
		pipelineTx(t, db, func(tx *sql.Tx) error {
			var ok bool
			var err error
			c, ok, err = p.FreezeDeliveryTx(ctx, tx, d, nonce, 1, limits)
			if err == nil && !ok {
				t.Fatal("attempt was not frozen")
			}
			return err
		})
		return c
	}
	d, err := p.NextDelivery(ctx, db, "source")
	if err != nil {
		t.Fatal(err)
	}
	original := freeze(d, "first")
	if _, err := p.NextDelivery(ctx, db, "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pipeline_effect_jobs SET lease_until=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}

	// A pre-send failure can defer an expired attempt without changing its
	// ownership, leaving another project eligible in the same scheduler pass.
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.DeferPipelineWorkTx(ctx, tx, original) })
	if _, err := p.NextDelivery(ctx, db, "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO pipeline_effect_definitions
 SELECT 'other','other',id,incarnation,rule_revision,destination_revision,creator_id,state,rule_key,rule_version,rule_sha,rule_bytes
 FROM pipeline_effect_definitions WHERE project_id='project';
 INSERT INTO pipeline_effect_jobs(source_id,project_id,catalog_id,stream_id,tenant_id,id,batch_id,pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,state,refs,bytes,rows,artifacts)
 SELECT source_id,'other',catalog_id,'other-stream','other',id,batch_id,pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,'pending',refs,bytes,rows,artifacts
 FROM pipeline_effect_jobs WHERE project_id='project'`); err != nil {
		t.Fatal(err)
	}
	other, err := p.NextDelivery(ctx, db, "source")
	if err != nil || other.Claim.Scope.ProjectID != "other" {
		t.Fatal(other, err)
	}
	if _, err := db.Exec(`DELETE FROM pipeline_effect_jobs WHERE project_id='other';
 UPDATE pipeline_effect_jobs SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := p.NextDelivery(ctx, db, "source")
	if err != nil || reclaimed.Attempts != 1 || reclaimed.Body != d.Body || reclaimed.Claim.Generation != original.Generation {
		t.Fatal(reclaimed, err)
	}
	current := freeze(reclaimed, "second")
	pipelineTx(t, db, func(tx *sql.Tx) error {
		if err := p.CompleteDeliveryTx(ctx, tx, d, original, 200, "", false, 3); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		return p.CompleteDeliveryTx(ctx, tx, reclaimed, current, 503, "http_rejected", true, 3)
	})
	if _, err := p.NextDelivery(ctx, db, "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE pipeline_effect_jobs SET next_attempt_at=clock_timestamp()-interval '1 second';
 UPDATE pipeline_effect_cooldowns SET not_before=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	last, err := p.NextDelivery(ctx, db, "source")
	if err != nil || last.Attempts != 2 || last.Body != d.Body {
		t.Fatal(last, err)
	}
	final := freeze(last, "third")
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteDeliveryTx(ctx, tx, last, final, 204, "", false, 3) })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		if err := p.CompleteDeliveryTx(ctx, tx, last, final, 204, "", false, 3); !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		return nil
	})
	if n := pipelineCount(t, db, `SELECT sum(webhook_success) FROM usage_rollups`); n != 1 {
		t.Fatal(n)
	}
	if n := pipelineCount(t, db, `SELECT used_refs FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`); n != 1 {
		t.Fatalf("retained references after delivery completion = %d, want 1", n)
	}
}
