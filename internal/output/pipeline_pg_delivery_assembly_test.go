package output

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/delivery"
)

func TestSharedPipelineAssemblyFreezesCrossCommitProgress(t *testing.T) {
	t.Parallel()

	t.Run("frozen delivery", func(t *testing.T) { testPipelineAssemblyRecomposition(t, true) })
	t.Run("unfrozen delivery", func(t *testing.T) { testPipelineAssemblyRecomposition(t, false) })
}

func testPipelineAssemblyRecomposition(t *testing.T, frozen bool) {
	t.Helper()
	db, p := pipelineFixture(t)
	ctx := context.Background()
	for i, id := range []string{"first", "second"} {
		b, plan, v := pipelinePrepare(t, db, p)
		b.BatchID = id
		b.ArtifactPrefix += id + "/"
		b.AcquisitionID = id
		v.Sequence = int64(i + 1)
		o := &v.Outputs[0]
		o.ID = id
		o.Kind = "fragment"
		o.Members[0].UUID = id
		o.Fragment = &PipelineFragmentSpec{WindowStartMS: 0, WindowEndMS: 60000, Rule: delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60}}
		o.Charge.QueuedBytes = 1034
		b.Demand.QueuedBytes = 1034
		b.PreparedSHA256 = PipelinePreparedSHA256(v.Outputs)
		pipelineTx(t, db, func(tx *sql.Tx) error {
			if err := p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs); err != nil {
				return err
			}
			_, err := p.PublishReservedEffectsTx(ctx, tx, b, v)
			return err
		})
	}
	first, err := p.NextFragment(ctx, db, "source")
	if err != nil {
		t.Fatal(err)
	}
	fragments, err := p.AssemblyFragments(ctx, db, first, 1<<20)
	if err != nil || len(fragments) != 2 || fragments[0].Sequence != 1 || fragments[1].Sequence != 2 {
		t.Fatal(fragments, err)
	}
	a := PipelineAssembly{Scope: first.Claim.Scope, TenantID: "tenant", Generation: "composition", Attempt: "composition-upload", Prefix: "assembly/attempt/", Cut: 2, ArtifactLimit: 10000}
	for _, f := range fragments {
		a.Inputs = append(a.Inputs, PipelineAssemblyInput{Fragment: f, Eligible: []int{0}})
	}
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveAssemblyTx(ctx, tx, a) })
	got, err := p.Assembly(ctx, db, a.Scope, a.TenantID)
	if err != nil || pipelineDigest(a) != pipelineDigest(got) {
		t.Fatal(got, err)
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, err := p.ClaimCleanupTx(ctx, tx, a.Scope, a.TenantID, a.Generation, "cleanup-worker")
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal("active assembly lost cleanup protection", err)
		}
		return nil
	})
	r := PipelineAssemblyResult{ID: "assembled", Body: PipelineRef{a.Prefix + "body", "v1", strings.Repeat("b", 64), 100}, Manifest: PipelineRef{a.Prefix + "manifest", "v2", strings.Repeat("c", 64), 100}, Count: 3}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.CompleteAssemblyTx(ctx, tx, a, r); !errors.Is(err, ErrPipelineConflict) {
		t.Fatal("count exceeds frozen eligible membership", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := pipelineCount(t, db, `SELECT sum(next_member) FROM pipeline_effect_jobs`); n != 0 {
		t.Fatal(n)
	}
	r.Count = 2
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteAssemblyTx(ctx, tx, a, r) })
	if n := pipelineCount(t, db, `SELECT sum(next_member) FROM pipeline_effect_jobs`); n != 2 {
		t.Fatal(n)
	}
	if n := pipelineCount(t, db, `SELECT used_refs FROM pipeline_capacity WHERE scope_kind='project'`); n != 2 {
		t.Fatal(n)
	}
	d, err := p.NextDelivery(ctx, db, "source")
	if err != nil || d.Claim.ID != r.ID || d.Body != r.Body {
		t.Fatal(d, err)
	}
	members, err := p.DeliveryMembers(ctx, db, d.Claim)
	if err != nil || len(members) != 2 || members[0].UUID != "first" || members[1].UUID != "second" {
		t.Fatal(members, err)
	}
	if frozen {
		if _, err := db.Exec(`UPDATE pipeline_effect_jobs SET send_cut=sequence WHERE id='assembled'`); err != nil {
			t.Fatal(err)
		}
	}

	pipelineTx(t, db, func(tx *sql.Tx) error {
		return p.CancelHostedWebhookEventTx(ctx, tx, a.Scope, a.TenantID, "first")
	})
	if _, err := p.NextDelivery(ctx, db, "source"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("deleted member remained sendable", err)
	}
	canceled, err := p.NextFragment(ctx, db, "source")
	if err != nil || !canceled.Recompose || canceled.Claim.ID != r.ID {
		t.Fatal(canceled, err)
	}
	candidates, err := p.AssemblyFragments(ctx, db, canceled, 1<<20)
	if err != nil || len(candidates) != 1 {
		t.Fatal(candidates, err)
	}
	recompose := PipelineAssembly{Scope: a.Scope, TenantID: a.TenantID, Generation: "recomposition", Attempt: "recomposition-upload", Prefix: "assembly/recompose/", Cut: 2, ArtifactLimit: 10000,
		Inputs: []PipelineAssemblyInput{{Fragment: candidates[0], Eligible: []int{1}}}}
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveAssemblyTx(ctx, tx, recompose) })
	replacement := PipelineAssemblyResult{ID: "replacement", Body: PipelineRef{recompose.Prefix + "body", "v3", strings.Repeat("d", 64), 100},
		Manifest: PipelineRef{recompose.Prefix + "manifest", "v4", strings.Repeat("e", 64), 100}, Count: 1}
	if !frozen {
		replacement.ID = r.ID
	}

	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteAssemblyTx(ctx, tx, recompose, replacement) })
	d, err = p.NextDelivery(ctx, db, "source")
	if err != nil || d.Claim.ID != replacement.ID || d.Body != replacement.Body {
		t.Fatal(d, err)
	}
	members, err = p.DeliveryMembers(ctx, db, d.Claim)
	if err != nil || len(members) != 1 || members[0].UUID != "second" {
		t.Fatal(members, err)
	}
	if n := pipelineCount(t, db, `SELECT used_refs FROM pipeline_capacity WHERE scope_kind='project'`); n != 2 {
		t.Fatal(n)
	}

}
