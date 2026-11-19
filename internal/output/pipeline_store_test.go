package output

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func pipelineFixture(t *testing.T) (*sql.DB, *PGPipelineEffects) {
	t.Helper()
	db, e := sql.Open("pgx", pipelinePostgresDSN(t))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { db.Close() })
	_, e = db.Exec(`CREATE TABLE projects(id TEXT PRIMARY KEY,tenant_id TEXT);CREATE TABLE access_keys(id TEXT PRIMARY KEY,permissions TEXT,projects TEXT,revoked_at TEXT);CREATE TABLE tenant_credentials(principal_id TEXT PRIMARY KEY,tenant_id TEXT);CREATE TABLE project_deletions(project_id TEXT);CREATE TABLE tenant_deletions(tenant_id TEXT);CREATE TABLE service_api_keys(principal_id TEXT);CREATE TABLE email_humans(principal_id TEXT);CREATE TABLE oauth_interactive_grants(principal_id TEXT);` + PipelineSchemaV1 + PipelineDeliverySchemaV1 + PipelineTenantCapacitySchema)
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(`INSERT INTO projects VALUES('project','tenant'),('other','other');INSERT INTO access_keys VALUES('creator','["pipelines:write","events:read"]','["project"]',NULL);INSERT INTO tenant_credentials VALUES('creator','tenant');INSERT INTO pipeline_effect_definitions VALUES('tenant','project','pipeline','incarnation','rule','destination','creator','active','rule-key','','` + strings.Repeat("a", 64) + `',10)`)
	if e != nil {
		t.Fatal(e)
	}
	p, e := NewPGPipelineEffects(PipelineNativeCredentialAuthority{})
	if e != nil {
		t.Fatal(e)
	}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		for _, s := range [][2]string{{"tenant", "tenant"}, {"project", "project"}, {"tenant", "other"}, {"project", "other"}} {
			if e := p.ConfigureCapacityTx(context.Background(), tx, s[0], s[1], PipelineDemand{100, 1 << 20, 1000, 1 << 20, 10}); e != nil {
				return e
			}
		}
		return nil
	})
	return db, p
}
func pipelineTx(t *testing.T, db *sql.DB, f func(*sql.Tx) error) {
	t.Helper()
	tx, e := db.Begin()
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	if e = f(tx); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
}
func pipelinePrepare(t *testing.T, db *sql.DB, p *PGPipelineEffects) (PipelineBinding, PipelinePlan, PipelinePublication) {
	t.Helper()
	var plan PipelinePlan
	s := PipelineScope{"source", "project", "catalog", "stream"}
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var e error
		plan, e = p.FreezeLivePlanTx(context.Background(), tx, "tenant", s)
		return e
	})
	body := PipelineRef{"immutable-body", "version", strings.Repeat("b", 64), 100}
	o := PipelineOutput{ID: "delivery", PipelineID: "pipeline", Incarnation: "incarnation", RuleRevision: "rule", DestinationRevision: "destination", Generation: "generation", Kind: "delivery", Body: body, Members: []PipelineMember{{"uuid", 10}}, Charge: PipelineDemand{1, 100, 2, 100, 0}}
	v := PipelinePublication{Manifest: PipelineRef{"manifest", "version", strings.Repeat("f", 64), 10}, Sequence: 1, ProofSHA256: strings.Repeat("c", 64), Outputs: []PipelineOutput{o}}
	b := PipelineBinding{Scope: s, TenantID: "tenant", BatchID: "batch", PlanID: plan.ID, PreparedSHA256: PipelinePreparedSHA256(v.Outputs), AcquisitionID: "acquisition", ArtifactPrefix: "namespace/attempt/", Epoch: 1, ManifestBytesLimit: 10, PlanRef: PipelineRef{"plan", "version", strings.Repeat("d", 64), 10}, Demand: PipelineDemand{1, 200, 10, 1000, 1}}
	return b, plan, v
}
func pipelineCount(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var n int64
	if e := db.QueryRow(query).Scan(&n); e != nil {
		t.Fatal(e)
	}
	return n
}
func TestSharedPipelineEffectsReservePublishRollbackReplay(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	// A second batch on this stream cannot native-commit before publication.
	tx, _ := db.Begin()
	other := b
	other.BatchID = "second"
	if e := p.ReserveEffectsTx(ctx, tx, other, plan, v.Outputs); !errors.Is(e, ErrPipelineConflict) {
		t.Fatal(e)
	}
	tx.Rollback()
	// Policy reduction cannot strand a reservation that was already admitted.
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return p.ConfigureCapacityTx(ctx, tx, "project", "project", PipelineDemand{1, 1, 1, 1, 1})
	})
	tx, _ = db.Begin()
	if _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); e != nil {
		t.Fatal(e)
	}
	tx.Rollback()
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_jobs`) != 0 || pipelineCount(t, db, `SELECT sequence FROM pipeline_effect_streams`) != 0 {
		t.Fatal("rollback leaked publication")
	}
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); return e })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		changed, e := p.PublishReservedEffectsTx(ctx, tx, b, v)
		if changed {
			t.Fatal("replay changed state")
		}
		return e
	})
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_reservations`) != 0 || pipelineCount(t, db, `SELECT used_refs FROM pipeline_capacity WHERE scope_kind='project'`) != 1 {
		t.Fatal("capacity settlement mismatch")
	}
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_members`) != 1 {
		t.Fatal("members missing")
	}
}
func TestSharedPipelineEffectsPauseRetainsBacklog(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, effects := pipelineFixture(t)
	binding, plan, publication := pipelinePrepare(t, db, effects)
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return effects.ReserveEffectsTx(ctx, tx, binding, plan, publication.Outputs)
	})
	setState := func(state string) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `UPDATE pipeline_effect_definitions SET state=$1`, state); err != nil {
			t.Fatal(err)
		}
	}
	setState("paused")
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, err := effects.PublishReservedEffectsTx(ctx, tx, binding, publication)
		return err
	})
	if _, err := effects.NextDeliveryOutsideProjects(ctx, db, binding.Scope.SourceID, nil); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("selected a paused delivery: %v", err)
	}
	if pipelineCount(t, db, `SELECT used_refs FROM pipeline_capacity WHERE scope_kind='project'`) != 1 {
		t.Fatal("pause refunded backlog")
	}

	setState("active")
	delivery, err := effects.NextDeliveryOutsideProjects(ctx, db, binding.Scope.SourceID, nil)
	if err != nil || delivery.Body != publication.Outputs[0].Body {
		t.Fatalf("resumed backlog lost its frozen body: %+v %v", delivery, err)
	}
	setState("paused")
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, frozen, err := effects.FreezeDeliveryTx(ctx, tx, delivery, "attempt-paused", publication.Sequence, HostedWebhookLimits{MaxAttempts: 3, Timeout: time.Second})
		if frozen {
			t.Error("froze a delivery selected before pause")
		}
		return err
	})
	if pipelineCount(t, db, `SELECT attempts FROM pipeline_effect_jobs`) != 0 {
		t.Fatal("pause consumed a delivery attempt")
	}
}
func TestSharedPipelineEffectsDestinationChangeAndUnknown(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	tx, _ := db.Begin()
	if e := p.ReleaseAbsentReservationTx(ctx, tx, b, PipelineSettlement{}); !errors.Is(e, ErrPipelineConflict) {
		t.Fatal(e)
	}
	tx.Rollback()
	db.Exec(`UPDATE pipeline_effect_definitions SET destination_revision='new-auth-same-url'`)
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); return e })
	var state, dest string
	db.QueryRow(`SELECT state,destination_revision FROM pipeline_effect_jobs`).Scan(&state, &dest)
	pipelineTx(t, db, func(tx *sql.Tx) error {
		job, e := p.JobTx(ctx, tx, b.Scope, b.TenantID, "delivery")
		if job.Output.Body != v.Outputs[0].Body || job.Output.Members[0] != v.Outputs[0].Members[0] {
			t.Fatal("immutable metadata changed")
		}
		return e
	})
	if state != "retired" || dest != "destination" {
		t.Fatal("old body retargeted", state, dest)
	}
}
func TestPipelineSettlementRequiresSuccessor(t *testing.T) {
	t.Parallel()
	b := PipelineBinding{Epoch: 4}
	for _, s := range []PipelineSettlement{{}, {SuccessorEpoch: 4, Outcome: PipelineReleaseReservation}, {SuccessorEpoch: 5, Outcome: PipelinePublishReservation}} {
		if PipelineSettlementDecision(b, s) != PipelineKeepReservation {
			t.Fatal(s)
		}
	}
	if PipelineSettlementDecision(b, PipelineSettlement{SuccessorEpoch: 5, Outcome: PipelineReleaseReservation}) != PipelineReleaseReservation {
		t.Fatal("proven absence rejected")
	}
}

func TestSharedPipelineEffectsFullQueueOtherTenantAndAbsentCleanup(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ConfigureCapacityTx(ctx, tx, "project", "project", b.Demand) })
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	cold, _ := NewPGPipelineEffects(PipelineNativeCredentialAuthority{})
	pipelineTx(t, db, func(tx *sql.Tx) error {
		got, e := cold.ReservationTx(ctx, tx, b.Scope)
		if got != b {
			t.Fatal("cold binding changed")
		}
		return e
	})
	s := b.Scope
	s.StreamID = "second-stream"
	var secondPlan PipelinePlan
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var e error
		secondPlan, e = p.FreezeLivePlanTx(ctx, tx, b.TenantID, s)
		return e
	})
	second := b
	second.Scope = s
	second.PlanID = secondPlan.ID
	second.BatchID = "second"
	tx, _ := db.Begin()
	if e := p.ReserveEffectsTx(ctx, tx, second, secondPlan, v.Outputs); !errors.Is(e, ErrPipelineCapacity) {
		t.Fatal(e)
	}
	tx.Rollback()
	// Another tenant still progresses under the shared aggregate cap.
	s = PipelineScope{"source", "other", "other-catalog", "other-stream"}
	var otherPlan PipelinePlan
	pipelineTx(t, db, func(tx *sql.Tx) error { var e error; otherPlan, e = p.FreezeLivePlanTx(ctx, tx, "other", s); return e })
	other := b
	other.Scope = s
	other.TenantID = "other"
	other.PlanID = otherPlan.ID
	other.BatchID = "other"
	other.Demand.References = 0
	other.ManifestBytesLimit = 0
	other.PreparedSHA256 = PipelinePreparedSHA256(nil)
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, other, otherPlan, nil) })
	// Lease expiry has no refund API. Only successor-fenced absence may settle.
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return p.ReleaseAbsentReservationTx(ctx, tx, b, PipelineSettlement{SuccessorEpoch: b.Epoch + 1, Outcome: PipelineReleaseReservation})
	})
	if pipelineCount(t, db, `SELECT used_artifacts FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`) != b.Demand.ArtifactBytes {
		t.Fatal("uncertain uploads refunded before cleanup")
	}
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_reservations WHERE project_id='project'`) != 0 {
		t.Fatal("settled reservation retained")
	}
	retry := b
	retry.AcquisitionID = "new-acquisition"
	retry.ArtifactPrefix = "namespace/retry/"
	tx, _ = db.Begin()
	if e := p.ReserveEffectsTx(ctx, tx, retry, plan, v.Outputs); !errors.Is(e, ErrPipelineCapacity) {
		t.Fatal("retry escaped charged prior attempt capacity", e)
	}
	tx.Rollback()
	var clean PipelineCleanupClaim
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var e error
		clean, e = p.ClaimCleanupTx(ctx, tx, b.Scope, b.TenantID, b.AcquisitionID, "cleanup")
		return e
	})
	stale := clean
	stale.Attempt = "stale"
	tx, _ = db.Begin()
	if e := p.CompleteCleanupTx(ctx, tx, stale); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal(e)
	}
	tx.Rollback()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteCleanupTx(ctx, tx, clean) })
	if pipelineCount(t, db, `SELECT used_refs+used_bytes+used_rows+used_artifacts+used_proofs FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`) != 1 {
		t.Fatal("cleanup must retain one checkpoint metadata row")
	}
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, retry, plan, v.Outputs) })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return p.ReleaseAbsentReservationTx(ctx, tx, retry, PipelineSettlement{SuccessorEpoch: retry.Epoch + 1, Outcome: PipelineReleaseReservation})
	})
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var e error
		clean, e = p.ClaimCleanupTx(ctx, tx, retry.Scope, retry.TenantID, retry.AcquisitionID, "cleanup2")
		return e
	})
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteCleanupTx(ctx, tx, clean) })
	if pipelineCount(t, db, `SELECT used_rows FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`) != 1 {
		t.Fatal("retry recharged checkpoint metadata")
	}

}
func TestSharedPipelineEffectsCreatorRevocationAndScope(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	db.Exec(`UPDATE access_keys SET revoked_at='now'`)
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); return e })
	var state string
	db.QueryRow(`SELECT state FROM pipeline_effect_jobs`).Scan(&state)
	if state != "retired" {
		t.Fatal("revoked creator published pending output")
	}
	wrong := b
	wrong.Scope.CatalogID = "other"
	tx, _ := db.Begin()
	if _, e := p.PublishReservedEffectsTx(ctx, tx, wrong, v); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal(e)
	}
	tx.Rollback()
}

func TestSharedPipelineEffectsDefinitionReplacementRetiresPending(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); return e })
	old := plan.Targets[0]
	next := PipelineDefinition{TenantID: b.TenantID, ProjectID: b.Scope.ProjectID, State: "paused", Target: old}
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompareAndSwapDefinitionTx(ctx, tx, &old, next) })
	var state string
	db.QueryRow(`SELECT state FROM pipeline_effect_jobs`).Scan(&state)
	if state != "pending" {
		t.Fatal("pause retired backlog")
	}
	next.State = "active"
	next.Target.DestinationRevision = "same-url-other-account"
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompareAndSwapDefinitionTx(ctx, tx, &old, next) })
	db.QueryRow(`SELECT state FROM pipeline_effect_jobs`).Scan(&state)
	if state != "retired" {
		t.Fatal("destination replacement retained pending work")
	}
	tx, _ := db.Begin()
	if e := p.CompareAndSwapDefinitionTx(ctx, tx, &old, next); !errors.Is(e, ErrPipelineConflict) {
		t.Fatal(e)
	}
	tx.Rollback()
}

func TestSharedPipelineEffectsPlanArtifactAndCheckpointRemainCharged(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := context.Background()
	// Plan10 plus body100 cannot fit an artifact reservation of100.
	b.Demand.ArtifactBytes = 100
	tx, _ := db.Begin()
	if e := p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs); !errors.Is(e, ErrPipelineCapacity) {
		t.Fatal(e)
	}
	tx.Rollback()
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_reservations`) != 0 {
		t.Fatal("invalid preparation admitted")
	}
	// Even when plan+body fits, the manifest must have its own allowance.
	b.Demand.ArtifactBytes = 119
	tx, _ = db.Begin()
	if e := p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs); !errors.Is(e, ErrPipelineCapacity) {
		t.Fatal("manifest allowance missing", e)
	}
	tx.Rollback()
	b.Demand.ArtifactBytes = 120
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, b, v); return e })
	if pipelineCount(t, db, `SELECT artifacts FROM pipeline_effect_cleanup`) != 20 {
		t.Fatal("plan and manifest ownership not retained")
	}
}

func TestSharedPipelineEffectsAbsentAttemptDoesNotOwnRetry(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, v := pipelinePrepare(t, db, p)
	ctx := t.Context()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, v.Outputs) })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return p.ReleaseAbsentReservationTx(ctx, tx, b, PipelineSettlement{SuccessorEpoch: 2, Outcome: PipelineReleaseReservation})
	})
	retry := b
	retry.Epoch = 2
	retry.AcquisitionID = "retry-acquisition"
	retry.ArtifactPrefix = "namespace/retry/"
	v.Outputs[0].Generation = retry.AcquisitionID
	retry.PreparedSHA256 = PipelinePreparedSHA256(v.Outputs)
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, retry, plan, v.Outputs) })
	if pipelineCount(t, db, `SELECT used_artifacts FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`) != 2*b.Demand.ArtifactBytes {
		t.Fatal("old uncertain attempt not charged alongside retry")
	}
	var c PipelineCleanupClaim
	pipelineTx(t, db, func(tx *sql.Tx) error {
		var e error
		c, e = p.ClaimCleanupTx(ctx, tx, b.Scope, b.TenantID, b.AcquisitionID, "clean-old")
		return e
	})
	if c.Prefix != b.ArtifactPrefix || c.Generation != b.AcquisitionID {
		t.Fatal("cleanup crossed attempts", c)
	}
	wrong := c
	wrong.Prefix = retry.ArtifactPrefix
	tx, _ := db.Begin()
	if e := p.CompleteCleanupTx(ctx, tx, wrong); !errors.Is(e, sql.ErrNoRows) {
		t.Fatal("wrong cleanup namespace accepted", e)
	}
	tx.Rollback()
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.CompleteCleanupTx(ctx, tx, c) })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		got, e := p.ReservationTx(ctx, tx, b.Scope)
		if e == nil && got != retry {
			t.Fatal("cleanup modified retry", got)
		}
		return e
	})
	pipelineTx(t, db, func(tx *sql.Tx) error { _, e := p.PublishReservedEffectsTx(ctx, tx, retry, v); return e })
}

func TestSharedPipelineEffectsNoSelectedOutputsNeedNoArtifacts(t *testing.T) {
	t.Parallel()
	db, p := pipelineFixture(t)
	b, plan, _ := pipelinePrepare(t, db, p)
	ctx := t.Context()
	b.PlanRef = PipelineRef{}
	b.PreparedSHA256 = PipelinePreparedSHA256([]PipelineOutput{})
	b.ManifestBytesLimit = 0
	b.Demand = PipelineDemand{MetadataRows: int64(3 + len(plan.Targets)), ProofSlots: 1}
	pipelineTx(t, db, func(tx *sql.Tx) error { return p.ReserveEffectsTx(ctx, tx, b, plan, []PipelineOutput{}) })
	pipelineTx(t, db, func(tx *sql.Tx) error {
		_, e := p.PublishReservedEffectsTx(ctx, tx, b, PipelinePublication{Sequence: 1, ProofSHA256: strings.Repeat("e", 64), Outputs: []PipelineOutput{}})
		return e
	})
	if pipelineCount(t, db, `SELECT count(*) FROM pipeline_effect_cleanup`) != 0 {
		t.Fatal("empty selection left artifact cleanup")
	}
	if pipelineCount(t, db, `SELECT used_refs+used_bytes+used_rows+used_artifacts+used_proofs FROM pipeline_capacity WHERE scope_kind='project' AND scope_id='project'`) != 1 {
		t.Fatal("empty selection retained transient charge")
	}
}
func TestPipelinePreparationContentDigestRetainsVersionPins(t *testing.T) {
	t.Parallel()
	original := []PipelineOutput{{ID: "body", Body: PipelineRef{Key: "key", SHA256: strings.Repeat("b", 64), Bytes: 12}}}
	digest := PipelinePreparedSHA256(original)
	original[0].Body.VersionID = "actual-returned-pin"
	if PipelinePreparedSHA256(original) != digest || original[0].Body.VersionID != "actual-returned-pin" {
		t.Fatal("version pin changed preparation or was discarded")
	}
	original[0].Body.Bytes++
	if PipelinePreparedSHA256(original) == digest {
		t.Fatal("content changed without changing preparation")
	}
}

func TestSharedPipelineTenantCapacityIsolation(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	db, effects := pipelineFixture(t)
	binding, plan, publication := pipelinePrepare(t, db, effects)
	pipelineTx(t, db, func(tx *sql.Tx) error {
		limits := binding.Demand
		return effects.ConfigureCapacityTx(ctx, tx, "tenant", binding.TenantID, limits)
	})
	pipelineTx(t, db, func(tx *sql.Tx) error {
		return effects.ReserveEffectsTx(ctx, tx, binding, plan, publication.Outputs)
	})

	// A new stream of the same tenant cannot borrow capacity from its neighbors.
	next := binding
	next.Scope.StreamID = "next-stream"
	nextPlan := plan
	nextPlan.Scope = next.Scope
	nextPlan.ID = pipelinePlanID(nextPlan)
	next.PlanID = nextPlan.ID
	tryReserve := func(b PipelineBinding, outputs []PipelineOutput) error {
		t.Helper()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if err := effects.ReserveCurrentEffectsTx(ctx, tx, b, outputs); err != nil {
			return err
		}
		return tx.Commit()
	}
	if err := tryReserve(next, publication.Outputs); !errors.Is(err, ErrPipelineCapacity) || errors.Is(err, ErrPipelineDemandTooLarge) {
		t.Fatalf("full tenant should report occupied capacity, got %v", err)
	}
	next.Demand.ArtifactBytes++
	if err := tryReserve(next, publication.Outputs); !errors.Is(err, ErrPipelineDemandTooLarge) || !errors.Is(err, ErrPipelineCapacity) {
		t.Fatalf("demand that cannot fit should report its own size, got %v", err)
	}

	// A second tenant can reserve the same demand while the first still owns it.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO access_keys VALUES('other-creator','["pipelines:write","events:read"]','["other"]',NULL);
		INSERT INTO tenant_credentials VALUES('other-creator','other');
		INSERT INTO pipeline_effect_definitions
		SELECT 'other','other',id,incarnation,rule_revision,destination_revision,'other-creator',state,rule_key,rule_version,rule_sha,rule_bytes
		FROM pipeline_effect_definitions WHERE project_id='project';
	`); err != nil {
		t.Fatal(err)
	}
	other := binding
	other.TenantID = "other"
	other.Scope.ProjectID = "other"
	otherPlan := plan
	otherPlan.TenantID = other.TenantID
	otherPlan.Scope = other.Scope
	otherPlan.Targets = append([]PipelineTarget(nil), plan.Targets...)
	otherPlan.Targets[0].CreatorID = "other-creator"
	otherPlan.ID = pipelinePlanID(otherPlan)
	other.PlanID = otherPlan.ID
	if err := tryReserve(other, publication.Outputs); err != nil {
		t.Fatalf("full tenant blocked another tenant: %v", err)
	}
	if got := pipelineCount(t, db, `SELECT count(*) FROM pipeline_capacity WHERE scope_kind='tenant' AND used_refs=1 AND used_proofs=1`); got != 2 {
		t.Fatalf("charged %d tenant counters, want 2", got)
	}
}
