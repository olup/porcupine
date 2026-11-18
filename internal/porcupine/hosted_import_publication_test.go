package porcupine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresNativeImportPublication(t *testing.T) {
	t.Parallel()
	runNativeImportPublication(t, false)
}
func TestDuckLakeS3SharedPostgresNativeImportPublicationVersioned(t *testing.T) {
	t.Parallel()
	runNativeImportPublication(t, true)
}
func runNativeImportPublication(t *testing.T, versioned bool) {
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	if versioned {
		fixture.storage.Bucket += "-imports-" + eventapi.NewID()[:8]
		if _, err := fixture.client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(fixture.storage.Bucket)}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{Bucket: aws.String(fixture.storage.Bucket), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
			t.Fatal(err)
		}
	}
	a, b := sharedNativeJourneyPair(t, fixture)
	for _, n := range []*sharedJourneyNode{a, b} {
		n.service.usage.cancel()
		<-n.service.usage.done
	}
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "native imports")
	sa, sb := a.service.lake.(*lakeapi.DuckLakeStore), b.service.lake.(*lakeapi.DuckLakeStore)
	key := lakeapi.ProjectWriteKey{SourceID: sa.Config.SharedSourceID, ProjectID: project}
	scope, err := sa.InitializeHostedProject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := lakeapi.NewPostgresProjectCoordinator(a.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	cb, err := lakeapi.NewPostgresProjectCoordinator(b.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	fa, err := newHostedLiveFlusher(a.service.control, sa, ca, a.service.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := newHostedLiveFlusher(b.service.control, sb, cb, b.service.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	limits := output.PipelineDemand{References: 10, QueuedBytes: 1 << 20, MetadataRows: 100, ArtifactBytes: 1 << 20, ProofSlots: 2}
	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	live := func(id, uuid string) bufferedFlushBatch {
		e := eventapi.Event{ProjectID: project, UUID: uuid, Event: "page", DistinctID: "browser", Properties: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Timestamp: "2026-11-17T01:00:00Z", ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		return bufferedFlushBatch{id: id, projectID: project, events: []bufferedLiveEvent{{input: validatedLiveEvent{event: e}, admittedAt: time.Now().UTC()}}}
	}
	hash := func(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
	makeInput := func(file string, previous lakeapi.ImportFileCheckpoint, phase string, ids ...string) lakeapi.ImportChunkInput {
		raw, _ := json.Marshal(struct{ Generation int64 }{previous.Generation + 1})
		ref, e := sa.Remote.PutImmutableImportCheckpoint(t.Context(), scope, "job", file, raw)
		if e != nil {
			t.Fatal(e)
		}
		input := lakeapi.ImportChunkInput{Key: lakeapi.ImportChunkKey{Scope: scope, JobID: "job", FileID: file, Start: previous.Offset, ExpectedEnd: previous.Offset + 1, Phase: phase}, TenantID: defaultTenantID, ManifestSHA256: hash([]byte("manifest")), SourceID: "archive", Format: "native", InputSHA256: hash([]byte("x")), Previous: previous, Parser: ref, LineNumber: previous.LineNumber + int64(len(ids)), Parsed: importer.ImportCounts{Rows: int64(len(ids))}}
		if phase == "finalize" {
			input.Key.ExpectedEnd = input.Key.Start
			input.InputSHA256 = hash(nil)
		}
		for i, id := range ids {
			input.Candidates = append(input.Candidates, lakeapi.ImportCandidate{Position: previous.LineNumber + int64(i) + 1, Row: importer.Row{Kind: "events", Event: eventapi.Event{ProjectID: project, UUID: id, SourceID: "archive", Event: "historical", Properties: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Timestamp: "2001-01-01T00:00:00Z"}}})
		}
		return input
	}
	commit := func(input lakeapi.ImportChunkInput) (lakeapi.NativeCommitEvidence, error) {
		session, e := ca.Acquire(t.Context(), key)
		if e != nil {
			return lakeapi.NativeCommitEvidence{}, e
		}
		defer session.Release()
		var result lakeapi.NativeCommitEvidence
		e = session.RunNative(t.Context(), func(ctx context.Context) error {
			fence := session.Fence(scope.StreamID)
			if e := sa.AdvanceRecoveryFence(ctx, fence); e != nil {
				return e
			}
			if e := fa.publisher.transaction(ctx, func(tx *sql.Tx) error {
				return fa.publisher.effects.PrepareNoOutputStreamTx(ctx, tx, pipelineScope(scope), input.TenantID)
			}); e != nil {
				return e
			}
			var ids lakeapi.EventIDReservation
			var e error
			if len(input.Candidates) > 0 {
				ids, e = session.ReserveEventIDs(ctx, scope.StreamID, input.Key.ID(), len(input.Candidates))
				if e != nil {
					return e
				}
			}
			result, e = sa.CommitImportChunk(ctx, fence, input, ids)
			return e
		})
		return result, e
	}
	// A different real project's durable cursor fills tenant metadata capacity.
	// A first import must fail before committing native data when no row fits.
	otherProject, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "metadata owner")
	otherKey := lakeapi.ProjectWriteKey{SourceID: key.SourceID, ProjectID: otherProject}
	otherScope, err := sa.InitializeHostedProject(t.Context(), otherKey)
	if err != nil {
		t.Fatal(err)
	}
	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, "project", otherProject, limits); e != nil {
			return e
		}
		one := limits
		one.MetadataRows = 1
		return fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, "tenant", defaultTenantID, one)
	}); err != nil {
		t.Fatal(err)
	}
	otherInput := makeInput("metadata-owner", lakeapi.ImportFileCheckpoint{}, "finalize")
	otherInput.Key.Scope = otherScope
	otherInput.Parser, err = sa.Remote.PutImmutableImportCheckpoint(t.Context(), otherScope, "job", "metadata-owner", []byte(`{"Generation":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = fa.CommitImport(t.Context(), otherInput); err != nil {
		t.Fatal("real metadata owner", err)
	}
	bootstrap := makeInput("bootstrap", lakeapi.ImportFileCheckpoint{}, "finalize")
	if _, err = fa.CommitImport(t.Context(), bootstrap); !errors.Is(err, output.ErrPipelineCapacity) {
		t.Fatal("native import bypassed first-cursor metadata admission", err)
	}
	before, err := sa.PendingNativeEffects(t.Context(), scope, 0, 1)
	if err != nil || before.NativeHighWater != 0 || len(before.Evidence) != 0 {
		t.Fatal("metadata rejection left invisible native outcome", before, err)
	}
	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		return fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, "tenant", defaultTenantID, limits)
	}); err != nil {
		t.Fatal(err)
	}
	bootstrapProof, err := fa.CommitImport(t.Context(), bootstrap)
	if err != nil {
		t.Fatal("first metadata-only native cursor", err)
	}
	if bootstrapProof.AcceptedImport != 0 || bootstrapProof.Sequence != 1 {
		t.Fatal(bootstrapProof)
	}
	var refs, queued, artifacts, proofs, metadata int64
	if err = a.service.control.QueryRowContext(t.Context(), `SELECT used_refs,used_bytes,used_artifacts,used_proofs,used_rows FROM pipeline_capacity WHERE scope_kind='tenant' AND scope_id='default'`).Scan(&refs, &queued, &artifacts, &proofs, &metadata); err != nil || refs != 0 || queued != 0 || artifacts != 0 || proofs != 0 || metadata != 2 {
		t.Fatal("import charged output capacity", refs, queued, artifacts, proofs, metadata, err)
	}
	firstID := eventapi.NewID()
	if err = fa.FlushLive(t.Context(), live("first-live", firstID)); err != nil {
		t.Fatal(err)
	}
	importedID := eventapi.NewID()
	input := makeInput("events", lakeapi.ImportFileCheckpoint{}, "bytes", firstID, importedID, importedID)
	p, release, err := sa.LeaseProject(t.Context(), project)
	if err != nil {
		t.Fatal(err)
	}
	realDB := p.DB
	var loseNative, failACK atomic.Bool
	bridge, err := installHostedNativeBoundary(realDB, &loseNative, &failACK)
	if err != nil {
		t.Fatal(err)
	}
	p.DB = bridge
	release()
	defer func() { p.DB = realDB; bridge.Close() }()
	loseNative.Store(true)
	if _, err = fa.CommitImport(t.Context(), input); err == nil || loseNative.Load() {
		t.Fatal("actual native response loss not exercised", err)
	}
	// No PG import cursor exists yet, but the original native result is durable.
	page, err := sb.PendingNativeEffects(t.Context(), scope, 2, 1)
	if err != nil || len(page.Evidence) != 1 {
		t.Fatal(page, err)
	}
	original := page.Evidence[0]
	if original.Import.Delta != (importer.ImportCounts{Rows: 3, Accepted: 1, Duplicates: 2}) {
		t.Fatal(original.Import)
	}
	// The actual usage participant can reject the PG transaction. Its cursor
	// update must roll back with usage even though the native outcome is durable.
	if _, err = a.service.control.ExecContext(t.Context(), `CREATE FUNCTION reject_import_usage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected import usage failure'; END $$; CREATE TRIGGER reject_import_usage BEFORE INSERT OR UPDATE ON usage_rollups FOR EACH ROW EXECUTE FUNCTION reject_import_usage()`); err != nil {
		t.Fatal(err)
	}
	if err = fb.RecoverProject(t.Context(), key); err == nil {
		t.Fatal("failed import usage participant published")
	}
	seqBefore, _, err := fb.publisher.checkpoint(t.Context(), scope, defaultTenantID)
	if err != nil || seqBefore != 2 {
		t.Fatal("usage failure moved import cursor", seqBefore, err)
	}
	if _, err = a.service.control.ExecContext(t.Context(), `DROP TRIGGER reject_import_usage ON usage_rollups; DROP FUNCTION reject_import_usage()`); err != nil {
		t.Fatal(err)
	}
	// Original physical checkpoint pins survive replacement of the latest
	// object. Unversioned content tampering fails before the PG cursor moves.
	parserBody, err := sa.Remote.ReadImmutableImportCheckpoint(t.Context(), scope, "job", "events", original.Import.Next.Parser)
	if err != nil {
		t.Fatal(err)
	}
	putParser := func(body []byte) {
		if _, e := fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(original.Import.Next.Parser.Key), Body: bytes.NewReader(body)}); e != nil {
			t.Fatal(e)
		}
	}
	putParser([]byte(`{"tampered":true}`))
	if versioned {
		if original.Import.Next.Parser.VersionID == "" {
			t.Fatal("missing actual checkpoint version pin")
		}
		missing := original.Import.Next.Parser
		missing.VersionID += "-absent"
		if _, err = sa.Remote.ReadImmutableImportCheckpoint(t.Context(), scope, "job", "events", missing); err == nil {
			t.Fatal("missing version pin was guessed")
		}
		if _, err = sa.Remote.ReadImmutableImportCheckpoint(t.Context(), scope, "other-job", "events", original.Import.Next.Parser); err == nil {
			t.Fatal("checkpoint crossed job scope")
		}
		if err = fb.RecoverProject(t.Context(), key); err != nil {
			t.Fatal("pinned checkpoint changed with latest version", err)
		}
	} else {
		if err = fb.RecoverProject(t.Context(), key); err == nil {
			t.Fatal("unversioned checkpoint tamper published")
		}
		seq, _, e := fb.publisher.checkpoint(t.Context(), scope, defaultTenantID)
		if e != nil || seq != 2 {
			t.Fatal("tamper moved common cursor", seq, e)
		}
		putParser(parserBody)
		if err = fb.RecoverProject(t.Context(), key); err != nil {
			t.Fatal("restored original checkpoint", err)
		}
	}
	recovered, err := fb.CommitImport(t.Context(), input)
	if err != nil || !reflect.DeepEqual(original, recovered) {
		t.Fatal("original native outcome changed", original, recovered, err)
	}
	if err = fb.FlushLive(t.Context(), live("after-import", eventapi.NewID())); err != nil {
		t.Fatal("mixed live/import/live", err)
	}
	if err = fa.FlushLive(t.Context(), live("import-dedup", importedID)); err != nil {
		t.Fatal("live/import dedup", err)
	}
	// Parser-only EOF uses no ID allocation and publishes cumulative totals with
	// zero accepted increment. PG commit loss rereads the same common cursor.
	eof := makeInput("events", original.Import.Next, "finalize")
	eofProof, err := commit(eof)
	if err != nil {
		t.Fatal(err)
	}
	var losePG atomic.Bool
	lostDB, err := installLostHostedCommit(a.controlDSN, fa.publisher, &losePG)
	if err != nil {
		t.Fatal(err)
	}
	defer lostDB.Close()
	losePG.Store(true)
	failACK.Store(true)
	if err = fa.RecoverProject(t.Context(), key); err != nil || losePG.Load() || failACK.Load() {
		t.Fatal("import PG/ACK response boundary", err)
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold ACK retry", err)
	}
	seq, proof, err := fb.publisher.checkpoint(t.Context(), scope, defaultTenantID)
	if err != nil || seq != eofProof.Sequence || proof != eofProof.ProofFingerprint {
		t.Fatal(seq, proof, err)
	}
	var liveCount, importCount int64
	if err = a.service.control.QueryRowContext(t.Context(), `SELECT coalesce(sum(live_events),0),coalesce(sum(import_events),0) FROM usage_rollups WHERE project_id=$1`, project).Scan(&liveCount, &importCount); err != nil || liveCount != 2 || importCount != 1 {
		t.Fatal("mixed cumulative usage", liveCount, importCount, err)
	}
	for _, table := range []string{"pipeline_effect_jobs", "pipeline_effect_reservations", "pipeline_effect_plans"} {
		var count int
		if err = a.service.control.QueryRowContext(t.Context(), `SELECT count(*) FROM `+table).Scan(&count); err != nil || count != 0 {
			t.Fatal("import created live output metadata", table, count, err)
		}
	}
	// A real pending live reservation cannot be skipped by import publication.
	next := makeInput("empty", lakeapi.ImportFileCheckpoint{}, "finalize")
	nextProof, err := commit(next)
	if err != nil {
		t.Fatal(err)
	}
	pendingSession, err := ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	var pendingBinding output.PipelineBinding
	err = pendingSession.RunNative(t.Context(), func(ctx context.Context) error {
		fence := pendingSession.Fence(scope.StreamID)
		if err := sa.AdvanceRecoveryFence(ctx, fence); err != nil {
			return err
		}
		plan, routing, err := fa.publisher.plan(ctx, defaultTenantID, scope)
		if err != nil {
			return err
		}
		stager := reservingHostedStager{publisher: fa.publisher, session: pendingSession, fence: fence, plan: plan, routing: routing, admission: &fa.policy}
		if _, err = stager.StageLiveEffects(ctx, lakeapi.SelectedLiveEffects{Scope: scope, TenantID: defaultTenantID, BatchID: "pending-live", Sequence: nextProof.Sequence + 1}); err != nil {
			return err
		}
		pendingBinding, err = fa.publisher.reservation(ctx, scope)
		return err
	})
	pendingSession.Release()
	if err != nil {
		t.Fatal(err)
	}
	if err = fa.publisher.publish(t.Context(), output.PipelineBinding{}, nextProof); !errors.Is(err, output.ErrPipelineConflict) {
		t.Fatal("import skipped live reservation", err)
	}
	session, err := ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	err = session.RunNative(t.Context(), func(ctx context.Context) error {
		fence := session.Fence(scope.StreamID)
		if e := sa.AdvanceRecoveryFence(ctx, fence); e != nil {
			return e
		}
		if e := fa.publisher.transaction(ctx, func(tx *sql.Tx) error {
			return fa.publisher.effects.ReleaseAbsentReservationTx(ctx, tx, pendingBinding, output.PipelineSettlement{SuccessorEpoch: fence.Epoch, Outcome: output.PipelineReleaseReservation})
		}); e != nil {
			return e
		}
		_, e := sa.SealHostedProject(ctx, fence)
		return e
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []*sharedJourneyNode{a, b} {
		node.service.deletionCancel()
		<-node.service.deletionDone
	}
	if err := a.service.beginControlProjectDeletion(t.Context(), key.ProjectID); err != nil {
		t.Fatal(err)
	}
	if err := fb.sealDeletedProject(t.Context(), key); !errors.Is(err, output.ErrPipelineConflict) {
		t.Fatal("deletion acknowledged an import without a checkpoint provider", err)
	}
	if current, _, err := fb.publisher.checkpoint(t.Context(), scope, defaultTenantID); err != nil || current != seq {
		t.Fatal("unconfirmed import advanced the shared cursor", current, err)
	}
	outcome := nextProof.Import
	if _, err := a.service.control.ExecContext(t.Context(), `INSERT INTO import_uploads
(id,project_id,actor_id,source_id,format,idempotency_hash,request_hash,state,created_at,updated_at)
VALUES($1,$2,'test',$3,$4,'deletion-replay',$5,'uploading','test','test')`,
		outcome.Key.JobID, project, outcome.SourceID, outcome.Format, outcome.ManifestSHA256); err != nil {
		t.Fatal(err)
	}
	if _, err := a.service.control.ExecContext(t.Context(), `INSERT INTO import_upload_files
(id,job_id,ordinal,path,kind,size_bytes,sha256)
VALUES($1,$2,0,'empty','events',0,$3)`, outcome.Key.FileID, outcome.Key.JobID, next.InputSHA256); err != nil {
		t.Fatal(err)
	}
	newHostedImportManager(b.service.eventRules, fb)
	if err = fb.sealDeletedProject(t.Context(), key); err != nil {
		t.Fatal("sealed import recovery", err)
	}
	seq, proof, err = fb.publisher.checkpoint(t.Context(), scope, defaultTenantID)
	if err != nil || seq != nextProof.Sequence || proof != nextProof.ProofFingerprint {
		t.Fatal("deletion fence lost the committed import outcome", seq, proof, err)
	}
	var state string
	var generation int64
	if err := a.service.control.QueryRowContext(t.Context(), `SELECT state,generation FROM import_upload_files WHERE job_id=$1 AND id=$2`,
		outcome.Key.JobID, outcome.Key.FileID).Scan(&state, &generation); err != nil || state != "completed" || generation != outcome.Next.Generation {
		t.Fatal("deletion fence lost the import checkpoint", state, generation, err)
	}
}

// The caller owns a real paused live backlog. Only its configured limit changes;
// no fabricated used counters or output reservations establish this regression.
func assertFirstImportIgnoresWebhookOvershoot(t *testing.T, f *hostedLiveFlusher, node *sharedJourneyNode, limits output.PipelineDemand) {
	t.Helper()
	ctx := t.Context()
	var before, after output.PipelineDemand
	var originalLimit int64
	if err := node.service.control.QueryRowContext(ctx, `SELECT limit_refs,used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity WHERE scope_kind='tenant' AND scope_id='default'`).Scan(&originalLimit, &before.References, &before.QueuedBytes, &before.MetadataRows, &before.ArtifactBytes, &before.ProofSlots); err != nil {
		t.Fatal(err)
	}
	if before.References < 2 {
		t.Fatal("fixture has no real backlog to exceed reduced limit", before)
	}
	if _, err := node.service.control.ExecContext(ctx, `UPDATE pipeline_capacity SET limit_refs=used_refs-1 WHERE scope_kind='tenant' AND scope_id='default'`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := node.service.control.ExecContext(ctx, `UPDATE pipeline_capacity SET limit_refs=$1 WHERE scope_kind='tenant' AND scope_id='default'`, originalLimit); err != nil {
			t.Error(err)
		}
	}()
	project, _ := sharedJourneyProject(t, node.host.URL, defaultTenantID, "import over output backlog")
	scope, err := f.native.InitializeHostedProject(ctx, lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: project})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.publisher.transaction(ctx, func(tx *sql.Tx) error {
		return f.publisher.effects.ConfigureCapacityTx(ctx, tx, "project", project, limits)
	}); err != nil {
		t.Fatal(err)
	}
	ref, err := f.native.Remote.PutImmutableImportCheckpoint(ctx, scope, "capacity-import", "empty", []byte(`{"Generation":1}`))
	if err != nil {
		t.Fatal(err)
	}
	emptyDigest := sha256.Sum256(nil)
	hash := hex.EncodeToString(emptyDigest[:])
	input := lakeapi.ImportChunkInput{Key: lakeapi.ImportChunkKey{Scope: scope, JobID: "capacity-import", FileID: "empty", Phase: "finalize"}, TenantID: defaultTenantID, SourceID: "archive", Format: "native", ManifestSHA256: hash, InputSHA256: hash, Parser: ref}
	proof, err := f.CommitImport(ctx, input)
	if err != nil {
		t.Fatal("first import borrowed exhausted webhook capacity", err)
	}
	if proof.Sequence != 1 || proof.AcceptedImport != 0 {
		t.Fatal(proof)
	}
	if err = node.service.control.QueryRowContext(ctx, `SELECT used_refs,used_bytes,used_rows,used_artifacts,used_proofs FROM pipeline_capacity WHERE scope_kind='tenant' AND scope_id='default'`).Scan(&after.References, &after.QueuedBytes, &after.MetadataRows, &after.ArtifactBytes, &after.ProofSlots); err != nil {
		t.Fatal(err)
	}
	expected := before
	expected.MetadataRows++
	if after != expected {
		t.Fatal("import charged output backlog", before, after)
	}
}
