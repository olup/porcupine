package lake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/importer"
)

func nativeImportInput(t *testing.T, s *DuckLakeStore, scope HostedProjectScope, job, file string, previous ImportFileCheckpoint, phase string, rows ...importer.Row) ImportChunkInput {
	t.Helper()
	body, _ := json.Marshal(struct{ Generation int64 }{previous.Generation + 1})
	ref, err := s.Remote.PutImmutableImportCheckpoint(t.Context(), scope, job, file, body)
	if err != nil {
		t.Fatal(err)
	}
	input := ImportChunkInput{Key: ImportChunkKey{Scope: scope, JobID: job, FileID: file, Start: previous.Offset, ExpectedEnd: previous.Offset + 1, Phase: phase}, TenantID: "tenant-a", ManifestSHA256: nativeDigest([]byte(job)), SourceID: "archive", Format: "posthog", InputSHA256: nativeDigest([]byte("x")), Previous: previous, Parser: ref, LineNumber: previous.LineNumber + int64(len(rows)), Parsed: importer.ImportCounts{Rows: int64(len(rows))}}
	if phase == "finalize" {
		input.Key.ExpectedEnd = input.Key.Start
		input.InputSHA256 = nativeDigest(nil)
	}
	for i, row := range rows {
		input.Candidates = append(input.Candidates, ImportCandidate{Position: previous.LineNumber + int64(i) + 1, Row: row})
	}
	return input
}
func nativeImportRow(project, id, name, target string) importer.Row {
	return importer.Row{Kind: "events", Event: event.Event{ProjectID: project, UUID: id, Event: name, DistinctID: target, SourceID: "archive", Properties: json.RawMessage(`{}`), Context: json.RawMessage(`{}`), Timestamp: "2001-01-01T00:00:00Z"}}
}
func commitNativeImport(t *testing.T, s *DuckLakeStore, session *ProjectWriteSession, input ImportChunkInput) NativeCommitEvidence {
	t.Helper()
	var ids EventIDReservation
	var err error
	if len(input.Candidates) > 0 {
		ids, err = session.ReserveEventIDs(t.Context(), input.Key.Scope.StreamID, input.Key.ID(), len(input.Candidates))
		if err != nil {
			t.Fatal(err)
		}
	}
	var result NativeCommitEvidence
	err = session.RunNative(t.Context(), func(ctx context.Context) error {
		result, err = s.CommitImportChunk(ctx, session.Fence(input.Key.Scope.StreamID), input, ids)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func TestDuckLakeS3SharedPostgresNativeImportOutcomes(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "import-outcomes"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireFenceSession(t, coordinator, key)
	fence := owner.Fence(scope.StreamID)
	if err = a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	originalTime := time.Date(2026, 11, 17, 23, 59, 59, 123, time.UTC)
	a.liveClock = func() time.Time { return originalTime }
	uuid := event.NewID()
	row := nativeImportRow(key.ProjectID, uuid, "old-page", "browser")
	first := nativeImportInput(t, a, scope, "job", "events", ImportFileCheckpoint{}, "bytes", row, row)
	first.Parsed.Rows++
	first.Parsed.Invalid = 1
	first.Failures = []importer.RowFailure{{Position: 3, Reason: "invalid_row"}}
	first.LineNumber++
	first.Samples = []importer.ImportRowSample{{FileID: "events", RowNumber: 3, Reason: "invalid_row"}}
	result := commitNativeImport(t, a, owner, first)
	if result.Sequence != 1 || result.AcceptedImport != 1 || result.Import.Delta != (importer.ImportCounts{Rows: 3, Accepted: 1, Duplicates: 1, Invalid: 1}) || result.Import.Rows[0].EventID != result.Import.Rows[1].EventID {
		t.Fatal(result)
	}
	p, release, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var imported int
	var occurred, received string
	if err = p.DB.QueryRowContext(ctx, `SELECT is_imported,timestamp,received_at FROM lake.current_events WHERE uuid=?`, uuid).Scan(&imported, &occurred, &received); err != nil || imported != 1 || occurred != "2001-01-01T00:00:00Z" || received != originalTime.Format(time.RFC3339Nano) {
		t.Fatal(imported, occurred, received, err)
	}
	// Native committed before any hypothetical control checkpoint. A new worker
	// retrieves the original IDs/counts despite a changed retry clock/reservation.
	a.liveClock = func() time.Time { return originalTime.Add(24 * time.Hour) }
	retried := first
	retried.Parser.VersionID = "candidate-version-not-in-logical-identity"
	retried.Candidates = append([]ImportCandidate(nil), first.Candidates...)
	retried.Candidates[0].Row.Event.ReceivedAt = "retry-clock"
	recovered := commitNativeImport(t, b, owner, retried)
	if !reflect.DeepEqual(result, recovered) {
		t.Fatal("original outcome changed", result, recovered)
	}
	conflict := first
	conflict.Key.ExpectedEnd++
	ids, err := owner.ReserveEventIDs(ctx, scope.StreamID, conflict.Key.ID(), len(conflict.Candidates))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.CommitImportChunk(ctx, fence, conflict, ids); !errors.Is(err, ErrImportChunkConflict) {
		t.Fatal("different-end bypass", err)
	}
	conflict = first
	conflict.InputSHA256 = nativeDigest([]byte("changed"))
	ids, err = owner.ReserveEventIDs(ctx, scope.StreamID, conflict.Key.ID(), len(conflict.Candidates))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.CommitImportChunk(ctx, fence, conflict, ids); !errors.Is(err, ErrImportChunkConflict) {
		t.Fatal("fingerprint conflict", err)
	}
	// Dedup includes tombstones and both live/import orders.
	if _, err = p.DB.ExecContext(ctx, `UPDATE lake.heads SET is_deleted=true WHERE uuid=?`, uuid); err != nil {
		t.Fatal(err)
	}
	live := LiveCommitBatch{ID: "known-import", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "live", uuid, "", `{}`)}}
	if _, ok := commitLiveFixture(t, a, owner, live, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}).(DuplicateLiveBatch); !ok {
		t.Fatal("live redelivered imported tombstone")
	}
	liveID := event.NewID()
	live = LiveCommitBatch{ID: "new-live", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "live", liveID, "", `{}`)}}
	commitLiveFixture(t, a, owner, live, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID})
	duplicates := nativeImportInput(t, a, scope, "job", "events", result.Import.Next, "bytes", row, nativeImportRow(key.ProjectID, liveID, "old-live", ""))
	duplicateResult := commitNativeImport(t, a, owner, duplicates)
	if duplicateResult.Sequence != 3 || duplicateResult.Import.Delta.Duplicates != 2 || duplicateResult.AcceptedImport != 0 || duplicateResult.Totals[0].Live != 1 || duplicateResult.Totals[0].Import != 0 {
		t.Fatal(duplicateResult)
	}
	invalid := nativeImportInput(t, a, scope, "job", "events", duplicateResult.Import.Next, "bytes")
	invalid.LineNumber++
	invalid.Parsed = importer.ImportCounts{Rows: 1, Invalid: 1}
	invalid.Failures = []importer.RowFailure{{Position: invalid.LineNumber, Reason: "invalid_row"}}
	invalidResult := commitNativeImport(t, a, owner, invalid)
	tail := nativeImportInput(t, a, scope, "job", "events", invalidResult.Import.Next, "bytes")
	tailResult := commitNativeImport(t, a, owner, tail)
	eof := nativeImportInput(t, a, scope, "job", "events", tailResult.Import.Next, "finalize")
	eofResult := commitNativeImport(t, a, owner, eof)
	empty := nativeImportInput(t, a, scope, "job", "empty", ImportFileCheckpoint{}, "finalize")
	emptyResult := commitNativeImport(t, a, owner, empty)
	if eofResult.Sequence != 6 || !eofResult.Import.Next.Completed || emptyResult.Sequence != 7 || emptyResult.AcceptedImport != 0 {
		t.Fatal(eofResult, emptyResult)
	}
	page, err := b.PendingNativeEffects(ctx, scope, 0, 20)
	if err != nil || len(page.Evidence) != 7 || page.NativeHighWater != 7 {
		t.Fatal(page, err)
	}
	if _, err = a.SealHostedProject(ctx, fence); err != nil {
		t.Fatal(err)
	}
	found, ok, err := b.FindImportChunk(ctx, fence, first)
	if err != nil || !ok || !reflect.DeepEqual(found, result) {
		t.Fatal("sealed original outcome", found, ok, err)
	}
	if err = a.AcknowledgeNativeEffects(ctx, fence, scope, 7); err != nil {
		t.Fatal(err)
	}
	if again := commitNativeImport(t, b, owner, first); !reflect.DeepEqual(again, result) {
		t.Fatal("sealed exact retry", again)
	}
}

func TestDuckLakeS3SharedPostgresNativeImportPersonOwnership(t *testing.T) {
	t.Parallel()
	ctx, a, _, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "import-person"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	session := acquireFenceSession(t, coordinator, key)
	fence := session.Fence(scope.StreamID)
	if err = a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	person := func(target string, version uint64, deleted bool) importer.Row {
		r := nativeImportRow(key.ProjectID, event.NewID(), "$set", target)
		r.Kind = "persons"
		r.Person = importer.Person{InternalID: "internal-" + target, IdentifiedID: target, PersonVersion: version, MappingVersion: math.MaxUint64, Properties: json.RawMessage(`{"archived":1}`), Deleted: deleted}
		return r
	}
	hydrate := person("person", math.MaxUint64-1, false)
	stale := person("person", math.MaxUint64-2, false)
	deleted := person("deleted", math.MaxUint64, true)
	identify := nativeImportRow(key.ProjectID, event.NewID(), "$identify", "deleted")
	first := nativeImportInput(t, a, scope, "persons", "people", ImportFileCheckpoint{}, "bytes", hydrate, stale, deleted, identify)
	proof := commitNativeImport(t, a, session, first)
	if proof.Import.Delta != (importer.ImportCounts{Rows: 4, Accepted: 2, Ignored: 2, Unresolved: 3}) {
		t.Fatal(proof.Import)
	}
	p, release, err := a.LeaseProject(ctx, key.ProjectID)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var pv, mv uint64
	var found int
	if err = p.DB.QueryRowContext(ctx, `SELECT person_version,mapping_version FROM lake.import_person_state WHERE distinct_id='person'`).Scan(&pv, &mv); err != nil || pv != math.MaxUint64-1 || mv != math.MaxUint64 {
		t.Fatal(pv, mv, err)
	}
	if err = p.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.identity_profiles WHERE identified_id='deleted'`).Scan(&found); err != nil || found != 0 {
		t.Fatal("deleted resurrected", found, err)
	}
	live := LiveCommitBatch{ID: "live-profile", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "$set", event.NewID(), "person", `{"$set":{"live":true}}`)}}
	commitLiveFixture(t, a, session, live, &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID})
	second := nativeImportInput(t, a, scope, "persons", "people", proof.Import.Next, "bytes", person("person", math.MaxUint64, false))
	protected := commitNativeImport(t, a, session, second)
	if protected.Import.Delta.Ignored != 1 || protected.AcceptedImport != 0 {
		t.Fatal("live profile overwritten", protected.Import)
	}
	var traits string
	if err = p.DB.QueryRowContext(ctx, `SELECT traits FROM lake.identity_profiles WHERE identified_id='person'`).Scan(&traits); err != nil || traits != `{"archived":1,"live":true}` {
		t.Fatal(traits, err)
	}
	// Transaction-local state follows row order, including mapping ownership,
	// exact UUID duplicates, deletion, safe identify and archived control no-ops.
	mappingA := person("mapping", 1, false)
	mappingA.Person.MappingVersion = 10
	mappingB := person("mapping", 100, false)
	mappingB.Person.InternalID = "replacement"
	mappingB.Person.MappingVersion = 10
	mappingC := mappingB
	mappingC.Event.UUID = event.NewID()
	mappingC.Person.MappingVersion = 11
	mappingC.Person.PersonVersion = 1
	repeated := mappingC
	repeated.Person.PersonVersion = math.MaxUint64
	owned := person("owned", 1, false)
	identifyTarget := person("identify-target", 1, false)
	safeIdentify := nativeImportRow(key.ProjectID, event.NewID(), "$identify", "identify-target")
	safeIdentify.Event.Properties = json.RawMessage(`{"$anon_distinct_id":"anonymous","$set":{"unsafe":true}}`)
	archivedSet := nativeImportRow(key.ProjectID, event.NewID(), "$set", "person")
	archivedSet.Event.Properties = json.RawMessage(`{"$set":{"unsafe":true}}`)
	deletedOwn := person("deleted-owned", 1, false)
	deleteOwned := person("deleted-owned", 2, true)
	oldDeleted := person("deleted-owned", 1, false)
	identifyDeleted := nativeImportRow(key.ProjectID, event.NewID(), "$identify", "deleted-owned")
	ordered := nativeImportInput(t, a, scope, "ordered", "people", ImportFileCheckpoint{}, "bytes", mappingA, mappingB, mappingC, repeated, owned, identifyTarget, safeIdentify, archivedSet, deletedOwn, deleteOwned, oldDeleted, identifyDeleted)
	orderedProof := commitNativeImport(t, a, session, ordered)
	if orderedProof.Import.Delta.Duplicates != 1 || orderedProof.Import.Delta.Ignored != 2 || orderedProof.Import.Rows[2].EventID != orderedProof.Import.Rows[3].EventID {
		t.Fatal(orderedProof.Import)
	}
	var personID string
	if err = p.DB.QueryRowContext(ctx, `SELECT person_id,person_version,mapping_version FROM lake.import_person_state WHERE distinct_id='mapping'`).Scan(&personID, &pv, &mv); err != nil || personID != "replacement" || pv != 1 || mv != 11 {
		t.Fatal("mapping or dedup order", personID, pv, mv, err)
	}
	var identified bool
	if err = p.DB.QueryRowContext(ctx, `SELECT traits,is_identified FROM lake.identity_profiles WHERE identified_id='identify-target'`).Scan(&traits, &identified); err != nil || traits != `{"archived":1}` || !identified {
		t.Fatal("unsafe identify traits", traits, identified, err)
	}
	var target string
	if err = p.DB.QueryRowContext(ctx, `SELECT identified_id FROM lake.identity_links WHERE anonymous_id='anonymous'`).Scan(&target); err != nil || target != "identify-target" {
		t.Fatal(target, err)
	}
	if err = p.DB.QueryRowContext(ctx, `SELECT traits,is_identified FROM lake.identity_profiles WHERE identified_id='deleted-owned'`).Scan(&traits, &identified); err != nil || traits != `{}` || identified {
		t.Fatal("deleted person revived", traits, identified, err)
	}
	if err = p.DB.QueryRowContext(ctx, `SELECT traits FROM lake.identity_profiles WHERE identified_id='person'`).Scan(&traits); err != nil || traits != `{"archived":1,"live":true}` {
		t.Fatal("archived incremental set applied", traits, err)
	}
	foreign := person("owned", 2, false)
	foreign.Event.SourceID = "another-source"
	foreignInput := nativeImportInput(t, a, scope, "foreign", "people", ImportFileCheckpoint{}, "bytes", foreign)
	foreignInput.SourceID = "another-source"
	foreignProof := commitNativeImport(t, a, session, foreignInput)
	if foreignProof.Import.Delta.Ignored != 1 || foreignProof.Import.Delta.Unresolved != 1 {
		t.Fatal("foreign snapshot claimed profile", foreignProof.Import)
	}

}

func TestDuckLakeS3SharedPostgresHostedPrototypeUpgrade(t *testing.T) {
	t.Parallel()
	for _, populated := range []bool{false, true} {
		t.Run(fmt.Sprint(populated), func(t *testing.T) {
			t.Parallel()
			ctx, store, _, config := sharedNativeStores(t)
			key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "prototype"}
			p, release, err := store.LeaseProject(ctx, key.ProjectID)
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			stream := event.NewID()
			tx, err := p.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			for _, step := range hostedCatalogSteps {
				if step.version > 4 {
					break
				}
				if _, err = tx.ExecContext(ctx, step.sql); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = tx.ExecContext(ctx, `INSERT INTO lake.hosted_write_guard (id,source_id,project_id,catalog_id,stream_id,fence_epoch,fence_nonce,generation,state,next_effect_seq,ack_effect_seq,sealed_effect_seq,identity_ready) VALUES(1,?,?,?,?,0,'',0,'open',0,0,NULL,true)`, key.SourceID, key.ProjectID, SharedNativeCatalogID(key.SourceID, key.ProjectID), stream); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.ExecContext(ctx, `DELETE FROM lake.schema_version; INSERT INTO lake.schema_version VALUES(4)`); err != nil {
				t.Fatal(err)
			}
			if populated {
				if _, err = tx.ExecContext(ctx, `INSERT INTO lake.live_commit_proofs(batch_id) VALUES('preserved-prototype-proof')`); err != nil {
					t.Fatal(err)
				}
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			scope, err := store.InitializeHostedProject(ctx, key)
			if populated {
				if err == nil || !strings.Contains(err.Error(), "rebuild") {
					t.Fatal(scope, err)
				}
				var version, count int
				var batch string
				if err = p.DB.QueryRowContext(ctx, `SELECT version FROM lake.schema_version`).Scan(&version); err != nil || version != 4 {
					t.Fatal(version, err)
				}
				if err = p.DB.QueryRowContext(ctx, `SELECT batch_id FROM lake.live_commit_proofs`).Scan(&batch); err != nil || batch != "preserved-prototype-proof" {
					t.Fatal(batch, err)
				}
				if err = p.DB.QueryRowContext(ctx, `SELECT count(*) FROM duckdb_tables() WHERE database_name='lake' AND table_name='native_commit_proofs'`).Scan(&count); err != nil || count != 0 {
					t.Fatal("refusal altered native schema", count, err)
				}
			} else {
				if err != nil || scope.StreamID != stream {
					t.Fatal(scope, err)
				}
				version, err := lakeCatalogVersion(ctx, p.DB, false)
				if err != nil || version != 5 {
					t.Fatal(version, err)
				}
			}
		})
	}
}

func TestDuckLakeS3SharedPostgresNativeImportSampleBound(t *testing.T) {
	t.Parallel()
	ctx, a, _, config := sharedNativeStores(t)
	_, coordinator := nativeFenceControl(t)
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "import-samples"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	session := acquireFenceSession(t, coordinator, key)
	if err = a.AdvanceWriteFence(ctx, session.Fence(scope.StreamID)); err != nil {
		t.Fatal(err)
	}
	// This fixture has no event sequence at all. Parser-only transitions cannot
	// accidentally reserve a dummy candidate ID.
	invalid := func(file string) ImportChunkInput {
		data := []byte(strings.Repeat("invalid-json\n", 25))
		parsed, err := importer.ParseChunk(ctx, importer.ChunkState{FileID: file, Kind: "events", Format: "posthog"}, data, false)
		if err != nil {
			t.Fatal(err)
		}
		input := nativeImportInput(t, a, scope, "job", file, ImportFileCheckpoint{}, "bytes")
		input.Key.ExpectedEnd = int64(len(data))
		input.InputSHA256 = nativeDigest(data)
		input.LineNumber = parsed.State.LineNumber
		input.Parsed = parsed.State.Counts
		input.Failures = parsed.Failures
		input.Samples = parsed.Samples
		raw, err := json.Marshal(parsed.State)
		if err != nil {
			t.Fatal(err)
		}
		input.Parser, err = a.Remote.PutImmutableImportCheckpoint(ctx, scope, "job", file, raw)
		if err != nil {
			t.Fatal(err)
		}
		return input
	}
	first := commitNativeImport(t, a, session, invalid("first"))
	second := commitNativeImport(t, a, session, invalid("second"))
	if len(first.Import.Samples) != 20 || len(first.Import.Rows) != 25 || len(second.Import.Samples) != 0 || len(second.Import.Rows) != 25 || second.Sequence != 2 || second.AcceptedImport != 0 {
		t.Fatal(first, second)
	}
	for i, row := range first.Import.Rows {
		if row.Position != int64(i)+1 || row.Reason != "invalid_row" || row.EventID != 0 {
			t.Fatal(row)
		}
	}
}

func TestDuckLakeS3SharedPostgresNativeImportPrivatePersonUUID(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	control, coordinator := nativeFenceControl(t)
	if _, err := control.ExecContext(ctx, `CREATE SEQUENCE shared_event_id_sequence AS BIGINT INCREMENT BY 1024 MINVALUE 1 CACHE 1 NO CYCLE`); err != nil {
		t.Fatal(err)
	}
	key := ProjectWriteKey{SourceID: config.SharedSourceID, ProjectID: "private-person"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	session := acquireFenceSession(t, coordinator, key)
	fence := session.Fence(scope.StreamID)
	if err = a.AdvanceWriteFence(ctx, fence); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"person_id":"internal","distinct_id":"person","properties":{"plan":"archive"},"person_version":1,"person_distinct_id_version":1,"created_at":"2001-01-01T00:00:00Z"}` + "\n")
	parsed, err := importer.ParseChunk(ctx, importer.ChunkState{FileID: "people", Kind: "persons", Format: "posthog"}, data, false)
	if err != nil || len(parsed.Rows) != 1 {
		t.Fatal(parsed, err)
	}
	row := parsed.Rows[0]
	row.Event.ProjectID = key.ProjectID
	row.Event.SourceID = "posthog"
	if !strings.HasPrefix(row.Event.UUID, importer.PostHogPersonUUIDPrefix) || len(row.Event.UUID) <= 128 {
		t.Fatal("fixture did not parse real private person UUID", row.Event.UUID)
	}
	input := nativeImportInput(t, a, scope, "job", "people", ImportFileCheckpoint{}, "bytes", row)
	input.SourceID = "posthog"
	input.Key.ExpectedEnd = int64(len(data))
	input.InputSHA256 = nativeDigest(data)
	raw, err := json.Marshal(parsed.State)
	if err != nil {
		t.Fatal(err)
	}
	input.Parser, err = a.Remote.PutImmutableImportCheckpoint(ctx, scope, "job", "people", raw)
	if err != nil {
		t.Fatal(err)
	}
	proof := commitNativeImport(t, a, session, input)
	if proof.AcceptedImport != 1 || proof.Import.Rows[0].UUID != row.Event.UUID || proof.Import.Rows[0].EventID <= 0 {
		t.Fatal(proof)
	}
	page, err := b.PendingNativeEffects(ctx, scope, 0, 1)
	if err != nil || len(page.Evidence) != 1 || !reflect.DeepEqual(proof, page.Evidence[0]) {
		t.Fatal("private person proof round trip", page, err)
	}
	live := LiveCommitBatch{ID: "reserved-person-live", TenantID: "tenant-a", Scope: scope, Inputs: []LiveCommitInput{newLiveInput(t, key.ProjectID, "page", row.Event.UUID, "person", `{}`)}}
	ids, err := session.ReserveEventIDs(ctx, scope.StreamID, live.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	stager := &liveProofStager{objects: a.Remote, attempt: fence.AcquisitionID}
	if _, err = a.CommitLive(ctx, fence, live, ids, stager); err == nil || len(stager.selected) != 0 {
		t.Fatal("live writer accepted private person namespace", err)
	}
	after, err := b.PendingNativeEffects(ctx, scope, 0, 2)
	if err != nil || after.NativeHighWater != 1 {
		t.Fatal(after, err)
	}
}
