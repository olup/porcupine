package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/importer"
	"github.com/olup/porcupine/internal/ingest"
)

var ErrImportChunkConflict = errors.New("native import chunk input or predecessor conflicts with original outcome")

func importInputFingerprint(input ImportChunkInput) (string, error) {
	// Physical version pins returned by PUT and candidate receipt times are
	// excluded from logical identity. The original outcome retains actual pins.
	input.Previous.Parser.VersionID = ""
	input.Parser.VersionID = ""
	input.Candidates = append([]ImportCandidate(nil), input.Candidates...)
	for i := range input.Candidates {
		input.Candidates[i].Row.Event.ID = 0
		input.Candidates[i].Row.Event.ReceivedAt = ""
	}
	raw, err := json.Marshal(struct {
		Version int
		Input   ImportChunkInput
	}{1, input})
	if err != nil {
		return "", err
	}
	return nativeDigest(raw), nil
}
func validateImportInput(fence ProjectWriteFence, input ImportChunkInput, ids EventIDReservation) (string, error) {
	k := input.Key
	if !validProjectWriteFence(fence) || k.Scope.Key != fence.Key || k.Scope.StreamID != fence.StreamID || k.Scope.CatalogID != SharedNativeCatalogID(fence.Key.SourceID, fence.Key.ProjectID) {
		return "", ErrProjectWriteFenceStale
	}
	if k.JobID == "" || len(k.JobID) > 200 || k.FileID == "" || len(k.FileID) > 200 || input.TenantID == "" || input.SourceID == "" || len(input.SourceID) > 200 {
		return "", errors.New("invalid native import identity")
	}
	if k.Start < 0 || k.ExpectedEnd < k.Start || k.ExpectedEnd-k.Start > importer.MaxChunkBytes || !(k.Phase == "bytes" && k.ExpectedEnd > k.Start || k.Phase == "finalize" && k.ExpectedEnd == k.Start) {
		return "", errors.New("invalid native import range or phase")
	}
	if !(input.Format == "native" || input.Format == "segment" || input.Format == "posthog") || !validNativeHash(input.ManifestSHA256) || !validNativeHash(input.InputSHA256) || !validImportParserRef(input.Parser) {
		return "", errors.New("invalid native import format, digest or parser reference")
	}
	if input.Previous.Generation < 0 || input.Previous.Generation == math.MaxInt64 || input.Previous.Offset != k.Start || input.Previous.Completed || input.Previous.LineNumber < 0 || input.LineNumber == math.MaxInt64 || input.LineNumber < input.Previous.LineNumber || !validImportCounts(input.Previous.Counts) || len(input.Previous.Samples) > importer.MaxSamples {
		return "", errors.New("invalid native import predecessor")
	}
	if len(input.Candidates) > importer.MaxChunkRows || len(input.Samples) > importer.MaxSamples || input.Parsed.Rows != input.Parsed.Invalid+int64(len(input.Candidates)) || input.Parsed.Rows > importer.MaxChunkRows || input.Parsed.Invalid != int64(len(input.Failures)) || input.Parsed.Accepted != 0 || input.Parsed.Duplicates != 0 || input.Parsed.Ignored != 0 || input.Parsed.Unresolved != 0 {
		return "", errors.New("invalid native import parsed counts")
	}
	if k.Phase == "finalize" && input.InputSHA256 != nativeDigest(nil) {
		return "", errors.New("finalize requires empty input digest")
	}
	if input.Previous.Generation == 0 && (k.Start != 0 || input.Previous.Parser != (ImmutableObjectRef{}) || input.Previous.LineNumber != 0 || input.Previous.Counts != (importer.ImportCounts{}) || len(input.Previous.Samples) != 0) {
		return "", ErrImportChunkConflict
	}
	if len(input.Candidates) > 0 && !ids.matches(fence, k.ID(), len(input.Candidates)) {
		return "", errors.New("invalid native import ID reservation")
	}
	lastFailure := int64(0)
	positions := map[int64]bool{}
	for _, failure := range input.Failures {
		if failure.Position <= lastFailure || failure.Position <= input.Previous.LineNumber || failure.Position > input.LineNumber+1 || !(failure.Reason == "invalid_row" || failure.Reason == "row_too_large" || failure.Reason == "invalid_metadata") {
			return "", errors.New("invalid native parse-failure position")
		}
		positions[failure.Position] = true
		lastFailure = failure.Position
	}
	last := int64(0)
	for _, c := range input.Candidates {
		e := c.Row.Event
		if positions[c.Position] || c.Position <= last || c.Position <= input.Previous.LineNumber || c.Position > input.LineNumber+1 || !(c.InvalidReason == "" || c.InvalidReason == "invalid_metadata" || c.InvalidReason == "invalid_row") || c.Row.Kind != "events" && c.Row.Kind != "persons" || c.Row.Kind == "persons" && input.Format != "posthog" || e.ID != 0 || e.ProjectID != k.Scope.Key.ProjectID || !ValidStoredEventUUID(e.UUID) || e.SourceID != input.SourceID || e.Delivered || e.IdentitySnapshot != "" {
			return "", errors.New("invalid native import candidate")
		}
		last = c.Position
	}
	for _, sample := range input.Samples {
		if sample.FileID != k.FileID || sample.RowNumber <= input.Previous.LineNumber || sample.RowNumber > input.LineNumber+1 || !(sample.Reason == "invalid_row" || sample.Reason == "row_too_large" || sample.Reason == "invalid_metadata") {
			return "", errors.New("invalid import parser sample")
		}
	}
	return importInputFingerprint(input)
}
func nativeDigest(raw []byte) string { sum := sha256.Sum256(raw); return fmt.Sprintf("%x", sum) }
func validImportCounts(c importer.ImportCounts) bool {
	return c.Rows >= 0 && c.Accepted >= 0 && c.Duplicates >= 0 && c.Invalid >= 0 && c.Ignored >= 0 && c.Unresolved >= 0 && c.Accepted <= c.Rows && c.Duplicates <= c.Rows-c.Accepted && c.Invalid <= c.Rows-c.Accepted-c.Duplicates && c.Ignored == c.Rows-c.Accepted-c.Duplicates-c.Invalid && c.Unresolved <= c.Accepted+c.Ignored
}
func addImportCounts(a, b importer.ImportCounts) (importer.ImportCounts, error) {
	av := []int64{a.Rows, a.Accepted, a.Duplicates, a.Ignored, a.Unresolved, a.Invalid}
	bv := []int64{b.Rows, b.Accepted, b.Duplicates, b.Ignored, b.Unresolved, b.Invalid}
	for i := range av {
		if bv[i] < 0 || av[i] > math.MaxInt64-bv[i] {
			return importer.ImportCounts{}, errors.New("import count exhausted")
		}
		av[i] += bv[i]
	}
	return importer.ImportCounts{Rows: av[0], Accepted: av[1], Duplicates: av[2], Ignored: av[3], Unresolved: av[4], Invalid: av[5]}, nil
}
func validateImportOutcome(o ImportChunkOutcome) error {
	if o.Key.JobID == "" || len(o.Key.JobID) > 200 || o.Key.FileID == "" || len(o.Key.FileID) > 200 || !validNativeHash(o.ManifestSHA256) || !validNativeHash(o.InputSHA256) || o.SourceID == "" || o.AccountedAt.IsZero() {
		return errors.New("invalid original native import identity")
	}
	if o.Key.Start < 0 || o.Previous.Offset != o.Key.Start || o.Key.ExpectedEnd < o.Key.Start || o.Key.ExpectedEnd-o.Key.Start > importer.MaxChunkBytes || !(o.Key.Phase == "bytes" && o.Key.ExpectedEnd > o.Key.Start || o.Key.Phase == "finalize" && o.Key.ExpectedEnd == o.Key.Start && o.InputSHA256 == nativeDigest(nil)) {
		return errors.New("invalid original native import range")
	}
	if o.Previous.Generation < 0 || o.Previous.Generation == math.MaxInt64 || o.Previous.Completed || o.Next.Generation != o.Previous.Generation+1 || o.Next.Offset != o.Key.ExpectedEnd || o.Previous.LineNumber < 0 || o.Next.LineNumber == math.MaxInt64 || o.Next.LineNumber < o.Previous.LineNumber || o.Next.Completed != (o.Key.Phase == "finalize") || !validImportParserRef(o.Next.Parser) || (o.Previous.Generation > 0 && !validImportParserRef(o.Previous.Parser)) {
		return errors.New("invalid original native import checkpoint")
	}
	if len(o.Rows) > importer.MaxChunkRows || int64(len(o.Rows)) != o.Delta.Rows || len(o.Samples) > importer.MaxSamples || len(o.Next.Samples) > importer.MaxSamples || !validImportCounts(o.Previous.Counts) || !validImportCounts(o.Delta) || !validImportCounts(o.Next.Counts) {
		return errors.New("invalid original native import counts")
	}
	expected, err := addImportCounts(o.Previous.Counts, o.Delta)
	if err != nil || expected != o.Next.Counts {
		return errors.New("invalid original import cumulative counts")
	}
	var accepted, duplicates, ignored, unresolved, invalid int64
	last := int64(0)
	for _, r := range o.Rows {
		if r.Position <= last || r.Position <= o.Previous.LineNumber || r.Position > o.Next.LineNumber+1 || (r.UUID != "" && !ValidStoredEventUUID(r.UUID)) || (r.UUID == "" && r.Counts.Invalid != 1) || r.Counts.Rows != 1 || !validImportCounts(r.Counts) || !r.ReceivedAt.Equal(o.AccountedAt) || !validImportReason(r.Reason) || (r.Counts.Accepted+r.Counts.Duplicates > 0 && r.EventID <= 0) {
			return errors.New("invalid original import row outcome")
		}
		last = r.Position
		accepted += r.Counts.Accepted
		duplicates += r.Counts.Duplicates
		ignored += r.Counts.Ignored
		unresolved += r.Counts.Unresolved
		invalid += r.Counts.Invalid
	}
	if accepted != o.Delta.Accepted || duplicates != o.Delta.Duplicates || ignored != o.Delta.Ignored || unresolved != o.Delta.Unresolved || invalid != o.Delta.Invalid {
		return errors.New("import row totals disagree with outcome")
	}
	return nil
}

// CommitImportChunk returns the exact original result before any reclassification.
// The integration caller must preflight its shared PG cursor before this call.
// The outcome, imported canonical records, identity and common usage/proof commit
// together. It publishes no output and never advances the control cursor itself.
func (s *DuckLakeStore) CommitImportChunk(ctx context.Context, fence ProjectWriteFence, input ImportChunkInput, ids EventIDReservation, checkExceptions func(context.Context, int64) error) (NativeCommitEvidence, error) {
	fingerprint, err := validateImportInput(fence, input, ids)
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	if s.Remote == nil || s.SharedCatalog == nil {
		return NativeCommitEvidence{}, errors.New("native imports require shared metadata and immutable checkpoints")
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, fence.Key.ProjectID)
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	defer release()
	if err = p.write.Lock(ctx); err != nil {
		return NativeCommitEvidence{}, err
	}
	defer p.write.Unlock()
	var result NativeCommitEvidence
	err = retryNativeTransaction(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		prior, found, err := readNativeCommitProof(ctx, tx, input.Key.Scope, input.Key.ID())
		if err != nil {
			return err
		}
		if found {
			if prior.Kind != NativeCommitImport || prior.InputFingerprint != fingerprint || prior.TenantID != input.TenantID {
				return ErrImportChunkConflict
			}
			result = prior
			return nil
		}
		sequence, err := p.beginHostedIngestionWrite(ctx, tx, fence, input.TenantID)
		if err != nil {
			return err
		}
		if err = matchImportPredecessor(ctx, tx, input); err != nil {
			return err
		}
		// Validate exact immutable parser state, including optional physical pins,
		// before recording its reference. Original retries keep their original refs.
		if _, err = s.Remote.ReadImmutableImportCheckpoint(ctx, input.Key.Scope, input.Key.JobID, input.Key.FileID, input.Parser); err != nil {
			return err
		}
		at := s.liveCommitTime()
		outcome := ImportChunkOutcome{Key: input.Key, ManifestSHA256: input.ManifestSHA256, SourceID: input.SourceID, Format: input.Format, InputSHA256: input.InputSHA256, Previous: input.Previous, AccountedAt: at, Delta: input.Parsed, Rows: []ImportRowOutcome{}, Samples: append([]importer.ImportRowSample(nil), input.Samples...)}
		outcome.Next = ImportFileCheckpoint{Generation: input.Previous.Generation + 1, Offset: input.Key.ExpectedEnd, LineNumber: input.LineNumber, Completed: input.Key.Phase == "finalize", Parser: input.Parser, Samples: append([]importer.ImportRowSample(nil), input.Previous.Samples...)}
		var exceptions int64
		for position, c := range input.Candidates {
			rowResult := ImportRowOutcome{Position: c.Position, UUID: c.Row.Event.UUID, ReceivedAt: at, Counts: importer.ImportCounts{Rows: 1}}
			var existing int64
			err = tx.QueryRowContext(ctx, `SELECT event_id FROM lake.heads WHERE uuid=?`, c.Row.Event.UUID).Scan(&existing)
			switch {
			case err == nil:
				rowResult.EventID = existing
				rowResult.Counts.Duplicates = 1
			case !errors.Is(err, sql.ErrNoRows):
				return err
			case c.InvalidReason != "":
				rowResult.Counts.Invalid = 1
				rowResult.Reason = c.InvalidReason
				outcome.Samples = appendImportSample(outcome.Samples, input.Key.FileID, c.Position, c.InvalidReason)
			default:
				target, anonymous := importer.HistoricalStateIDs(c.Row)
				state := importer.HistoricalState{}
				state.Target, err = loadNativeHistoricalState(ctx, tx, target)
				if err != nil {
					return err
				}
				if anonymous != "" && anonymous != target {
					state.Anonymous, err = loadNativeHistoricalState(ctx, tx, anonymous)
					if err != nil {
						return err
					}
				}
				decision, e := importer.ReduceHistorical(c.Row, state, at)
				if errors.Is(e, importer.ErrRow) {
					rowResult.Counts.Invalid = 1
					rowResult.Reason = "invalid_row"
					outcome.Samples = appendImportSample(outcome.Samples, input.Key.FileID, c.Position, "invalid_row")
					break
				}
				if e != nil {
					return e
				}
				rowResult.Counts.Accepted, rowResult.Counts.Ignored, rowResult.Counts.Unresolved = decision.Accepted, decision.Ignored, decision.Unresolved
				if decision.Accepted > 0 {
					canonical := decision.Event
					canonical.ID = ids.ids[position]
					canonical.ReceivedAt = at.Format(time.RFC3339Nano)
					canonical, err = applyNativeHistoricalDecision(ctx, tx, canonical, decision)
					if err != nil {
						return err
					}
					prepared, e := PrepareLakeProjection(EventProjection{Event: EventWithMetadata(canonical)})
					if e != nil {
						return e
					}
					if err = insertCanonicalEvents(ctx, tx, []PreparedLakeProjection{prepared}); err != nil {
						return err
					}
					if canonical.Event == "$exception" {
						exceptions++
					}
					rowResult.EventID = canonical.ID
				} else if decision.Person != nil {
					if err = saveNativeImportedPerson(ctx, tx, target, *decision.Person, 0); err != nil {
						return err
					}
				}
				if decision.Unresolved > 0 {
					rowResult.Reason = "unresolved_identity"
					outcome.Samples = appendImportSample(outcome.Samples, input.Key.FileID, c.Position, "unresolved_identity")
				} else if decision.Ignored > 0 {
					rowResult.Reason = "ignored_row"
					outcome.Samples = appendImportSample(outcome.Samples, input.Key.FileID, c.Position, "ignored_row")
				}
			}
			// Parsed.Rows already includes every candidate; only its classification is new.
			delta := rowResult.Counts
			delta.Rows = 0
			outcome.Delta, err = addImportCounts(outcome.Delta, delta)
			if err != nil {
				return err
			}
			outcome.Rows = append(outcome.Rows, rowResult)
		}
		if exceptions > 0 && checkExceptions != nil {
			if err := checkExceptions(ctx, exceptions); err != nil {
				return err
			}
		}
		for _, failure := range input.Failures {
			outcome.Rows = append(outcome.Rows, ImportRowOutcome{Position: failure.Position, Reason: failure.Reason, Counts: importer.ImportCounts{Rows: 1, Invalid: 1}, ReceivedAt: at})
		}
		sort.Slice(outcome.Rows, func(i, j int) bool { return outcome.Rows[i].Position < outcome.Rows[j].Position })
		outcome.Next.Counts, err = addImportCounts(input.Previous.Counts, outcome.Delta)
		if err != nil {
			return err
		}
		budget, err := importSampleBudget(ctx, tx, input.Key.JobID)
		if err != nil {
			return err
		}
		outcome.Samples = outcome.Samples[:min(len(outcome.Samples), budget)]
		sort.Slice(outcome.Samples, func(i, j int) bool { return outcome.Samples[i].RowNumber < outcome.Samples[j].RowNumber })
		outcome.Next.Samples = append(outcome.Next.Samples, outcome.Samples...)
		totals, err := updateNativeSourceTotals(ctx, tx, sequence, at, 0, outcome.Delta.Accepted)
		if err != nil {
			return err
		}
		result = NativeCommitEvidence{Kind: NativeCommitImport, Scope: input.Key.Scope, TenantID: input.TenantID, Sequence: sequence, OperationID: input.Key.ID(), InputFingerprint: fingerprint, FenceEpoch: fence.Epoch, AccountedAt: at, AcceptedImport: outcome.Delta.Accepted, AcceptedExceptions: exceptions, Import: &outcome, Totals: totals}
		result.ProofFingerprint, err = nativeEvidenceFingerprint(result)
		if err != nil {
			return err
		}
		if err = insertNativeCommitProof(ctx, tx, result); err != nil {
			return err
		}
		raw, err := json.Marshal(outcome.Next)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM lake.import_file_heads WHERE job_id=? AND file_id=?`, input.Key.JobID, input.Key.FileID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO lake.import_file_heads VALUES(?,?,?,?,?,?,?)`, input.Key.JobID, input.Key.FileID, input.ManifestSHA256, input.SourceID, input.Format, sequence, string(raw)); err != nil {
			return err
		}
		return tx.Commit()
	})
	if err != nil {
		return NativeCommitEvidence{}, err
	}
	return result, nil
}
func validImportReason(reason string) bool {
	return reason == "" || reason == "invalid_row" || reason == "invalid_metadata" || reason == "row_too_large" || reason == "unresolved_identity" || reason == "ignored_row"
}
func validImportParserRef(ref ImmutableObjectRef) bool {
	return ref.Key != "" && len(ref.Key) <= 4096 && len(ref.VersionID) <= 1024 && validNativeHash(ref.SHA256) && ref.Bytes > 0 && ref.Bytes <= MaxImmutableImportCheckpointBytes
}
func appendImportSample(samples []importer.ImportRowSample, file string, position int64, reason string) []importer.ImportRowSample {
	if len(samples) < importer.MaxSamples {
		return append(samples, importer.ImportRowSample{FileID: file, RowNumber: position, Reason: reason})
	}
	return samples
}
func importSampleBudget(ctx context.Context, tx *sql.Tx, job string) (int, error) {
	rows, err := tx.QueryContext(ctx, `SELECT checkpoint_json FROM lake.import_file_heads WHERE job_id=?`, job)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	used := 0
	for rows.Next() {
		var raw string
		var c ImportFileCheckpoint
		if err = rows.Scan(&raw); err != nil {
			return 0, err
		}
		if err = json.Unmarshal([]byte(raw), &c); err != nil {
			return 0, err
		}
		used += len(c.Samples)
		if used > importer.MaxSamples {
			return 0, errors.New("invalid native import sample history")
		}
	}
	return importer.MaxSamples - used, rows.Err()
}
func matchImportPredecessor(ctx context.Context, tx *sql.Tx, input ImportChunkInput) error {
	var raw, manifest, source, format string
	var sequence int64
	err := tx.QueryRowContext(ctx, `SELECT manifest_sha256,source_id,format,effect_seq,checkpoint_json FROM lake.import_file_heads WHERE job_id=? AND file_id=?`, input.Key.JobID, input.Key.FileID).Scan(&manifest, &source, &format, &sequence, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		if input.Previous.Generation != 0 {
			return ErrImportChunkConflict
		}
		return nil
	}
	if err != nil {
		return err
	}
	var checkpoint ImportFileCheckpoint
	if err = json.Unmarshal([]byte(raw), &checkpoint); err != nil {
		return err
	}
	if manifest != input.ManifestSHA256 || source != input.SourceID || format != input.Format || !reflect.DeepEqual(checkpoint, input.Previous) {
		return ErrImportChunkConflict
	}
	proof, err := scanNativeCommitProof(tx.QueryRowContext(ctx, `SELECT `+nativeProofColumns+` FROM lake.native_commit_proofs WHERE effect_seq=?`, sequence), input.Key.Scope)
	if err != nil {
		return err
	}
	if proof.Kind != NativeCommitImport || proof.Import.Key.JobID != input.Key.JobID || proof.Import.Key.FileID != input.Key.FileID || !reflect.DeepEqual(proof.Import.Next, checkpoint) {
		return ErrImportChunkConflict
	}
	return nil
}
func loadNativeHistoricalState(ctx context.Context, tx *sql.Tx, id string) (importer.HistoricalIdentityState, error) {
	var state importer.HistoricalIdentityState
	if id == "" {
		return state, nil
	}
	p := &state.Person
	err := tx.QueryRowContext(ctx, `SELECT person_id,person_version,mapping_version,profile_version,deleted,source_id FROM lake.import_person_state WHERE distinct_id=?`, id).Scan(&p.PersonID, &p.PersonVersion, &p.MappingVersion, &p.ProfileVersion, &p.Deleted, &p.SourceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, err
	}
	p.Found = err == nil
	var traits string
	err = tx.QueryRowContext(ctx, `SELECT traits,version,is_identified FROM lake.identity_profiles WHERE identified_id=?`, id).Scan(&traits, &state.ProfileVersion, &state.Profile.Identified)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, err
	}
	if err == nil {
		state.ProfileFound = true
		state.Profile.Traits = json.RawMessage(traits)
	}
	err = tx.QueryRowContext(ctx, `SELECT identified_id,version FROM lake.identity_links WHERE anonymous_id=?`, id).Scan(&state.Link.Target, &state.Link.Version)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return state, err
}
func saveNativeImportedPerson(ctx context.Context, tx *sql.Tx, target string, p importer.HistoricalPersonState, version uint64) error {
	if _, err := tx.ExecContext(ctx, `DELETE FROM lake.import_person_state WHERE distinct_id=?`, target); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO lake.import_person_state VALUES(?,?,CAST(? AS UBIGINT),CAST(? AS UBIGINT),?,?,?)`, target, p.PersonID, strconv.FormatUint(p.PersonVersion, 10), strconv.FormatUint(p.MappingVersion, 10), version, p.Deleted, p.SourceID)
	return err
}
func applyNativeHistoricalDecision(ctx context.Context, tx *sql.Tx, e event.Event, d importer.HistoricalDecision) (event.Event, error) {
	if !d.Control {
		return e, nil
	}
	snapshot := emptyLiveIdentitySnapshot(fmt.Sprint(e.ID))
	if profile := d.Profile; profile != nil {
		if _, err := tx.ExecContext(ctx, `DELETE FROM lake.identity_profiles WHERE identified_id=?`, profile.Target); err != nil {
			return e, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO lake.identity_profiles VALUES(?,?,?,?)`, profile.Target, uint64(e.ID), string(profile.Traits), profile.Identified); err != nil {
			return e, err
		}
		snapshot.IdentifiedID, snapshot.Identified = profile.Target, profile.Identified
		if err := ingest.DecodeJSON(profile.Traits, &snapshot.Traits); err != nil {
			return e, err
		}
		if link := profile.Link; link != nil {
			version := link.Version
			if link.Create {
				version = uint64(e.ID)
				if _, err := tx.ExecContext(ctx, `INSERT INTO lake.identity_links VALUES(?,?,?)`, link.Anonymous, link.Target, version); err != nil {
					return e, err
				}
			}
			snapshot.Links = append(snapshot.Links, IdentityLinkSnapshot{AnonymousID: link.Anonymous, IdentifiedID: link.Target, Version: fmt.Sprint(version)})
		}
	}
	if d.Person != nil {
		target := snapshot.IdentifiedID
		if err := saveNativeImportedPerson(ctx, tx, target, *d.Person, uint64(e.ID)); err != nil {
			return e, err
		}
	}
	var err error
	e.IdentitySnapshot, err = encodeNativeLiveSnapshot(snapshot)
	return e, err
}

// FindImportChunk checks retained proof continuity and returns the original
// outcome. Sealed recovery does not require allocating candidate IDs.
func (s *DuckLakeStore) FindImportChunk(ctx context.Context, fence ProjectWriteFence, input ImportChunkInput) (NativeCommitEvidence, bool, error) {
	fingerprint, err := validateImportInput(fence, input, EventIDReservation{fence: fence, batchID: input.Key.ID(), ids: make([]int64, len(input.Candidates))})
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, input.Key.Scope.Key.ProjectID)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	defer tx.Rollback()
	scope, err := readHostedProjectScopeState(ctx, tx, fence.Key, true)
	if err != nil {
		return NativeCommitEvidence{}, false, err
	}
	if scope != input.Key.Scope {
		return NativeCommitEvidence{}, false, ErrProjectWriteFenceStale
	}
	var high int64
	if err = tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1`).Scan(&high); err != nil {
		return NativeCommitEvidence{}, false, err
	}
	if err = p.validateNativeEffectHistory(ctx, tx, high); err != nil {
		return NativeCommitEvidence{}, false, err
	}
	e, found, err := readNativeCommitProof(ctx, tx, scope, input.Key.ID())
	if err == nil && found && (e.Kind != NativeCommitImport || e.TenantID != input.TenantID || e.InputFingerprint != fingerprint) {
		err = ErrImportChunkConflict
	}
	return e, found, err
}
