package lake

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
	"github.com/olup/porcupine/internal/ingest"
)

// CommitLive owns the native transaction. It never acknowledges volatile input
// or publishes PG jobs; its proof makes those later effects recoverable.
func (s *DuckLakeStore) CommitLive(ctx context.Context, fence ProjectWriteFence, batch LiveCommitBatch, reservation EventIDReservation, stager LiveEffectStager) (LiveCommitResult, error) {
	return s.commitLive(ctx, fence, batch, reservation, stager, nil, func(_ context.Context, commit func() error) error { return commit() }, nil)
}

// NativeEffectCheckpoint is the last publication committed by the control store.
type NativeEffectCheckpoint struct {
	Sequence    int64
	Fingerprint string
}

// ErrNativeEffectsPending means a successor must reconcile committed evidence
// before attempting another live batch.
var ErrNativeEffectsPending = errors.New("native effects await publication")

// ErrInvalidLiveProjection means the validated input cannot form a native row.
var ErrInvalidLiveProjection = errors.New("invalid hosted live projection")

// ValidateLiveProjection checks admitted fields without a reserved event ID or identity state.
func ValidateLiveProjection(e event.Event) error {
	if !validLiveEventUUID(e.UUID) {
		return ErrInvalidLiveProjection
	}
	e.ID = 1
	_, err := PrepareLakeProjection(EventProjection{Event: e})
	return err
}

// LiveCommitPhase identifies a native step within one live transaction attempt.
type LiveCommitPhase string

const (
	LiveCommitPredecessor LiveCommitPhase = "native_predecessor"
	LiveCommitProofLookup LiveCommitPhase = "native_proof_lookup"
	LiveCommitFence       LiveCommitPhase = "native_fence"
	LiveCommitHeads       LiveCommitPhase = "native_heads"
	LiveCommitTotals      LiveCommitPhase = "native_totals"
	LiveCommitEventRows   LiveCommitPhase = "native_event_rows"
	LiveCommitProofInsert LiveCommitPhase = "native_proof_insert"
	LiveCommitTransaction LiveCommitPhase = "native_commit"
)

// LiveCommitObserver reports each reached phase synchronously, including failed
// calls and repeats across native retries. A nil observer disables reporting.
type LiveCommitObserver func(LiveCommitPhase, time.Duration)

func (observe LiveCommitObserver) since(phase LiveCommitPhase, started time.Time) {
	if observe != nil {
		observe(phase, time.Since(started))
	}
}

// CommitLiveFromCheckpoint advances the write fence and acknowledges the
// published predecessor in the same transaction as the next live outcome.
// The guard checks the control fence and invokes COMMIT synchronously,
// at most once.
func (s *DuckLakeStore) CommitLiveFromCheckpoint(ctx context.Context, fence ProjectWriteFence, batch LiveCommitBatch, reservation EventIDReservation, stager LiveEffectStager, checkpoint NativeEffectCheckpoint, guard func(context.Context, func() error) error, observe LiveCommitObserver) (LiveCommitResult, error) {
	if guard == nil {
		return nil, errors.New("hosted live commit requires a control fence guard")
	}
	return s.commitLive(ctx, fence, batch, reservation, stager, &checkpoint, guard, observe)
}

func (s *DuckLakeStore) commitLive(ctx context.Context, fence ProjectWriteFence, batch LiveCommitBatch, reservation EventIDReservation, stager LiveEffectStager, checkpoint *NativeEffectCheckpoint, guard func(context.Context, func() error) error, observe LiveCommitObserver) (LiveCommitResult, error) {
	fingerprint, err := validateLiveCommitBatch(fence, batch, reservation)
	if err != nil {
		return nil, err
	}
	if stager == nil || s.Remote == nil || s.SharedCatalog == nil {
		return nil, errors.New("hosted live commit requires immutable effects storage and shared native metadata")
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, fence.Key.ProjectID)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := p.write.Lock(ctx); err != nil {
		return nil, err
	}
	defer p.write.Unlock()
	var result LiveCommitResult
	err = retryNativeTransaction(ctx, func() error {
		tx, err := p.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if checkpoint != nil {
			started := time.Now()
			err := verifyLivePredecessor(ctx, tx, batch.Scope, *checkpoint)
			observe.since(LiveCommitPredecessor, started)
			if err != nil {
				return err
			}
		}
		started := time.Now()
		prior, found, err := readNativeCommitProof(ctx, tx, batch.Scope, batch.ID)
		observe.since(LiveCommitProofLookup, started)
		if err != nil {
			return err
		}
		if found {
			if prior.Kind != NativeCommitLive || prior.InputFingerprint != fingerprint || prior.TenantID != batch.TenantID {
				return errors.New("native batch identity was reused with different validated input")
			}
			result = CommittedLiveBatch{Evidence: prior}
			return nil
		}
		var sequence int64
		started = time.Now()
		if checkpoint != nil {
			sequence, err = beginCheckpointedLiveWrite(ctx, tx, fence, batch, *checkpoint)
		} else {
			sequence, err = p.beginHostedIngestionWrite(ctx, tx, fence, batch.TenantID)
		}
		observe.since(LiveCommitFence, started)
		if err != nil {
			return err
		}
		started = time.Now()
		known, err := liveHeads(ctx, tx, batch.Inputs)
		observe.since(LiveCommitHeads, started)
		if err != nil {
			return err
		}
		var exceptions int64
		selected := make([]PreparedLakeProjection, 0, len(batch.Inputs))
		eligible := make([]event.Event, 0, len(batch.Inputs))
		for position, input := range batch.Inputs {
			if known[input.Event.UUID] {
				continue
			}
			known[input.Event.UUID] = true
			canonical := input.Event
			canonical.ID = reservation.ids[position]
			if input.Identity != nil {
				canonical, err = p.applyHostedIdentity(ctx, tx, canonical, *input.Identity)
				if err != nil {
					return err
				}
			}
			prepared, err := PrepareLakeProjection(EventProjection{Event: canonical})
			if err != nil {
				return fmt.Errorf("%w: %v", ErrInvalidLiveProjection, err)
			}
			if input.Identity == nil {
				eligible = append(eligible, prepared.projection.Event)
			}
			if canonical.Event == "$exception" {
				exceptions++
			}
			selected = append(selected, prepared)
		}
		if len(selected) == 0 {
			result = DuplicateLiveBatch{}
			return nil
		}
		effects, err := stager.StageLiveEffects(ctx, SelectedLiveEffects{Scope: batch.Scope, TenantID: batch.TenantID, BatchID: batch.ID, Sequence: sequence, Events: eligible})
		if err != nil {
			return err
		}
		if err := s.validateStagedEffects(batch.Scope, effects); err != nil {
			return err
		}
		accountedAt := s.liveCommitTime()
		started = time.Now()
		totals, err := updateNativeSourceTotals(ctx, tx, sequence, accountedAt, int64(len(selected)), 0)
		observe.since(LiveCommitTotals, started)
		if err != nil {
			return err
		}
		started = time.Now()
		err = insertCanonicalEvents(ctx, tx, selected)
		observe.since(LiveCommitEventRows, started)
		if err != nil {
			return err
		}
		started = time.Now()
		evidence := NativeCommitEvidence{Kind: NativeCommitLive, Scope: batch.Scope, TenantID: batch.TenantID, Sequence: sequence, OperationID: batch.ID, InputFingerprint: fingerprint, FenceEpoch: fence.Epoch, AccountedAt: accountedAt, AcceptedLive: int64(len(selected)), AcceptedExceptions: exceptions, Effects: effects, Totals: totals}
		evidence.ProofFingerprint, err = nativeEvidenceFingerprint(evidence)
		if err == nil {
			err = insertNativeCommitProof(ctx, tx, evidence)
		}
		observe.since(LiveCommitProofInsert, started)
		if err != nil {
			return err
		}
		if err := guard(ctx, func() error {
			started := time.Now()
			err := tx.Commit()
			observe.since(LiveCommitTransaction, started)
			return err
		}); err != nil {
			return err
		}
		result = CommittedLiveBatch{Evidence: evidence, Fresh: true}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func (s *DuckLakeStore) liveCommitTime() time.Time {
	if s.liveClock != nil {
		return s.liveClock().UTC()
	}
	return time.Now().UTC()
}
func validateLiveCommitBatch(fence ProjectWriteFence, batch LiveCommitBatch, reservation EventIDReservation) (string, error) {
	if !validProjectWriteFence(fence) || batch.ID == "" || len(batch.ID) > 200 || batch.TenantID == "" || batch.Scope.Key != fence.Key || batch.Scope.StreamID != fence.StreamID || batch.Scope.CatalogID != SharedNativeCatalogID(fence.Key.SourceID, fence.Key.ProjectID) || len(batch.Inputs) == 0 || len(batch.Inputs) > 100000 || !reservation.matches(fence, batch.ID, len(batch.Inputs)) {
		return "", errors.New("invalid hosted live batch scope or ID reservation")
	}
	for _, input := range batch.Inputs {
		e := input.Event
		control := e.Event == "$identify" || e.Event == "$set"
		if e.ID != 0 || e.ProjectID != fence.Key.ProjectID || !validLiveEventUUID(e.UUID) || e.Imported || e.Delivered || e.IdentitySnapshot != "" || control != (input.Identity != nil) {
			return "", errors.New("invalid validated live event state")
		}
		if control && (e.DistinctID != "" || string(e.Properties) != "{}") {
			return "", errors.New("identity command inputs must be separate from canonical event fields")
		}
	}
	// Reserved IDs and attempt clocks are deliberately absent from this versioned
	// fingerprint. Input order, canonical commands and authoritative scope remain.
	raw, err := json.Marshal(struct {
		Version  int
		Scope    HostedProjectScope
		TenantID string
		Inputs   []LiveCommitInput
	}{1, batch.Scope, batch.TenantID, batch.Inputs})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}
func (p *ProjectLake) beginHostedIngestionWrite(ctx context.Context, tx *sql.Tx, fence ProjectWriteFence, tenant string) (int64, error) {
	var previous int64
	if err := tx.QueryRowContext(ctx, `SELECT CAST(next_effect_seq AS BIGINT) FROM lake.hosted_write_guard WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND identity_ready AND state='open' AND fence_epoch=? AND fence_nonce=? AND (tenant_id IS NULL OR tenant_id=?)`, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID, tenant).Scan(&previous); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrProjectWriteFenceStale
		}
		return 0, err
	}
	if err := p.validateNativeEffectHistory(ctx, tx, previous); err != nil {
		return 0, err
	}
	if previous == math.MaxInt64 {
		return 0, errors.New("native effect sequence exhausted")
	}
	if err := nativeUpdateExactlyOne(ctx, tx, `UPDATE lake.hosted_write_guard SET generation=generation+1,next_effect_seq=next_effect_seq+1,tenant_id=coalesce(tenant_id,?) WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND identity_ready AND state='open' AND fence_epoch=? AND fence_nonce=? AND next_effect_seq=? AND (tenant_id IS NULL OR tenant_id=?)`, tenant, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID, previous, tenant); err != nil {
		return 0, err
	}
	if err := checkNativeProjectNotDeleted(ctx, tx); err != nil {
		return 0, err
	}
	return previous + 1, nil
}
func liveHeads(ctx context.Context, tx *sql.Tx, inputs []LiveCommitInput) (map[string]bool, error) {
	known := make(map[string]bool, len(inputs))
	if len(inputs) == 0 {
		return known, nil
	}
	uuids := make([]string, len(inputs))
	for i, input := range inputs {
		uuids[i] = input.Event.UUID
	}
	// Look up the whole batch in one scan. Deleted heads still count.
	rows, err := tx.QueryContext(ctx, `
		SELECT h.uuid FROM lake.heads AS h
		SEMI JOIN UNNEST(CAST(? AS VARCHAR[])) AS batch(uuid) ON h.uuid = batch.uuid
	`, uuids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			return nil, err
		}
		known[uuid] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return known, nil
}

type nativeIdentityReader struct{ tx *sql.Tx }

func (r nativeIdentityReader) Profile(ctx context.Context, id string) (identityapi.Profile, error) {
	var raw string
	var result identityapi.Profile
	err := r.tx.QueryRowContext(ctx, "SELECT traits,is_identified FROM lake.identity_profiles WHERE identified_id=?", id).Scan(&raw, &result.Identified)
	if errors.Is(err, sql.ErrNoRows) {
		return identityapi.Profile{}, nil
	}
	if err != nil {
		return identityapi.Profile{}, err
	}
	result.Traits = []byte(raw)
	return result, nil
}
func (r nativeIdentityReader) Link(ctx context.Context, id string) (identityapi.Link, error) {
	var result identityapi.Link
	err := r.tx.QueryRowContext(ctx, "SELECT identified_id,version FROM lake.identity_links WHERE anonymous_id=?", id).Scan(&result.Target, &result.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return identityapi.Link{}, nil
	}
	return result, err
}
func (p *ProjectLake) applyHostedIdentity(ctx context.Context, tx *sql.Tx, e event.Event, command identityapi.Command) (event.Event, error) {
	decision, err := identityapi.Evaluate(ctx, command, nativeIdentityReader{tx})
	if err != nil {
		return event.Event{}, err
	}
	snapshot := emptyLiveIdentitySnapshot(fmt.Sprint(e.ID))
	if !decision.Ignored {
		if _, err := tx.ExecContext(ctx, `DELETE FROM lake.identity_profiles WHERE identified_id=?`, decision.Target); err != nil {
			return event.Event{}, err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO lake.identity_profiles (identified_id,version,traits,is_identified) VALUES(?,?,?,?)`, decision.Target, e.ID, string(decision.Traits), decision.Identified); err != nil {
			return event.Event{}, err
		}
		snapshot.IdentifiedID, snapshot.Identified = decision.Target, decision.Identified
		if err := ingest.DecodeJSON(decision.Traits, &snapshot.Traits); err != nil {
			return event.Event{}, err
		}
		if link := decision.Link; link != nil {
			version := link.Version
			if link.Create {
				version = uint64(e.ID)
				if _, err := tx.ExecContext(ctx, `INSERT INTO lake.identity_links (anonymous_id,identified_id,version) VALUES(?,?,?)`, link.Anonymous, link.Target, version); err != nil {
					return event.Event{}, err
				}
			}
			snapshot.Links = append(snapshot.Links, IdentityLinkSnapshot{AnonymousID: link.Anonymous, IdentifiedID: link.Target, Version: fmt.Sprint(version)})
		}
	}
	e.IdentitySnapshot, err = encodeNativeLiveSnapshot(snapshot)
	return e, err
}
func boolToUint8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func insertCanonicalEvents(ctx context.Context, tx *sql.Tx, events []PreparedLakeProjection) error {
	if len(events) == 0 {
		return nil
	}
	rows := make([]lakeProjectionRow, 0, len(events))
	for _, prepared := range events {
		e := prepared.projection.Event
		rows = append(rows, lakeProjectionRow{UUID: e.UUID, ID: e.ID, ProjectID: e.ProjectID, Event: e.Event, DistinctID: e.DistinctID, Properties: string(e.Properties), Timestamp: e.Timestamp, TimestampMS: prepared.timestamp.UnixMilli(), EventTime: prepared.timestamp.Format(time.RFC3339Nano), ReceivedAt: e.ReceivedAt, ReceivedAtMS: prepared.received.UnixMilli(), SourceID: e.SourceID, MessageID: e.MessageID, EventType: e.EventType, Context: string(e.Context), Imported: boolToUint8(e.Imported), Identity: e.IdentitySnapshot})
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, lakeStageProjections, string(encoded)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO lake.heads (uuid,event_id,revision,is_deleted,received_at) SELECT uuid,id,0,false,received_at FROM _pc_incoming;
INSERT INTO lake.current_events (id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot)
 SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,received_at,received_at_ms,0,source_id,message_id,event_type,context,is_imported,identity_snapshot,'' FROM _pc_incoming;
DROP TABLE _pc_incoming`)
	return err
}
func updateNativeSourceTotals(ctx context.Context, tx *sql.Tx, sequence int64, at time.Time, live, imported int64) ([]NativeSourceTotals, error) {
	day := at.UTC().Format("2006-01-02T00:00:00Z")
	total := NativeSourceTotals{Day: day, Revision: sequence}
	err := tx.QueryRowContext(ctx, "SELECT accepted_live,accepted_import FROM lake.native_usage_totals WHERE day=?", day).Scan(&total.Live, &total.Import)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if live < 0 || imported < 0 || total.Live > math.MaxInt64-live || total.Import > math.MaxInt64-imported {
		return nil, errors.New("native usage counter exhausted")
	}
	total.Live += live
	total.Import += imported
	if _, err := tx.ExecContext(ctx, `DELETE FROM lake.native_usage_totals WHERE day=?`, day); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO lake.native_usage_totals (day,revision,accepted_live,accepted_import) VALUES(?,?,?,?)`, day, sequence, total.Live, total.Import); err != nil {
		return nil, err
	}
	return []NativeSourceTotals{total}, nil
}
func (s *DuckLakeStore) validateStagedEffects(scope HostedProjectScope, effects StagedLiveEffects) error {
	if effects.PlanID == "" || effects.OutputCount < 0 {
		return errors.New("immutable effect plan is unavailable")
	}
	ref := effects.Manifest
	if effects.OutputCount == 0 && ref.Key == "" && ref.Bytes == 0 && ref.SHA256 == "" && ref.VersionID == "" {
		return nil
	}
	digest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(digest) != sha256.Size || ref.Bytes <= 0 || !strings.HasPrefix(ref.Key, s.Remote.ProjectPrefix(scope.Key.ProjectID)+"effects/"+scope.StreamID+"/") {
		return errors.New("invalid immutable effect manifest reference")
	}
	return nil
}
