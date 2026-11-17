package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/olup/porcupine/internal/importer"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func loadHistoricalIdentity(ctx context.Context, tx *sql.Tx, project, distinct string) (importer.HistoricalIdentityState, error) {
	var state importer.HistoricalIdentityState
	if distinct == "" {
		return state, nil
	}
	var pv, mv string
	person := &state.Person
	err := tx.QueryRowContext(ctx, `SELECT person_id,person_version,mapping_version,profile_version,deleted,source_id FROM posthog_import_person_state WHERE project_id=$1 AND distinct_id=$2`, project, distinct).Scan(&person.PersonID, &pv, &mv, &person.ProfileVersion, &person.Deleted, &person.SourceID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, err
	}
	if err == nil {
		person.Found = true
		person.PersonVersion, err = strconv.ParseUint(pv, 10, 64)
		if err != nil {
			return state, err
		}
		person.MappingVersion, err = strconv.ParseUint(mv, 10, 64)
		if err != nil {
			return state, err
		}
	}
	var traits string
	err = tx.QueryRowContext(ctx, `SELECT traits,version,is_identified FROM identity_profiles WHERE project_id=$1 AND identified_id=$2`, project, distinct).Scan(&traits, &state.ProfileVersion, &state.Profile.Identified)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return state, err
	}
	if err == nil {
		state.ProfileFound, state.Profile.Traits = true, json.RawMessage(traits)
	}
	err = tx.QueryRowContext(ctx, `SELECT identified_id,version FROM identity_links WHERE project_id=$1 AND anonymous_id=$2`, project, distinct).Scan(&state.Link.Target, &state.Link.Version)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return state, err
}

func saveImportedPerson(ctx context.Context, tx *sql.Tx, project, distinct string, state importer.HistoricalPersonState, profileVersion int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO posthog_import_person_state(project_id,distinct_id,person_id,person_version,mapping_version,profile_version,deleted,source_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT(project_id,distinct_id) DO UPDATE SET person_id=excluded.person_id,person_version=excluded.person_version,mapping_version=excluded.mapping_version,profile_version=excluded.profile_version,deleted=excluded.deleted,source_id=excluded.source_id`, project, distinct, state.PersonID, strconv.FormatUint(state.PersonVersion, 10), strconv.FormatUint(state.MappingVersion, 10), profileVersion, state.Deleted, state.SourceID)
	return err
}

// The upload owner checkpoints the source file cursor in the same
// transaction as local receipt checks and historical row acceptance.
// No network work or outbound destination enqueueing occurs here.
// known holds events found in DuckLake by coldKnown before the transaction.
func (rules *eventRules) AcceptHistoricalTx(ctx context.Context, tx *sql.Tx, project string, row importer.Row, known map[string]int64) (ImportRowResult, error) {
	var result ImportRowResult
	if row.Kind != "events" && row.Kind != "persons" {
		return result, importer.ErrRow
	}
	duplicate, err := localReceipt(ctx, tx, project, row.Event.UUID)
	if err != nil {
		return result, err
	}
	if _, cold := known[row.Event.UUID]; duplicate || cold {
		return ImportRowResult{Duplicates: 1}, nil
	}
	target, anonymous := importer.HistoricalStateIDs(row)
	var state importer.HistoricalState
	state.Target, err = loadHistoricalIdentity(ctx, tx, project, target)
	if err != nil {
		return result, err
	}
	if anonymous != "" && anonymous != target {
		state.Anonymous, err = loadHistoricalIdentity(ctx, tx, project, anonymous)
		if err != nil {
			return result, err
		}
	}
	acceptedAt := rules.clock().UTC()
	decision, err := importer.ReduceHistorical(row, state, acceptedAt)
	if err != nil {
		return result, err
	}
	result = ImportRowResult{Ignored: decision.Ignored, Unresolved: decision.Unresolved}
	if decision.Accepted == 0 {
		if decision.Person != nil {
			err = saveImportedPerson(ctx, tx, project, target, *decision.Person, 0)
		}
		return result, err
	}
	event := decision.Event
	event.ProjectID = project
	event.ReceivedAt = acceptedAt.Format(time.RFC3339Nano)
	event = lakeapi.EventWithMetadata(event)
	var pending, pendingBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).Scan(&pending, &pendingBytes); err != nil {
		return result, err
	}
	if pending >= int64(rules.queueMax) || queueEventBytes(event) > rules.queueMaxBytes-pendingBytes {
		return result, errors.New("import ingestion queue is full")
	}
	event.ID, err = rules.insertAcceptedEvent(ctx, tx, event)
	if err != nil {
		return result, err
	}
	if decision.Control {
		snapshot := lakeapi.IdentitySnapshot{Version: strconv.FormatInt(event.ID, 10), Traits: map[string]any{}, Links: []lakeapi.IdentityLinkSnapshot{}}
		if profile := decision.Profile; profile != nil {
			if _, err = tx.ExecContext(ctx, `INSERT INTO identity_profiles(project_id,identified_id,is_identified,traits,version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(project_id,identified_id) DO UPDATE SET is_identified=excluded.is_identified,traits=excluded.traits,version=excluded.version`, project, profile.Target, profile.Identified, string(profile.Traits), event.ID); err != nil {
				return result, err
			}
			if link := profile.Link; link != nil {
				version := link.Version
				if link.Create {
					version = uint64(event.ID)
					if _, err = tx.ExecContext(ctx, `INSERT INTO identity_links(project_id,anonymous_id,identified_id,version) VALUES($1,$2,$3,$4)`, project, link.Anonymous, link.Target, version); err != nil {
						return result, err
					}
				}
				snapshot.Links = append(snapshot.Links, lakeapi.IdentityLinkSnapshot{AnonymousID: link.Anonymous, IdentifiedID: link.Target, Version: strconv.FormatUint(version, 10)})
			}
			var traits map[string]any
			if err = ingestfmt.DecodeJSON(profile.Traits, &traits); err != nil {
				return result, err
			}
			snapshot.IdentifiedID, snapshot.Traits = profile.Target, traits
		}
		if decision.Person != nil {
			if err = saveImportedPerson(ctx, tx, project, target, *decision.Person, event.ID); err != nil {
				return result, err
			}
		}
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			return result, err
		}
		event.IdentitySnapshot = string(encoded)
		if _, err = tx.ExecContext(ctx, `UPDATE events SET identity_snapshot=$1 WHERE id=$2 AND project_id=$3`, event.IdentitySnapshot, event.ID, project); err != nil {
			return result, err
		}
	}
	if queueEventBytes(event) > rules.queueMaxBytes-pendingBytes {
		return result, errors.New("import ingestion queue is full")
	}
	if err := rules.consumeAcceptedEvents(ctx, tx, project, acceptedAt, 1); err != nil {
		return result, err
	}
	result.Accepted = 1
	return result, nil
}
