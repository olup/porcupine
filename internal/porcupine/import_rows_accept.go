package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

func (s *Service) initializeImportRows(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS posthog_import_person_state(
 project_id TEXT NOT NULL REFERENCES projects(id),distinct_id TEXT NOT NULL,person_id TEXT NOT NULL,source_id TEXT NOT NULL DEFAULT 'posthog',
 person_version TEXT NOT NULL,mapping_version TEXT NOT NULL,profile_version INTEGER NOT NULL,
 deleted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(project_id,distinct_id)) WITHOUT ROWID;`)
	if err != nil {
		return err
	}
	var exists bool
	if err = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('posthog_import_person_state') WHERE name='source_id')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		_, err = s.db.ExecContext(ctx, `ALTER TABLE posthog_import_person_state ADD COLUMN source_id TEXT NOT NULL DEFAULT 'posthog'`)
	}
	return err
}
func (s *Service) prepareImportRows(ctx context.Context, project string, rows []ImportRow) (*receiptLookup, error) {
	events := make([]Event, 0, len(rows))
	for i := range rows {
		rows[i].Event.ProjectID = project
		rows[i].Event.Imported = true
		events = append(events, rows[i].Event)
	}
	lookup, err := s.prepareReceiptLookup(ctx, project, events)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		rows[i].receiptKnown = lookup.known[rows[i].Event.UUID]
	}
	return lookup, nil
}

type importedPersonState struct {
	sourceID                      string
	found                         bool
	personID                      string
	personVersion, mappingVersion uint64
	profileVersion                int64
	deleted                       bool
}

func loadImportedPerson(ctx context.Context, tx *sql.Tx, project, distinct string) (importedPersonState, error) {
	var state importedPersonState
	var pv, mv string
	err := tx.QueryRowContext(ctx, `SELECT person_id,person_version,mapping_version,profile_version,deleted,source_id FROM posthog_import_person_state WHERE project_id=? AND distinct_id=?`, project, distinct).Scan(&state.personID, &pv, &mv, &state.profileVersion, &state.deleted, &state.sourceID)
	if errors.Is(err, sql.ErrNoRows) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	state.found = true
	state.personVersion, err = strconv.ParseUint(pv, 10, 64)
	if err != nil {
		return state, err
	}
	state.mappingVersion, err = strconv.ParseUint(mv, 10, 64)
	return state, err
}
func importProfileOwned(ctx context.Context, tx *sql.Tx, project, distinct, source string) (bool, error) {
	var version int64
	var identified bool
	err := tx.QueryRowContext(ctx, `SELECT version,is_identified FROM identity_profiles WHERE project_id=? AND identified_id=?`, project, distinct).Scan(&version, &identified)
	if errors.Is(err, sql.ErrNoRows) {
		state, e := loadImportedPerson(ctx, tx, project, distinct)
		return !state.found || state.sourceID == source, e
	}
	if err != nil {
		return false, err
	}
	if identified {
		return false, nil
	}
	state, err := loadImportedPerson(ctx, tx, project, distinct)
	return state.found && state.profileVersion == version && state.sourceID == source, err
}
func importIdentityEvidenceOwned(ctx context.Context, tx *sql.Tx, project, distinct, source string) (bool, error) {
	owned, err := importProfileOwned(ctx, tx, project, distinct, source)
	if err != nil || !owned {
		return owned, err
	}
	state, err := loadImportedPerson(ctx, tx, project, distinct)
	return !state.deleted, err
}

func saveImportedPerson(ctx context.Context, tx *sql.Tx, project string, p ImportPerson, profileVersion int64, source string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO posthog_import_person_state(project_id,distinct_id,person_id,person_version,mapping_version,profile_version,deleted,source_id) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(project_id,distinct_id) DO UPDATE SET person_id=excluded.person_id,person_version=excluded.person_version,mapping_version=excluded.mapping_version,profile_version=excluded.profile_version,deleted=excluded.deleted,source_id=excluded.source_id`, project, p.IdentifiedID, p.InternalID, strconv.FormatUint(p.PersonVersion, 10), strconv.FormatUint(p.MappingVersion, 10), profileVersion, p.Deleted, source)
	return err
}

// The upload owner validates its prepared receipt lookup and checkpoints the
// source file cursor in the very same transaction as this row acceptance.
// No ClickHouse/network work or outbound destination enqueueing occurs here.
func (s *Service) acceptImportRowTx(ctx context.Context, tx *sql.Tx, project string, row ImportRow) (ImportRowResult, error) {
	var result ImportRowResult
	if row.Kind != "events" && row.Kind != "persons" {
		return result, errImportRow
	}
	event := row.Event
	event.ProjectID = project
	event.Imported = true
	source := event.SourceID
	if source == "" {
		source = "posthog"
	}
	duplicate, err := localReceipt(ctx, tx, project, event.UUID)
	if err != nil {
		return result, err
	}
	if duplicate || row.receiptKnown {
		return ImportRowResult{Duplicates: 1}, nil
	}
	var personState importedPersonState
	if row.Kind == "persons" {
		p := row.Person
		if !validIdentityID(p.IdentifiedID) {
			return ImportRowResult{Ignored: 1, Unresolved: 1}, nil
		}
		personState, err = loadImportedPerson(ctx, tx, project, p.IdentifiedID)
		if err != nil {
			return result, err
		}
		if personState.found && (personState.sourceID != source || p.InternalID != personState.personID && p.MappingVersion <= personState.mappingVersion || p.MappingVersion < personState.mappingVersion || p.MappingVersion == personState.mappingVersion && p.PersonVersion <= personState.personVersion) {
			return ImportRowResult{Ignored: 1, Unresolved: 1}, nil
		}
		owned, err := importProfileOwned(ctx, tx, project, p.IdentifiedID, source)
		if err != nil {
			return result, err
		}
		link, _, err := identityLink(ctx, tx, project, p.IdentifiedID)
		if err != nil {
			return result, err
		}
		if !owned || link != "" {
			return ImportRowResult{Ignored: 1, Unresolved: 1}, nil
		}
		if p.Deleted {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM identity_profiles WHERE project_id=? AND identified_id=?)`, project, p.IdentifiedID).Scan(&exists); err != nil {
				return result, err
			}
			if !exists {
				err = saveImportedPerson(ctx, tx, project, p, 0, source)
				return ImportRowResult{Ignored: 1}, err
			}
		}
		properties := p.Properties
		if p.Deleted {
			properties = json.RawMessage(`{}`)
		}
		event.Event, event.DistinctID = "$set", p.IdentifiedID
		event.Properties = properties
		event.Timestamp = p.Timestamp.UTC().Format(time.RFC3339Nano)
		if p.Timestamp.IsZero() {
			event.Timestamp = s.clock().UTC().Format(time.RFC3339Nano)
		}
	}
	event.ReceivedAt = s.clock().UTC().Format(time.RFC3339Nano)
	event = eventWithMetadata(event)
	// Imported control arguments cannot be replayed as live incremental updates.
	// Persons snapshots hydrate profiles separately with explicit version guards.
	control := isIdentityControl(event.Event) || event.Event == "$create_alias" || event.Event == "$groupidentify"
	if control {
		event.DistinctID = ""
		event.Properties = json.RawMessage(`{}`)
		event.Context = json.RawMessage(`{}`)
	}
	var pending, pendingBytes int64
	if err = tx.QueryRowContext(ctx, `SELECT pending_count,pending_bytes FROM queue_usage WHERE id=1`).Scan(&pending, &pendingBytes); err != nil {
		return result, err
	}
	if pending >= int64(s.queueMax) || queueEventBytes(event) > s.queueMaxBytes-pendingBytes {
		return result, errors.New("import ingestion queue is full")
	}
	inserted, err := tx.ExecContext(ctx, `INSERT INTO events(project_id,uuid,event,distinct_id,properties,timestamp,received_at,capture_protocol,ingestion_metadata,is_imported) VALUES(?,?,?,?,?,?,?,1,?,1)`, project, event.UUID, event.Event, event.DistinctID, string(event.Properties), event.Timestamp, event.ReceivedAt, encodeIngestionMetadata(event))
	if err != nil {
		return result, err
	}
	event.ID, err = inserted.LastInsertId()
	if err != nil {
		return result, err
	}
	if control {
		snapshot := identitySnapshot{Version: strconv.FormatInt(event.ID, 10), Traits: map[string]any{}, Links: []identityLinkSnapshot{}}
		if row.Kind == "persons" {
			var traits map[string]any
			properties := row.Person.Properties
			if row.Person.Deleted {
				properties = json.RawMessage(`{}`)
			}
			if decodeIngestionJSON(properties, &traits) != nil {
				return result, errImportRow
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO identity_profiles(project_id,identified_id,is_identified,traits,version) VALUES(?,?,0,?,?) ON CONFLICT(project_id,identified_id) DO UPDATE SET traits=excluded.traits,version=excluded.version`, project, row.Person.IdentifiedID, string(properties), event.ID)
			if err != nil {
				return result, err
			}
			snapshot.IdentifiedID, snapshot.Traits = row.Person.IdentifiedID, traits
			if err = saveImportedPerson(ctx, tx, project, row.Person, event.ID, source); err != nil {
				return result, err
			}
			if !row.Person.Deleted {
				result.Unresolved = 1
			} // The internal person UUID does not prove an app-user target.
		} else if row.Event.Event == "$identify" {
			// Only an actual exported anonymous->identified assertion is evidence.
			var properties map[string]any
			decodeIngestionJSON(row.Event.Properties, &properties)
			anonymous, _ := properties["$anon_distinct_id"].(string)
			if validIdentityID(row.Event.DistinctID) && validIdentityID(anonymous) && anonymous != row.Event.DistinctID {
				targetOwned, e := importIdentityEvidenceOwned(ctx, tx, project, row.Event.DistinctID, source)
				if e != nil {
					return result, e
				}
				sourceOwned, e := importIdentityEvidenceOwned(ctx, tx, project, anonymous, source)
				if e != nil {
					return result, e
				}
				existing, _, e := identityLink(ctx, tx, project, anonymous)
				if e != nil {
					return result, e
				}
				targetLink, _, e := identityLink(ctx, tx, project, row.Event.DistinctID)
				if e != nil {
					return result, e
				}
				if targetOwned && sourceOwned && existing == "" && targetLink == "" {
					safe := row.Event
					safe.ID = event.ID
					safe.ProjectID = project
					safe.Properties, _ = json.Marshal(map[string]any{"$anon_distinct_id": anonymous})
					ignored, e := applyIdentityState(ctx, tx, safe, &snapshot)
					if e != nil {
						return result, e
					}
					if ignored {
						result.Unresolved = 1
					}
				} else {
					result.Unresolved = 1
				}
			} else {
				result.Unresolved = 1
			}
		} else {
			result.Unresolved = 1
		}
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			return result, err
		}
		event.IdentitySnapshot = string(encoded)
		if _, err = tx.ExecContext(ctx, `UPDATE events SET identity_snapshot=? WHERE id=? AND project_id=?`, event.IdentitySnapshot, event.ID, project); err != nil {
			return result, err
		}
	}
	if queueEventBytes(event) > s.queueMaxBytes-pendingBytes {
		return result, errors.New("import ingestion queue is full")
	}
	result.Accepted = 1
	return result, nil
}
