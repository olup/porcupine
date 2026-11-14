//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
)

const duckLakeSchema = `
CREATE TABLE IF NOT EXISTS lake.heads (
 uuid VARCHAR, event_id BIGINT, revision UBIGINT, is_deleted BOOLEAN, received_at VARCHAR
);
CREATE TABLE IF NOT EXISTS lake.current_events (
 id BIGINT, project_id VARCHAR, uuid VARCHAR, event VARCHAR, distinct_id VARCHAR,
 properties VARCHAR, timestamp VARCHAR, timestamp_ms BIGINT, event_time TIMESTAMPTZ,
 received_at VARCHAR, received_at_ms BIGINT, revision UBIGINT,
 source_id VARCHAR, message_id VARCHAR, event_type VARCHAR, context VARCHAR, is_imported UTINYINT,
 identity_snapshot VARCHAR, payload VARCHAR
);
CREATE TABLE IF NOT EXISTS lake.identity_profiles (
 identified_id VARCHAR, version UBIGINT, traits VARCHAR
);
CREATE TABLE IF NOT EXISTS lake.identity_links (
 anonymous_id VARCHAR, identified_id VARCHAR, version UBIGINT
);
CREATE OR REPLACE VIEW events AS
 SELECT e.id,e.id AS event_id,e.is_imported,e.source_id,e.message_id,e.event_type,e.context,
 e.revision,e.project_id,e.uuid,e.event,e.distinct_id,
 COALESCE(l.identified_id,e.distinct_id) AS person_id,
 COALESCE(p.traits,'{}') AS person_properties,
 e.properties,e.timestamp,e.timestamp_ms,e.event_time,e.received_at,e.received_at_ms
 FROM lake.current_events e
 LEFT JOIN lake.identity_links l ON l.anonymous_id=e.distinct_id
 LEFT JOIN lake.identity_profiles p ON p.identified_id=COALESCE(l.identified_id,e.distinct_id)
 WHERE e.identity_snapshot='';
`

type preparedLakeProjection struct {
	projection eventProjection
	payload    string
	timestamp  time.Time
	received   time.Time
	identity   *identitySnapshot
}

func prepareLakeProjection(p eventProjection) (preparedLakeProjection, error) {
	if p.ID <= 0 || p.ProjectID == "" || p.UUID == "" {
		return preparedLakeProjection{}, errors.New("invalid DuckLake projection identity")
	}
	received, err := time.Parse(time.RFC3339Nano, p.ReceivedAt)
	if err != nil {
		return preparedLakeProjection{}, errors.New("invalid DuckLake receipt time")
	}
	prepared := preparedLakeProjection{projection: p, received: received.UTC()}
	if p.IsDeleted {
		if p.Revision == 0 {
			return preparedLakeProjection{}, errors.New("invalid DuckLake deletion revision")
		}
		return prepared, nil
	}
	p.Event = eventWithMetadata(p.Event)
	timestamp, err := time.Parse(time.RFC3339Nano, p.Timestamp)
	if err != nil || !json.Valid(p.Properties) || !validEventContext(p.Context) || !json.Valid([]byte(p.OriginalSnapshot)) {
		return preparedLakeProjection{}, errors.New("invalid DuckLake projection payload")
	}
	p.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
	p.ReceivedAt = received.UTC().Format(time.RFC3339Nano)
	if p.IdentitySnapshot != "" {
		var snapshot identitySnapshot
		if json.Unmarshal([]byte(p.IdentitySnapshot), &snapshot) != nil {
			return preparedLakeProjection{}, errors.New("invalid DuckLake identity snapshot")
		}
		if _, err := strconv.ParseUint(snapshot.Version, 10, 64); err != nil {
			return preparedLakeProjection{}, errors.New("invalid DuckLake identity version")
		}
		for _, link := range snapshot.Links {
			if _, err := strconv.ParseUint(link.Version, 10, 64); err != nil {
				return preparedLakeProjection{}, errors.New("invalid DuckLake identity link version")
			}
		}
		prepared.identity = &snapshot
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return preparedLakeProjection{}, errors.New("cannot encode DuckLake projection")
	}
	prepared.projection, prepared.payload, prepared.timestamp = p, string(payload), timestamp.UTC()
	return prepared, nil
}

func (s *duckLakeStore) Apply(ctx context.Context, projections []eventProjection) error {
	groups := make(map[string][]preparedLakeProjection)
	for _, projection := range projections {
		prepared, err := prepareLakeProjection(projection)
		if err != nil {
			return err
		}
		groups[projection.ProjectID] = append(groups[projection.ProjectID], prepared)
	}
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p, release, err := s.leaseProject(ctx, id)
		if err != nil {
			return err
		}
		err = p.apply(ctx, groups[id])
		release()
		if err != nil {
			return err
		}
	}
	return nil
}

// Allocation retries are bounded separately from successful chunks. A fixed total
// attempt limit could replay the same committed prefix forever without allowing
// the delivery worker to acknowledge the complete selection.
const duckLakeAllocationRetries = 16

func (p *projectLake) apply(ctx context.Context, projections []preparedLakeProjection) error {
	chunkSize, retries := len(projections), duckLakeAllocationRetries
	for offset := 0; offset < len(projections); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(offset+chunkSize, len(projections))
		err := p.applyChunk(ctx, projections[offset:end])
		if err == nil {
			offset = end
			continue
		}
		var nativeError *duckdb.Error
		if !errors.As(err, &nativeError) || nativeError.Type != duckdb.ErrorTypeOutOfMemory || end-offset == 1 || retries == 0 {
			return err
		}
		// The failed transaction has rolled back. Keep stable IDs and revisions for
		// every retry, including already committed chunks if a later chunk fails.
		chunkSize = (end - offset) / 2
		retries--
	}
	return nil
}

func (p *projectLake) applyChunk(ctx context.Context, projections []preparedLakeProjection) error {
	rows := make([]lakeProjectionRow, 0, len(projections))
	controls := make(map[string]preparedLakeProjection)
	for _, prepared := range projections {
		projection := prepared.projection
		row := lakeProjectionRow{
			UUID: projection.UUID, ID: projection.ID, ProjectID: projection.ProjectID,
			Revision: projection.Revision, Deleted: projection.IsDeleted, ReceivedAt: projection.ReceivedAt,
			Event: projection.Event.Event, DistinctID: projection.DistinctID, Properties: string(projection.Properties),
			Timestamp: projection.Timestamp, TimestampMS: prepared.timestamp.UnixMilli(), EventTime: prepared.timestamp.Format(time.RFC3339Nano),
			ReceivedAtMS: prepared.received.UnixMilli(), SourceID: projection.SourceID, MessageID: projection.MessageID,
			EventType: projection.EventType, Context: string(projection.Context), Imported: boolByte(projection.Imported),
			Identity: projection.IdentitySnapshot, Payload: prepared.payload,
		}
		if projection.IsDeleted {
			row.EventTime, row.TimestampMS = "1970-01-01T00:00:00Z", 0
		}
		rows = append(rows, row)
		if prepared.identity != nil {
			previous, exists := controls[projection.UUID]
			if !exists || projection.Revision > previous.projection.Revision {
				controls[projection.UUID] = prepared
			}
		}
	}
	encoded, err := json.Marshal(rows)
	if err != nil {
		return errors.New("cannot encode DuckLake projection batch")
	}
	if err := p.write.Lock(ctx); err != nil {
		return err
	}
	defer p.write.Unlock()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, lakeStageProjections, string(encoded)); err != nil {
		return err
	}
	var conflicting bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(
 SELECT 1 FROM _pc_incoming GROUP BY uuid HAVING min(id) <> max(id)
 UNION ALL
 SELECT 1 FROM _pc_incoming i JOIN lake.heads h ON i.uuid=h.uuid WHERE i.id<>h.event_id
)`).Scan(&conflicting); err != nil {
		return err
	}
	if conflicting {
		return errors.New("DuckLake event ID changed")
	}
	if _, err := tx.ExecContext(ctx, `CREATE OR REPLACE TEMP TABLE _pc_changes AS
 SELECT i.* FROM _pc_incoming i LEFT JOIN lake.heads h ON i.uuid=h.uuid
 WHERE h.uuid IS NULL OR i.revision>h.revision
 QUALIFY row_number() OVER(PARTITION BY i.uuid ORDER BY i.revision DESC,i.position ASC)=1`); err != nil {
		return err
	}
	if len(controls) > 0 {
		applied, err := tx.QueryContext(ctx, "SELECT uuid FROM _pc_changes WHERE identity_snapshot<>'' AND NOT is_deleted")
		if err != nil {
			return err
		}
		var identities []identitySnapshot
		for applied.Next() {
			var uuid string
			if err := applied.Scan(&uuid); err != nil {
				applied.Close()
				return err
			}
			identities = append(identities, *controls[uuid].identity)
		}
		readErr := applied.Err()
		applied.Close()
		if readErr != nil {
			return readErr
		}
		for _, identity := range identities {
			if err := applyLakeIdentity(ctx, tx, identity); err != nil {
				return err
			}
		}
	}
	if _, err := tx.ExecContext(ctx, `
 DELETE FROM lake.current_events WHERE uuid IN (SELECT uuid FROM _pc_changes);
 DELETE FROM lake.heads WHERE uuid IN (SELECT uuid FROM _pc_changes);
 INSERT INTO lake.heads SELECT uuid,id,revision,is_deleted,received_at FROM _pc_changes;
 INSERT INTO lake.current_events
 SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,
 received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,payload
 FROM _pc_changes WHERE NOT is_deleted;
 DROP TABLE _pc_changes;
 DROP TABLE _pc_incoming;
 `); err != nil {
		return err
	}
	return tx.Commit()
}

type lakeProjectionRow struct {
	UUID         string `json:"uuid"`
	ID           int64  `json:"id"`
	ProjectID    string `json:"project_id"`
	Revision     uint64 `json:"revision"`
	Deleted      bool   `json:"is_deleted"`
	Event        string `json:"event"`
	DistinctID   string `json:"distinct_id"`
	Properties   string `json:"properties"`
	Timestamp    string `json:"timestamp"`
	TimestampMS  int64  `json:"timestamp_ms"`
	EventTime    string `json:"event_time"`
	ReceivedAt   string `json:"received_at"`
	ReceivedAtMS int64  `json:"received_at_ms"`
	SourceID     string `json:"source_id"`
	MessageID    string `json:"message_id"`
	EventType    string `json:"event_type"`
	Context      string `json:"context"`
	Imported     uint8  `json:"is_imported"`
	Identity     string `json:"identity_snapshot"`
	Payload      string `json:"payload"`
}

const lakeStageProjections = `CREATE OR REPLACE TEMP TABLE _pc_incoming AS SELECT
 CAST(key AS BIGINT) AS position,
 json_extract_string(value,'$.uuid') AS uuid,
 CAST(json_extract_string(value,'$.id') AS BIGINT) AS id,
 json_extract_string(value,'$.project_id') AS project_id,
 CAST(json_extract_string(value,'$.revision') AS UBIGINT) AS revision,
 CAST(json_extract_string(value,'$.is_deleted') AS BOOLEAN) AS is_deleted,
 json_extract_string(value,'$.event') AS event,
 json_extract_string(value,'$.distinct_id') AS distinct_id,
 json_extract_string(value,'$.properties') AS properties,
 json_extract_string(value,'$.timestamp') AS timestamp,
 CAST(json_extract_string(value,'$.timestamp_ms') AS BIGINT) AS timestamp_ms,
 CAST(json_extract_string(value,'$.event_time') AS TIMESTAMPTZ) AS event_time,
 json_extract_string(value,'$.received_at') AS received_at,
 CAST(json_extract_string(value,'$.received_at_ms') AS BIGINT) AS received_at_ms,
 json_extract_string(value,'$.source_id') AS source_id,
 json_extract_string(value,'$.message_id') AS message_id,
 json_extract_string(value,'$.event_type') AS event_type,
 json_extract_string(value,'$.context') AS context,
 CAST(json_extract_string(value,'$.is_imported') AS UTINYINT) AS is_imported,
 json_extract_string(value,'$.identity_snapshot') AS identity_snapshot,
 json_extract_string(value,'$.payload') AS payload
 FROM json_each(CAST(? AS JSON))`

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func applyLakeIdentity(ctx context.Context, tx *sql.Tx, snapshot identitySnapshot) error {
	version, err := strconv.ParseUint(snapshot.Version, 10, 64)
	if err != nil {
		return err
	}
	if snapshot.IdentifiedID != "" {
		var current uint64
		err := tx.QueryRowContext(ctx, "SELECT version FROM lake.identity_profiles WHERE identified_id=?", snapshot.IdentifiedID).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) || version > current {
			traits, err := json.Marshal(snapshot.Traits)
			if err != nil {
				return err
			}
			if string(traits) == "null" {
				traits = []byte("{}")
			}
			if _, err = tx.ExecContext(ctx, "DELETE FROM lake.identity_profiles WHERE identified_id=?", snapshot.IdentifiedID); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO lake.identity_profiles VALUES(?,?,?)", snapshot.IdentifiedID, version, string(traits)); err != nil {
				return err
			}
		}
	}
	for _, link := range snapshot.Links {
		version, err := strconv.ParseUint(link.Version, 10, 64)
		if err != nil {
			return err
		}
		var current uint64
		err = tx.QueryRowContext(ctx, "SELECT version FROM lake.identity_links WHERE anonymous_id=?", link.AnonymousID).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if errors.Is(err, sql.ErrNoRows) || version > current {
			if _, err = tx.ExecContext(ctx, "DELETE FROM lake.identity_links WHERE anonymous_id=?", link.AnonymousID); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, "INSERT INTO lake.identity_links VALUES(?,?,?)", link.AnonymousID, link.IdentifiedID, version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *duckLakeStore) Load(ctx context.Context, project, uuid string) (eventProjection, error) {
	p, release, err := s.leaseProject(ctx, project)
	if err != nil {
		return eventProjection{}, err
	}
	defer release()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return eventProjection{}, err
	}
	defer tx.Rollback()
	var revision uint64
	var id int64
	var deleted bool
	var received string
	err = tx.QueryRowContext(ctx, "SELECT event_id,revision,is_deleted,received_at FROM lake.heads WHERE uuid=?", uuid).Scan(&id, &revision, &deleted, &received)
	if errors.Is(err, sql.ErrNoRows) {
		return eventProjection{}, errEventNotFound
	}
	if err != nil {
		return eventProjection{}, err
	}
	if deleted {
		return eventProjection{Event: Event{ID: id, ProjectID: project, UUID: uuid, ReceivedAt: received}, Revision: revision, IsDeleted: true}, nil
	}
	var payload, identity string
	if err = tx.QueryRowContext(ctx, "SELECT payload,identity_snapshot FROM lake.current_events WHERE uuid=?", uuid).Scan(&payload, &identity); err != nil {
		return eventProjection{}, err
	}
	var projection eventProjection
	if json.Unmarshal([]byte(payload), &projection) != nil || projection.ProjectID != project || projection.UUID != uuid || projection.ID != id || projection.Revision != revision || projection.IsDeleted {
		return eventProjection{}, errors.New("invalid stored DuckLake projection")
	}
	projection.IdentitySnapshot = identity
	return projection, nil
}
