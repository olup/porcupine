package lake

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/olup/porcupine/internal/event"
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
 identity_snapshot VARCHAR, original_snapshot VARCHAR
);
CREATE TABLE IF NOT EXISTS lake.identity_profiles (
 identified_id VARCHAR, version UBIGINT, traits VARCHAR
);
CREATE TABLE IF NOT EXISTS lake.identity_links (
 anonymous_id VARCHAR, identified_id VARCHAR, version UBIGINT
);
`

const duckLakeEventsView = `CREATE OR REPLACE VIEW events AS ` + duckLakeEventsSelect + `;`

const duckLakeEventsSelect = `SELECT e.id,e.id AS event_id,e.is_imported,e.source_id,e.message_id,e.event_type,e.context,
 e.revision,e.project_id,e.uuid,e.event,e.distinct_id,
 COALESCE(l.identified_id,e.distinct_id) AS person_id,
 COALESCE(p.traits,'{}') AS person_properties,
 e.properties,e.timestamp,e.timestamp_ms,e.event_time,e.received_at,e.received_at_ms
 FROM lake.current_events e
 LEFT JOIN lake.identity_links l ON l.anonymous_id=e.distinct_id
 LEFT JOIN lake.identity_profiles p ON p.identified_id=COALESCE(l.identified_id,e.distinct_id)
 WHERE e.identity_snapshot=''`

type PreparedLakeProjection struct {
	projection EventProjection
	timestamp  time.Time
	received   time.Time
	identity   *IdentitySnapshot
}

func PrepareLakeProjection(p EventProjection) (PreparedLakeProjection, error) {
	if p.ID <= 0 || p.ProjectID == "" || p.UUID == "" {
		return PreparedLakeProjection{}, errors.New("invalid DuckLake projection identity")
	}
	received, err := time.Parse(time.RFC3339Nano, p.ReceivedAt)
	if err != nil {
		return PreparedLakeProjection{}, errors.New("invalid DuckLake receipt time")
	}
	prepared := PreparedLakeProjection{projection: p, received: received.UTC()}
	if p.IsDeleted {
		if p.Revision == 0 {
			return PreparedLakeProjection{}, errors.New("invalid DuckLake deletion revision")
		}
		return prepared, nil
	}
	p.Event = EventWithMetadata(p.Event)
	timestamp, err := time.Parse(time.RFC3339Nano, p.Timestamp)
	if err != nil || !json.Valid(p.Properties) || !validEventContext(p.Context) || (p.OriginalSnapshot != "" && !json.Valid([]byte(p.OriginalSnapshot))) {
		return PreparedLakeProjection{}, errors.New("invalid DuckLake projection payload")
	}
	p.Timestamp = timestamp.UTC().Format(time.RFC3339Nano)
	p.ReceivedAt = received.UTC().Format(time.RFC3339Nano)
	if p.IdentitySnapshot != "" {
		var snapshot IdentitySnapshot
		if json.Unmarshal([]byte(p.IdentitySnapshot), &snapshot) != nil {
			return PreparedLakeProjection{}, errors.New("invalid DuckLake identity snapshot")
		}
		if _, err := strconv.ParseUint(snapshot.Version, 10, 64); err != nil {
			return PreparedLakeProjection{}, errors.New("invalid DuckLake identity version")
		}
		for _, link := range snapshot.Links {
			if _, err := strconv.ParseUint(link.Version, 10, 64); err != nil {
				return PreparedLakeProjection{}, errors.New("invalid DuckLake identity link version")
			}
		}
		prepared.identity = &snapshot
	}
	prepared.projection, prepared.timestamp = p, timestamp.UTC()
	return prepared, nil
}

func (s *DuckLakeStore) Apply(ctx context.Context, projections []EventProjection) error {
	groups := make(map[string][]PreparedLakeProjection)
	for _, projection := range projections {
		prepared, err := PrepareLakeProjection(projection)
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
		p, release, err := s.LeaseProject(ctx, id)
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

func (p *ProjectLake) apply(ctx context.Context, projections []PreparedLakeProjection) error {
	chunkSize, retries := len(projections), duckLakeAllocationRetries
	for offset := 0; offset < len(projections); {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(offset+chunkSize, len(projections))
		operation := func() error { return p.ApplyChunk(ctx, projections[offset:end]) }
		err := retryNativeTransaction(ctx, operation)
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

func (p *ProjectLake) ApplyChunk(ctx context.Context, projections []PreparedLakeProjection) error {
	rows := make([]lakeProjectionRow, 0, len(projections))
	controls := make(map[string]PreparedLakeProjection)
	for _, prepared := range projections {
		projection := prepared.projection
		row := prepared.row()
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
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.acquireNativeWrite(ctx, tx); err != nil {
		return err
	}
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
		var identities []IdentitySnapshot
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
 INSERT INTO lake.heads (uuid,event_id,revision,is_deleted,received_at) SELECT uuid,id,revision,is_deleted,received_at FROM _pc_changes;
 INSERT INTO lake.current_events (id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,
 received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot)
 SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,event_time,
 received_at,received_at_ms,revision,source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot
 FROM _pc_changes WHERE NOT is_deleted;
 DROP TABLE _pc_changes;
 DROP TABLE _pc_incoming;
 `); err != nil {
		return err
	}
	return tx.Commit()
}

func (prepared PreparedLakeProjection) row() lakeProjectionRow {
	projection := prepared.projection
	return lakeProjectionRow{
		UUID: projection.UUID, ID: projection.ID, ProjectID: projection.ProjectID,
		Revision: projection.Revision, Deleted: projection.IsDeleted, ReceivedAt: projection.ReceivedAt,
		Event: projection.Event.Event, DistinctID: projection.DistinctID, Properties: string(projection.Properties),
		Timestamp: projection.Timestamp, TimestampMS: prepared.timestamp.UnixMilli(), EventTime: prepared.timestamp.Format(time.RFC3339Nano),
		ReceivedAtMS: prepared.received.UnixMilli(), SourceID: projection.SourceID, MessageID: projection.MessageID,
		EventType: projection.EventType, Context: string(projection.Context), Imported: boolByte(projection.Imported),
		Identity: projection.IdentitySnapshot, OriginalSnapshot: projection.OriginalSnapshot,
	}
}

type lakeProjectionRow struct {
	UUID             string `json:"uuid"`
	ID               int64  `json:"id"`
	ProjectID        string `json:"project_id"`
	Revision         uint64 `json:"revision"`
	Deleted          bool   `json:"is_deleted"`
	Event            string `json:"event"`
	DistinctID       string `json:"distinct_id"`
	Properties       string `json:"properties"`
	Timestamp        string `json:"timestamp"`
	TimestampMS      int64  `json:"timestamp_ms"`
	EventTime        string `json:"event_time"`
	ReceivedAt       string `json:"received_at"`
	ReceivedAtMS     int64  `json:"received_at_ms"`
	SourceID         string `json:"source_id"`
	MessageID        string `json:"message_id"`
	EventType        string `json:"event_type"`
	Context          string `json:"context"`
	Imported         uint8  `json:"is_imported"`
	Identity         string `json:"identity_snapshot"`
	OriginalSnapshot string `json:"original_snapshot"`
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
 json_extract_string(value,'$.original_snapshot') AS original_snapshot
 FROM json_each(CAST(? AS JSON))`

func boolByte(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

func applyLakeIdentity(ctx context.Context, tx *sql.Tx, snapshot IdentitySnapshot) error {
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
			if _, err = tx.ExecContext(ctx, "INSERT INTO lake.identity_profiles (identified_id,version,traits) VALUES(?,?,?)", snapshot.IdentifiedID, version, string(traits)); err != nil {
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
			if _, err = tx.ExecContext(ctx, "INSERT INTO lake.identity_links (anonymous_id,identified_id,version) VALUES(?,?,?)", link.AnonymousID, link.IdentifiedID, version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *DuckLakeStore) Load(ctx context.Context, project, uuid string) (EventProjection, error) {
	p, release, err := s.LeaseProject(ctx, project)
	if err != nil {
		return EventProjection{}, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return EventProjection{}, err
	}
	defer tx.Rollback()
	return loadEventProjection(ctx, tx, project, uuid)
}

func loadEventProjection(ctx context.Context, tx *sql.Tx, project, uuid string) (EventProjection, error) {
	var revision uint64
	var id int64
	var deleted bool
	var received string
	err := tx.QueryRowContext(ctx, "SELECT event_id,revision,is_deleted,received_at FROM lake.heads WHERE uuid=?", uuid).Scan(&id, &revision, &deleted, &received)
	if errors.Is(err, sql.ErrNoRows) {
		return EventProjection{}, ErrEventNotFound
	}
	if err != nil {
		return EventProjection{}, err
	}
	if deleted {
		return EventProjection{Event: event.Event{ID: id, ProjectID: project, UUID: uuid, ReceivedAt: received}, Revision: revision, IsDeleted: true}, nil
	}
	var e event.Event
	var properties, context, original string
	var storedRevision uint64
	var imported uint8
	err = tx.QueryRowContext(ctx, `SELECT id,project_id,uuid,event,distinct_id,properties,timestamp,received_at,revision,
 source_id,message_id,event_type,context,is_imported,identity_snapshot,original_snapshot FROM lake.current_events WHERE uuid=?`, uuid).Scan(
		&e.ID, &e.ProjectID, &e.UUID, &e.Event, &e.DistinctID, &properties, &e.Timestamp, &e.ReceivedAt, &storedRevision,
		&e.SourceID, &e.MessageID, &e.EventType, &context, &imported, &e.IdentitySnapshot, &original)
	if err != nil {
		return EventProjection{}, err
	}
	if e.ProjectID != project || e.ID != id || storedRevision != revision || !json.Valid([]byte(properties)) || !json.Valid([]byte(context)) {
		return EventProjection{}, errors.New("invalid stored DuckLake projection")
	}
	e.Properties, e.Context, e.Imported = json.RawMessage(properties), json.RawMessage(context), imported == 1
	return EventProjection{Event: e, Revision: revision, OriginalSnapshot: original}, nil
}

// KnownEvents returns the event IDs of the given UUIDs already projected in a
// project, deleted events included. Acceptance uses it once local receipts
// have been pruned.
func (s *DuckLakeStore) KnownEvents(ctx context.Context, project string, uuids []string) (map[string]int64, error) {
	known := map[string]int64{}
	if len(uuids) == 0 {
		return known, nil
	}
	p, release, err := s.LeaseProject(ctx, project)
	if err != nil {
		return nil, err
	}
	defer release()
	for start := 0; start < len(uuids); start += 500 {
		chunk := uuids[start:min(start+500, len(uuids))]
		args := make([]any, len(chunk))
		for i, uuid := range chunk {
			args[i] = uuid
		}
		rows, err := p.DB.QueryContext(ctx, "SELECT uuid,event_id FROM lake.heads WHERE uuid IN (?"+strings.Repeat(",?", len(chunk)-1)+")", args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var uuid string
			var id int64
			if err := rows.Scan(&uuid, &id); err != nil {
				rows.Close()
				return nil, err
			}
			if id <= 0 || known[uuid] != 0 && known[uuid] != id {
				rows.Close()
				return nil, errors.New("invalid DuckLake event receipt")
			}
			known[uuid] = id
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
	}
	return known, nil
}

// lakeSchemaVersion 2 replaced the duplicated payload column with
// original_snapshot and partitions current events by month of occurrence.
const lakeSchemaVersion = 2

// lakeCatalogVersion reads the format before any catalog DDL. Refusing a newer
// or unknown layout must leave its persistent schema and snapshots untouched.
func lakeCatalogVersion(ctx context.Context, db *sql.DB, initializing bool) (int, error) {
	return applicationCatalogVersion(ctx, db, "lake", initializing)
}

// The caller selects one fixed catalog alias. Restore uses the same read-only
// format validation as ordinary opening before translating a supported archive.
func applicationCatalogVersion(ctx context.Context, db *sql.DB, catalog string, initializing bool) (int, error) {
	if catalog != "lake" && catalog != "archive" {
		return 0, errors.New("invalid analytical catalog alias")
	}
	rows, err := db.QueryContext(ctx, "SELECT table_name,column_name,data_type FROM duckdb_columns() WHERE database_name=? AND schema_name='main'", catalog)
	if err != nil {
		return 0, err
	}
	tables := map[string]map[string]string{}
	for rows.Next() {
		var table, name, kind string
		if err := rows.Scan(&table, &name, &kind); err != nil {
			rows.Close()
			return 0, err
		}
		if tables[table] == nil {
			tables[table] = map[string]string{}
		}
		tables[table][name] = kind
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	var foreignSchema bool
	if err := db.QueryRowContext(ctx, "SELECT count(*)>0 FROM duckdb_tables() WHERE database_name=? AND schema_name<>'main'", catalog).Scan(&foreignSchema); err != nil {
		return 0, err
	}
	if foreignSchema {
		return 0, errors.New("unknown DuckLake catalog schema")
	}
	expected := map[string]map[string]string{
		"catalog_identity": {"id": "VARCHAR"},
		"schema_version":   {"version": "INTEGER"},
		"heads":            {"uuid": "VARCHAR", "event_id": "BIGINT", "revision": "UBIGINT", "is_deleted": "BOOLEAN", "received_at": "VARCHAR"},
		"current_events": {
			"id": "BIGINT", "project_id": "VARCHAR", "uuid": "VARCHAR", "event": "VARCHAR", "distinct_id": "VARCHAR",
			"properties": "VARCHAR", "timestamp": "VARCHAR", "timestamp_ms": "BIGINT", "event_time": "TIMESTAMP WITH TIME ZONE",
			"received_at": "VARCHAR", "received_at_ms": "BIGINT", "revision": "UBIGINT", "source_id": "VARCHAR", "message_id": "VARCHAR",
			"event_type": "VARCHAR", "context": "VARCHAR", "is_imported": "UTINYINT", "identity_snapshot": "VARCHAR", "original_snapshot": "VARCHAR",
		},
		"identity_profiles":   {"identified_id": "VARCHAR", "version": "UBIGINT", "traits": "VARCHAR"},
		"identity_links":      {"anonymous_id": "VARCHAR", "identified_id": "VARCHAR", "version": "UBIGINT"},
		"project_write_guard": {"id": "INTEGER", "generation": "UBIGINT", "deleted": "BOOLEAN"},
	}
	version := 0
	if columns, exists := tables["schema_version"]; exists {
		if len(columns) != 1 || columns["version"] != "INTEGER" {
			return 0, errors.New("unknown DuckLake schema version table")
		}
		var count int
		var stored sql.NullInt64
		if err := db.QueryRowContext(ctx, "SELECT count(*),max(version) FROM "+catalog+".schema_version").Scan(&count, &stored); err != nil {
			return 0, err
		}
		if count > 1 || count == 1 && (!stored.Valid || stored.Int64 < 1) {
			return 0, errors.New("invalid DuckLake catalog schema version")
		}
		if stored.Int64 > hostedLakeSchemaVersion {
			return 0, fmt.Errorf("DuckLake catalog schema version %d is newer than this server supports (%d)", stored.Int64, hostedLakeSchemaVersion)
		}
		version = int(stored.Int64)
	}
	if version >= hostedFenceSchemaVersion {
		expected["identity_profiles"]["is_identified"] = "BOOLEAN"
		expected["hosted_write_guard"] = map[string]string{"id": "INTEGER", "source_id": "VARCHAR", "project_id": "VARCHAR", "catalog_id": "VARCHAR", "stream_id": "VARCHAR", "fence_epoch": "BIGINT", "fence_nonce": "VARCHAR", "generation": "UBIGINT", "state": "VARCHAR", "next_effect_seq": "UBIGINT", "ack_effect_seq": "UBIGINT", "sealed_effect_seq": "UBIGINT", "identity_ready": "BOOLEAN"}
		if tables["hosted_write_guard"] == nil {
			return 0, errors.New("hosted native write guard is missing")
		}
	}
	if version >= 4 {
		expected["hosted_write_guard"]["tenant_id"] = "VARCHAR"
		expected["live_commit_proofs"] = map[string]string{"proof_version": "INTEGER", "effect_seq": "BIGINT", "batch_id": "VARCHAR", "batch_fingerprint": "VARCHAR", "proof_fingerprint": "VARCHAR", "tenant_id": "VARCHAR", "fence_epoch": "BIGINT", "accounted_at": "VARCHAR", "accepted_live": "BIGINT", "plan_id": "VARCHAR", "manifest_key": "VARCHAR", "manifest_version": "VARCHAR", "manifest_sha256": "VARCHAR", "manifest_bytes": "BIGINT", "output_count": "BIGINT", "totals_json": "VARCHAR"}
		expected["live_usage_totals"] = map[string]string{"day": "VARCHAR", "revision": "BIGINT", "accepted_live": "BIGINT", "accepted_import": "BIGINT"}
		for _, table := range []string{"live_commit_proofs", "live_usage_totals"} {
			if version >= 5 {
				continue
			}
			if tables[table] == nil {
				return 0, fmt.Errorf("hosted native %s is missing", table)
			}
		}
	}
	if version >= 5 {
		delete(expected, "live_commit_proofs")
		delete(expected, "live_usage_totals")
		expected["native_usage_totals"] = map[string]string{"day": "VARCHAR", "revision": "BIGINT", "accepted_live": "BIGINT", "accepted_import": "BIGINT"}
		expected["native_commit_proofs"] = map[string]string{"proof_version": "INTEGER", "effect_seq": "BIGINT", "operation_id": "VARCHAR", "input_fingerprint": "VARCHAR", "proof_fingerprint": "VARCHAR", "kind": "VARCHAR", "evidence_json": "VARCHAR"}
		expected["import_file_heads"] = map[string]string{"job_id": "VARCHAR", "file_id": "VARCHAR", "manifest_sha256": "VARCHAR", "source_id": "VARCHAR", "format": "VARCHAR", "effect_seq": "BIGINT", "checkpoint_json": "VARCHAR"}
		expected["import_person_state"] = map[string]string{"distinct_id": "VARCHAR", "person_id": "VARCHAR", "person_version": "UBIGINT", "mapping_version": "UBIGINT", "profile_version": "UBIGINT", "deleted": "BOOLEAN", "source_id": "VARCHAR"}
		for _, table := range []string{"native_usage_totals", "native_commit_proofs", "import_file_heads", "import_person_state"} {
			if tables[table] == nil {
				return 0, fmt.Errorf("hosted native %s is missing", table)
			}
		}
	}
	if version < 2 && tables["current_events"]["payload"] == "VARCHAR" {
		delete(expected["current_events"], "original_snapshot")
		expected["current_events"]["payload"] = "VARCHAR"
	}
	for table, columns := range tables {
		want, known := expected[table]
		if !known || len(columns) != len(want) {
			return 0, fmt.Errorf("unknown DuckLake %s schema", table)
		}
		for name, kind := range want {
			if columns[name] != kind {
				return 0, fmt.Errorf("unknown DuckLake %s schema", table)
			}
		}
	}
	if initializing && version == 0 && len(tables) == 0 {
		return 0, nil
	}
	// Catalog identity commits before application tables are created. Only an
	// unfinished opening may fill missing known tables; ready catalogs must be
	// complete, even if they predate the explicit schema version.
	if tables["catalog_identity"] == nil {
		return 0, errors.New("DuckLake catalog identity is missing")
	}
	var identities, invalid int
	if err := db.QueryRowContext(ctx, "SELECT count(*),count(*) FILTER(WHERE id IS NULL OR id='') FROM "+catalog+".catalog_identity").Scan(&identities, &invalid); err != nil {
		return 0, err
	}
	if identities != 1 || invalid != 0 {
		return 0, errors.New("invalid DuckLake catalog identity")
	}
	if !initializing || version > 0 {
		for _, table := range []string{"heads", "current_events", "identity_profiles", "identity_links"} {
			if tables[table] == nil {
				return 0, fmt.Errorf("DuckLake %s table is missing", table)
			}
		}
	}
	return version, nil
}

// migrateLakeCatalog upgrades one project catalog. The column rewrite is one
// transaction; partitioning and the version marker can be replayed after a crash.
func migrateLakeCatalog(ctx context.Context, db *sql.DB) error {
	version, err := lakeCatalogVersion(ctx, db, false)
	if err != nil {
		return err
	}
	if version >= lakeSchemaVersion {
		return nil
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS lake.schema_version (version INTEGER NOT NULL)"); err != nil {
		return err
	}
	// Each step is safe to repeat if the process stops before the version is
	// recorded. DuckLake cannot repartition a table changed in the same transaction.
	var legacy bool
	if err := db.QueryRowContext(ctx, "SELECT count(*)>0 FROM duckdb_columns() WHERE database_name='lake' AND table_name='current_events' AND column_name='payload'").Scan(&legacy); err != nil {
		return err
	}
	if legacy {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, `ALTER TABLE lake.current_events ADD COLUMN IF NOT EXISTS original_snapshot VARCHAR;
 UPDATE lake.current_events SET original_snapshot=CASE WHEN revision>0 THEN coalesce(json_extract_string(payload,'$.OriginalSnapshot'),'') ELSE '' END;
 ALTER TABLE lake.current_events DROP COLUMN payload`); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE lake.current_events SET PARTITIONED BY (year(event_time), month(event_time))"); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM lake.schema_version; INSERT INTO lake.schema_version VALUES(?)", lakeSchemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}
