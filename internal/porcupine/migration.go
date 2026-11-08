package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
)

const migrationPrefix = "/* porcupine:schema */ "

func (c *clickHouse) initialize(ctx context.Context) error {
	ctx, release, err := c.projectionGuard.shared(ctx)
	if err != nil {
		return err
	}
	defer release()
	if err := c.schemaMu.Lock(ctx); err != nil {
		return err
	}
	defer c.schemaMu.Unlock()
	database := c.config.Database
	if _, err := c.admin(ctx, migrationPrefix+"CREATE DATABASE IF NOT EXISTS "+database); err != nil {
		return err
	}
	if _, err := c.admin(ctx, migrationPrefix+`CREATE TABLE IF NOT EXISTS `+database+`.schema_state (name String,legacy_uuid String,version UInt64) ENGINE=ReplacingMergeTree(version) ORDER BY name SETTINGS fsync_after_insert=1,fsync_part_directory=1`); err != nil {
		return err
	}
	state, err := c.migrationState(ctx)
	if err != nil {
		return err
	}
	tables, err := c.migrationTables(ctx)
	if err != nil {
		return err
	}
	table := database + "." + eventTable
	if state != nil {
		if tables[eventTable] == "" {
			return errors.New("completed event migration table is missing")
		}
		if err = c.ensureOperatorPolicy(ctx, table); err != nil {
			return err
		}
		if err = c.applyStorage(ctx, table); err != nil {
			return err
		}
		return c.retireLegacy(ctx, tables["events"], state.LegacyUUID)
	}
	if _, err = c.admin(ctx, migrationPrefix+`CREATE TABLE IF NOT EXISTS `+table+` (
event_id UInt64,
project_id String,
uuid String,
event String,
distinct_id String,
properties String,
timestamp String,
timestamp_ms Int64,
received_at String,
received_at_ms Int64,
revision UInt64,
is_deleted UInt8,
original_snapshot String
) ENGINE = ReplacingMergeTree(revision,is_deleted)
PARTITION BY toYYYYMM(fromUnixTimestamp64Milli(received_at_ms,'UTC'))
ORDER BY (project_id,uuid)
SETTINGS fsync_after_insert=1,fsync_part_directory=1,allow_experimental_replacing_merge_with_cleanup=0`); err != nil {
		return err
	}
	if err = c.ensureOperatorPolicy(ctx, table); err != nil {
		return err
	}
	if err = c.applyStorage(ctx, table); err != nil {
		return err
	}
	legacyUUID := tables["events"]
	if legacyUUID != "" {
		// A rotated provisioning account must see every source project before
		// verification. ALL privileges do not bypass existing row policies.
		if err = c.ensureOperatorPolicy(ctx, database+".events"); err != nil {
			return err
		}
		if err = c.copyLegacy(ctx); err != nil {
			return err
		}
		if err = c.verifyLegacy(ctx); err != nil {
			return err
		}
	}
	// The marker is committed before source retirement. Its legacy table UUID
	// identifies the exact verified source across a lost acknowledgment or restart.
	if _, err = c.admin(ctx, migrationPrefix+`INSERT INTO `+database+`.schema_state SETTINGS async_insert=0 VALUES ('events_v2',unhex('`+hex.EncodeToString([]byte(legacyUUID))+`'),2)`); err != nil {
		return err
	}
	return c.retireLegacy(ctx, legacyUUID, legacyUUID)
}

func (c *clickHouse) ensureOperatorPolicy(ctx context.Context, table string) error {
	_, err := c.admin(ctx, migrationPrefix+"CREATE ROW POLICY OR REPLACE porcupine_operator ON "+table+" USING 1 TO "+quoteAnalyticsIdentifier(c.config.User))
	return err
}

type eventMigrationState struct {
	LegacyUUID string      `json:"legacy_uuid"`
	Version    json.Number `json:"version"`
}

func (c *clickHouse) migrationState(ctx context.Context) (*eventMigrationState, error) {
	data, err := c.admin(ctx, migrationPrefix+`SELECT legacy_uuid,version FROM `+c.config.Database+`.schema_state WHERE name='events_v2' ORDER BY version DESC LIMIT 1 FORMAT JSONEachRow`)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	var state eventMigrationState
	if json.Unmarshal(data, &state) != nil || state.Version != "2" {
		return nil, errors.New("invalid event migration state")
	}
	return &state, nil
}

func (c *clickHouse) migrationTables(ctx context.Context) (map[string]string, error) {
	data, err := c.admin(ctx, migrationPrefix+`SELECT name,toString(uuid) AS uuid FROM system.tables WHERE database=unhex('`+hex.EncodeToString([]byte(c.config.Database))+`') AND name IN ('events','events_v2') FORMAT JSONEachRow`)
	if err != nil {
		return nil, err
	}
	tables := map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for range 3 {
		var row struct {
			Name string `json:"name"`
			UUID string `json:"uuid"`
		}
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			return tables, nil
		}
		if err != nil || (row.Name != "events" && row.Name != eventTable) || row.UUID == "" || tables[row.Name] != "" {
			return nil, errors.New("invalid event migration table metadata")
		}
		if row.Name == "events" && row.UUID == "00000000-0000-0000-0000-000000000000" {
			return nil, errors.New("legacy event table has no stable migration identity")
		}
		tables[row.Name] = row.UUID
	}
	return nil, errors.New("invalid event migration table metadata")
}

func (c *clickHouse) retireLegacy(ctx context.Context, actualUUID, verifiedUUID string) error {
	if actualUUID == "" {
		return nil
	}
	if actualUUID != verifiedUUID {
		return errors.New("unverified legacy event table remains")
	}
	_, err := c.admin(ctx, migrationPrefix+"DROP TABLE IF EXISTS "+c.config.Database+".events SYNC")
	return err
}

func legacyOriginalSnapshotSQL() string {
	return `concat('{"event":',toJSONString(event),',"distinct_id":',toJSONString(distinct_id),',"properties":',properties,',"timestamp":',toJSONString(timestamp),'}')`
}

const projectionColumns = "event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision,is_deleted,original_snapshot"

func (c *clickHouse) copyLegacy(ctx context.Context) error {
	_, err := c.admin(ctx, migrationPrefix+"INSERT INTO "+c.config.Database+"."+eventTable+" ("+projectionColumns+") SELECT event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,toUInt64(0),toUInt8(0),"+legacyOriginalSnapshotSQL()+" FROM "+c.config.Database+".events FINAL SETTINGS async_insert=0")
	return err
}

func (c *clickHouse) verifyLegacy(ctx context.Context) error {
	base := c.config.Database + ".events"
	target := c.config.Database + "." + eventTable
	immutable := `source.event_id!=projected.event_id OR source.received_at!=projected.received_at OR source.received_at_ms!=projected.received_at_ms`
	payload := `source.event!=projected.event OR source.distinct_id!=projected.distinct_id OR source.properties!=projected.properties OR source.timestamp!=projected.timestamp OR source.timestamp_ms!=projected.timestamp_ms`
	original := `JSONExtractString(projected.original_snapshot,'event')!=source.event OR JSONExtractString(projected.original_snapshot,'distinct_id')!=source.distinct_id OR JSONExtractRaw(projected.original_snapshot,'properties')!=source.properties OR JSONExtractString(projected.original_snapshot,'timestamp')!=source.timestamp`
	queries := []string{
		`SELECT count() AS mismatches FROM (SELECT * FROM ` + base + ` FINAL) AS source LEFT JOIN (SELECT ` + projectionColumns + ` FROM ` + target + ` ORDER BY revision DESC LIMIT 1 BY project_id,uuid) AS projected USING(project_id,uuid) WHERE isNull(projected.event_id) OR ` + immutable + ` OR (projected.revision=0 AND (` + payload + `)) OR (projected.revision>0 AND projected.is_deleted=0 AND (` + original + `)) SETTINGS join_use_nulls=1 FORMAT JSONEachRow`,
		// Reverse verification also examines duplicate revision-zero copies. A
		// pre-existing conflicting original must not disappear behind a valid copy.
		`SELECT count() AS mismatches FROM (SELECT * FROM ` + target + ` WHERE revision=0) AS projected INNER JOIN (SELECT * FROM ` + base + ` FINAL) AS source USING(project_id,uuid) WHERE ` + immutable + ` OR ` + payload + ` SETTINGS join_use_nulls=1 FORMAT JSONEachRow`,
	}
	for _, query := range queries {
		data, err := c.admin(ctx, migrationPrefix+query)
		if err != nil {
			return err
		}
		var result struct {
			Mismatches json.Number `json:"mismatches"`
		}
		if json.Unmarshal(bytes.TrimSpace(data), &result) != nil {
			return errors.New("cannot verify event migration")
		}
		count, err := strconv.ParseUint(string(result.Mismatches), 10, 64)
		if err != nil || count != 0 {
			return errors.New("event migration verification failed")
		}
	}
	return nil
}
