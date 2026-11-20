package catalog

// ControlSchema creates the summary tables in the numbered control migrations.
const ControlSchema = `CREATE TABLE catalog_refresh_state (
project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
catalog_id TEXT NOT NULL DEFAULT '', snapshot_version BIGINT NOT NULL DEFAULT -1,
refreshed_at_ms BIGINT NOT NULL DEFAULT 0, eligible_at_ms BIGINT NOT NULL,
lease_token TEXT NOT NULL DEFAULT '', failures BIGINT NOT NULL DEFAULT 0,
summary_rows BIGINT NOT NULL DEFAULT 0 CHECK(summary_rows BETWEEN 0 AND 20000));
CREATE INDEX catalog_refresh_due ON catalog_refresh_state(eligible_at_ms,project_id);
CREATE TABLE catalog_summary (
project_id TEXT NOT NULL REFERENCES catalog_refresh_state(project_id) ON DELETE CASCADE,
event_name TEXT NOT NULL, first_id BIGINT NOT NULL, last_id BIGINT NOT NULL,
event_count BIGINT NOT NULL CHECK(event_count BETWEEN 1 AND 9223372036854775807),
first_received_at TEXT NOT NULL, last_received_at TEXT NOT NULL,
latest_event_uuid TEXT NOT NULL, latest_event_timestamp TEXT NOT NULL,
PRIMARY KEY(project_id,event_name));
CREATE INDEX catalog_summary_page ON catalog_summary(project_id,last_id DESC,event_name);`
