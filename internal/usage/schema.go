package usage

const RollupSchema = `CREATE TABLE usage_rollups (
source_id TEXT NOT NULL, tenant_id TEXT NOT NULL, project_id TEXT NOT NULL, day_start TEXT NOT NULL,
revision BIGINT NOT NULL CHECK(revision>0),
live_events BIGINT NOT NULL DEFAULT 0 CHECK(live_events>=0),import_events BIGINT NOT NULL DEFAULT 0 CHECK(import_events>=0),
query_count BIGINT NOT NULL DEFAULT 0 CHECK(query_count>=0),query_micros BIGINT NOT NULL DEFAULT 0 CHECK(query_micros>=0),
export_bytes BIGINT NOT NULL DEFAULT 0 CHECK(export_bytes>=0),pipeline_bytes BIGINT NOT NULL DEFAULT 0 CHECK(pipeline_bytes>=0),webhook_success BIGINT NOT NULL DEFAULT 0 CHECK(webhook_success>=0),
unresolved_queries BIGINT NOT NULL DEFAULT 0 CHECK(unresolved_queries>=0),last_observed_at TEXT NOT NULL DEFAULT '',published_revision BIGINT NOT NULL DEFAULT 0,
PRIMARY KEY(source_id,tenant_id,project_id,day_start));
CREATE INDEX usage_rollups_period ON usage_rollups(day_start,tenant_id,project_id);`

const JournalSchema = RollupSchema + `
CREATE TABLE usage_executions (
id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,day_start TEXT NOT NULL,operation TEXT NOT NULL CHECK(operation IN ('sql','catalog','profile')),
elapsed_us BIGINT NOT NULL DEFAULT 0 CHECK(elapsed_us>=0),closed BIGINT NOT NULL DEFAULT 0 CHECK(closed IN (0,1)),lost BIGINT NOT NULL DEFAULT 0 CHECK(lost IN (0,1)),last_observed_at TEXT NOT NULL);
CREATE INDEX usage_executions_open ON usage_executions(closed,day_start);`

const ControlSchema = RollupSchema + `
CREATE TABLE usage_storage_samples (tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,day_start TEXT NOT NULL,observed_at TEXT NOT NULL,bytes BIGINT NOT NULL CHECK(bytes>=0),PRIMARY KEY(tenant_id,project_id,day_start));
CREATE TABLE usage_snapshots (id TEXT PRIMARY KEY,epoch TEXT NOT NULL,query_hash TEXT NOT NULL,query_json TEXT NOT NULL,created_at TEXT NOT NULL,expires_at TEXT NOT NULL,period_start TEXT NOT NULL,total_bytes BIGINT NOT NULL,page_count BIGINT NOT NULL,limit_rows BIGINT NOT NULL);
CREATE INDEX usage_snapshots_expiry ON usage_snapshots(expires_at);
CREATE TABLE usage_snapshot_pages (snapshot_id TEXT NOT NULL REFERENCES usage_snapshots(id) ON DELETE CASCADE,page_index BIGINT NOT NULL,payload TEXT NOT NULL,PRIMARY KEY(snapshot_id,page_index));` + SnapshotBuildSchema

const SnapshotBuildSchema = `CREATE TABLE usage_snapshot_build(id TEXT PRIMARY KEY,token TEXT NOT NULL,reserved_bytes BIGINT NOT NULL,expires_at TEXT NOT NULL);`
