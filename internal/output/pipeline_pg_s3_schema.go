package output

// ScheduledPipelinePostgresSchemaV1 persists one current window per schedule,
// bounded history, and the exact objects owned by an unpublished attempt.
const ScheduledPipelinePostgresSchemaV1 = `
ALTER TABLE hosted_pipeline_configs ADD COLUMN kind TEXT NOT NULL DEFAULT 'webhook' CHECK(kind IN ('webhook','s3'));
CREATE TABLE hosted_s3_pipeline_state (
 project_id TEXT NOT NULL, pipeline_id TEXT NOT NULL, cursor_ms BIGINT NOT NULL,
 interval_ms BIGINT NOT NULL CHECK(interval_ms BETWEEN 60000 AND 86400000),
 last_attempt TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
 PRIMARY KEY(project_id,pipeline_id),
 FOREIGN KEY(project_id,pipeline_id) REFERENCES hosted_pipeline_configs(project_id,id) ON DELETE CASCADE);
CREATE TABLE hosted_s3_pipeline_runs (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL, pipeline_id TEXT NOT NULL,
 window_start_ms BIGINT NOT NULL, window_end_ms BIGINT NOT NULL,
 snapshot_catalog_id TEXT NOT NULL, snapshot_version BIGINT NOT NULL CHECK(snapshot_version>=0),
 state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','failed','canceled')),
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 claim_id TEXT NOT NULL DEFAULT '', lease_until TIMESTAMPTZ,
 attempt_id TEXT NOT NULL DEFAULT '', destination_revision TEXT NOT NULL DEFAULT '', secret BYTEA,
 manifest TEXT NOT NULL DEFAULT '', error_code TEXT NOT NULL DEFAULT '',
 created_at_ms BIGINT NOT NULL, completed_at_ms BIGINT NOT NULL DEFAULT 0,
 row_count BIGINT NOT NULL DEFAULT 0, byte_count BIGINT NOT NULL DEFAULT 0,
 reserved_bytes BIGINT NOT NULL CHECK(reserved_bytes>0),
 UNIQUE(project_id,pipeline_id,window_start_ms),
 FOREIGN KEY(project_id,pipeline_id) REFERENCES hosted_s3_pipeline_state(project_id,pipeline_id) ON DELETE CASCADE);
CREATE INDEX hosted_s3_pipeline_snapshots ON hosted_s3_pipeline_runs(project_id,snapshot_catalog_id,snapshot_version) WHERE state IN ('queued','running','failed');
CREATE INDEX hosted_s3_pipeline_cleanup ON hosted_s3_pipeline_runs(next_attempt,id) WHERE state='canceled' AND attempt_id<>'';
CREATE TABLE hosted_s3_pipeline_files (
 run_id TEXT NOT NULL REFERENCES hosted_s3_pipeline_runs(id) ON DELETE CASCADE,
 object_key TEXT NOT NULL, upload_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(run_id,object_key));
`
