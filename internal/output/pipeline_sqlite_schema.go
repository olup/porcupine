package output

// PipelineSQLiteRetirementSchemaV1 is journal migration 8. Retired ownership has
// no foreign key to live project, actor or pipeline rows.
const PipelineSQLiteRetirementSchemaV1 = `
ALTER TABLE pipeline_s3_runs ADD COLUMN claim_id TEXT NOT NULL DEFAULT '';
ALTER TABLE webhook_deliveries ADD COLUMN attempt_id TEXT NOT NULL DEFAULT '';
CREATE TABLE pipeline_retired_destinations(
 id TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL,project_id TEXT NOT NULL,
 config TEXT NOT NULL,secret BLOB NOT NULL,window_start_ms INTEGER NOT NULL,
 manifest TEXT NOT NULL,attempt_id TEXT NOT NULL DEFAULT '',created_at_ms INTEGER NOT NULL,next_attempt_ms INTEGER NOT NULL DEFAULT 0);
CREATE INDEX pipeline_retired_project ON pipeline_retired_destinations(project_id,id);
CREATE TABLE pipeline_retired_files(
 destination_id TEXT NOT NULL REFERENCES pipeline_retired_destinations(id) ON DELETE CASCADE,
 object_key TEXT NOT NULL,upload_id TEXT NOT NULL,
 PRIMARY KEY(destination_id,object_key)) WITHOUT ROWID;`
