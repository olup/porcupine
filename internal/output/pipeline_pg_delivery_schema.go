package output

// PipelineDeliverySchemaV1 adds send attempts to the immutable effect jobs.
const PipelineDeliverySchemaV1 = `
ALTER TABLE pipeline_effect_jobs
 ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN completed_at TIMESTAMPTZ,
 ADD COLUMN next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 ADD COLUMN send_cut BIGINT CHECK(send_cut>=sequence),
 ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
 ADD COLUMN window_start_ms BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN window_end_ms BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN transferred_refs BIGINT NOT NULL DEFAULT 0 CHECK(transferred_refs BETWEEN 0 AND refs),
 ADD COLUMN transferred_bytes BIGINT NOT NULL DEFAULT 0 CHECK(transferred_bytes BETWEEN 0 AND bytes),
 ADD COLUMN needs_recompose BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN assembly_manifest_key TEXT NOT NULL DEFAULT '',
 ADD COLUMN assembly_manifest_version TEXT NOT NULL DEFAULT '',
 ADD COLUMN assembly_manifest_sha TEXT NOT NULL DEFAULT '',
 ADD COLUMN assembly_manifest_bytes BIGINT NOT NULL DEFAULT 0,
 ADD COLUMN fragment_spec TEXT NOT NULL DEFAULT '',
 ADD COLUMN last_status INTEGER NOT NULL DEFAULT 0,
 ADD COLUMN error_code TEXT NOT NULL DEFAULT '';
CREATE INDEX pipeline_effect_fragments_due ON pipeline_effect_jobs(source_id,window_end_ms,project_id,sequence,id)
 WHERE kind='fragment' AND state='pending' AND fragment_spec<>'';
CREATE TABLE pipeline_effect_assemblies (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 generation TEXT NOT NULL,attempt TEXT NOT NULL,cut BIGINT NOT NULL CHECK(cut>0),
 snapshot TEXT NOT NULL CHECK(octet_length(snapshot)<=524288),
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id));
CREATE INDEX pipeline_effect_deliveries_due ON pipeline_effect_jobs(source_id,next_attempt_at,project_id,id)
 WHERE kind='delivery' AND state IN ('pending','running');
CREATE INDEX pipeline_effect_deliveries_history ON pipeline_effect_jobs(project_id,pipeline_id,created_at DESC,id)
 WHERE kind='delivery';
CREATE INDEX pipeline_effect_members_by_uuid ON pipeline_effect_members(source_id,project_id,catalog_id,stream_id,uuid,job_id);
CREATE TABLE pipeline_effect_cooldowns (
 project_id TEXT NOT NULL,pipeline_id TEXT NOT NULL,destination_revision TEXT NOT NULL,
 not_before TIMESTAMPTZ NOT NULL,failures INTEGER NOT NULL CHECK(failures>0),
 PRIMARY KEY(project_id,pipeline_id));
`
