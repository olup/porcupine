package output

// PipelineSchemaV1 is appended as control migration 8. Only numeric metadata,
// authority revisions, immutable object identities and bounded UUIDs live here.
const PipelineSchemaV1 = `
CREATE TABLE pipeline_capacity (
 scope_kind TEXT NOT NULL CHECK(scope_kind IN ('instance','tenant','project')), scope_id TEXT NOT NULL,
 limit_refs BIGINT NOT NULL CHECK(limit_refs>0), limit_bytes BIGINT NOT NULL CHECK(limit_bytes>0),
 limit_rows BIGINT NOT NULL CHECK(limit_rows>0), limit_artifacts BIGINT NOT NULL CHECK(limit_artifacts>0),limit_proofs BIGINT NOT NULL CHECK(limit_proofs>0),
 used_refs BIGINT NOT NULL DEFAULT 0 CHECK(used_refs>=0),used_bytes BIGINT NOT NULL DEFAULT 0 CHECK(used_bytes>=0),
 used_rows BIGINT NOT NULL DEFAULT 0 CHECK(used_rows>=0),used_artifacts BIGINT NOT NULL DEFAULT 0 CHECK(used_artifacts>=0),used_proofs BIGINT NOT NULL DEFAULT 0 CHECK(used_proofs>=0),
 PRIMARY KEY(scope_kind,scope_id));
CREATE TABLE pipeline_effect_definitions (
 tenant_id TEXT NOT NULL,project_id TEXT NOT NULL,id TEXT NOT NULL,incarnation TEXT NOT NULL,rule_revision TEXT NOT NULL,destination_revision TEXT NOT NULL,creator_id TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','paused','retired')),
 rule_key TEXT NOT NULL,rule_version TEXT NOT NULL,rule_sha TEXT NOT NULL,rule_bytes BIGINT NOT NULL CHECK(rule_bytes>0),
 PRIMARY KEY(project_id,id));
CREATE TABLE pipeline_effect_plans (
 id TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL);
CREATE TABLE pipeline_effect_targets (
 plan_id TEXT NOT NULL REFERENCES pipeline_effect_plans(id) ON DELETE CASCADE,id TEXT NOT NULL,incarnation TEXT NOT NULL,rule_revision TEXT NOT NULL,destination_revision TEXT NOT NULL,creator_id TEXT NOT NULL,
 rule_key TEXT NOT NULL,rule_version TEXT NOT NULL,rule_sha TEXT NOT NULL,rule_bytes BIGINT NOT NULL,PRIMARY KEY(plan_id,id));
CREATE TABLE pipeline_effect_streams (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 sequence BIGINT NOT NULL DEFAULT 0 CHECK(sequence>=0),proof_sha TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id));
CREATE TABLE pipeline_effect_reservations (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 batch_id TEXT NOT NULL,artifact_prefix TEXT NOT NULL,binding_sha TEXT NOT NULL,epoch BIGINT NOT NULL,acquisition_id TEXT NOT NULL,plan_id TEXT NOT NULL REFERENCES pipeline_effect_plans(id),prepared_sha TEXT NOT NULL,
 plan_key TEXT NOT NULL,plan_version TEXT NOT NULL,plan_sha TEXT NOT NULL,plan_bytes BIGINT NOT NULL,manifest_bytes_limit BIGINT NOT NULL CHECK(manifest_bytes_limit>=0),
 refs BIGINT NOT NULL,bytes BIGINT NOT NULL,rows BIGINT NOT NULL,artifacts BIGINT NOT NULL,proofs BIGINT NOT NULL CHECK(proofs=1),checkpoint_rows BIGINT NOT NULL CHECK(checkpoint_rows IN (0,1)),
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id),
 FOREIGN KEY(source_id,project_id,catalog_id,stream_id) REFERENCES pipeline_effect_streams(source_id,project_id,catalog_id,stream_id));
CREATE TABLE pipeline_effect_jobs (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 id TEXT NOT NULL,batch_id TEXT NOT NULL,pipeline_id TEXT NOT NULL,incarnation TEXT NOT NULL,rule_revision TEXT NOT NULL,destination_revision TEXT NOT NULL,
 creator_id TEXT NOT NULL,generation TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('delivery','fragment')),sequence BIGINT NOT NULL,
 body_key TEXT NOT NULL,body_version TEXT NOT NULL,body_sha TEXT NOT NULL,body_bytes BIGINT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('pending','running','retired','succeeded')),attempt TEXT NOT NULL DEFAULT '',version BIGINT NOT NULL DEFAULT 0,lease_until TIMESTAMPTZ,
 next_member BIGINT NOT NULL DEFAULT 0,refs BIGINT NOT NULL,bytes BIGINT NOT NULL,rows BIGINT NOT NULL,artifacts BIGINT NOT NULL,
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id,id));
CREATE TABLE pipeline_effect_members (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,job_id TEXT NOT NULL,position BIGINT NOT NULL,uuid TEXT NOT NULL,encoded_bytes BIGINT NOT NULL CHECK(encoded_bytes>0),
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id,job_id,position),
 FOREIGN KEY(source_id,project_id,catalog_id,stream_id,job_id) REFERENCES pipeline_effect_jobs(source_id,project_id,catalog_id,stream_id,id) ON DELETE CASCADE);
CREATE TABLE pipeline_effect_cleanup (
 source_id TEXT NOT NULL,project_id TEXT NOT NULL,catalog_id TEXT NOT NULL,stream_id TEXT NOT NULL,tenant_id TEXT NOT NULL,
 owner_id TEXT NOT NULL,generation TEXT NOT NULL,prefix TEXT NOT NULL,attempt TEXT NOT NULL DEFAULT '',version BIGINT NOT NULL DEFAULT 0,lease_until TIMESTAMPTZ,
 refs BIGINT NOT NULL,bytes BIGINT NOT NULL,rows BIGINT NOT NULL,artifacts BIGINT NOT NULL,
 PRIMARY KEY(source_id,project_id,catalog_id,stream_id,owner_id));
`
