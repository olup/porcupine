-- Journal schema version 1: accepted events, identity, imports and delivery state.
-- Applied once to an empty database. Later changes are new numbered steps in
-- schema_migrations.go; never edit a released version.

CREATE TABLE access_keys (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL, permissions TEXT NOT NULL, projects TEXT NOT NULL, revoked_at TEXT, created_at TEXT NOT NULL);

CREATE TABLE cookieless_keys (day TEXT PRIMARY KEY, secret BLOB NOT NULL CHECK(length(secret)=32));

CREATE TABLE delivery_quarantine (
event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
error_code INTEGER NOT NULL CHECK(error_code IN (321,469)),quarantined_at TEXT NOT NULL);

CREATE TABLE delivery_ready (event_id INTEGER PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE);

CREATE TABLE event_heads (
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
revision INTEGER NOT NULL, is_deleted INTEGER NOT NULL, pending INTEGER NOT NULL, event_id INTEGER NOT NULL DEFAULT 0,
PRIMARY KEY(project_id,uuid)) WITHOUT ROWID;

CREATE TABLE event_receipts (project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL, event_id INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(project_id,uuid)) WITHOUT ROWID;

CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL, event TEXT NOT NULL, distinct_id TEXT NOT NULL, properties TEXT NOT NULL, timestamp TEXT NOT NULL, received_at TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, ingestion_metadata TEXT NOT NULL DEFAULT '', identity_snapshot TEXT NOT NULL DEFAULT '', is_imported INTEGER NOT NULL DEFAULT 0, UNIQUE(project_id,uuid));

CREATE TABLE export_files (
job_id TEXT NOT NULL REFERENCES export_jobs(id), attempt_id TEXT NOT NULL, file_index INTEGER NOT NULL,
object_key TEXT NOT NULL, upload_id TEXT NOT NULL DEFAULT '', complete INTEGER NOT NULL DEFAULT 0,
rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0, decoded_bytes INTEGER NOT NULL DEFAULT 0,
sha256 TEXT NOT NULL DEFAULT '', PRIMARY KEY(job_id,attempt_id,file_index)) WITHOUT ROWID;

CREATE TABLE export_jobs (
id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), actor_id TEXT NOT NULL REFERENCES access_keys(id),
sql TEXT NOT NULL, parameters TEXT NOT NULL, format TEXT NOT NULL,
idempotency_hash TEXT NOT NULL, request_hash TEXT NOT NULL, state TEXT NOT NULL,
attempt_id TEXT NOT NULL DEFAULT '',
event_watermark INTEGER NOT NULL, mutation_watermark INTEGER NOT NULL,
cancel_requested INTEGER NOT NULL DEFAULT 0,
rows INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0, decoded_bytes INTEGER NOT NULL DEFAULT 0,
file_count INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
created_at TEXT NOT NULL, started_at TEXT NOT NULL DEFAULT '', completed_at TEXT NOT NULL DEFAULT '',
expires_at TEXT NOT NULL DEFAULT '', expires_at_ms INTEGER NOT NULL DEFAULT 0,
error TEXT NOT NULL DEFAULT '', columns_json TEXT NOT NULL DEFAULT '[]', cleanup_pending INTEGER NOT NULL DEFAULT 0,
UNIQUE(project_id,actor_id,idempotency_hash));

CREATE TABLE identity_links (
project_id TEXT NOT NULL REFERENCES projects(id),anonymous_id TEXT NOT NULL,identified_id TEXT NOT NULL,
version INTEGER NOT NULL CHECK(version>0),PRIMARY KEY(project_id,anonymous_id),
FOREIGN KEY(project_id,identified_id) REFERENCES identity_profiles(project_id,identified_id)) WITHOUT ROWID;

CREATE TABLE identity_profiles (
project_id TEXT NOT NULL REFERENCES projects(id),identified_id TEXT NOT NULL,
is_identified INTEGER NOT NULL CHECK(is_identified IN (0,1)),
traits TEXT NOT NULL,version INTEGER NOT NULL CHECK(version>0),
PRIMARY KEY(project_id,identified_id)) WITHOUT ROWID;

CREATE TABLE import_upload_files (
id TEXT PRIMARY KEY,job_id TEXT NOT NULL REFERENCES import_uploads(id),ordinal INTEGER NOT NULL,path TEXT NOT NULL,kind TEXT NOT NULL,
size_bytes INTEGER NOT NULL,offset_bytes INTEGER NOT NULL DEFAULT 0,sha256 TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'uploading',
line_number INTEGER NOT NULL DEFAULT 0,tail BLOB NOT NULL DEFAULT X'',discarding INTEGER NOT NULL DEFAULT 0,hash_state BLOB NOT NULL,
last_offset INTEGER NOT NULL DEFAULT -1,last_size INTEGER NOT NULL DEFAULT 0,last_sha256 TEXT NOT NULL DEFAULT '',
rows INTEGER NOT NULL DEFAULT 0,accepted INTEGER NOT NULL DEFAULT 0,duplicates INTEGER NOT NULL DEFAULT 0,
ignored INTEGER NOT NULL DEFAULT 0,unresolved INTEGER NOT NULL DEFAULT 0,invalid INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',
UNIQUE(job_id,path),UNIQUE(job_id,ordinal));

CREATE TABLE import_upload_samples (
job_id TEXT NOT NULL REFERENCES import_uploads(id),file_id TEXT NOT NULL REFERENCES import_upload_files(id),row_number INTEGER NOT NULL,reason TEXT NOT NULL,
PRIMARY KEY(job_id,file_id,row_number)) WITHOUT ROWID;

CREATE TABLE import_uploads (
id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),actor_id TEXT NOT NULL REFERENCES access_keys(id),source_id TEXT NOT NULL DEFAULT 'posthog',format TEXT NOT NULL DEFAULT 'posthog',delivery_cursor INTEGER NOT NULL DEFAULT 0,
idempotency_hash TEXT NOT NULL,request_hash TEXT NOT NULL,state TEXT NOT NULL,
created_at TEXT NOT NULL,updated_at TEXT NOT NULL,completed_at TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
UNIQUE(project_id,actor_id,idempotency_hash));

CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE mutation_audit (
seq INTEGER PRIMARY KEY AUTOINCREMENT, mutation_id TEXT NOT NULL UNIQUE,
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
actor_id TEXT NOT NULL, operation TEXT NOT NULL, idempotency_key_hash TEXT NOT NULL,
request_hash TEXT NOT NULL, expected_revision INTEGER NOT NULL,
state TEXT NOT NULL, created_at TEXT NOT NULL, applied_at TEXT NOT NULL DEFAULT '',
UNIQUE(project_id,idempotency_key_hash));

CREATE TABLE mutation_outbox (
seq INTEGER PRIMARY KEY REFERENCES mutation_audit(seq),
project_id TEXT NOT NULL REFERENCES projects(id), uuid TEXT NOT NULL,
payload TEXT NOT NULL, UNIQUE(project_id,uuid));

CREATE TABLE pipeline_s3_files(
 run_id TEXT NOT NULL REFERENCES pipeline_s3_runs(id) ON DELETE CASCADE,object_key TEXT NOT NULL,upload_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(run_id,object_key));

CREATE TABLE pipeline_s3_runs(
 id TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
 window_start_ms INTEGER NOT NULL,window_end_ms INTEGER NOT NULL,state TEXT NOT NULL,
 attempts INTEGER NOT NULL DEFAULT 0,created_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL DEFAULT 0,
 next_attempt_ms INTEGER NOT NULL DEFAULT 0,error_code TEXT NOT NULL DEFAULT '',attempt_id TEXT NOT NULL DEFAULT '',
 event_watermark INTEGER NOT NULL DEFAULT 0,mutation_watermark INTEGER NOT NULL DEFAULT 0,
 manifest TEXT NOT NULL DEFAULT '',row_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,UNIQUE(pipeline_id,window_start_ms));

CREATE TABLE pipeline_s3_state(
 pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,cursor_ms INTEGER NOT NULL,last_attempt_ms INTEGER NOT NULL DEFAULT 0);

CREATE TABLE pipelines (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), actor_id TEXT NOT NULL REFERENCES access_keys(id),
 kind TEXT NOT NULL CHECK(kind IN ('webhook','s3')), name TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','paused')), config TEXT NOT NULL, secret BLOB NOT NULL, created_at_ms INTEGER NOT NULL);

CREATE TABLE posthog_import_person_state(
 project_id TEXT NOT NULL REFERENCES projects(id),distinct_id TEXT NOT NULL,person_id TEXT NOT NULL,source_id TEXT NOT NULL DEFAULT 'posthog',
 person_version TEXT NOT NULL,mapping_version TEXT NOT NULL,profile_version INTEGER NOT NULL,
 deleted INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(project_id,distinct_id)) WITHOUT ROWID;

CREATE TABLE project_deletions (project_id TEXT PRIMARY KEY, completed INTEGER NOT NULL CHECK(completed IN (0,1)));

CREATE TABLE projects (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id), name TEXT NOT NULL, created_at TEXT NOT NULL, ingestion_mode TEXT NOT NULL DEFAULT 'mixed' CHECK(ingestion_mode IN ('mixed','server_only')));

CREATE TABLE queue_usage (
id INTEGER PRIMARY KEY CHECK(id=1),
pending_count INTEGER NOT NULL CHECK(pending_count>=0),
pending_bytes INTEGER NOT NULL CHECK(pending_bytes>=0)
);

CREATE TABLE tenant_usage (
tenant_id TEXT NOT NULL REFERENCES tenants(id),period_start TEXT NOT NULL,
accepted_events INTEGER NOT NULL CHECK(accepted_events>=0),
PRIMARY KEY(tenant_id,period_start)) WITHOUT ROWID;

CREATE TABLE tenants (
id TEXT PRIMARY KEY,name TEXT NOT NULL,created_at TEXT NOT NULL,
ingestion_allowed INTEGER NOT NULL DEFAULT 1 CHECK(ingestion_allowed IN (0,1)),
ingestion_monthly_events INTEGER CHECK(ingestion_monthly_events IS NULL OR ingestion_monthly_events>=0),
hot_query_max_concurrent INTEGER NOT NULL CHECK(hot_query_max_concurrent BETWEEN 1 AND 64),
hot_query_timeout_ms INTEGER NOT NULL CHECK(hot_query_timeout_ms BETWEEN 1 AND 60000),
hot_query_max_rows INTEGER NOT NULL CHECK(hot_query_max_rows BETWEEN 1 AND 100000),
hot_query_max_result_bytes INTEGER NOT NULL CHECK(hot_query_max_result_bytes BETWEEN 256 AND 67108864));

CREATE TABLE webhook_cooldowns(pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,not_before_ms INTEGER NOT NULL,failures INTEGER NOT NULL);

CREATE TABLE webhook_deliveries (
 id TEXT PRIMARY KEY,pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
 project_id TEXT NOT NULL,event_uuid TEXT NOT NULL,payload TEXT NOT NULL,bytes INTEGER NOT NULL,
 frozen INTEGER NOT NULL DEFAULT 0,event_count INTEGER NOT NULL DEFAULT 1,window_start_ms INTEGER NOT NULL DEFAULT 0,window_end_ms INTEGER NOT NULL DEFAULT 0,
 state TEXT NOT NULL CHECK(state IN ('queued','sending','delivered','failed','canceled')),
 attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL,last_status INTEGER NOT NULL DEFAULT 0,
 error_code TEXT NOT NULL DEFAULT '',created_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL DEFAULT 0,
 UNIQUE(pipeline_id,event_uuid));

CREATE TABLE webhook_members(project_id TEXT NOT NULL,pipeline_id TEXT NOT NULL,event_uuid TEXT NOT NULL,delivery_id TEXT NOT NULL REFERENCES webhook_deliveries(id) ON DELETE CASCADE,PRIMARY KEY(pipeline_id,event_uuid)) WITHOUT ROWID;

CREATE TABLE webhook_queue_usage(scope TEXT PRIMARY KEY,count INTEGER NOT NULL,bytes INTEGER NOT NULL) WITHOUT ROWID;

CREATE TABLE shared_tenant_cache (
tenant_id TEXT NOT NULL REFERENCES tenants(id),period_start TEXT NOT NULL,
global_accepted INTEGER NOT NULL CHECK(global_accepted>=0),
own_published INTEGER NOT NULL CHECK(own_published>=0),refreshed_at_ms INTEGER NOT NULL,
PRIMARY KEY(tenant_id,period_start)) WITHOUT ROWID;

CREATE INDEX events_pending ON events(delivered,id);

CREATE INDEX events_project_id ON events(project_id,id);

CREATE INDEX export_jobs_expiration ON export_jobs(state,expires_at_ms);

CREATE INDEX export_jobs_pending ON export_jobs(state,cleanup_pending);

CREATE INDEX identity_links_target ON identity_links(project_id,identified_id);

CREATE INDEX import_upload_files_order ON import_upload_files(job_id,ordinal);

CREATE INDEX import_uploads_active ON import_uploads(state);

CREATE INDEX import_uploads_project ON import_uploads(project_id,created_at,id);

CREATE INDEX mutation_audit_project_sequence ON mutation_audit(project_id,seq);

CREATE INDEX pipelines_project ON pipelines(project_id,created_at_ms,id);

CREATE INDEX projects_tenant ON projects(tenant_id,id);

CREATE INDEX webhook_appendable ON webhook_deliveries(pipeline_id,window_end_ms,created_at_ms DESC,id DESC) WHERE state='queued' AND frozen=0 AND event_count<100 AND bytes<1048576;

CREATE INDEX webhook_chunk ON webhook_deliveries(pipeline_id,state,window_end_ms,attempts,created_at_ms);

CREATE INDEX webhook_completed ON webhook_deliveries(completed_at_ms);

CREATE INDEX webhook_due ON webhook_deliveries(state,next_attempt_ms,created_at_ms);

CREATE INDEX webhook_project_completed ON webhook_deliveries(project_id,completed_at_ms,id);

CREATE INDEX webhook_project_event ON webhook_deliveries(project_id,event_uuid);

CREATE TRIGGER delivery_quarantine_insert AFTER INSERT ON delivery_quarantine BEGIN
DELETE FROM delivery_ready WHERE event_id=NEW.event_id;
END;

CREATE TRIGGER delivery_quarantine_retry AFTER DELETE ON delivery_quarantine BEGIN
INSERT OR IGNORE INTO delivery_ready(event_id) SELECT id FROM events WHERE id=OLD.event_id AND delivered=0;
END;

CREATE TRIGGER delivery_ready_insert AFTER INSERT ON events WHEN NEW.delivered=0 BEGIN
INSERT OR IGNORE INTO delivery_ready(event_id) VALUES(NEW.id);
END;

CREATE TRIGGER delivery_ready_update AFTER UPDATE OF delivered ON events BEGIN
DELETE FROM delivery_ready WHERE event_id=NEW.id;
INSERT OR IGNORE INTO delivery_ready(event_id) SELECT NEW.id WHERE NEW.delivered=0 AND NOT EXISTS(SELECT 1 FROM delivery_quarantine WHERE event_id=NEW.id);
END;

CREATE TRIGGER queue_usage_delete AFTER DELETE ON events WHEN OLD.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count-1,pending_bytes=pending_bytes-(64+length(CAST(OLD.project_id AS BLOB))+length(CAST(OLD.uuid AS BLOB))+length(CAST(OLD.event AS BLOB))+length(CAST(OLD.distinct_id AS BLOB))+length(CAST(OLD.properties AS BLOB))+length(CAST(OLD.timestamp AS BLOB))+length(CAST(OLD.received_at AS BLOB))+length(CAST(OLD.identity_snapshot AS BLOB))+length(CAST(OLD.ingestion_metadata AS BLOB))) WHERE id=1;
END;

CREATE TRIGGER queue_usage_insert AFTER INSERT ON events WHEN NEW.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count+1,pending_bytes=pending_bytes+(64+length(CAST(NEW.project_id AS BLOB))+length(CAST(NEW.uuid AS BLOB))+length(CAST(NEW.event AS BLOB))+length(CAST(NEW.distinct_id AS BLOB))+length(CAST(NEW.properties AS BLOB))+length(CAST(NEW.timestamp AS BLOB))+length(CAST(NEW.received_at AS BLOB))+length(CAST(NEW.identity_snapshot AS BLOB))+length(CAST(NEW.ingestion_metadata AS BLOB))) WHERE id=1;
END;

CREATE TRIGGER queue_usage_update AFTER UPDATE ON events WHEN OLD.delivered=0 OR NEW.delivered=0 BEGIN
UPDATE queue_usage SET pending_count=pending_count+(NEW.delivered=0)-(OLD.delivered=0),
pending_bytes=pending_bytes+CASE WHEN NEW.delivered=0 THEN (64+length(CAST(NEW.project_id AS BLOB))+length(CAST(NEW.uuid AS BLOB))+length(CAST(NEW.event AS BLOB))+length(CAST(NEW.distinct_id AS BLOB))+length(CAST(NEW.properties AS BLOB))+length(CAST(NEW.timestamp AS BLOB))+length(CAST(NEW.received_at AS BLOB))+length(CAST(NEW.identity_snapshot AS BLOB))+length(CAST(NEW.ingestion_metadata AS BLOB))) ELSE 0 END-CASE WHEN OLD.delivered=0 THEN (64+length(CAST(OLD.project_id AS BLOB))+length(CAST(OLD.uuid AS BLOB))+length(CAST(OLD.event AS BLOB))+length(CAST(OLD.distinct_id AS BLOB))+length(CAST(OLD.properties AS BLOB))+length(CAST(OLD.timestamp AS BLOB))+length(CAST(OLD.received_at AS BLOB))+length(CAST(OLD.identity_snapshot AS BLOB))+length(CAST(OLD.ingestion_metadata AS BLOB))) ELSE 0 END WHERE id=1;
END;

CREATE TRIGGER webhook_history_change AFTER UPDATE OF payload ON webhook_deliveries WHEN OLD.payload<>'' AND NEW.payload='' BEGIN UPDATE metadata SET value=CAST(value AS INTEGER)+1 WHERE key='webhook_history_changes'; END;

CREATE TRIGGER webhook_usage_delete AFTER DELETE ON webhook_deliveries WHEN OLD.payload<>'' BEGIN
 UPDATE webhook_queue_usage SET count=count-OLD.event_count,bytes=bytes-OLD.bytes WHERE scope IN ('instance','project:'||OLD.project_id); END;

CREATE TRIGGER webhook_usage_insert AFTER INSERT ON webhook_deliveries WHEN NEW.payload<>'' BEGIN
 INSERT INTO webhook_queue_usage(scope,count,bytes) VALUES('project:'||NEW.project_id,NEW.event_count,NEW.bytes) ON CONFLICT(scope) DO UPDATE SET count=count+NEW.event_count,bytes=bytes+NEW.bytes;
 UPDATE webhook_queue_usage SET count=count+NEW.event_count,bytes=bytes+NEW.bytes WHERE scope='instance'; END;

CREATE TRIGGER webhook_usage_update AFTER UPDATE OF payload,bytes,event_count ON webhook_deliveries BEGIN
 INSERT OR IGNORE INTO webhook_queue_usage(scope,count,bytes) VALUES('project:'||NEW.project_id,0,0);
 UPDATE webhook_queue_usage SET count=count+CASE WHEN NEW.payload<>'' THEN NEW.event_count ELSE 0 END-CASE WHEN OLD.payload<>'' THEN OLD.event_count ELSE 0 END,bytes=bytes+NEW.bytes-OLD.bytes WHERE scope IN ('instance','project:'||NEW.project_id); END;
