CREATE TABLE import_uploads (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    actor_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    format TEXT NOT NULL CHECK (format IN ('native','posthog','segment')),
    idempotency_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('uploading','completed','cancelled','failed')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    delivery_cursor BIGINT NOT NULL DEFAULT 0,
    UNIQUE (project_id,actor_id,idempotency_hash)
);
CREATE INDEX import_uploads_project_created ON import_uploads(project_id,created_at DESC,id DESC);
CREATE INDEX import_uploads_active ON import_uploads(state) WHERE state='uploading';

CREATE TABLE import_upload_files (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES import_uploads(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL,
    path TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('persons','events')),
    size_bytes BIGINT NOT NULL CHECK (size_bytes>=0),
    sha256 TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'uploading' CHECK (state IN ('uploading','completed','failed')),
    offset_bytes BIGINT NOT NULL DEFAULT 0 CHECK (offset_bytes>=0 AND offset_bytes<=size_bytes),
    line_number BIGINT NOT NULL DEFAULT 0,
    generation BIGINT NOT NULL DEFAULT 0,
    parser_key TEXT NOT NULL DEFAULT '',
    parser_version TEXT NOT NULL DEFAULT '',
    parser_sha256 TEXT NOT NULL DEFAULT '',
    parser_bytes BIGINT NOT NULL DEFAULT 0,
    last_offset BIGINT NOT NULL DEFAULT -1,
    last_size BIGINT NOT NULL DEFAULT 0,
    last_sha256 TEXT NOT NULL DEFAULT '',
    rows BIGINT NOT NULL DEFAULT 0,
    accepted BIGINT NOT NULL DEFAULT 0,
    duplicates BIGINT NOT NULL DEFAULT 0,
    ignored BIGINT NOT NULL DEFAULT 0,
    unresolved BIGINT NOT NULL DEFAULT 0,
    invalid BIGINT NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    UNIQUE(job_id,id),
    UNIQUE(job_id,path),
    UNIQUE(job_id,ordinal)
);

CREATE TABLE import_upload_samples (
    job_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    row_number BIGINT NOT NULL CHECK (row_number>0),
    reason TEXT NOT NULL CHECK (reason IN ('invalid_row','invalid_metadata','row_too_large','ignored_row','unresolved_identity')),
    PRIMARY KEY(job_id,file_id,row_number),
    FOREIGN KEY(job_id,file_id) REFERENCES import_upload_files(job_id,id) ON DELETE CASCADE
);
