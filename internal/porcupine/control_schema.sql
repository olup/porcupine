-- Control metadata and authentication only. Event payloads, identity state and jobs
-- stay in each project owner's durable SQLite database.

CREATE TABLE IF NOT EXISTS tenants (
id TEXT PRIMARY KEY,name TEXT NOT NULL,created_at TEXT NOT NULL,
ingestion_allowed BIGINT NOT NULL DEFAULT 1 CHECK(ingestion_allowed IN (0,1)),
ingestion_monthly_events BIGINT CHECK(ingestion_monthly_events IS NULL OR ingestion_monthly_events>=0),
hot_query_max_concurrent BIGINT NOT NULL CHECK(hot_query_max_concurrent BETWEEN 1 AND 64),
hot_query_timeout_ms BIGINT NOT NULL CHECK(hot_query_timeout_ms BETWEEN 1 AND 60000),
hot_query_max_rows BIGINT NOT NULL CHECK(hot_query_max_rows BETWEEN 1 AND 100000),
hot_query_max_result_bytes BIGINT NOT NULL CHECK(hot_query_max_result_bytes BETWEEN 256 AND 67108864));

CREATE TABLE IF NOT EXISTS tenant_usage (
tenant_id TEXT NOT NULL REFERENCES tenants(id),period_start TEXT NOT NULL,
accepted_events BIGINT NOT NULL CHECK(accepted_events>=0),
PRIMARY KEY(tenant_id,period_start));

CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id), name TEXT NOT NULL, created_at TEXT NOT NULL, ingestion_mode TEXT NOT NULL DEFAULT 'mixed' CHECK(ingestion_mode IN ('mixed','server_only')));

CREATE INDEX IF NOT EXISTS projects_tenant ON projects(tenant_id,id);

CREATE TABLE IF NOT EXISTS project_deletions (project_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL DEFAULT '', completed BIGINT NOT NULL CHECK(completed IN (0,1)));

CREATE TABLE IF NOT EXISTS access_keys (id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL, permissions TEXT NOT NULL, projects TEXT NOT NULL, revoked_at TEXT, created_at TEXT NOT NULL, source_id TEXT NOT NULL DEFAULT '');

CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS cookieless_keys (day TEXT PRIMARY KEY, secret BYTEA NOT NULL CHECK(length(secret)=32));

CREATE TABLE IF NOT EXISTS oauth_clients (
client_id TEXT PRIMARY KEY REFERENCES access_keys(id),
secret_hash TEXT NOT NULL
, redirect_uris TEXT NOT NULL DEFAULT '[]', token_endpoint_auth_method TEXT NOT NULL DEFAULT 'client_secret_basic', grant_types TEXT NOT NULL DEFAULT '["client_credentials"]');

CREATE TABLE IF NOT EXISTS service_api_keys(
		principal_id TEXT PRIMARY KEY REFERENCES access_keys(id),
		client_id TEXT NOT NULL REFERENCES oauth_clients(client_id),
		audience TEXT NOT NULL CHECK(audience IN ('api','mcp'))
	);

CREATE INDEX IF NOT EXISTS service_api_keys_by_client ON service_api_keys(client_id);

CREATE TABLE IF NOT EXISTS oauth_tokens (
token_hash TEXT PRIMARY KEY,client_id TEXT NOT NULL REFERENCES access_keys(id),resource TEXT NOT NULL,scope TEXT NOT NULL,projects_json TEXT NOT NULL,
issued_at_ms BIGINT NOT NULL,expires_at_ms BIGINT NOT NULL, interactive_grant_id TEXT NOT NULL DEFAULT '');

CREATE INDEX IF NOT EXISTS oauth_tokens_client ON oauth_tokens(client_id);

CREATE INDEX IF NOT EXISTS oauth_tokens_expiry ON oauth_tokens(expires_at_ms);

CREATE TABLE IF NOT EXISTS oauth_authorization_requests(
 request_hash TEXT PRIMARY KEY,client_id TEXT NOT NULL REFERENCES access_keys(id),redirect_uri TEXT NOT NULL,
 scope TEXT NOT NULL,resource TEXT NOT NULL,state TEXT NOT NULL,challenge TEXT NOT NULL,expires_at_ms BIGINT NOT NULL,consumed BIGINT NOT NULL DEFAULT 0,peer_hash TEXT NOT NULL DEFAULT '');

CREATE TABLE IF NOT EXISTS oauth_interactive_grants(
 id TEXT PRIMARY KEY,principal_id TEXT NOT NULL UNIQUE REFERENCES access_keys(id),human_id TEXT NOT NULL REFERENCES access_keys(id),
 client_id TEXT NOT NULL REFERENCES access_keys(id),resource TEXT NOT NULL,created_at_ms BIGINT NOT NULL,expires_at_ms BIGINT NOT NULL);

CREATE INDEX IF NOT EXISTS oauth_grants_human ON oauth_interactive_grants(human_id);

CREATE INDEX IF NOT EXISTS oauth_grants_client ON oauth_interactive_grants(client_id);

CREATE TABLE IF NOT EXISTS oauth_authorization_codes(
 code_hash TEXT PRIMARY KEY,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),client_id TEXT NOT NULL,
 redirect_uri TEXT NOT NULL,challenge TEXT NOT NULL,expires_at_ms BIGINT NOT NULL,used_at_ms BIGINT NOT NULL DEFAULT 0);

CREATE TABLE IF NOT EXISTS oauth_refresh_tokens(
 token_hash TEXT PRIMARY KEY,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),family_id TEXT NOT NULL,
 client_id TEXT NOT NULL,scope TEXT NOT NULL,projects_json TEXT NOT NULL,expires_at_ms BIGINT NOT NULL,used_at_ms BIGINT NOT NULL DEFAULT 0);

CREATE INDEX IF NOT EXISTS oauth_refresh_grant ON oauth_refresh_tokens(grant_id);

CREATE INDEX IF NOT EXISTS oauth_refresh_expiry ON oauth_refresh_tokens(expires_at_ms);

CREATE INDEX IF NOT EXISTS oauth_code_grant ON oauth_authorization_codes(grant_id);

CREATE TABLE IF NOT EXISTS oauth_interactive_access(
 token_hash TEXT PRIMARY KEY REFERENCES oauth_tokens(token_hash) ON DELETE CASCADE,grant_id TEXT NOT NULL REFERENCES oauth_interactive_grants(id),family_id TEXT NOT NULL);

CREATE INDEX IF NOT EXISTS oauth_tokens_interactive_grant ON oauth_tokens(interactive_grant_id);

CREATE TABLE IF NOT EXISTS email_humans (
 principal_id TEXT PRIMARY KEY REFERENCES access_keys(id), email TEXT NOT NULL UNIQUE,
 project_grants TEXT NOT NULL, deployment_admin BIGINT NOT NULL DEFAULT 0 CHECK(deployment_admin IN (0,1)),
 notice_sent BIGINT NOT NULL DEFAULT 0, notice_attempt TEXT NOT NULL DEFAULT '', notice_retry_after_ms BIGINT NOT NULL DEFAULT 0);

CREATE UNIQUE INDEX IF NOT EXISTS email_deployment_admin ON email_humans(deployment_admin) WHERE deployment_admin=1;


CREATE TABLE IF NOT EXISTS tenant_memberships (
 tenant_id TEXT NOT NULL REFERENCES tenants(id),
 human_id TEXT NOT NULL REFERENCES email_humans(principal_id) ON DELETE CASCADE,
 role TEXT NOT NULL CHECK(role IN ('admin','member')),
 project_scope TEXT NOT NULL CHECK(project_scope IN ('all','selected')),
 permissions TEXT NOT NULL,project_grants TEXT NOT NULL,
 created_at TEXT NOT NULL,updated_at TEXT NOT NULL,revoked_at TEXT,
 notice_sent BIGINT NOT NULL DEFAULT 0 CHECK(notice_sent IN (0,1)),
 notice_attempt TEXT NOT NULL DEFAULT '',notice_retry_after_ms BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(tenant_id,human_id));
CREATE INDEX IF NOT EXISTS tenant_memberships_human ON tenant_memberships(human_id,tenant_id);
CREATE TABLE IF NOT EXISTS tenant_credentials (
 principal_id TEXT PRIMARY KEY REFERENCES access_keys(id),tenant_id TEXT NOT NULL REFERENCES tenants(id));
CREATE INDEX IF NOT EXISTS tenant_credentials_tenant ON tenant_credentials(tenant_id,principal_id);
CREATE TABLE IF NOT EXISTS oauth_grant_tenants (
 grant_id TEXT PRIMARY KEY REFERENCES oauth_interactive_grants(id) ON DELETE CASCADE,
 tenant_id TEXT NOT NULL REFERENCES tenants(id));
CREATE TABLE IF NOT EXISTS oidc_identities (
 issuer TEXT NOT NULL,subject TEXT NOT NULL,
 principal_id TEXT NOT NULL REFERENCES email_humans(principal_id) ON DELETE CASCADE,
 linked_email TEXT NOT NULL,created_at TEXT NOT NULL,
 PRIMARY KEY(issuer,subject),UNIQUE(issuer,principal_id));
