CREATE OR REPLACE FUNCTION porcupine_revoke_dependent_keys() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL) OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects OR NEW.source_id<>OLD.source_id THEN
    UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,CURRENT_TIMESTAMP::text)
      WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM service_api_keys WHERE client_id=NEW.id);
  END IF;
  IF NEW.revoked_at IS NOT NULL OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects THEN
    UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,CURRENT_TIMESTAMP::text)
      WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.id OR client_id=NEW.id);
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS revoke_dependent_keys ON access_keys;
CREATE TRIGGER revoke_dependent_keys AFTER UPDATE OF revoked_at,permissions,projects,source_id ON access_keys FOR EACH ROW EXECUTE FUNCTION porcupine_revoke_dependent_keys();

CREATE OR REPLACE FUNCTION porcupine_email_grants_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.project_grants<>OLD.project_grants THEN
    UPDATE access_keys SET revoked_at=CURRENT_TIMESTAMP::text WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.principal_id);
  END IF;
  RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS email_grant_change ON email_humans;
CREATE TRIGGER email_grant_change AFTER UPDATE OF project_grants ON email_humans FOR EACH ROW EXECUTE FUNCTION porcupine_email_grants_changed();

-- Shared-only tables formerly created by worker startup code.
CREATE TABLE IF NOT EXISTS oauth_browser_sessions (
 session_hash TEXT PRIMARY KEY,payload TEXT NOT NULL,expires_at_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS oauth_browser_sessions_expiry ON oauth_browser_sessions(expires_at_ms);
CREATE TABLE IF NOT EXISTS oauth_email_delivery_counts (
 period BIGINT NOT NULL,scope TEXT NOT NULL,count BIGINT NOT NULL,PRIMARY KEY(period,scope));
CREATE TABLE IF NOT EXISTS oauth_email_delivery_attempts (
 id TEXT PRIMARY KEY,expires_at_ms BIGINT NOT NULL);
CREATE INDEX IF NOT EXISTS oauth_email_attempts_expiry ON oauth_email_delivery_attempts(expires_at_ms);
CREATE TABLE IF NOT EXISTS shared_query_permits (
  permit_id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  worker_id TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
 );
CREATE INDEX IF NOT EXISTS shared_query_permits_tenant_expiry ON shared_query_permits(tenant_id,expires_at);
CREATE INDEX IF NOT EXISTS shared_query_permits_project_expiry ON shared_query_permits(project_id,expires_at);
CREATE TABLE IF NOT EXISTS shared_workers (
worker_id TEXT PRIMARY KEY,volume_id TEXT NOT NULL UNIQUE,route_url TEXT NOT NULL,
registration_token TEXT NOT NULL,registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp());
CREATE TABLE IF NOT EXISTS shared_project_owners (
project_id TEXT PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
worker_id TEXT NOT NULL REFERENCES shared_workers(worker_id),epoch BIGINT NOT NULL DEFAULT 1 CHECK(epoch>0));
CREATE TABLE IF NOT EXISTS shared_route_nonces (
nonce TEXT PRIMARY KEY,expires_at TIMESTAMPTZ NOT NULL);
CREATE INDEX IF NOT EXISTS shared_route_nonce_expiry ON shared_route_nonces(expires_at);
CREATE SEQUENCE IF NOT EXISTS shared_event_id_sequence INCREMENT BY 1024 START WITH 1;
CREATE TABLE IF NOT EXISTS worker_tenant_usage (
worker_id TEXT NOT NULL,tenant_id TEXT NOT NULL REFERENCES tenants(id),period_start TEXT NOT NULL,
accepted_events BIGINT NOT NULL CHECK(accepted_events>=0),
PRIMARY KEY(worker_id,tenant_id,period_start));
CREATE TABLE IF NOT EXISTS shared_cookieless_keys (
day TEXT PRIMARY KEY,secret BYTEA NOT NULL CHECK(octet_length(secret)=32));
