-- SQLite-only additions to control_schema.sql for local control version 1.
-- Shared PostgreSQL control has equivalent PL/pgSQL triggers in shared_control_schema.sql.

CREATE TRIGGER email_grant_change AFTER UPDATE OF project_grants ON email_humans
 WHEN NEW.project_grants<>OLD.project_grants BEGIN
 UPDATE access_keys SET revoked_at=datetime('now') WHERE revoked_at IS NULL AND
 id IN (SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.principal_id);
 END;

CREATE TRIGGER oauth_human_client_revocation AFTER UPDATE OF revoked_at,permissions,projects ON access_keys
 WHEN NEW.revoked_at IS NOT NULL OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects BEGIN
 UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,datetime('now'))
 WHERE id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.id OR client_id=NEW.id) AND revoked_at IS NULL;
 END;

CREATE TRIGGER revoke_service_api_keys AFTER UPDATE OF revoked_at,permissions,projects,source_id ON access_keys
	WHEN (OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL) OR NEW.permissions<>OLD.permissions
	     OR NEW.projects<>OLD.projects OR NEW.source_id<>OLD.source_id
	BEGIN
		UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,datetime('now')) WHERE revoked_at IS NULL
		AND id IN (SELECT principal_id FROM service_api_keys WHERE client_id=NEW.id);
	END;
