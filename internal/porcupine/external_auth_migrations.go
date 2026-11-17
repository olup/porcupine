package porcupine

// Machine-key revocation remains active in both modes. Human consent revocation
// belongs only to the integrated identity provider.
const externalAuthSQLiteTriggers = `DROP TRIGGER oauth_human_client_revocation;
CREATE TRIGGER oauth_human_client_revocation AFTER UPDATE OF revoked_at,permissions,projects ON access_keys
WHEN coalesce((SELECT value FROM metadata WHERE key='auth_mode'),'builtin')<>'external'
AND (NEW.revoked_at IS NOT NULL OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects) BEGIN
UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,datetime('now'))
WHERE id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.id OR client_id=NEW.id) AND revoked_at IS NULL;
END;
DROP TRIGGER email_grant_change;
CREATE TRIGGER email_grant_change AFTER UPDATE OF project_grants ON email_humans
WHEN coalesce((SELECT value FROM metadata WHERE key='auth_mode'),'builtin')<>'external'
AND NEW.project_grants<>OLD.project_grants BEGIN
UPDATE access_keys SET revoked_at=datetime('now') WHERE revoked_at IS NULL
AND id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.principal_id);
END;`

const externalAuthPostgresTriggers = `CREATE OR REPLACE FUNCTION porcupine_revoke_dependent_keys() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
IF (OLD.revoked_at IS NULL AND NEW.revoked_at IS NOT NULL) OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects OR NEW.source_id<>OLD.source_id THEN
UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,CURRENT_TIMESTAMP::text)
WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM service_api_keys WHERE client_id=NEW.id);
END IF;
IF coalesce((SELECT value FROM metadata WHERE key='auth_mode'),'builtin')<>'external'
AND (NEW.revoked_at IS NOT NULL OR NEW.permissions<>OLD.permissions OR NEW.projects<>OLD.projects) THEN
UPDATE access_keys SET revoked_at=coalesce(NEW.revoked_at,CURRENT_TIMESTAMP::text)
WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.id OR client_id=NEW.id);
END IF;
RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION porcupine_email_grants_changed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
IF coalesce((SELECT value FROM metadata WHERE key='auth_mode'),'builtin')<>'external' AND NEW.project_grants<>OLD.project_grants THEN
UPDATE access_keys SET revoked_at=CURRENT_TIMESTAMP::text WHERE revoked_at IS NULL AND id IN(SELECT principal_id FROM oauth_interactive_grants WHERE human_id=NEW.principal_id);
END IF;
RETURN NEW;
END $$;`
