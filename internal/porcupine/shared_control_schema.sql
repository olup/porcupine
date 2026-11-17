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
