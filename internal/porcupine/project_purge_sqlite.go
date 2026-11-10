package porcupine

import (
	"context"
	"database/sql"
	"fmt"
)

// purgeProjectSQLite removes local state in the caller's transaction. The caller
// drains delivery and handles external objects before their manifests are removed.
func purgeProjectSQLite(ctx context.Context, tx *sql.Tx, projectID string) error {
	if err := purgeProjectGrants(ctx, tx, projectID); err != nil {
		return err
	}
	for _, statement := range []string{
		"DELETE FROM export_files WHERE job_id IN (SELECT id FROM export_jobs WHERE project_id=?)",
		"DELETE FROM export_jobs WHERE project_id=?",
		"DELETE FROM import_upload_samples WHERE job_id IN (SELECT id FROM import_uploads WHERE project_id=?)",
		"DELETE FROM import_upload_files WHERE job_id IN (SELECT id FROM import_uploads WHERE project_id=?)",
		"DELETE FROM import_uploads WHERE project_id=?",
		"DELETE FROM posthog_import_person_state WHERE project_id=?",
		// Deleting pipelines cascades to webhook deliveries, members, S3 runs and files.
		"DELETE FROM pipelines WHERE project_id=?",
		"DELETE FROM webhook_queue_usage WHERE scope='project:' || ?",
		"DELETE FROM mutation_outbox WHERE project_id=?",
		"DELETE FROM mutation_audit WHERE project_id=?",
		"DELETE FROM event_heads WHERE project_id=?",
		"DELETE FROM identity_links WHERE project_id=?",
		"DELETE FROM identity_profiles WHERE project_id=?",
		"DELETE FROM receipt_lookup_leases WHERE project_id=?",
		"DELETE FROM event_receipts WHERE project_id=?",
		// Ready and quarantine entries cascade. Queue triggers return capacity to other projects.
		"DELETE FROM events WHERE project_id=?",
		"DELETE FROM metadata WHERE key='last_delivery_success:' || ?",
		"DELETE FROM metadata WHERE key='receipt_reconcile_cursor' AND json_extract(value,'$.project_id')=?",
		"DELETE FROM projects WHERE id=?",
	} {
		if _, err := tx.ExecContext(ctx, statement, projectID); err != nil {
			return fmt.Errorf("purging project storage: %w", err)
		}
	}
	return nil
}

func purgeProjectGrants(ctx context.Context, tx *sql.Tx, projectID string) error {
	// Existing revocation triggers invalidate child sessions and service keys
	// when their parent loses a grant. Previously revoked credentials stay revoked.
	if _, err := tx.ExecContext(ctx, `UPDATE email_humans
SET project_grants=(SELECT json_group_array(json(value)) FROM json_each(project_grants) WHERE value->>'project_id'<>?1)
WHERE EXISTS (SELECT 1 FROM json_each(project_grants) WHERE value->>'project_id'=?1)`, projectID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET
projects=(SELECT json_group_array(value) FROM json_each(projects) WHERE value<>?1),
permissions=CASE WHEN id IN (SELECT principal_id FROM email_humans) THEN
 (SELECT json_group_array(value) FROM (SELECT DISTINCT perm.value FROM email_humans h,
 json_each(h.project_grants) g, json_each(g.value,'$.permissions') perm
 WHERE h.principal_id=access_keys.id ORDER BY perm.value))
 ELSE permissions END,
revoked_at=CASE WHEN NOT EXISTS (SELECT 1 FROM json_each(projects) WHERE value<>?1)
 AND id NOT IN (SELECT principal_id FROM email_humans UNION ALL SELECT principal_id FROM oidc_humans) THEN COALESCE(revoked_at,?) ELSE revoked_at END
WHERE EXISTS (SELECT 1 FROM json_each(projects) WHERE value=?1)`, projectID, now()); err != nil {
		return err
	}
	for _, table := range []string{"oauth_tokens", "oauth_refresh_tokens"} {
		// Clear exhausted bearer/refresh tokens. Keep the remaining projects on
		// existing multi-project tokens so a user can continue working there.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+` WHERE EXISTS
 (SELECT 1 FROM json_each(projects_json) WHERE value=?1)
 AND NOT EXISTS (SELECT 1 FROM json_each(projects_json) WHERE value<>?1)`, projectID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE "+table+` SET projects_json=
 (SELECT json_group_array(value) FROM json_each(projects_json) WHERE value<>?1)
 WHERE EXISTS (SELECT 1 FROM json_each(projects_json) WHERE value=?1)`, projectID); err != nil {
			return err
		}
	}
	return nil
}
