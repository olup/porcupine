package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
)

// purgeProjectLocalRecords removes only owner-local payloads and jobs. Shared
// credential grants are pruned separately in the control transaction.
func purgeProjectLocalRecords(ctx context.Context, tx *sql.Tx, projectID string) error {
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
		"DELETE FROM event_receipts WHERE project_id=?",
		// Ready and quarantine entries cascade. Queue triggers return capacity to other projects.
		"DELETE FROM events WHERE project_id=?",
		"DELETE FROM metadata WHERE key='last_delivery_success:' || ?",
		"DELETE FROM metadata WHERE key='receipts_pruned:' || ?",
		"DELETE FROM projects WHERE id=?",
	} {
		if _, err := tx.ExecContext(ctx, statement, projectID); err != nil {
			return fmt.Errorf("purging project storage: %w", err)
		}
	}
	return nil
}

func purgeProjectGrants(ctx context.Context, tx *sql.Tx, projectID string) error {
	if err := purgeProjectHumanGrants(ctx, tx, projectID); err != nil {
		return err
	}
	if err := purgeProjectTenantMemberships(ctx, tx, projectID); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT k.id,k.permissions,k.projects,h.project_grants FROM access_keys k LEFT JOIN email_humans h ON h.principal_id=k.id`)
	if err != nil {
		return err
	}
	type principalGrant struct {
		id, permissions, projects string
		humanGrants               sql.NullString
	}
	var principals []principalGrant
	for rows.Next() {
		var p principalGrant
		if err := rows.Scan(&p.id, &p.permissions, &p.projects, &p.humanGrants); err != nil {
			rows.Close()
			return err
		}
		principals = append(principals, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, p := range principals {
		var projects []string
		if err := json.Unmarshal([]byte(p.projects), &projects); err != nil {
			return fmt.Errorf("invalid stored principal projects: %w", err)
		}
		if !slices.Contains(projects, projectID) {
			continue
		}
		retained := make([]string, 0, len(projects))
		for _, project := range projects {
			if project != projectID {
				retained = append(retained, project)
			}
		}
		permissions := p.permissions
		if p.humanGrants.Valid {
			var grants []HumanProjectGrant
			if err := json.Unmarshal([]byte(p.humanGrants.String), &grants); err != nil {
				return fmt.Errorf("invalid stored human grants: %w", err)
			}
			rights := []string{}
			for _, grant := range grants {
				rights = append(rights, grant.Permissions...)
			}
			encoded, err := json.Marshal(sortedGrants(rights))
			if err != nil {
				return err
			}
			permissions = string(encoded)
		}
		var revoked any
		if len(retained) == 0 && !p.humanGrants.Valid {
			revoked = now()
		}
		encoded, err := json.Marshal(retained)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE access_keys SET projects=$1,permissions=$2,revoked_at=coalesce(revoked_at,$3) WHERE id=$4 AND projects=$5 AND permissions=$6`, string(encoded), permissions, revoked, p.id, p.projects, p.permissions)
		if err != nil {
			return err
		}
		if err := projectGrantUpdateApplied(result); err != nil {
			return err
		}
	}
	for _, table := range []string{"oauth_tokens", "oauth_refresh_tokens"} {
		if err := purgeProjectTokenScopes(ctx, tx, table, projectID); err != nil {
			return err
		}
	}
	return nil
}

func purgeProjectTenantMemberships(ctx context.Context, tx *sql.Tx, projectID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT tenant_id,human_id,project_grants FROM tenant_memberships
WHERE tenant_id=(SELECT tenant_id FROM projects WHERE id=$1)`, projectID)
	if err != nil {
		return err
	}
	type storedMembership struct{ tenant, human, grants string }
	var memberships []storedMembership
	for rows.Next() {
		var membership storedMembership
		if err := rows.Scan(&membership.tenant, &membership.human, &membership.grants); err != nil {
			rows.Close()
			return err
		}
		memberships = append(memberships, membership)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, membership := range memberships {
		var grants []TenantProjectGrant
		if err := json.Unmarshal([]byte(membership.grants), &grants); err != nil {
			return fmt.Errorf("invalid stored tenant project grants: %w", err)
		}
		retained := make([]TenantProjectGrant, 0, len(grants))
		for _, grant := range grants {
			if grant.ProjectID != projectID {
				retained = append(retained, grant)
			}
		}
		if len(retained) == len(grants) {
			continue
		}
		encoded, err := json.Marshal(retained)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE tenant_memberships SET project_grants=$1,updated_at=$2
WHERE tenant_id=$3 AND human_id=$4 AND project_grants=$5`, string(encoded), now(), membership.tenant, membership.human, membership.grants)
		if err != nil {
			return err
		}
		if err := projectGrantUpdateApplied(result); err != nil {
			return err
		}
	}
	return nil
}

func purgeProjectHumanGrants(ctx context.Context, tx *sql.Tx, projectID string) error {
	rows, err := tx.QueryContext(ctx, `SELECT principal_id,project_grants FROM email_humans`)
	if err != nil {
		return err
	}
	type humanGrant struct{ id, raw string }
	var humans []humanGrant
	for rows.Next() {
		var h humanGrant
		if err := rows.Scan(&h.id, &h.raw); err != nil {
			rows.Close()
			return err
		}
		humans = append(humans, h)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, h := range humans {
		var grants []HumanProjectGrant
		if err := json.Unmarshal([]byte(h.raw), &grants); err != nil {
			return fmt.Errorf("invalid stored human grants: %w", err)
		}
		retained := make([]HumanProjectGrant, 0, len(grants))
		for _, grant := range grants {
			if grant.ProjectID != projectID {
				retained = append(retained, grant)
			}
		}
		if len(retained) == len(grants) {
			continue
		}
		encoded, err := json.Marshal(retained)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE email_humans SET project_grants=$1 WHERE principal_id=$2 AND project_grants=$3`, string(encoded), h.id, h.raw)
		if err != nil {
			return err
		}
		if err := projectGrantUpdateApplied(result); err != nil {
			return err
		}
	}
	return nil
}

func purgeProjectTokenScopes(ctx context.Context, tx *sql.Tx, table, projectID string) error {
	// Table names come only from the fixed caller list, never from a request.
	rows, err := tx.QueryContext(ctx, `SELECT token_hash,projects_json FROM `+table)
	if err != nil {
		return err
	}
	type tokenScope struct{ hash, raw string }
	var tokens []tokenScope
	for rows.Next() {
		var token tokenScope
		if err := rows.Scan(&token.hash, &token.raw); err != nil {
			rows.Close()
			return err
		}
		tokens = append(tokens, token)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, token := range tokens {
		var projects []string
		if err := json.Unmarshal([]byte(token.raw), &projects); err != nil {
			return fmt.Errorf("invalid stored token projects: %w", err)
		}
		if !slices.Contains(projects, projectID) {
			continue
		}
		retained := make([]string, 0, len(projects))
		for _, project := range projects {
			if project != projectID {
				retained = append(retained, project)
			}
		}
		if len(retained) == 0 {
			if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE token_hash=$1`, token.hash); err != nil {
				return err
			}
			continue
		}
		encoded, err := json.Marshal(retained)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE `+table+` SET projects_json=$1 WHERE token_hash=$2 AND projects_json=$3`, string(encoded), token.hash, token.raw)
		if err != nil {
			return err
		}
		if err := projectGrantUpdateApplied(result); err != nil {
			return err
		}
	}
	return nil
}

func projectGrantUpdateApplied(result sql.Result) error {
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("project grants changed concurrently; retry deletion")
	}
	return nil
}
