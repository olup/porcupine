package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/olup/porcupine/internal/output"
)

// purgeProjectLocalRecords removes only local payloads and jobs. Shared
// credential grants are pruned separately in the control transaction.
func purgeProjectLocalRecords(ctx context.Context, tx *sql.Tx, projectID string) error {
	if err := output.StageExportProjectCleanupTx(ctx, tx, projectID); err != nil {
		return err
	}
	if err := output.DeletePipelineProjectTx(ctx, tx, projectID); err != nil {
		return err
	}
	for _, statement := range []string{
		"DELETE FROM import_upload_samples WHERE job_id IN (SELECT id FROM import_uploads WHERE project_id=?)",
		"DELETE FROM import_upload_files WHERE job_id IN (SELECT id FROM import_uploads WHERE project_id=?)",
		"DELETE FROM import_uploads WHERE project_id=?",
		"DELETE FROM posthog_import_person_state WHERE project_id=?",
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

func (s *Service) purgeProjectGrants(ctx context.Context, tx *sql.Tx, projectID string) error {
	return purgeProjectGrants(ctx, tx, projectID, !s.externalAuthEnabled())
}

func purgeProjectGrants(ctx context.Context, tx *sql.Tx, projectID string, builtin bool) error {
	var tenant string
	var deletingTenant bool
	if err := tx.QueryRowContext(ctx, `SELECT coalesce((SELECT tenant_id FROM projects WHERE id=$1),''),
EXISTS(SELECT 1 FROM projects p JOIN tenant_deletions d ON d.tenant_id=p.tenant_id WHERE p.id=$1)`, projectID).Scan(&tenant, &deletingTenant); err != nil {
		return err
	}
	if builtin && !deletingTenant {
		if err := purgeProjectHumanGrants(ctx, tx, projectID); err != nil {
			return err
		}
	}
	if builtin {
		if err := purgeProjectTenantMemberships(ctx, tx, projectID); err != nil {
			return err
		}
	}
	statement := `SELECT k.id,k.permissions,k.projects,NULL,t.principal_id IS NOT NULL FROM access_keys k LEFT JOIN tenant_credentials t ON t.principal_id=k.id`
	if builtin {
		statement = `SELECT k.id,k.permissions,k.projects,h.project_grants,t.principal_id IS NOT NULL FROM access_keys k LEFT JOIN email_humans h ON h.principal_id=k.id LEFT JOIN tenant_credentials t ON t.principal_id=k.id`
	}
	rows, err := tx.QueryContext(ctx, statement)
	if err != nil {
		return err
	}
	type principalGrant struct {
		id, permissions, projects string
		humanGrants               sql.NullString
		tenantCredential          bool
	}
	var principals []principalGrant
	for rows.Next() {
		var p principalGrant
		if err := rows.Scan(&p.id, &p.permissions, &p.projects, &p.humanGrants, &p.tenantCredential); err != nil {
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
		if deletingTenant {
			// These are globally shared parents. Their existing update triggers
			// revoke every derived connection, including unrelated tenants.
			// Keep inert historical IDs here and prune each tool/token instead.
			var sharedParent bool
			parentQuery := `SELECT EXISTS(SELECT 1 FROM oauth_clients WHERE client_id=$1 AND NOT EXISTS(SELECT 1 FROM tenant_credentials WHERE principal_id=$1))`
			if builtin {
				parentQuery = `SELECT EXISTS(SELECT 1 FROM email_humans WHERE principal_id=$1 UNION ALL SELECT 1 FROM oauth_clients WHERE client_id=$1 AND NOT EXISTS(SELECT 1 FROM tenant_credentials WHERE principal_id=$1))`
			}
			if err := tx.QueryRowContext(ctx, parentQuery, p.id).Scan(&sharedParent); err != nil {
				return err
			}
			if sharedParent {
				continue
			}
		}
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
		owner := ""
		// A project deletion must not turn a blocked legacy team credential
		// into a valid single-project credential.
		if p.tenantCredential || len(retained) == 0 && !p.humanGrants.Valid {
			revoked = now()
			// Preserve the tenant of legacy credentials before the last scope
			// is removed. Ownership does not authorize tenant operations.
			var rights []string
			if !deletingTenant && !strings.HasPrefix(p.id, "external:") && json.Unmarshal([]byte(p.permissions), &rights) == nil && projectCredentialScope(rights, projects) {
				owner = tenant
			}
		}
		encoded, err := json.Marshal(retained)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE access_keys SET projects=$1,permissions=$2,revoked_at=coalesce(revoked_at,$3),client_ingestion_token=NULL,
owner_tenant_id=coalesce(nullif(owner_tenant_id,''),$7)
WHERE id=$4 AND projects=$5 AND permissions=$6`, string(encoded), permissions, revoked, p.id, p.projects, p.permissions, owner)
		if err != nil {
			return err
		}
		if err := projectGrantUpdateApplied(result); err != nil {
			return err
		}
	}
	if builtin {
		for _, table := range []string{"oauth_tokens", "oauth_refresh_tokens"} {
			if err := purgeProjectTokenScopes(ctx, tx, table, projectID); err != nil {
				return err
			}
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
	rows, err := tx.QueryContext(ctx, `SELECT o.token_hash,o.projects_json,t.principal_id IS NOT NULL
FROM `+table+` o LEFT JOIN tenant_credentials t ON t.principal_id=o.client_id`)
	if err != nil {
		return err
	}
	type tokenScope struct {
		hash, raw        string
		tenantCredential bool
	}
	var tokens []tokenScope
	for rows.Next() {
		var token tokenScope
		if err := rows.Scan(&token.hash, &token.raw, &token.tenantCredential); err != nil {
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
		// A previously narrowed team account can still have a token issued
		// with several projects. Delete it instead of making it usable again.
		if token.tenantCredential || len(retained) == 0 {
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
