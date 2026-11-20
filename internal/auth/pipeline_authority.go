package auth

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
)

// CanExecutePipeline checks the project permissions required of a pipeline creator.
func CanExecutePipeline(principal Principal, project string) bool {
	return Authorize(principal, "pipelines:write", project) && Authorize(principal, "events:read", project)
}

// PipelineNativeCredentialAuthority resolves explicit tenant credentials within
// the publication transaction. Human consent is handled by the caller.
type PipelineNativeCredentialAuthority struct {
	// ConstrainServiceKey revalidates a derived key against its current parent.
	// A nil function denies derived keys.
	ConstrainServiceKey func(context.Context, *sql.Tx, Principal) (Principal, error)
}

func (a PipelineNativeCredentialAuthority) ResolveCreatorTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	if a.ConstrainServiceKey != nil {
		var derived bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM service_api_keys WHERE principal_id=$1)`, creator).Scan(&derived); err != nil {
			return false, err
		}
		if derived {
			return a.resolveServiceKeyTx(ctx, tx, tenant, project, creator)
		}
	}

	var owner, permissions, projects string
	var revoked sql.NullString
	err := tx.QueryRowContext(ctx, `
SELECT p.tenant_id,a.permissions,a.projects,a.revoked_at
FROM projects p
JOIN tenant_credentials tc ON tc.tenant_id=p.tenant_id
JOIN access_keys a ON a.id=tc.principal_id
WHERE p.id=$1 AND tc.principal_id=$2
  AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
  AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id)
  AND NOT EXISTS(SELECT 1 FROM service_api_keys s WHERE s.principal_id=a.id)
  AND NOT EXISTS(SELECT 1 FROM email_humans h WHERE h.principal_id=a.id)
  AND NOT EXISTS(SELECT 1 FROM oauth_interactive_grants g WHERE g.principal_id=a.id)
FOR UPDATE OF p,tc,a`, project, creator).Scan(&owner, &permissions, &projects, &revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner != tenant || revoked.Valid {
		return false, nil
	}
	principal := Principal{ID: creator}
	if err = json.Unmarshal([]byte(permissions), &principal.Permissions); err != nil {
		return false, err
	}
	if err = json.Unmarshal([]byte(projects), &principal.Projects); err != nil {
		return false, err
	}
	return slices.Equal(principal.Projects, []string{project}) && CanExecutePipeline(principal, project), nil
}

func (a PipelineNativeCredentialAuthority) resolveServiceKeyTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	var owner, permissions, projects, audience string
	principal := Principal{ID: creator, ServiceKey: true}
	err := tx.QueryRowContext(ctx, `
SELECT tc.tenant_id,k.permissions,k.projects,k.source_id,b.audience
FROM service_api_keys b
JOIN access_keys k ON k.id=b.principal_id
JOIN access_keys parent ON parent.id=b.client_id
JOIN tenant_credentials tc ON tc.principal_id=k.id
JOIN tenant_credentials pt ON pt.principal_id=parent.id
JOIN oauth_clients c ON c.client_id=parent.id
WHERE k.id=$1 AND k.revoked_at IS NULL AND parent.revoked_at IS NULL
  AND tc.tenant_id=$2 AND pt.tenant_id=$2
FOR UPDATE OF b,k,parent,tc,pt,c`, creator, tenant).Scan(&owner, &permissions, &projects, &principal.SourceID, &audience)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner != tenant || !(audience == "api" || audience == "mcp") {
		return false, nil
	}
	if err = json.Unmarshal([]byte(permissions), &principal.Permissions); err != nil {
		return false, err
	}
	if err = json.Unmarshal([]byte(projects), &principal.Projects); err != nil {
		return false, err
	}
	principal, err = a.ConstrainServiceKey(ctx, tx, principal)
	if errors.Is(err, ErrUnavailable) {
		return false, err
	}
	if err != nil {
		return false, nil
	}
	return slices.Equal(principal.TenantIDs, []string{tenant}) && CanExecutePipeline(principal, project), nil
}
