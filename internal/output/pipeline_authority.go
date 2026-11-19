package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

// PipelineNativeCredentialAuthority supports explicit tenant native credentials.
// Human consent and derived service keys require the common auth adapter, which
// must revalidate their parent grants/clients. They fail closed here.
type PipelineNativeCredentialAuthority struct{}

func (PipelineNativeCredentialAuthority) ResolveCreatorTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	var bound, permissions, projects string
	var revoked sql.NullString
	e := tx.QueryRowContext(ctx, `SELECT p.tenant_id,a.permissions,a.projects,a.revoked_at FROM projects p JOIN tenant_credentials tc ON tc.tenant_id=p.tenant_id JOIN access_keys a ON a.id=tc.principal_id WHERE p.id=$1 AND tc.principal_id=$2 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id) AND NOT EXISTS(SELECT 1 FROM service_api_keys s WHERE s.principal_id=a.id) AND NOT EXISTS(SELECT 1 FROM email_humans h WHERE h.principal_id=a.id) AND NOT EXISTS(SELECT 1 FROM oauth_interactive_grants g WHERE g.principal_id=a.id) FOR UPDATE OF p,tc,a`, project, creator).Scan(&bound, &permissions, &projects, &revoked)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil {
		return false, e
	}
	if bound != tenant || revoked.Valid {
		return false, nil
	}
	principal := auth.Principal{ID: creator}
	if e = json.Unmarshal([]byte(permissions), &principal.Permissions); e != nil {
		return false, e
	}
	if e = json.Unmarshal([]byte(projects), &principal.Projects); e != nil {
		return false, e
	}
	if len(principal.Projects) != 1 || principal.Projects[0] != project {
		return false, nil
	}
	return auth.Authorize(principal, "pipelines:write", project) && auth.Authorize(principal, "events:read", project), nil
}
