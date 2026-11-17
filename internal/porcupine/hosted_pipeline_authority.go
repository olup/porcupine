package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/output"
)

// hostedPipelineNativeAuthority reuses the current native service-key and parent
// restrictions under the publication transaction's row locks. Human execution
// consent is deliberately unavailable here.
type hostedPipelineNativeAuthority struct{}

func (hostedPipelineNativeAuthority) ResolveCreatorTx(ctx context.Context, tx *sql.Tx, tenant, project, creator string) (bool, error) {
	var derived bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM service_api_keys WHERE principal_id=$1)`, creator).Scan(&derived); err != nil {
		return false, err
	}
	if !derived {
		return (output.PipelineNativeCredentialAuthority{}).ResolveCreatorTx(ctx, tx, tenant, project, creator)
	}
	var owner, permissions, projects, audience string
	a := auth.Principal{ID: creator, ServiceKey: true}
	err := tx.QueryRowContext(ctx, `SELECT tc.tenant_id,k.permissions,k.projects,k.source_id,b.audience FROM service_api_keys b JOIN access_keys k ON k.id=b.principal_id JOIN access_keys parent ON parent.id=b.client_id JOIN tenant_credentials tc ON tc.principal_id=k.id JOIN tenant_credentials pt ON pt.principal_id=parent.id JOIN oauth_clients c ON c.client_id=parent.id WHERE k.id=$1 AND k.revoked_at IS NULL AND parent.revoked_at IS NULL AND tc.tenant_id=$2 AND pt.tenant_id=$2 FOR UPDATE OF b,k,parent,tc,pt,c`, creator, tenant).Scan(&owner, &permissions, &projects, &a.SourceID, &audience)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if owner != tenant || !(audience == "api" || audience == "mcp") {
		return false, nil
	}
	if err = json.Unmarshal([]byte(permissions), &a.Permissions); err != nil {
		return false, err
	}
	if err = json.Unmarshal([]byte(projects), &a.Projects); err != nil {
		return false, err
	}
	a, err = constrainServiceKeyActor(ctx, tx, a)
	if errors.Is(err, auth.ErrUnavailable) {
		return false, err
	}
	if err != nil {
		return false, nil
	}
	if !slices.Equal(a.TenantIDs, []string{tenant}) {
		return false, nil
	}
	return auth.Authorize(a, "pipelines:write", project) && auth.Authorize(a, "events:read", project), nil
}
