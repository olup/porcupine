package porcupine

import (
	"context"
	"slices"

	"github.com/olup/porcupine/internal/auth"
)

func projectCredentialScope(permissions, projects []string) bool {
	return len(projects) > 0 && !slices.Contains(projects, "*") && subset(permissions, auth.ProjectPermissions())
}

// Call with the issuance transaction after checking that the projects are active.
func credentialTenantOwner(ctx context.Context, query oauthQueryer, permissions, projects []string) (string, error) {
	if !projectCredentialScope(permissions, projects) {
		return "", nil
	}
	var owner string
	for _, project := range projects {
		var tenant string
		if err := query.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant); err != nil {
			return "", err
		}
		if owner != "" && owner != tenant {
			return "", nil
		}
		owner = tenant
	}
	return owner, nil
}
