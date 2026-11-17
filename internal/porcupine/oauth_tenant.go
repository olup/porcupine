package porcupine

import (
	"context"
	"errors"
	"slices"

	"github.com/olup/porcupine/internal/auth"
)

const tenantAdminScope = "tenant:admin"

func tenantAdminConsent(permissions []string) bool {
	return slices.Equal(permissions, []string{tenantAdminScope})
}

type tenantConsentOption struct {
	ID   string
	Name string
}

func (s *Service) tenantAdminOptions(ctx context.Context, query oauthQueryer, human auth.Principal) ([]tenantConsentOption, error) {
	if !human.EmailHuman {
		return nil, errors.New("tenant administration requires a verified human")
	}
	statement := `SELECT t.id,t.name FROM tenants t JOIN tenant_memberships m ON m.tenant_id=t.id
		JOIN access_keys k ON k.id=m.human_id WHERE m.human_id=$1 AND m.role='admin'
		AND m.revoked_at IS NULL AND k.revoked_at IS NULL AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=t.id) ORDER BY t.name,t.id`
	args := []any{human.ID}
	if human.Admin() {
		statement = `SELECT id,name FROM tenants WHERE NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=tenants.id) ORDER BY name,id`
		args = nil
	}
	rows, err := query.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	options := []tenantConsentOption{}
	for rows.Next() {
		var option tenantConsentOption
		if err := rows.Scan(&option.ID, &option.Name); err != nil {
			return nil, err
		}
		options = append(options, option)
	}
	return options, rows.Err()
}

func (s *Service) tenantAdminActor(ctx context.Context, query oauthQueryer, human auth.Principal, tenantID string) (auth.Principal, error) {
	if err := requireTenantActive(ctx, query, tenantID); err != nil {
		return auth.Principal{}, errInteractiveGrant
	}
	if !human.EmailHuman || tenantID == "" || tenantID == "*" {
		return auth.Principal{}, errInteractiveGrant
	}
	var allowed bool
	statement := `SELECT EXISTS(SELECT 1 FROM tenant_memberships m JOIN access_keys k ON k.id=m.human_id
		WHERE m.tenant_id=$1 AND m.human_id=$2 AND m.role='admin' AND m.revoked_at IS NULL AND k.revoked_at IS NULL)`
	args := []any{tenantID, human.ID}
	if human.Admin() {
		statement = `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`
		args = []any{tenantID}
	}
	if err := query.QueryRowContext(ctx, statement, args...).Scan(&allowed); err != nil || !allowed {
		return auth.Principal{}, errInteractiveGrant
	}
	a := auth.Principal{
		ID: human.ID, EmailHuman: true, Interactive: true,
		TenantAdmins: []string{tenantID}, TenantIDs: []string{tenantID},
		Permissions: append(slices.Clone(auth.ProjectPermissions()), tenantAdminScope),
		Projects:    []string{}, ProjectPermissions: map[string][]string{},
	}
	rows, err := query.QueryContext(ctx, `SELECT p.id FROM projects p WHERE p.tenant_id=$1
		AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) ORDER BY p.id`, tenantID)
	if err != nil {
		return auth.Principal{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return auth.Principal{}, err
		}
		a.Projects = append(a.Projects, project)
		a.ProjectPermissions[project] = slices.Clone(auth.ProjectPermissions())
	}
	return a, rows.Err()
}
