package porcupine

import (
	"context"
	"errors"
	"slices"
)

const tenantAdminScope = "tenant:admin"

func tenantAdminConsent(permissions []string) bool {
	return slices.Equal(permissions, []string{tenantAdminScope})
}

type tenantConsentOption struct {
	ID   string
	Name string
}

func (s *Service) tenantAdminOptions(ctx context.Context, query oauthQueryer, human actor) ([]tenantConsentOption, error) {
	if !human.emailHuman {
		return nil, errors.New("tenant administration requires a verified human")
	}
	statement := `SELECT t.id,t.name FROM tenants t JOIN tenant_memberships m ON m.tenant_id=t.id
		JOIN access_keys k ON k.id=m.human_id WHERE m.human_id=$1 AND m.role='admin'
		AND m.revoked_at IS NULL AND k.revoked_at IS NULL ORDER BY t.name,t.id`
	args := []any{human.ID}
	if human.admin() {
		statement = `SELECT id,name FROM tenants ORDER BY name,id`
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

func (s *Service) tenantAdminActor(ctx context.Context, query oauthQueryer, human actor, tenantID string) (actor, error) {
	if !human.emailHuman || tenantID == "" || tenantID == "*" {
		return actor{}, errInteractiveGrant
	}
	var allowed bool
	statement := `SELECT EXISTS(SELECT 1 FROM tenant_memberships m JOIN access_keys k ON k.id=m.human_id
		WHERE m.tenant_id=$1 AND m.human_id=$2 AND m.role='admin' AND m.revoked_at IS NULL AND k.revoked_at IS NULL)`
	args := []any{tenantID, human.ID}
	if human.admin() {
		statement = `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`
		args = []any{tenantID}
	}
	if err := query.QueryRowContext(ctx, statement, args...).Scan(&allowed); err != nil || !allowed {
		return actor{}, errInteractiveGrant
	}
	a := actor{
		ID: human.ID, emailHuman: true, interactive: true,
		tenantAdmins: []string{tenantID}, tenantIDs: []string{tenantID},
		Permissions: append(slices.Clone(projectPermissionNames), tenantAdminScope),
		Projects:    []string{}, projectPermissions: map[string][]string{},
	}
	rows, err := query.QueryContext(ctx, `SELECT p.id FROM projects p WHERE p.tenant_id=$1
		AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) ORDER BY p.id`, tenantID)
	if err != nil {
		return actor{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return actor{}, err
		}
		a.Projects = append(a.Projects, project)
		a.projectPermissions[project] = slices.Clone(projectPermissionNames)
	}
	return a, rows.Err()
}
