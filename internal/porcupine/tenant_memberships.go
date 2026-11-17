package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sort"
)

type TenantProjectGrant struct {
	ProjectID   string   `json:"project_id" minLength:"1" maxLength:"128"`
	Role        string   `json:"role,omitempty" enum:"reader,contributor,manager" doc:"Convenient project permission preset; cannot be combined with permissions."`
	Permissions []string `json:"permissions,omitempty" minItems:"1" maxItems:"32" doc:"Exact project permissions instead of a preset."`
}

type TenantMembership struct {
	TenantID     string               `json:"tenant_id"`
	HumanID      string               `json:"human_id"`
	Email        string               `json:"email"`
	Role         string               `json:"role" enum:"admin,member"`
	ProjectScope string               `json:"project_scope" enum:"all,selected"`
	Permissions  []string             `json:"permissions" doc:"Default project permissions. Admin membership always includes all project capabilities."`
	Grants       []TenantProjectGrant `json:"grants" doc:"Selected projects, or per-project overrides of the all-project default."`
	CreatedAt    string               `json:"created_at"`
	UpdatedAt    string               `json:"updated_at"`
	RevokedAt    *string              `json:"revoked_at" nullable:"true"`
}

type TenantMembershipSpec struct {
	Role         string               `json:"role" enum:"admin,member"`
	ProjectScope string               `json:"project_scope,omitempty" enum:"all,selected" doc:"all includes future projects in this tenant; selected grants only the listed projects."`
	ProjectRole  string               `json:"project_role,omitempty" enum:"reader,contributor,manager" doc:"Default permission preset. Omit for reader. Admin membership has fixed tenant-local authority."`
	Permissions  []string             `json:"permissions,omitempty" minItems:"1" maxItems:"32" doc:"Default project permissions instead of project_role."`
	Grants       []TenantProjectGrant `json:"grants,omitempty" maxItems:"64"`
}

func projectRolePermissions(role string) ([]string, error) {
	switch role {
	case "", "reader":
		return []string{"events:read", "management:read"}, nil
	case "contributor":
		return []string{"events:read", "events:write", "identity:write", "imports:write", "management:read"}, nil
	case "manager":
		return slices.Clone(projectPermissionNames), nil
	default:
		return nil, apiError(400, "unknown project role")
	}
}

func canonicalProjectPermissions(role string, permissions []string) ([]string, error) {
	if role != "" && permissions != nil {
		return nil, apiError(400, "supply a project role or permissions, not both")
	}
	if permissions == nil {
		return projectRolePermissions(role)
	}
	if len(permissions) == 0 || len(permissions) > 32 {
		return nil, apiError(400, "expected 1 to 32 project permissions")
	}
	for _, p := range permissions {
		if !slices.Contains(projectPermissionNames, p) {
			return nil, apiError(400, "unknown or non-project permission")
		}
	}
	return sortedGrants(permissions), nil
}

func canonicalMembership(ctx context.Context, query oauthQueryer, tenantID string, input TenantMembershipSpec) (TenantMembership, error) {
	m := TenantMembership{TenantID: tenantID, Role: input.Role, ProjectScope: input.ProjectScope, Grants: []TenantProjectGrant{}}
	if m.Role != "admin" && m.Role != "member" {
		return m, apiError(400, "role must be admin or member")
	}
	if m.Role == "admin" {
		if input.ProjectScope != "" && input.ProjectScope != "all" || input.ProjectRole != "" || input.Permissions != nil || len(input.Grants) > 0 {
			return m, apiError(400, "tenant administrators have fixed authority over all projects in their tenant")
		}
		m.ProjectScope = "all"
		m.Permissions = sortedGrants(projectPermissionNames)
		return m, nil
	}
	if m.ProjectScope == "" {
		m.ProjectScope = "selected"
	}
	if m.ProjectScope != "all" && m.ProjectScope != "selected" {
		return m, apiError(400, "project_scope must be all or selected")
	}
	permissions, err := canonicalProjectPermissions(input.ProjectRole, input.Permissions)
	if err != nil {
		return m, err
	}
	m.Permissions = sortedGrants(permissions)
	if len(input.Grants) > 64 {
		return m, apiError(400, "at most 64 explicit project grants are supported")
	}
	seen := map[string]bool{}
	for _, grant := range input.Grants {
		if grant.ProjectID == "" || grant.ProjectID == "*" || seen[grant.ProjectID] {
			return m, apiError(400, "project grants require distinct explicit project IDs")
		}
		seen[grant.ProjectID] = true
		var owner string
		if err := query.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, grant.ProjectID).Scan(&owner); err != nil || owner != tenantID {
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return m, apiError(503, "project ownership unavailable")
			}
			return m, apiError(400, "project does not belong to this tenant")
		}
		if grant.Role == "" && grant.Permissions == nil {
			permissions = slices.Clone(m.Permissions)
		} else {
			permissions, err = canonicalProjectPermissions(grant.Role, grant.Permissions)
			if err != nil {
				return m, err
			}
		}
		m.Grants = append(m.Grants, TenantProjectGrant{ProjectID: grant.ProjectID, Permissions: sortedGrants(permissions)})
	}
	sort.Slice(m.Grants, func(i, j int) bool { return m.Grants[i].ProjectID < m.Grants[j].ProjectID })
	return m, nil
}

const selectTenantMembers = `SELECT m.tenant_id,m.human_id,h.email,m.role,m.project_scope,m.permissions,m.project_grants,m.created_at,m.updated_at,m.revoked_at FROM tenant_memberships m JOIN email_humans h ON h.principal_id=m.human_id`

func scanTenantMembership(row interface{ Scan(...any) error }) (TenantMembership, error) {
	var m TenantMembership
	var permissions, grants string
	if err := row.Scan(&m.TenantID, &m.HumanID, &m.Email, &m.Role, &m.ProjectScope, &permissions, &grants, &m.CreatedAt, &m.UpdatedAt, &m.RevokedAt); err != nil {
		return m, err
	}
	if json.Unmarshal([]byte(permissions), &m.Permissions) != nil || json.Unmarshal([]byte(grants), &m.Grants) != nil {
		return m, errors.New("invalid tenant membership")
	}
	return m, nil
}

func addTenantMembershipAuthority(ctx context.Context, query oauthQueryer, human actor) (actor, error) {
	rows, err := query.QueryContext(ctx, selectTenantMembers+` JOIN access_keys k ON k.id=m.human_id WHERE m.human_id=$1 AND m.revoked_at IS NULL AND k.revoked_at IS NULL ORDER BY m.tenant_id`, human.ID)
	if err != nil {
		return actor{}, err
	}
	var memberships []TenantMembership
	for rows.Next() {
		m, err := scanTenantMembership(rows)
		if err != nil {
			rows.Close()
			return actor{}, err
		}
		memberships = append(memberships, m)
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return actor{}, err
	}
	if human.projectPermissions == nil {
		human.projectPermissions = map[string][]string{}
	}
	for _, m := range memberships {
		human.tenantIDs = append(human.tenantIDs, m.TenantID)
		if m.Role == "admin" {
			human.tenantAdmins = append(human.tenantAdmins, m.TenantID)
			human.Permissions = append(human.Permissions, "tenant:admin")
			human.Permissions = append(human.Permissions, projectPermissionNames...)
		}
		if m.Role != "admin" && m.Role != "member" || m.ProjectScope != "all" && m.ProjectScope != "selected" {
			return actor{}, errors.New("invalid membership role or scope")
		}
		overrides := map[string][]string{}
		for _, g := range m.Grants {
			overrides[g.ProjectID] = g.Permissions
		}
		projects, err := query.QueryContext(ctx, `SELECT id FROM projects WHERE tenant_id=$1 ORDER BY id`, m.TenantID)
		if err != nil {
			return actor{}, err
		}
		for projects.Next() {
			var id string
			if err := projects.Scan(&id); err != nil {
				projects.Close()
				return actor{}, err
			}
			perms, explicit := overrides[id]
			if m.Role == "admin" {
				perms = projectPermissionNames
			} else if !explicit {
				if m.ProjectScope == "selected" {
					continue
				}
				perms = m.Permissions
			}
			for _, p := range perms {
				if !slices.Contains(projectPermissionNames, p) {
					projects.Close()
					return actor{}, errors.New("invalid project membership permission")
				}
			}
			human.Projects = append(human.Projects, id)
			human.Permissions = append(human.Permissions, perms...)
			human.projectPermissions[id] = sortedGrants(append(human.projectPermissions[id], perms...))
		}
		if err := errors.Join(projects.Err(), projects.Close()); err != nil {
			return actor{}, err
		}
	}
	human.Projects = sortedGrants(human.Projects)
	human.Permissions = sortedGrants(human.Permissions)
	return human, nil
}

func (s *Service) projectAdministrationTenant(ctx context.Context, projectID string) (string, error) {
	var tenant string
	err := s.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1 UNION ALL SELECT tenant_id FROM project_deletions WHERE project_id=$1 AND tenant_id<>'' LIMIT 1`, projectID).Scan(&tenant)
	return tenant, err
}
