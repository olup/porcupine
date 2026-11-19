package auth

import "slices"

const TenantAdminScope = "tenant:admin"

type Principal struct {
	External           *ExternalGrant
	ProjectPermissions map[string][]string
	TenantAdmins       []string
	TenantIDs          []string
	ServiceKey         bool
	EmailHuman         bool // Verified durable email principal.
	Interactive        bool // Authority derived from a human consent grant.
	ID                 string
	SourceID           string
	Permissions        []string
	Projects           []string
}

var projectPermissionNames = []string{"management:read", "management:write", "events:read", "events:write", "events:edit", "events:delete", "imports:write", "identity:write", "pipelines:read", "pipelines:write"}

var validPermissions = append([]string{"instance:admin", "tenant:admin"}, projectPermissionNames...)

func (a Principal) CanAdminTenant(tenantID string) bool {
	return a.Admin() || tenantID != "" && slices.Contains(a.Permissions, TenantAdminScope) && slices.Contains(a.TenantAdmins, tenantID)
}

func (a Principal) CanAccessTenant(tenantID string) bool {
	return a.Admin() || tenantID != "" && slices.Contains(a.TenantIDs, tenantID)
}

func (a Principal) Admin() bool                { return slices.Contains(a.Permissions, "instance:admin") }
func (a Principal) Can(permission string) bool { return Authorize(a, permission, "") }

// Authorize is the shared permission and project boundary for credentials.
// An empty project checks an instance-level capability without project filtering.
func Authorize(a Principal, permission, project string) bool {
	if a.Admin() {
		return true
	}
	if permission != "" && !slices.Contains(a.Permissions, permission) {
		return false
	}
	if project == "" {
		return true
	}
	if !slices.Contains(a.Projects, "*") && !slices.Contains(a.Projects, project) {
		return false
	}
	if a.ProjectPermissions != nil {
		permissions, exists := a.ProjectPermissions[project]
		return exists && (permission == "" || slices.Contains(permissions, permission))
	}
	return true
}

func (a Principal) ContainsProjects(projects []string) bool {
	if len(projects) == 0 {
		return a.Admin()
	}
	for _, project := range projects {
		if !Authorize(a, "", project) {
			return false
		}
	}
	return true
}

// OperationAccess is shared by HTTP enforcement and MCP discovery.
type OperationAccess struct {
	Permission            string
	AdditionalPermissions []string
	ProjectParameter      string
	InstancePermissions   []string
	TenantParameter       string
	TenantAdmin           bool
	TenantMember          bool
}

func (access OperationAccess) Visible(a Principal) bool {
	if !a.Admin() && (access.TenantAdmin && (len(a.TenantAdmins) == 0 || !slices.Contains(a.Permissions, TenantAdminScope)) || access.TenantMember && len(a.TenantIDs) == 0) {
		return false
	}
	if access.Permission == "" || !Authorize(a, access.Permission, "") {
		return false
	}
	for _, permission := range access.AdditionalPermissions {
		if !Authorize(a, permission, "") {
			return false
		}
	}
	for _, permission := range access.InstancePermissions {
		if !Authorize(a, permission, "*") {
			return false
		}
	}
	return true
}

func (access OperationAccess) Allows(a Principal, project string, tenant ...string) bool {
	if !access.Visible(a) {
		return false
	}
	if access.TenantParameter != "" {
		if len(tenant) != 1 || tenant[0] == "" {
			return false
		}
		if access.TenantAdmin && !a.CanAdminTenant(tenant[0]) || access.TenantMember && !a.CanAccessTenant(tenant[0]) {
			return false
		}
	}
	if access.ProjectParameter != "" {
		if access.TenantAdmin {
			return project != "" && len(tenant) == 1 && a.CanAdminTenant(tenant[0])
		}
		if project == "" || !Authorize(a, access.Permission, project) {
			return false
		}
		for _, permission := range access.AdditionalPermissions {
			if !Authorize(a, permission, project) {
				return false
			}
		}
	}
	return true
}

func Permissions() []string        { return slices.Clone(validPermissions) }
func ProjectPermissions() []string { return slices.Clone(projectPermissionNames) }
