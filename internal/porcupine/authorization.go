package porcupine

import (
	"regexp"
	"slices"
	"strings"

	"github.com/danielgtaylor/huma/v2"
)

type actor struct {
	projectPermissions map[string][]string
	tenantAdmins       []string
	tenantIDs          []string
	serviceKey         bool
	emailHuman         bool // Verified durable email principal.
	interactive        bool // Authority derived from a human consent grant.
	ID                 string
	SourceID           string
	Permissions        []string
	Projects           []string
}

var projectPermissionNames = []string{"management:read", "management:write", "events:read", "events:write", "events:edit", "events:delete", "imports:write", "identity:write", "pipelines:read", "pipelines:write"}

var validPermissions = append([]string{"instance:admin", "tenant:admin"}, projectPermissionNames...)

func (a actor) canProvisionCredentials() bool {
	return a.admin() || len(a.tenantAdmins) > 0 && slices.Contains(a.Permissions, tenantAdminScope) || (!a.interactive && !a.serviceKey)
}

func (a actor) canAdminTenant(tenantID string) bool {
	return a.admin() || tenantID != "" && slices.Contains(a.Permissions, tenantAdminScope) && slices.Contains(a.tenantAdmins, tenantID)
}

func (a actor) canAccessTenant(tenantID string) bool {
	return a.admin() || tenantID != "" && slices.Contains(a.tenantIDs, tenantID)
}

func (a actor) admin() bool                { return slices.Contains(a.Permissions, "instance:admin") }
func (a actor) can(permission string) bool { return authorize(a, permission, "") }

// authorize is the shared permission and project boundary for credentials.
// An empty project checks an instance-level capability without project filtering.
func authorize(a actor, permission, project string) bool {
	if a.admin() {
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
	if a.projectPermissions != nil {
		permissions, exists := a.projectPermissions[project]
		return exists && (permission == "" || slices.Contains(permissions, permission))
	}
	return true
}

func (a actor) containsProjects(projects []string) bool {
	if len(projects) == 0 {
		return a.admin()
	}
	for _, project := range projects {
		if !authorize(a, "", project) {
			return false
		}
	}
	return true
}

// operationAccess is shared by HTTP enforcement and MCP discovery.
type operationAccess struct {
	Permission            string
	AdditionalPermissions []string
	ProjectParameter      string
	InstancePermissions   []string
	TenantParameter       string
	TenantAdmin           bool
	TenantMember          bool
}

func managementOperationAccess(op *huma.Operation) operationAccess {
	access, ok := op.Metadata["access"].(operationAccess)
	if !ok || op.OperationID == "" || !slices.Contains(validPermissions, access.Permission) {
		panic("management operation requires an explicit valid access policy and operation ID")
	}
	for _, permission := range append(slices.Clone(access.AdditionalPermissions), access.InstancePermissions...) {
		if !slices.Contains(validPermissions, permission) {
			panic("management operation uses an unknown permission")
		}
	}
	for _, parameter := range []string{access.ProjectParameter, access.TenantParameter} {
		if parameter != "" && !strings.Contains(op.Path, "{"+parameter+"}") {
			panic("management operation access policy references a missing path parameter")
		}
	}
	if access.TenantParameter != "" && !access.TenantAdmin && !access.TenantMember {
		panic("tenant path access requires an explicit tenant authorization rule")
	}
	return access
}

func (access operationAccess) visible(a actor) bool {
	if !a.admin() && (access.TenantAdmin && (len(a.tenantAdmins) == 0 || !slices.Contains(a.Permissions, tenantAdminScope)) || access.TenantMember && len(a.tenantIDs) == 0) {
		return false
	}
	if access.Permission == "" || !authorize(a, access.Permission, "") {
		return false
	}
	for _, permission := range access.AdditionalPermissions {
		if !authorize(a, permission, "") {
			return false
		}
	}
	for _, permission := range access.InstancePermissions {
		if !authorize(a, permission, "*") {
			return false
		}
	}
	return true
}

func (access operationAccess) allows(a actor, project string, tenant ...string) bool {
	if !access.visible(a) {
		return false
	}
	if access.TenantParameter != "" {
		if len(tenant) != 1 || tenant[0] == "" {
			return false
		}
		if access.TenantAdmin && !a.canAdminTenant(tenant[0]) || access.TenantMember && !a.canAccessTenant(tenant[0]) {
			return false
		}
	}
	if access.ProjectParameter != "" {
		if access.TenantAdmin {
			return project != "" && len(tenant) == 1 && a.canAdminTenant(tenant[0])
		}
		if project == "" || !authorize(a, access.Permission, project) {
			return false
		}
		for _, permission := range access.AdditionalPermissions {
			if !authorize(a, permission, project) {
				return false
			}
		}
	}
	return true
}

var sourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func validSourceID(value string) bool { return value == "" || sourceIDPattern.MatchString(value) }

func validateCredentialSource(sourceID string, permissions, projects []string) error {
	if sourceID == "posthog" {
		return apiError(400, "source_id posthog is reserved for legacy ingestion")
	}
	if !validSourceID(sourceID) {
		return apiError(400, "invalid source_id")
	}
	if sourceID != "" && (len(projects) != 1 || projects[0] == "*" || !slices.Contains(permissions, "events:write")) {
		return apiError(400, "source_id requires events:write and exactly one project")
	}
	return nil
}

func delegatedSourceID(a actor, sourceID string, permissions, projects []string) (string, error) {
	if a.SourceID != "" {
		if sourceID != "" && sourceID != a.SourceID {
			return "", apiError(403, "cannot delegate a different source_id")
		}
		if slices.Contains(permissions, "events:write") {
			sourceID = a.SourceID
		}
	}
	if err := validateCredentialSource(sourceID, permissions, projects); err != nil {
		return "", err
	}
	return sourceID, nil
}
