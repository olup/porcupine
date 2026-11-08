package porcupine

import "slices"

type actor struct {
	ID          string
	Permissions []string
	Projects    []string
}

var validPermissions = []string{"instance:admin", "management:read", "management:write", "events:read", "events:write", "events:edit", "events:delete", "identity:write", "pipelines:read", "pipelines:write"}

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
	return project == "" || slices.Contains(a.Projects, "*") || slices.Contains(a.Projects, project)
}

func (a actor) containsProjects(projects []string) bool {
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
}

func (access operationAccess) visible(a actor) bool {
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

func (access operationAccess) allows(a actor, project string) bool {
	return access.visible(a) && (access.ProjectParameter == "" || project != "" && authorize(a, "", project))
}
