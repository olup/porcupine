package auth

import "slices"

var interactiveOAuthScopes = []string{"events:read", "management:read"}

func InteractiveReadScopes() []string { return slices.Clone(interactiveOAuthScopes) }

func OnlyInteractiveReadScopes(scopes []string) bool {
	for _, scope := range scopes {
		if !slices.Contains(interactiveOAuthScopes, scope) {
			return false
		}
	}
	return len(scopes) > 0
}

// Interactive clients explicitly list the scopes they may request. A deployment
// administrator's client is separate from analytics clients and machine clients.
func (c Principal) PermitsInteractiveScope(permission string, grantTypes []string) bool {
	if c.SourceID != "" || !slices.Contains(c.Permissions, permission) {
		return false
	}
	if slices.Contains(c.Permissions, TenantAdminScope) {
		return permission == TenantAdminScope && len(c.Permissions) == 1 && slices.Equal(c.Projects, []string{"*"}) && !slices.Contains(grantTypes, "client_credentials")
	}
	if c.Admin() {
		return permission == "instance:admin" && len(c.Permissions) == 1 && slices.Equal(c.Projects, []string{"*"}) && !slices.Contains(grantTypes, "client_credentials")
	}
	return true
}

func InteractiveProjectsAllowed(human, client Principal, permissions, projects []string) bool {
	if len(projects) == 0 || len(projects) > 64 || slices.Contains(projects, "") {
		return false
	}
	if slices.Contains(permissions, "instance:admin") {
		return human.EmailHuman && human.Admin() && client.Admin() && slices.Contains(human.Projects, "*") && slices.Contains(client.Projects, "*") && slices.Equal(projects, []string{"*"})
	}
	if slices.Contains(projects, "*") || !human.ContainsProjects(projects) || !client.ContainsProjects(projects) {
		return false
	}
	if human.EmailHuman && !human.Admin() {
		for _, project := range projects {
			for _, permission := range permissions {
				if !slices.Contains(human.ProjectPermissions[project], permission) {
					return false
				}
			}
		}
	}
	return true
}
