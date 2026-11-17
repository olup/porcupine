package auth

import "testing"

func TestPrincipalDoesNotCombinePermissionsAcrossProjects(t *testing.T) {
	t.Parallel()
	p := Principal{Permissions: []string{"events:read", "events:write"}, Projects: []string{"read-project", "write-project"}, ProjectPermissions: map[string][]string{"read-project": {"events:read"}, "write-project": {"events:write"}}}
	for _, tc := range []struct {
		permission, project string
		allowed             bool
	}{{"events:read", "read-project", true}, {"events:write", "write-project", true}, {"events:read", "write-project", false}, {"events:write", "read-project", false}, {"events:read", "another-tenant-project", false}} {
		if got := Authorize(p, tc.permission, tc.project); got != tc.allowed {
			t.Fatalf("%s on %s: %v", tc.permission, tc.project, got)
		}
	}
	client := Principal{Permissions: p.Permissions, Projects: []string{"*"}}
	if !InteractiveProjectsAllowed(Principal{EmailHuman: true, Permissions: p.Permissions, Projects: p.Projects, ProjectPermissions: p.ProjectPermissions}, client, []string{"events:read"}, []string{"read-project"}) {
		t.Fatal("read grant denied")
	}
	if InteractiveProjectsAllowed(Principal{EmailHuman: true, Permissions: p.Permissions, Projects: p.Projects, ProjectPermissions: p.ProjectPermissions}, client, []string{"events:read"}, []string{"write-project"}) {
		t.Fatal("consent combined grants across projects")
	}
}

func TestTenantAdministrationRequiresExplicitScopeAndMembership(t *testing.T) {
	t.Parallel()
	op := OperationAccess{Permission: TenantAdminScope, TenantParameter: "tenant_id", TenantAdmin: true}
	p := Principal{Permissions: []string{TenantAdminScope}, TenantIDs: []string{"team-a"}, TenantAdmins: []string{"team-a"}, Interactive: true}
	if !op.Allows(p, "", "team-a") || op.Allows(p, "", "team-b") || op.Allows(p, "") {
		t.Fatal("tenant boundary failed")
	}
	p.Permissions = []string{"management:write"}
	if p.CanAdminTenant("team-a") || p.CanProvisionCredentials() || op.Visible(p) {
		t.Fatal("membership restored an unconsented administrator scope")
	}
}

func TestMachineAndInteractiveAdminGrantsRemainSeparate(t *testing.T) {
	t.Parallel()
	p := Principal{Permissions: []string{"instance:admin"}, Projects: []string{"*"}}
	if p.PermitsInteractiveScope("instance:admin", []string{"client_credentials", "authorization_code"}) {
		t.Fatal("machine client admitted interactive administration")
	}
	if !p.PermitsInteractiveScope("instance:admin", []string{"authorization_code", "refresh_token"}) {
		t.Fatal("explicit human administration denied")
	}
	if InteractiveProjectsAllowed(p, p, []string{"instance:admin"}, []string{"*"}) {
		t.Fatal("non-human principal became human administrator")
	}
	p.EmailHuman = true
	if !InteractiveProjectsAllowed(p, p, []string{"instance:admin"}, []string{"*"}) {
		t.Fatal("invited operator consent denied")
	}
}

func TestPermissionCatalogCannotBeChangedByCallers(t *testing.T) {
	t.Parallel()
	all, project, read := Permissions(), ProjectPermissions(), InteractiveReadScopes()
	all[0] = "changed"
	project[0] = "changed"
	read[0] = "changed"
	if Permissions()[0] == "changed" || ProjectPermissions()[0] == "changed" || InteractiveReadScopes()[0] == "changed" {
		t.Fatal("caller changed the authorization policy")
	}
}
