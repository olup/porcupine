package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/olup/porcupine/internal/auth"
)

func (s *Service) requireProjectActive(ctx context.Context, project string) error {
	var active bool
	err := s.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects p JOIN tenants t ON t.id=p.tenant_id
WHERE p.id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=t.id))`, project).Scan(&active)
	if err != nil {
		return apiError(503, "project authority unavailable")
	}
	if !active {
		return apiError(404, "project not found")
	}
	return nil
}

func requireTenantActive(ctx context.Context, query oauthQueryer, tenant string) error {
	var active bool
	err := query.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants t WHERE t.id=$1
AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=t.id))`, tenant).Scan(&active)
	if err != nil {
		return apiError(503, "tenant authority unavailable")
	}
	if !active {
		return apiError(404, "tenant not found")
	}
	return nil
}

// Global identities can retain historical project IDs. Only authoritative,
// active project rows contribute effective grants or appear in public views.
func activeProjects(ctx context.Context, query oauthQueryer, projects []string) ([]string, error) {
	active := map[string]bool{"*": true}
	for offset := 0; offset < len(projects); offset += 256 {
		batch := projects[offset:min(offset+256, len(projects))]
		args := make([]any, len(batch))
		markers := make([]string, len(batch))
		for i, id := range batch {
			args[i], markers[i] = id, "$"+strconv.Itoa(i+1)
		}
		rows, err := query.QueryContext(ctx, `SELECT p.id FROM projects p WHERE p.id IN (`+strings.Join(markers, ",")+`)
AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			active[id] = true
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
	}
	result := make([]string, 0, len(projects))
	for _, project := range projects {
		if active[project] {
			result = append(result, project)
		}
	}
	return result, nil
}

func activeHumanAuthority(ctx context.Context, query oauthQueryer, a auth.Principal) (auth.Principal, error) {
	projects, err := activeProjects(ctx, query, a.Projects)
	if err != nil {
		return auth.Principal{}, err
	}
	a.Projects = projects
	if a.Admin() {
		return a, nil
	}
	a.Permissions = nil
	if len(a.TenantAdmins) != 0 {
		a.Permissions = append(a.Permissions, tenantAdminScope)
	}
	for id, permissions := range a.ProjectPermissions {
		if !slices.Contains(projects, id) {
			delete(a.ProjectPermissions, id)
		} else {
			a.Permissions = append(a.Permissions, permissions...)
		}
	}
	a.Permissions = sortedGrants(a.Permissions)
	return a, nil
}

func tenantDeletionExists(ctx context.Context, query oauthQueryer, tenant string) (bool, error) {
	var id string
	err := query.QueryRowContext(ctx, `SELECT tenant_id FROM tenant_deletions WHERE tenant_id=$1`, tenant).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func requireActiveProjectSet(ctx context.Context, query oauthQueryer, projects []string) error {
	active, err := activeProjects(ctx, query, projects)
	if err != nil {
		return apiError(503, "project authority unavailable")
	}
	if len(active) != len(projects) {
		return apiError(404, "project not found")
	}
	return nil
}
