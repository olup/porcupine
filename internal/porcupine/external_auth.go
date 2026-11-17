package porcupine

import (
	"context"
	"database/sql"

	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/auth"
)

// externalPrincipal binds signed authority to current control-plane ownership.
// Project claims narrow access; they never establish a project's tenant.
func externalPrincipal(ctx context.Context, query oauthQueryer, grant *auth.ExternalGrant, now time.Time) (auth.Principal, error) {
	if !grant.Valid(now) {
		return auth.Principal{}, apiError(401, "external authorization expired")
	}
	actor := grant.Principal()
	if grant.Operator() {
		return actor, nil
	}
	if err := requireTenantActive(ctx, query, grant.Tenant()); err != nil {
		return auth.Principal{}, err
	}
	if grant.AllProjects() {
		rows, err := query.QueryContext(ctx, `SELECT id FROM projects p WHERE tenant_id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id) ORDER BY id`, grant.Tenant())
		if err != nil {
			return auth.Principal{}, auth.ErrUnavailable
		}
		defer rows.Close()
		actor.Projects = []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return auth.Principal{}, auth.ErrUnavailable
			}
			actor.Projects = append(actor.Projects, id)
		}
		if err := rows.Err(); err != nil {
			return auth.Principal{}, auth.ErrUnavailable
		}
	} else {
		args := []any{grant.Tenant()}
		markers := make([]string, len(actor.Projects))
		for i, project := range actor.Projects {
			markers[i] = "$" + strconv.Itoa(i+2)
			args = append(args, project)
		}
		var matched int
		if err := query.QueryRowContext(ctx, `SELECT count(*) FROM projects p WHERE tenant_id=$1 AND p.id IN (`+strings.Join(markers, ",")+`)
AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id)`, args...).Scan(&matched); err != nil {
			return auth.Principal{}, auth.ErrUnavailable
		}
		if matched != len(actor.Projects) {
			return auth.Principal{}, apiError(403, "signed project is outside the authorized tenant")
		}
	}
	return actor, nil
}

func (s *Service) currentAuthority(ctx context.Context, query oauthQueryer, caller auth.Principal) (auth.Principal, error) {
	if caller.External != nil {
		if !s.externalAuthEnabled() {
			return auth.Principal{}, errors.New("external authority is not enabled")
		}
		return externalPrincipal(ctx, query, caller.External, s.clock())
	}
	return s.loadCredentialActorFrom(ctx, query, caller.ID)
}

func checkControlAuthMode(ctx context.Context, query oauthQueryer, external bool) error {
	var mode string
	err := query.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='auth_mode'`).Scan(&mode)
	if errors.Is(err, sql.ErrNoRows) {
		mode = "builtin"
	} else if err != nil {
		return err
	}
	requested := "builtin"
	if external {
		requested = "external"
	}
	if requested != mode {
		return errors.New("control database authentication mode differs; start a fresh control database instead of adopting identities")
	}
	return nil
}

func (s *Service) requireExternalCurrent(ctx context.Context, query oauthQueryer) error {
	if !s.externalAuthEnabled() {
		return nil
	}
	actor := principal(ctx)
	if actor.External == nil {
		return nil
	}
	_, err := s.currentAuthority(ctx, query, actor)
	return err
}
