package porcupine

import (
	"context"
	"database/sql"

	"errors"
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
	var exists bool
	if err := query.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, grant.Tenant()).Scan(&exists); err != nil {
		return auth.Principal{}, apiError(503, "tenant authority unavailable")
	}
	if !exists {
		return auth.Principal{}, apiError(403, "tenant authority unavailable")
	}
	if grant.AllProjects() {
		rows, err := query.QueryContext(ctx, `SELECT id FROM projects WHERE tenant_id=$1 ORDER BY id`, grant.Tenant())
		if err != nil {
			return auth.Principal{}, err
		}
		defer rows.Close()
		actor.Projects = []string{}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return auth.Principal{}, err
			}
			actor.Projects = append(actor.Projects, id)
		}
		if err := rows.Err(); err != nil {
			return auth.Principal{}, err
		}
	} else {
		for _, project := range actor.Projects {
			var tenant string
			if err := query.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant); err != nil || tenant != grant.Tenant() {
				return auth.Principal{}, apiError(403, "signed project is outside the authorized tenant")
			}
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
