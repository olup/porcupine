package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
)

func (s *Service) initializeOIDCHumans(ctx context.Context) error {
	var err error
	if s.shared == nil {
		_, err = s.control.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oidc_humans (principal_id TEXT PRIMARY KEY REFERENCES access_keys(id),issuer TEXT NOT NULL,subject TEXT NOT NULL,UNIQUE(issuer,subject));`)
		if err != nil {
			return err
		}
	}
	if blocked, err := s.restoredIdentityBootstrapBlocked(ctx); err != nil || blocked {
		return err
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT principal_id,issuer,subject FROM oidc_humans`)
	if err != nil {
		return err
	}
	var removed []string
	for rows.Next() {
		var id, issuer, subject string
		if err := rows.Scan(&id, &issuer, &subject); err != nil {
			rows.Close()
			return err
		}
		current := false
		if s.oauth != nil && s.oauth.oidc != nil && issuer == s.oauth.oidc.issuer {
			_, current = s.oauth.oidc.bindings[subject]
		}
		if !current {
			removed = append(removed, id)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, id := range removed {
		if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,$1) WHERE id=$2`, now(), id); err != nil {
			return err
		}
	}
	if s.oauth == nil || s.oauth.oidc == nil {
		return tx.Commit()
	}
	for subject, grant := range s.oauth.oidc.bindings {
		var id string
		err = tx.QueryRowContext(ctx, `SELECT principal_id FROM oidc_humans WHERE issuer=$1 AND subject=$2`, s.oauth.oidc.issuer, subject).Scan(&id)
		permissions, _ := json.Marshal(grant.Permissions)
		// A deletion tombstone remains authoritative over static OIDC bindings.
		projectsList := make([]string, 0, len(grant.Projects))
		for _, project := range grant.Projects {
			var deleted bool
			if filterErr := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)`, project).Scan(&deleted); filterErr != nil {
				return filterErr
			}
			if !deleted {
				projectsList = append(projectsList, project)
			}
		}
		encodedProjects, _ := json.Marshal(projectsList)
		projects := string(encodedProjects)
		if errors.Is(err, sql.ErrNoRows) {
			id = newID()
			_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'OIDC human',$2,$3,$4,$5)`, id, hash("unusable-oidc-"+newID()+newID()), string(permissions), projects, now())
			if err == nil {
				_, err = tx.ExecContext(ctx, `INSERT INTO oidc_humans(principal_id,issuer,subject) VALUES($1,$2,$3)`, id, s.oauth.oidc.issuer, subject)
			}
		} else if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE access_keys SET permissions=$1,projects=$2 WHERE id=$3`, string(permissions), projects, id)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) loadOAuthHuman(ctx context.Context, query oauthQueryer, humanID string) (actor, error) {
	var subject, issuer, permissions, projects string
	var result actor
	if s.oauth != nil && s.oauth.config.Email != nil {
		return loadEmailHuman(ctx, query, humanID)
	}
	if s.oauth == nil || s.oauth.oidc == nil {
		return result, errors.New("interactive authorization disabled")
	}
	err := query.QueryRowContext(ctx, `SELECT h.issuer,h.subject,k.id,k.permissions,k.projects FROM oidc_humans h JOIN access_keys k ON k.id=h.principal_id WHERE h.principal_id=$1 AND k.revoked_at IS NULL`, humanID).Scan(&issuer, &subject, &result.ID, &permissions, &projects)
	if err != nil {
		return actor{}, err
	}
	configured, exists := s.oauth.oidc.bindings[subject]
	if !exists || issuer != s.oauth.oidc.issuer {
		return actor{}, errors.New("OIDC human binding removed")
	}
	if json.Unmarshal([]byte(permissions), &result.Permissions) != nil || json.Unmarshal([]byte(projects), &result.Projects) != nil {
		return actor{}, errors.New("invalid human grants")
	}
	result.Permissions = slices.DeleteFunc(result.Permissions, func(p string) bool { return !slices.Contains(configured.Permissions, p) })
	result.Projects = slices.DeleteFunc(result.Projects, func(p string) bool { return !slices.Contains(configured.Projects, p) })
	if len(result.Permissions) == 0 || len(result.Projects) == 0 {
		return actor{}, errors.New("human has no current grant")
	}
	return result, nil
}

func (s *Service) oidcHuman(ctx context.Context, subject string) (actor, error) {
	var id string
	if err := s.control.QueryRowContext(ctx, `SELECT principal_id FROM oidc_humans WHERE issuer=$1 AND subject=$2`, s.oauth.oidc.issuer, subject).Scan(&id); err != nil {
		return actor{}, err
	}
	return s.loadOAuthHuman(ctx, s.control, id)
}
