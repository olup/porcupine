package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
)

func (s *Service) initializeOIDCHumans(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS oidc_humans (principal_id TEXT PRIMARY KEY REFERENCES access_keys(id),issuer TEXT NOT NULL,subject TEXT NOT NULL,UNIQUE(issuer,subject));`)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
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
		if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,?) WHERE id=?`, now(), id); err != nil {
			return err
		}
	}
	if s.oauth == nil || s.oauth.oidc == nil {
		return tx.Commit()
	}
	for subject, grant := range s.oauth.oidc.bindings {
		var id string
		err = tx.QueryRowContext(ctx, `SELECT principal_id FROM oidc_humans WHERE issuer=? AND subject=?`, s.oauth.oidc.issuer, subject).Scan(&id)
		permissions, _ := json.Marshal(grant.Permissions)
		projects, _ := json.Marshal(grant.Projects)
		if errors.Is(err, sql.ErrNoRows) {
			id = newID()
			_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES(?,'OIDC human',?,?,?,?)`, id, hash("unusable-oidc-"+newID()+newID()), string(permissions), string(projects), now())
			if err == nil {
				_, err = tx.ExecContext(ctx, `INSERT INTO oidc_humans(principal_id,issuer,subject) VALUES(?,?,?)`, id, s.oauth.oidc.issuer, subject)
			}
		} else if err == nil {
			_, err = tx.ExecContext(ctx, `UPDATE access_keys SET permissions=?,projects=? WHERE id=?`, string(permissions), string(projects), id)
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
		human, err := loadEmailHuman(ctx, query, humanID)
		if err == nil || !errors.Is(err, sql.ErrNoRows) {
			return human, err
		}
	}
	if s.oauth != nil && s.oauth.config.LocalInvitations {
		return loadInvitedHuman(ctx, query, humanID)
	}
	if s.oauth == nil || s.oauth.oidc == nil {
		return result, errors.New("interactive authorization disabled")
	}
	err := query.QueryRowContext(ctx, `SELECT h.issuer,h.subject,k.id,k.permissions,k.projects FROM oidc_humans h JOIN access_keys k ON k.id=h.principal_id WHERE h.principal_id=? AND k.revoked_at IS NULL`, humanID).Scan(&issuer, &subject, &result.ID, &permissions, &projects)
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
	if err := s.db.QueryRowContext(ctx, `SELECT principal_id FROM oidc_humans WHERE issuer=? AND subject=?`, s.oauth.oidc.issuer, subject).Scan(&id); err != nil {
		return actor{}, err
	}
	return s.loadOAuthHuman(ctx, s.db, id)
}
