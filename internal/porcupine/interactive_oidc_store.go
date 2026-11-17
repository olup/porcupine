package porcupine

import (
	"context"
	"database/sql"
	"errors"
)

func (s *Service) loadOAuthHuman(ctx context.Context, query oauthQueryer, humanID string) (actor, error) {
	if s.oauth == nil || s.oauth.browser == nil {
		return actor{}, errors.New("interactive authorization disabled")
	}
	return loadEmailHuman(ctx, query, humanID)
}

func (s *Service) oidcHuman(ctx context.Context, subject, email string) (actor, error) {
	if s.oauth == nil || s.oauth.oidc == nil || subject == "" || len(subject) > 512 {
		return actor{}, errors.New("identity provider login unavailable")
	}
	email, err := normalizeEmail(email)
	if err != nil {
		return actor{}, errors.New("invalid verified identity")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return actor{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return actor{}, err
	}
	var id, linkedEmail, invitedEmail string
	err = tx.QueryRowContext(ctx, `SELECT i.principal_id,i.linked_email,h.email
FROM oidc_identities i JOIN email_humans h ON h.principal_id=i.principal_id
WHERE i.issuer=$1 AND i.subject=$2`, s.oauth.oidc.issuer, subject).Scan(&id, &linkedEmail, &invitedEmail)
	switch {
	case err == nil:
		if email != linkedEmail || email != invitedEmail {
			return actor{}, errors.New("provider identity does not match the invited account")
		}
	case errors.Is(err, sql.ErrNoRows):
		err = tx.QueryRowContext(ctx, `SELECT h.principal_id FROM email_humans h
JOIN access_keys k ON k.id=h.principal_id WHERE h.email=$1 AND k.revoked_at IS NULL`, email).Scan(&id)
		if err != nil {
			return actor{}, errors.New("account is not authorized")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO oidc_identities(issuer,subject,principal_id,linked_email,created_at)
VALUES($1,$2,$3,$4,$5)`, s.oauth.oidc.issuer, subject, id, email, now()); err != nil {
			return actor{}, errors.New("provider identity cannot be linked")
		}
	default:
		return actor{}, err
	}
	human, err := loadEmailHuman(ctx, tx, id)
	if err != nil {
		return actor{}, err
	}
	if err := tx.Commit(); err != nil {
		return actor{}, err
	}
	return human, nil
}
