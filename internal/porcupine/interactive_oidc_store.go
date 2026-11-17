package porcupine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

func (s *Service) loadOAuthHuman(ctx context.Context, query oauthQueryer, humanID string) (auth.Principal, error) {
	if s.oauth == nil || s.oauth.browser == nil {
		return auth.Principal{}, errors.New("interactive authorization disabled")
	}
	return loadEmailHuman(ctx, query, humanID)
}

func (s *Service) oidcHuman(ctx context.Context, subject, email string) (authenticatedHuman, error) {
	if s.oauth == nil || s.oauth.oidc == nil || subject == "" || len(subject) > 512 {
		return authenticatedHuman{}, errors.New("identity provider login unavailable")
	}
	email, err := auth.NormalizeEmail(email)
	if err != nil {
		return authenticatedHuman{}, errors.New("invalid verified identity")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return authenticatedHuman{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return authenticatedHuman{}, err
	}
	var id, linkedEmail, invitedEmail string
	err = tx.QueryRowContext(ctx, `SELECT i.principal_id,i.linked_email,h.email
FROM oidc_identities i JOIN email_humans h ON h.principal_id=i.principal_id
WHERE i.issuer=$1 AND i.subject=$2`, s.oauth.oidc.provider.Issuer(), subject).Scan(&id, &linkedEmail, &invitedEmail)
	switch {
	case err == nil:
		if email != linkedEmail || email != invitedEmail {
			return authenticatedHuman{}, errors.New("provider identity does not match the invited account")
		}
	case errors.Is(err, sql.ErrNoRows):
		err = tx.QueryRowContext(ctx, `SELECT h.principal_id FROM email_humans h
JOIN access_keys k ON k.id=h.principal_id WHERE h.email=$1 AND k.revoked_at IS NULL`, email).Scan(&id)
		if err != nil {
			return authenticatedHuman{}, errors.New("account is not authorized")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO oidc_identities(id,issuer,subject,principal_id,linked_email,created_at)
VALUES($1,$2,$3,$4,$5,$6)`, newID(), s.oauth.oidc.provider.Issuer(), subject, id, email, now()); err != nil {
			return authenticatedHuman{}, errors.New("provider identity cannot be linked")
		}
	default:
		return authenticatedHuman{}, err
	}
	human, err := loadEmailHuman(ctx, tx, id)
	if err != nil {
		return authenticatedHuman{}, err
	}
	var version int64
	if err := tx.QueryRowContext(ctx, `SELECT login_version FROM email_humans WHERE principal_id=$1`, id).Scan(&version); err != nil {
		return authenticatedHuman{}, err
	}
	if err := tx.Commit(); err != nil {
		return authenticatedHuman{}, err
	}
	return authenticatedHuman{Principal: human, proof: humanLoginProof{Version: version, Verified: true}}, nil
}

type humanLoginProof struct {
	Version  int64 `json:"version"`
	Verified bool  `json:"verified"`
}

type authenticatedHuman struct {
	auth.Principal
	proof humanLoginProof
}

func checkHumanLoginProof(ctx context.Context, query oauthQueryer, humanID string, proof humanLoginProof) error {
	if humanID == "" || !proof.Verified {
		return errors.New("human sign-in required")
	}
	var version int64
	if err := query.QueryRowContext(ctx, `SELECT login_version FROM email_humans WHERE principal_id=$1`, humanID).Scan(&version); err != nil {
		return err
	}
	if version != proof.Version {
		return errors.New("human sign-in was invalidated; restart sign-in")
	}
	return nil
}
