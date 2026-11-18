package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
)

type hostedCookielessKeys struct{ db *sql.DB }

func (k hostedCookielessKeys) CookielessKey(ctx context.Context, day string) ([]byte, error) {
	var latest sql.NullString
	shared, err := k.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer shared.Rollback()
	if _, err = shared.ExecContext(ctx, `SELECT pg_advisory_xact_lock(1886352211,3)`); err != nil {
		return nil, err
	}
	if err = shared.QueryRowContext(ctx, `SELECT max(day) FROM shared_cookieless_keys`).Scan(&latest); err != nil {
		return nil, err
	}
	if latest.Valid && latest.String > day {
		return nil, errors.New("cookieless clock moved before the active identity day")
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return nil, err
	}
	if _, err = shared.ExecContext(ctx, `INSERT INTO shared_cookieless_keys(day,secret) VALUES($1,$2) ON CONFLICT DO NOTHING`, day, secret); err != nil {
		return nil, err
	}
	if err = shared.QueryRowContext(ctx, `SELECT secret FROM shared_cookieless_keys WHERE day=$1`, day).Scan(&secret); err != nil {
		return nil, err
	}
	if _, err = shared.ExecContext(ctx, `DELETE FROM shared_cookieless_keys WHERE day<$1`, day); err != nil {
		return nil, err
	}
	if err = shared.Commit(); err != nil {
		return nil, err
	}
	return secret, nil
}
