package porcupine

import (
	"context"
	"database/sql"
	"errors"

	identityapi "github.com/olup/porcupine/internal/identity"
)

// The semantic evaluator reads the same caller-owned SQLite transaction that
// will accept the UUID and apply its canonical identity decision.
type sqliteIdentityReader struct {
	tx      *sql.Tx
	project string
}

func (r sqliteIdentityReader) Profile(ctx context.Context, id string) (identityapi.Profile, error) {
	var profile identityapi.Profile
	var raw string
	err := r.tx.QueryRowContext(ctx, `SELECT traits,is_identified FROM identity_profiles WHERE project_id=$1 AND identified_id=$2`, r.project, id).Scan(&raw, &profile.Identified)
	if errors.Is(err, sql.ErrNoRows) {
		return identityapi.Profile{}, nil
	}
	if err != nil {
		return identityapi.Profile{}, err
	}
	profile.Traits = []byte(raw)
	return profile, nil
}
func (r sqliteIdentityReader) Link(ctx context.Context, id string) (identityapi.Link, error) {
	var link identityapi.Link
	err := r.tx.QueryRowContext(ctx, `SELECT identified_id,version FROM identity_links WHERE project_id=$1 AND anonymous_id=$2`, r.project, id).Scan(&link.Target, &link.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return identityapi.Link{}, nil
	}
	return link, err
}
