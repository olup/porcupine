package porcupine

import (
	"context"
	"database/sql"
)

// importManager owns upload manifests, checkpoints and terminal states. The
// journal and hosted backends share the same HTTP contract.
type importManager struct {
	db              *sql.DB
	control         oauthQueryer
	rules           *eventRules
	deliveryEnabled bool
	hosted          *hostedLiveFlusher
}

func (m *importManager) authorityQuery(tx *sql.Tx) oauthQueryer {
	if m.control != nil {
		return m.control
	}
	return tx
}

func (m *importManager) projectExists(ctx context.Context, id string) error {
	var exists int
	if err := m.db.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=$1`, id).Scan(&exists); err != nil {
		return apiError(503, "project storage unavailable")
	}
	if exists == 0 {
		return apiError(404, "project not found")
	}
	return nil
}
