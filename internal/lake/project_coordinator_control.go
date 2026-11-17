package lake

import (
	"context"
	"database/sql"
)

// RunControlTx commits metadata on the connection that owns the project lock.
// A lost physical session aborts both the transaction and its advisory lock.
// The callback must only perform bounded control reads/writes on tx.
func (s *ProjectWriteSession) RunControlTx(ctx context.Context, work func(context.Context, *sql.Tx) error) error {
	if work == nil {
		return ErrProjectWriteFenceStale
	}
	return s.RunNative(ctx, func(ctx context.Context) error {
		s.operations.Lock()
		defer s.operations.Unlock()
		tx, err := s.conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var pid int
		if err := tx.QueryRowContext(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
			return err
		}
		if pid != s.pid {
			s.cancel(ErrProjectWriteSessionLost)
			return ErrProjectWriteSessionLost
		}
		if err := work(ctx, tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}
