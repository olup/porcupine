package porcupine

import (
	"context"
	"time"
)

// Wait for the renewable permits to release or expire. Hosted deletion also
// requires the separate actual-return barrier.
func (s *Service) waitSharedProjectQueries(ctx context.Context, project string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if _, err := s.control.ExecContext(ctx, `DELETE FROM shared_query_permits WHERE project_id=$1 AND expires_at<=clock_timestamp()`, project); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return apiError(503, "shared query state unavailable")
		}
		var active int
		if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM shared_query_permits WHERE project_id=$1`, project).Scan(&active); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return apiError(503, "shared query state unavailable")
		}
		if active == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
