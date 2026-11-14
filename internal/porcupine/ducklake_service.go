package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (s *Service) initializeLakeSource(ctx context.Context) error {
	var expected string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='ducklake_source_id'").Scan(&expected)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if s.lake == nil {
		if expected != "" {
			return errors.New("this SQLite database requires its configured DuckLake store")
		}
		return nil
	}
	var foreignBackendState bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM metadata WHERE key IN ('projection_table_uuid','query_master_key'))").Scan(&foreignBackendState); err != nil {
		return err
	}
	if foreignBackendState {
		return errors.New("DuckLake requires a dedicated SQLite database; automatic migration from another analytical backend is not implemented")
	}
	actual, err := s.lake.SourceID(ctx)
	if err != nil || actual == "" || (expected != "" && expected != actual) {
		return errors.New("DuckLake archive identity does not match the acceptance database")
	}
	if expected == "" {
		_, err = s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('ducklake_source_id',?)", actual)
	}
	return err
}

func (s *Service) startLakeMaintenance(interval time.Duration) {
	if interval == 0 {
		interval = 2 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.lakeMaintenanceCancel, s.lakeMaintenanceDone = cancel, make(chan struct{})
	go func() {
		defer close(s.lakeMaintenanceDone)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				maintenance, stop := context.WithTimeout(ctx, 30*time.Second)
				err := s.lake.Maintain(maintenance)
				stop()
				if ctx.Err() != nil {
					return
				}
				if err != nil {
					s.db.ExecContext(ctx, "INSERT INTO metadata(key,value) VALUES('ducklake_maintenance_error','maintenance failed') ON CONFLICT(key) DO UPDATE SET value=excluded.value")
				} else {
					s.db.ExecContext(ctx, "DELETE FROM metadata WHERE key='ducklake_maintenance_error'")
				}
				timer.Reset(interval)
			}
		}
	}()
}

func (s *Service) applyAnalyticalProjections(ctx context.Context, projections []eventProjection) error {
	if s.lake == nil {
		return errors.New("analytical delivery is disabled")
	}
	return s.lake.Apply(ctx, projections)
}
func (s *Service) loadAnalyticalProjection(ctx context.Context, project, uuid string) (eventProjection, error) {
	if s.lake == nil {
		return eventProjection{}, errEventNotFound
	}
	return s.lake.Load(ctx, project, uuid)
}
