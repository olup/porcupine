package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"strings"
)

// SharedConfig enables a PostgreSQL metadata/control store. Event payloads and
// project-local jobs remain in the owner's SQLite database before DuckLake.
type SharedConfig struct {
	DatabaseURL   string
	WorkerID      string
	RouteURL      string
	RoutingSecret []byte
}

type sharedRuntime struct {
	config  SharedConfig
	routing *sharedRoutingState
}

func (s *Service) bindSharedControl(ctx context.Context) error {
	var source string
	if err := s.control.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='shared_source_id'`).Scan(&source); err != nil || source == "" {
		return errors.New("shared control identity unavailable")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous string
	err = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='shared_control_source_id'`).Scan(&previous)
	if err == nil && previous != source {
		return errors.New("this worker journal belongs to a different shared control instance")
	}
	if errors.Is(err, sql.ErrNoRows) {
		var populated bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects)`).Scan(&populated); err != nil {
			return err
		}
		if populated {
			return errors.New("shared control requires a fresh worker journal; local data is not migrated")
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('shared_control_source_id',$1)`, source); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return tx.Commit()
}

func validateSharedConfiguration(c Config) error {
	if c.Shared == nil {
		if c.DuckLake != nil && c.DuckLake.PostgresURL != "" {
			return errors.New("PostgreSQL DuckLake metadata requires shared control configuration")
		}
		return nil
	}
	u, err := url.Parse(c.Shared.DatabaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return errors.New("shared control requires a PostgreSQL URL")
	}
	if strings.TrimSpace(c.Shared.WorkerID) == "" || len(c.Shared.WorkerID) > 128 || c.Shared.RouteURL == "" || len(c.Shared.RoutingSecret) < 32 {
		return errors.New("shared control requires a worker ID, internal route URL and routing secret of at least 32 bytes")
	}
	if c.NativeBackup != nil {
		return errors.New("local full-instance backup is unavailable with shared control; protect PostgreSQL, S3 and each worker volume separately")
	}
	if c.DBPath == ":memory:" {
		return errors.New("shared deployment requires a persistent SQLite worker volume")
	}
	if c.DuckLake != nil && (c.DuckLake.PostgresURL == "" || c.DuckLake.S3 == nil) {
		return errors.New("shared analytics requires PostgreSQL DuckLake metadata and S3 data storage")
	}
	return nil
}
