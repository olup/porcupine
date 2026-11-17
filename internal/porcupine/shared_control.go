package porcupine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed control_schema.sql
var controlSchema string

//go:embed shared_control_schema.sql
var sharedControlSchema string

// Both operational schemas store flags as integers. Normalize only parameter
// values; statements and transaction semantics are never translated at runtime.
type sharedPostgresConnector struct{ driver.Connector }
type sharedPostgresConn struct{ *stdlib.Conn }

func (c sharedPostgresConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	postgres, ok := conn.(*stdlib.Conn)
	if !ok {
		conn.Close()
		return nil, errors.New("unexpected PostgreSQL connector")
	}
	return &sharedPostgresConn{Conn: postgres}, nil
}

func (c *sharedPostgresConn) CheckNamedValue(value *driver.NamedValue) error {
	if flag, ok := value.Value.(bool); ok {
		value.Value = int64(0)
		if flag {
			value.Value = int64(1)
		}
	}
	return c.Conn.CheckNamedValue(value)
}

func openSharedControl(ctx context.Context, config SharedConfig) (*sql.DB, error) {
	if strings.TrimSpace(config.DatabaseURL) == "" {
		return nil, errors.New("shared PostgreSQL connection configuration is required")
	}
	connection, err := pgx.ParseConfig(config.DatabaseURL)
	if err != nil {
		return nil, errors.New("invalid shared PostgreSQL connection configuration")
	}
	db := sql.OpenDB(sharedPostgresConnector{Connector: stdlib.GetConnector(*connection)})
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("shared PostgreSQL connection unavailable: %w", err)
	}
	return db, nil
}

// Serialize schema installation and initial credentials across all processes.
// The lock is transactional, so a failed bootstrap leaves no partial authority.
func initializeSharedControl(ctx context.Context, db *sql.DB, bootstrapToken string, queries InteractiveQueryConfig) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(5786939852849845331)`); err != nil {
		return err
	}
	if err := migrateSharedControl(ctx, tx); err != nil {
		return err
	}
	var initialized string
	err = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='initialized'`).Scan(&initialized)
	if errors.Is(err, sql.ErrNoRows) {
		if len(bootstrapToken) < 16 {
			return errors.New("PORCUPINE_BOOTSTRAP_TOKEN must contain at least 16 characters on first shared initialization")
		}
		id := newID()
		if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'bootstrap admin',$2,'["instance:admin"]','["*"]',$3)`, id, hash(bootstrapToken), now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('initialized','1'),('oauth_bootstrap_id',$1)`, id); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenants(id,name,created_at,ingestion_allowed,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES($1,'Default',$2,1,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, defaultTenantID, now(), queries.MaxConcurrent, queries.TimeoutMS, queries.MaxRows, queries.MaxResultBytes); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('shared_source_id',$1) ON CONFLICT(key) DO NOTHING`, newID()); err != nil {
		return err
	}
	return tx.Commit()
}

func sharedColumnExists(ctx context.Context, query oauthQueryer, table, column string) (bool, error) {
	var exists bool
	err := query.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 AND column_name=$2)`, table, column).Scan(&exists)
	return exists, err
}
