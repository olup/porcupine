package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
)

func validateLocalDatabasePaths(journal, control string) error {
	if journal == ":memory:" || control == ":memory:" {
		if journal != control {
			return errors.New("local control and journal must both be persistent or both in memory")
		}
		return nil
	}
	first, err := filepath.Abs(journal)
	if err != nil {
		return err
	}
	second, err := filepath.Abs(control)
	if err != nil {
		return err
	}
	if first == second {
		return errors.New("control and ingestion journal require distinct SQLite files")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal", ".writer.lock", ".projection.lock"} {
		if first+suffix == second || second+suffix == first {
			return errors.New("control and ingestion journal must not overlap SQLite auxiliary files")
		}
	}
	left, leftErr := os.Stat(first)
	right, rightErr := os.Stat(second)
	if leftErr == nil && rightErr == nil && os.SameFile(left, right) {
		return errors.New("control and ingestion journal refer to the same SQLite file")
	}
	return nil
}

func localControlPath(journal string) string {
	if journal == ":memory:" {
		return ":memory:"
	}
	return journal + ".control"
}

func openLocalSQLite(path string) (*sql.DB, error) {
	dsn := path
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(absolute), 0700); err != nil {
			return nil, err
		}
		dsn = (&url.URL{Scheme: "file", Path: absolute}).String()
	}
	pragmas := url.Values{"_pragma": {"journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", "busy_timeout(5000)"}}
	db, err := sql.Open("sqlite", dsn+"?"+pragmas.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	if path != ":memory:" {
		if err := os.Chmod(path, 0600); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

func initializeLocalControl(ctx context.Context, db *sql.DB, bootstrap string, queries InteractiveQueryConfig) error {
	if err := migrateSQLite(ctx, db, localControlSchema); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var initialized string
	err = tx.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='initialized'`).Scan(&initialized)
	if errors.Is(err, sql.ErrNoRows) {
		if len(bootstrap) < 16 {
			return errors.New("PORCUPINE_BOOTSTRAP_TOKEN must contain at least 16 characters on first control initialization")
		}
		id := newID()
		if _, err := tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'bootstrap admin',$2,'["instance:admin"]','["*"]',$3)`, id, hash(bootstrap), now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('initialized','1'),('oauth_bootstrap_id',$1),('local_control_source_id',$2)`, id, newID()); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenants(id,name,created_at,ingestion_allowed,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES($1,'Default',$2,1,$3,$4,$5,$6) ON CONFLICT(id) DO NOTHING`, defaultTenantID, now(), queries.MaxConcurrent, queries.TimeoutMS, queries.MaxRows, queries.MaxResultBytes); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) bindLocalControl(ctx context.Context) error {
	var source string
	if err := s.control.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='local_control_source_id'`).Scan(&source); err != nil || source == "" {
		return errors.New("local control identity unavailable")
	}
	var journal string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='journal_source_id'`).Scan(&journal)
	if errors.Is(err, sql.ErrNoRows) {
		journal = newID()
		if _, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('journal_source_id',$1)`, journal); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := s.control.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('local_journal_source_id',$1) ON CONFLICT(key) DO NOTHING`, journal); err != nil {
		return err
	}
	var paired string
	if err := s.control.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='local_journal_source_id'`).Scan(&paired); err != nil || paired != journal {
		return errors.New("local control belongs to another ingestion journal; restore both databases together")
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('local_control_source_id',$1) ON CONFLICT(key) DO NOTHING`, source); err != nil {
		return err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key='local_control_source_id'`).Scan(&paired); err != nil || paired != source {
		return errors.New("ingestion journal belongs to another local control database; restore both databases together")
	}
	return nil
}

// A standalone worker refreshes authoritative policy for every acceptance batch.
// The admission lock covers this refresh and the journal commit, so competing
// requests cannot replace a newer policy snapshot with an older one.
func (s *Service) prepareLocalAcceptance(ctx context.Context, project string) error {
	return s.ensureProjectLocal(ctx, project, actor{})
}

// The journal is the durable accounting ledger. Control holds only its replayable
// high-water aggregate; a failed publication never loses accepted consumption.
func (s *Service) publishLocalUsage(ctx context.Context) error {
	if s.shared != nil {
		return nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,period_start,accepted_events FROM tenant_usage ORDER BY tenant_id,period_start`)
	if err != nil {
		return err
	}
	type usage struct {
		tenant, period string
		count          int64
	}
	var reports []usage
	for rows.Next() {
		var report usage
		if err := rows.Scan(&report.tenant, &report.period, &report.count); err != nil {
			rows.Close()
			return err
		}
		reports = append(reports, report)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(reports) == 0 {
		return err
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, report := range reports {
		if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_usage(tenant_id,period_start,accepted_events) VALUES($1,$2,$3)
ON CONFLICT(tenant_id,period_start) DO UPDATE SET accepted_events=CASE WHEN tenant_usage.accepted_events<excluded.accepted_events THEN excluded.accepted_events ELSE tenant_usage.accepted_events END`, report.tenant, report.period, report.count); err != nil {
			return err
		}
	}
	return tx.Commit()
}
