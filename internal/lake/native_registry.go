package lake

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/event"
)

type nativeProjectRecord struct {
	ProjectID, CatalogID string
	Ready, Deleted       bool
}

type nativeCleanupAttempt struct {
	ProjectID, ID string
}

type nativeCleanupSummary struct {
	Registered, Completed, Failed, Running int
	LastAttempt, LastSuccess               int64
}

// lookup never creates a catalog: proof recovery must not turn an unknown
// project into a new writable stream. ensure is only for authorized bootstrap.
type nativeRegistry interface {
	sourceID(context.Context) (string, error)
	lookup(context.Context, string) (nativeProjectRecord, error)
	scan(context.Context, string, int) ([]nativeProjectRecord, error)
	ensure(context.Context, string) (nativeProjectRecord, error)
	markReady(context.Context, string) error
	deleting(context.Context, string) (bool, error)
	claimCleanup(context.Context, time.Time, time.Time) (nativeCleanupAttempt, error)
	finishCleanup(context.Context, nativeCleanupAttempt, string, time.Time, time.Time) error
	cleanupSummary(context.Context) (nativeCleanupSummary, error)
}

type sqliteNativeRegistry struct{ db *sql.DB }

func (r *sqliteNativeRegistry) sourceID(ctx context.Context) (string, error) {
	var id string
	err := r.db.QueryRowContext(ctx, "SELECT id FROM source").Scan(&id)
	return id, err
}
func (r *sqliteNativeRegistry) lookup(ctx context.Context, project string) (nativeProjectRecord, error) {
	p := nativeProjectRecord{ProjectID: project}
	err := r.db.QueryRowContext(ctx, `SELECT id,ready,EXISTS(SELECT 1 FROM deleting_projects WHERE project_id=?) FROM catalogs WHERE project_id=?`, project, project).Scan(&p.CatalogID, &p.Ready, &p.Deleted)
	return p, err
}
func (r *sqliteNativeRegistry) scan(ctx context.Context, after string, limit int) ([]nativeProjectRecord, error) {
	if limit < 1 || limit > 256 {
		return nil, errors.New("native registry page size must be between 1 and 256")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT project_id,id,ready,deleted FROM (
SELECT c.project_id,c.id,c.ready,EXISTS(SELECT 1 FROM deleting_projects d WHERE d.project_id=c.project_id) AS deleted FROM catalogs c
UNION ALL SELECT d.project_id,'',0,1 FROM deleting_projects d WHERE NOT EXISTS(SELECT 1 FROM catalogs c WHERE c.project_id=d.project_id))
WHERE project_id>? ORDER BY project_id LIMIT ?`, after, limit)
	return readNativeRegistryPage(rows, err)
}
func (r *sqliteNativeRegistry) ensure(ctx context.Context, project string) (nativeProjectRecord, error) {
	p, err := r.lookup(ctx, project)
	if errors.Is(err, sql.ErrNoRows) {
		p.CatalogID = event.NewID()
		_, err = r.db.ExecContext(ctx, "INSERT INTO catalogs(project_id,id) VALUES(?,?)", project, p.CatalogID)
	}
	return p, err
}
func (r *sqliteNativeRegistry) markReady(ctx context.Context, project string) error {
	_, err := r.db.ExecContext(ctx, "UPDATE catalogs SET ready=1 WHERE project_id=?", project)
	return err
}
func (r *sqliteNativeRegistry) deleting(ctx context.Context, project string) (bool, error) {
	var deleted bool
	err := r.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM deleting_projects WHERE project_id=?)", project).Scan(&deleted)
	return deleted, err
}
func (r *sqliteNativeRegistry) claimCleanup(ctx context.Context, at, next time.Time) (nativeCleanupAttempt, error) {
	var a nativeCleanupAttempt
	err := r.db.QueryRowContext(ctx, `SELECT c.project_id FROM catalogs c LEFT JOIN cleanup_progress p ON p.project_id=c.project_id
WHERE c.ready=1 AND NOT EXISTS(SELECT 1 FROM deleting_projects d WHERE d.project_id=c.project_id) AND (p.next_attempt IS NULL OR p.next_attempt<=?)
ORDER BY COALESCE(p.last_attempt,0),c.project_id LIMIT 1`, at.UnixMilli()).Scan(&a.ProjectID)
	if err != nil {
		return a, err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO cleanup_progress(project_id,next_attempt,last_attempt,state) VALUES(?,?,?,'running')
ON CONFLICT(project_id) DO UPDATE SET next_attempt=excluded.next_attempt,last_attempt=excluded.last_attempt,state='running'`, a.ProjectID, next.UnixMilli(), at.UnixMilli())
	return a, err
}
func (r *sqliteNativeRegistry) finishCleanup(ctx context.Context, a nativeCleanupAttempt, state string, completed, next time.Time) error {
	if state == "ok" {
		_, err := r.db.ExecContext(ctx, `UPDATE cleanup_progress SET state='ok',failed=0,last_success=?,next_attempt=? WHERE project_id=?`, completed.UnixMilli(), next.UnixMilli(), a.ProjectID)
		return err
	}
	_, err := r.db.ExecContext(ctx, `UPDATE cleanup_progress SET state=?,failed=CASE WHEN ?='failed' THEN 1 ELSE failed END WHERE project_id=?`, state, state, a.ProjectID)
	return err
}
func (r *sqliteNativeRegistry) cleanupSummary(ctx context.Context) (nativeCleanupSummary, error) {
	var s nativeCleanupSummary
	err := r.db.QueryRowContext(ctx, `SELECT count(*),COALESCE(sum(p.last_success>0),0),COALESCE(sum(p.failed),0),COALESCE(sum(p.state='running'),0),COALESCE(max(p.last_attempt),0),COALESCE(max(p.last_success),0)
FROM catalogs c LEFT JOIN cleanup_progress p ON p.project_id=c.project_id WHERE c.ready=1`).Scan(&s.Registered, &s.Completed, &s.Failed, &s.Running, &s.LastAttempt, &s.LastSuccess)
	return s, err
}

type postgresNativeRegistry struct {
	db                  *sql.DB
	source, fingerprint string
	timeout             time.Duration
}

func (r *postgresNativeRegistry) sourceID(ctx context.Context) (string, error) {
	timeout := r.timeout
	if timeout == 0 {
		timeout = time.Duration(defaultDuckLakeMetadataTimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var fingerprint string
	if err := r.db.QueryRowContext(ctx, `SELECT fingerprint FROM pc_native_sources WHERE source_id=$1`, r.source).Scan(&fingerprint); err != nil {
		return "", err
	}
	if fingerprint != r.fingerprint {
		return "", errors.New("shared analytical storage configuration changed")
	}
	return r.source, nil
}
func (r *postgresNativeRegistry) lookup(ctx context.Context, project string) (nativeProjectRecord, error) {
	p := nativeProjectRecord{ProjectID: project}
	err := r.db.QueryRowContext(ctx, `SELECT catalog_id,ready<>0,deleted<>0 FROM pc_native_projects WHERE source_id=$1 AND project_id=$2`, r.source, project).Scan(&p.CatalogID, &p.Ready, &p.Deleted)
	if err == nil && p.CatalogID != SharedNativeCatalogID(r.source, project) {
		return p, errors.New("shared analytical catalog identity changed")
	}
	return p, err
}
func (r *postgresNativeRegistry) scan(ctx context.Context, after string, limit int) ([]nativeProjectRecord, error) {
	if limit < 1 || limit > 256 {
		return nil, errors.New("native registry page size must be between 1 and 256")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT project_id,catalog_id,ready<>0,deleted<>0 FROM pc_native_projects WHERE source_id=$1 AND project_id>$2 ORDER BY project_id LIMIT $3`, r.source, after, limit)
	page, err := readNativeRegistryPage(rows, err)
	if err != nil {
		return nil, err
	}
	for _, record := range page {
		if record.CatalogID != SharedNativeCatalogID(r.source, record.ProjectID) {
			return nil, errors.New("shared analytical catalog identity changed")
		}
	}
	return page, nil
}

func readNativeRegistryPage(rows *sql.Rows, err error) ([]nativeProjectRecord, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var page []nativeProjectRecord
	for rows.Next() {
		var record nativeProjectRecord
		if err := rows.Scan(&record.ProjectID, &record.CatalogID, &record.Ready, &record.Deleted); err != nil {
			return nil, err
		}
		page = append(page, record)
	}
	return page, rows.Err()
}
func (r *postgresNativeRegistry) ensure(ctx context.Context, project string) (nativeProjectRecord, error) {
	_, err := r.db.ExecContext(ctx, `INSERT INTO pc_native_projects(source_id,project_id,catalog_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, r.source, project, SharedNativeCatalogID(r.source, project))
	if err != nil {
		return nativeProjectRecord{}, err
	}
	return r.lookup(ctx, project)
}
func (r *postgresNativeRegistry) markReady(ctx context.Context, project string) error {
	result, err := r.db.ExecContext(ctx, `UPDATE pc_native_projects SET ready=1 WHERE source_id=$1 AND project_id=$2 AND deleted=0`, r.source, project)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return errSharedNativeDeleted
	}
	return err
}
func (r *postgresNativeRegistry) deleting(ctx context.Context, project string) (bool, error) {
	p, err := r.lookup(ctx, project)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return p.Deleted, err
}
func (r *postgresNativeRegistry) claimCleanup(ctx context.Context, at, next time.Time) (nativeCleanupAttempt, error) {
	a := nativeCleanupAttempt{ID: event.NewID()}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return a, err
	}
	defer tx.Rollback()
	err = tx.QueryRowContext(ctx, `SELECT c.project_id FROM pc_native_projects c
LEFT JOIN pc_native_cleanup p ON p.source_id=c.source_id AND p.project_id=c.project_id
WHERE c.source_id=$1 AND c.ready=1 AND c.deleted=0 AND (p.next_attempt IS NULL OR p.next_attempt<=$2)
ORDER BY coalesce(p.last_attempt,0),c.project_id FOR UPDATE OF c SKIP LOCKED LIMIT 1`, r.source, at.UnixMilli()).Scan(&a.ProjectID)
	if err != nil {
		return a, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO pc_native_cleanup(source_id,project_id,attempt_id,next_attempt,last_attempt,state)
VALUES($1,$2,$3,$4,$5,'running') ON CONFLICT(source_id,project_id) DO UPDATE
SET attempt_id=excluded.attempt_id,next_attempt=excluded.next_attempt,last_attempt=excluded.last_attempt,state='running'
WHERE pc_native_cleanup.next_attempt<=$5`, r.source, a.ProjectID, a.ID, next.UnixMilli(), at.UnixMilli())
	if err != nil {
		return a, err
	}
	// A statement snapshot may precede another claim's commit even when its
	// project row becomes lockable later. Recheck the actual conflict row.
	count, err := result.RowsAffected()
	if err != nil {
		return a, err
	}
	if count != 1 {
		return a, sql.ErrNoRows
	}
	return a, tx.Commit()
}
func (r *postgresNativeRegistry) finishCleanup(ctx context.Context, a nativeCleanupAttempt, state string, completed, next time.Time) error {
	result, err := r.db.ExecContext(ctx, `UPDATE pc_native_cleanup SET state=$1,
failed=CASE WHEN $1='ok' THEN 0 WHEN $1='failed' THEN 1 ELSE failed END,
last_success=CASE WHEN $1='ok' THEN $2 ELSE last_success END,
next_attempt=CASE WHEN $1='ok' THEN $3 ELSE next_attempt END
WHERE source_id=$4 AND project_id=$5 AND attempt_id=$6 AND state='running'`, state, completed.UnixMilli(), next.UnixMilli(), r.source, a.ProjectID, a.ID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return errors.New("native cleanup attempt was superseded")
	}
	return err
}
func (r *postgresNativeRegistry) cleanupSummary(ctx context.Context) (nativeCleanupSummary, error) {
	var s nativeCleanupSummary
	err := r.db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE p.last_success>0),
coalesce(sum(p.failed),0),count(*) FILTER(WHERE p.state='running'),coalesce(max(p.last_attempt),0),coalesce(max(p.last_success),0)
FROM pc_native_projects c LEFT JOIN pc_native_cleanup p ON p.source_id=c.source_id AND p.project_id=c.project_id
WHERE c.source_id=$1 AND c.ready=1 AND c.deleted=0`, r.source).Scan(&s.Registered, &s.Completed, &s.Failed, &s.Running, &s.LastAttempt, &s.LastSuccess)
	return s, err
}
