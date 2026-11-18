package porcupine

import (
	"context"
	"database/sql"
	"errors"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

const hostedProjectLifecycleSchema = `CREATE TABLE hosted_project_activity (
project_id TEXT NOT NULL, activity_id TEXT NOT NULL,
started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
PRIMARY KEY (project_id, activity_id));
CREATE TABLE hosted_project_seals (
project_id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL,
source_id TEXT NOT NULL, catalog_id TEXT NOT NULL, stream_id TEXT NOT NULL,
final_sequence BIGINT NOT NULL CHECK(final_sequence >= 0), proof_sha TEXT NOT NULL,
sealed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
CHECK ((final_sequence = 0 AND proof_sha = '') OR (final_sequence > 0 AND length(proof_sha) = 64)));`

var (
	errHostedProjectFenced = errors.New("hosted project is fenced")
	errHostedProjectBusy   = errors.New("hosted project still has incomplete operations")
)

// Take the same tenant/project locks as admission and deletion. Read tombstones
// in a new statement so that a waiter sees fences committed while it was blocked.
func lockActiveHostedProject(ctx context.Context, tx *sql.Tx, project string) (string, error) {
	var tenant, locked string
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant); err != nil {
		return "", hostedProjectLookupError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id=$1 FOR SHARE`, tenant).Scan(&locked); err != nil {
		return "", hostedProjectLookupError(err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1 FOR SHARE`, project).Scan(&locked); err != nil {
		return "", hostedProjectLookupError(err)
	}
	if tenant != locked {
		return "", errHostedProjectFenced
	}
	var fenced bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)
OR EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=$2)`, project, tenant).Scan(&fenced)
	if err == nil && fenced {
		return "", errHostedProjectFenced
	}
	return tenant, err
}

func hostedProjectLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return errHostedProjectFenced
	}
	return err
}

// Register in the dispatch transaction, before starting the native or external
// call. Reusing a ticket would make one return stand for two operations. The
// ticket lives until acknowledged or until its incarnation lease is reclaimed.
func (i *hostedIncarnation) registerTx(ctx context.Context, tx *sql.Tx, project, activityID string) (err error) {
	if activityID == "" {
		return errors.New("hosted project activity requires a unique identifier")
	}
	if _, err := lockActiveHostedProject(ctx, tx, project); err != nil {
		return err
	}
	incarnation, err := i.reserve(project, activityID)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			i.release(project, activityID)
		}
	}()
	result, err := tx.ExecContext(ctx, `INSERT INTO hosted_project_activity(project_id,activity_id,incarnation_id)
SELECT $1,$2,NULLIF($3,'') WHERE `+hostedIncarnationCurrent("$3"), project, activityID, incarnation)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n != 1 {
		return errors.Join(err, errHostedIncarnationLost)
	}
	return nil
}

// hostedIncarnationCurrent checks the lease named by a text parameter. An
// empty identifier stands for an unleased fixture ticket.
func hostedIncarnationCurrent(param string) string {
	return `(` + param + `='' OR EXISTS(SELECT 1 FROM hosted_worker_incarnations
WHERE incarnation_id=` + param + ` AND expires_at>clock_timestamp()))`
}

// Call only after actual return, or when dispatch never happened. A missed
// acknowledgement is safe to retry. A ticket whose incarnation lease was
// reclaimed is already gone, so the retry deletes nothing.
func finishHostedProjectActivity(ctx context.Context, db *sql.DB, project, activityID string) error {
	_, err := db.ExecContext(ctx, `DELETE FROM hosted_project_activity WHERE project_id=$1 AND activity_id=$2`, project, activityID)
	return err
}

func (i *hostedIncarnation) beginActivity(ctx context.Context, db *sql.DB, project, activityID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := i.registerTx(ctx, tx, project, activityID); err != nil {
		return err
	}
	return tx.Commit()
}

func endHostedProjectActivity(ctx context.Context, db *sql.DB, project, activityID string) error {
	bounded, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedQueryControlTimeout)
	defer cancel()
	return finishHostedProjectActivity(bounded, db, project, activityID)
}

// Run the whole operation under one ticket so cold opens, reconciliation, and
// object access before a downstream dispatch each participate in the drain.
func (f *hostedLiveFlusher) runActiveHostedProject(ctx context.Context, session *lakeapi.ProjectWriteSession, project string, run func(context.Context) error) (err error) {
	ticket := eventapi.NewID()
	defer func() { err = errors.Join(err, f.finishProjectActivity(ctx, project, ticket)) }()
	if err := f.incarnation.beginActivity(ctx, f.publisher.control, project, ticket); err != nil {
		return err
	}
	return session.RunNative(ctx, run)
}

type returnedHostedProjectActivity struct {
	project string
	ticket  string
}

// Remember actual return when control is unavailable. Recovery can retry the
// acknowledgement while this executor lives. The lease frees tickets only
// after the process stops, so a live worker still needs this retry.
func (f *hostedLiveFlusher) finishProjectActivity(ctx context.Context, project, ticket string) error {
	f.incarnation.release(project, ticket)
	err := endHostedProjectActivity(ctx, f.publisher.control, project, ticket)
	if err != nil {
		f.returnedActivities.Store(returnedHostedProjectActivity{project: project, ticket: ticket}, struct{}{})
	}
	return err
}

func (f *hostedLiveFlusher) flushReturnedProjectActivities(ctx context.Context) {
	f.returnedActivities.Range(func(key, _ any) bool {
		activity := key.(returnedHostedProjectActivity)
		if err := finishHostedProjectActivity(ctx, f.publisher.control, activity.project, activity.ticket); err != nil {
			return false
		}
		f.returnedActivities.Delete(key)
		return true
	})
}
