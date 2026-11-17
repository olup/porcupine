package usage

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

type Store struct {
	DB         *sql.DB
	Postgres   bool
	Config     Config
	Clock      func() time.Time
	localBuild chan struct{}
}

func NewStore(db *sql.DB, postgres bool, c Config, clock func() time.Time) *Store {
	return &Store{DB: db, Postgres: postgres, Config: c, Clock: clock, localBuild: make(chan struct{}, 1)}
}
func Stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000000000Z") }
func validDay(day string) bool {
	t, err := time.Parse(time.RFC3339, day)
	return err == nil && Day(t) == day
}
func validID(v string) bool {
	if v == "" || len(v) > 128 {
		return false
	}
	for _, c := range v {
		if c != '-' && c != '_' && !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

// PublishSourceTotals is the durable seam for later native-commit reconciliation.
// It accepts cumulative state, never unproved event rows or volatile callbacks.
func (s *Store) PublishSourceTotals(ctx context.Context, reports []SourceTotals) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = s.PublishSourceTotalsTx(ctx, tx, reports); err != nil {
		return err
	}
	return tx.Commit()
}

// PublishSourceTotalsTx joins the caller's durable effects transaction. It does
// not commit; native evidence and the caller's checkpoint determine replay.
func (s *Store) PublishSourceTotalsTx(ctx context.Context, tx *sql.Tx, reports []SourceTotals) error {
	if len(reports) > 1000 {
		return ErrTooLarge
	}
	for _, r := range reports {
		if !validID(r.SourceID) || !validID(r.TenantID) || !validID(r.ProjectID) || !validDay(r.Day) || r.Revision < 1 || !r.Counters.Valid() || r.Unresolved < 0 || r.Unresolved > r.Counters.Queries {
			return errors.New("invalid cumulative usage report")
		}
		if r.LastObserved != "" {
			at, err := time.Parse(time.RFC3339Nano, r.LastObserved)
			if err != nil || Stamp(at) != r.LastObserved {
				return errors.New("invalid cumulative usage observation time")
			}
		}
		if r.Day < Day(s.Clock().AddDate(0, 0, -s.Config.RetentionDays)) {
			continue
		}
		args := []any{r.SourceID, r.TenantID, r.ProjectID, r.Day, r.Revision}
		for _, v := range r.Counters.Values() {
			args = append(args, v)
		}
		args = append(args, r.Unresolved, r.LastObserved)
		set := []string{"revision=excluded.revision", "unresolved_queries=excluded.unresolved_queries", "last_observed_at=excluded.last_observed_at"}
		where := []string{"usage_rollups.revision<excluded.revision"}
		for _, c := range columns {
			set = append(set, c+"=excluded."+c)
			where = append(where, "usage_rollups."+c+"<=excluded."+c)
		}
		q := `INSERT INTO usage_rollups(source_id,tenant_id,project_id,day_start,revision,` + strings.Join(columns, ",") + `,unresolved_queries,last_observed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(source_id,tenant_id,project_id,day_start) DO UPDATE SET ` + strings.Join(set, ",") + ` WHERE ` + strings.Join(where, " AND ")
		result, err := tx.ExecContext(ctx, q, args...)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n == 1 {
			continue
		}
		var old SourceTotals
		if err = tx.QueryRowContext(ctx, `SELECT revision,`+strings.Join(columns, ",")+`,unresolved_queries,last_observed_at FROM usage_rollups WHERE source_id=$1 AND tenant_id=$2 AND project_id=$3 AND day_start=$4`, r.SourceID, r.TenantID, r.ProjectID, r.Day).Scan(&old.Revision, &old.Counters.Live, &old.Counters.Import, &old.Counters.Queries, &old.Counters.QueryMicros, &old.Counters.ExportBytes, &old.Counters.PipelineBytes, &old.Counters.Webhooks, &old.Unresolved, &old.LastObserved); err != nil {
			return err
		}
		if old.Revision > r.Revision {
			continue
		}
		if old.Revision == r.Revision && old.Counters == r.Counters && old.Unresolved == r.Unresolved && old.LastObserved == r.LastObserved {
			continue
		}
		return ErrConflict
	}
	return nil
}
func (s *Store) RecordStorageSample(ctx context.Context, v StorageSample) error {
	return recordStorageSample(ctx, s.DB, v)
}
func (s *Store) RecordStorageSampleTx(ctx context.Context, tx *sql.Tx, v StorageSample) error {
	return recordStorageSample(ctx, tx, v)
}
func recordStorageSample(ctx context.Context, db interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, v StorageSample) error {
	if !validID(v.TenantID) || !validID(v.ProjectID) || !validDay(v.Day) || v.Bytes < 0 {
		return errors.New("invalid usage storage sample")
	}
	at, err := time.Parse(time.RFC3339Nano, v.ObservedAt)
	if err != nil || Day(at) != v.Day {
		return errors.New("invalid usage storage sample time")
	}
	// Recheck the control anchor when publishing the sample: a native read may
	// finish before deletion while its delayed publication arrives afterwards.
	_, err = db.ExecContext(ctx, `INSERT INTO usage_storage_samples(tenant_id,project_id,day_start,observed_at,bytes) SELECT $1,$2,$3,$4,$5 WHERE EXISTS(SELECT 1 FROM projects WHERE id=$2 AND tenant_id=$1) ON CONFLICT(tenant_id,project_id,day_start) DO UPDATE SET observed_at=excluded.observed_at,bytes=excluded.bytes WHERE usage_storage_samples.observed_at<excluded.observed_at OR (usage_storage_samples.observed_at=excluded.observed_at AND excluded.bytes=0)`, v.TenantID, v.ProjectID, v.Day, Stamp(at), v.Bytes)
	return err
}
func (s *Store) Prune(ctx context.Context) error {
	cutoff := Day(s.Clock().AddDate(0, 0, -s.Config.RetentionDays))
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, statement := range []string{`DELETE FROM usage_rollups WHERE day_start<$1`, `DELETE FROM usage_storage_samples WHERE day_start<$1`, `DELETE FROM usage_snapshots WHERE period_start<$1`} {
		if _, err = tx.ExecContext(ctx, statement, cutoff); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM usage_snapshots WHERE expires_at<=$1`, Stamp(s.Clock())); err != nil {
		return err
	}
	return tx.Commit()
}
func (r *Recorder) Prune(ctx context.Context, days int) error {
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cutoff := Day(r.Clock().AddDate(0, 0, -days))
	for _, statement := range []string{`DELETE FROM usage_rollups WHERE day_start<$1`, `DELETE FROM usage_executions WHERE day_start<$1`} {
		if _, err = tx.ExecContext(ctx, statement, cutoff); err != nil {
			return err
		}
	}
	return tx.Commit()
}
