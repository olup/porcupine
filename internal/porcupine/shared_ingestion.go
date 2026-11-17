package porcupine

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"math"
	"strings"
	"sync"
	"time"
)

const sharedIngestionRefreshInterval = 10 * time.Second
const sharedEventIDBlockSize int64 = 1024

type sharedIngestionState struct {
	idsMu  sync.Mutex
	nextID int64
	endID  int64
	syncMu sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

func (s *Service) initializeSharedIngestion(ctx context.Context) error {
	if s.shared == nil {
		return nil
	}
	s.sharedIngestion = &sharedIngestionState{}
	s.eventRules.allocateID = s.nextSharedEventID
	s.eventRules.consumeTenant = s.consumeSharedTenantEvents
	s.eventRules.cookielessKey = s.sharedCookielessKey
	s.eventRules.prepareAcceptance = s.prepareSharedAcceptance
	return s.syncSharedIngestion(ctx)
}

func (s *Service) nextSharedEventID(ctx context.Context, _ *sql.Tx) (int64, error) {
	state := s.sharedIngestion
	state.idsMu.Lock()
	defer state.idsMu.Unlock()
	if state.nextID == 0 || state.nextID > state.endID {
		var first int64
		if err := s.control.QueryRowContext(ctx, `SELECT nextval('shared_event_id_sequence')`).Scan(&first); err != nil {
			return 0, errors.New("shared event ID allocation unavailable")
		}
		if first <= 0 || first > math.MaxInt64-sharedEventIDBlockSize+1 {
			return 0, errors.New("shared event ID range exhausted")
		}
		state.nextID, state.endID = first, first+sharedEventIDBlockSize-1
	}
	id := state.nextID
	state.nextID++
	return id, nil
}

func (s *Service) prepareSharedAcceptance(ctx context.Context, project string) error {
	if err := s.requireProjectActive(ctx, project); err != nil {
		return err
	}
	period, _ := tenantMonth(s.clock())
	var current bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM shared_tenant_cache c JOIN projects p ON p.tenant_id=c.tenant_id
WHERE p.id=$1 AND c.period_start=$2 AND c.refreshed_at_ms>=$3)`, project, period.Format(time.RFC3339), time.Now().Add(-sharedIngestionRefreshInterval).UnixMilli()).Scan(&current)
	if err != nil || current {
		return err
	}
	return s.syncSharedIngestion(ctx)
}

// Both acceptance owners call prepareSharedAcceptance before opening the
// journal transaction. Budget debits use that validated snapshot for the whole
// transaction; policy freshness is an admission check, not a per-row deadline.
func (s *Service) consumeSharedTenantEvents(ctx context.Context, tx *sql.Tx, project string, acceptedAt time.Time, count int64) error {
	if count == 0 {
		return nil
	}
	if count < 0 {
		return errors.New("negative tenant event consumption")
	}
	start, end := tenantMonth(acceptedAt)
	period := start.Format(time.RFC3339)
	var tenant string
	var allowed bool
	var allowance sql.NullInt64
	var local, global, own int64
	err := tx.QueryRowContext(ctx, `SELECT t.id,t.ingestion_allowed,t.ingestion_monthly_events,
coalesce(u.accepted_events,0),c.global_accepted,c.own_published
FROM projects p JOIN tenants t ON t.id=p.tenant_id
JOIN shared_tenant_cache c ON c.tenant_id=t.id AND c.period_start=$1
LEFT JOIN tenant_usage u ON u.tenant_id=t.id AND u.period_start=$1 WHERE p.id=$2`, period, project).Scan(&tenant, &allowed, &allowance, &local, &global, &own)
	if err != nil {
		return err
	}
	if !allowed {
		return &tenantIngestionError{code: "ingestion_suspended", message: "tenant ingestion is suspended", periodEnd: end.Format(time.RFC3339)}
	}
	other := max(int64(0), global-own)
	local = max(local, own)
	if local > math.MaxInt64-other {
		return errors.New("tenant event counter exhausted")
	}
	used := local + other
	if count > math.MaxInt64-used {
		return errors.New("tenant event counter exhausted")
	}
	if allowance.Valid && (used >= allowance.Int64 || count > allowance.Int64-used) {
		return &tenantIngestionError{code: "ingestion_budget_exceeded", message: "tenant monthly ingestion budget exceeded", periodEnd: end.Format(time.RFC3339)}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO tenant_usage(tenant_id,period_start,accepted_events) VALUES($1,$2,$3)
ON CONFLICT(tenant_id,period_start) DO UPDATE SET accepted_events=excluded.accepted_events`, tenant, period, local+count)
	return err
}

func (s *Service) syncSharedIngestion(ctx context.Context) error {
	if s.shared == nil {
		return nil
	}
	s.sharedIngestion.syncMu.Lock()
	defer s.sharedIngestion.syncMu.Unlock()
	if err := s.publishSharedUsage(ctx); err != nil {
		return err
	}
	return s.refreshSharedIngestionPolicy(ctx)
}

func (s *Service) publishSharedUsage(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id,period_start,accepted_events FROM tenant_usage ORDER BY tenant_id,period_start`)
	if err != nil {
		return err
	}
	type usage struct {
		tenant, period string
		count          int64
	}
	var pending []usage
	for rows.Next() {
		var u usage
		if err = rows.Scan(&u.tenant, &u.period, &u.count); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, u)
	}
	err = rows.Err()
	rows.Close()
	if err != nil || len(pending) == 0 {
		return err
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, u := range pending {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM tenants WHERE id=$1)`, u.tenant).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			deleted, err := tenantDeletionExists(ctx, tx, u.tenant)
			if err != nil {
				return err
			}
			if deleted {
				continue
			}
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO worker_tenant_usage(worker_id,tenant_id,period_start,accepted_events) VALUES($1,$2,$3,0) ON CONFLICT DO NOTHING`, s.shared.config.WorkerID, u.tenant, u.period); err != nil {
			return err
		}
		var previous int64
		if err = tx.QueryRowContext(ctx, `SELECT accepted_events FROM worker_tenant_usage WHERE worker_id=$1 AND tenant_id=$2 AND period_start=$3 FOR UPDATE`, s.shared.config.WorkerID, u.tenant, u.period).Scan(&previous); err != nil {
			return err
		}
		if u.count <= previous {
			continue
		}
		delta := u.count - previous
		if _, err = tx.ExecContext(ctx, `INSERT INTO tenant_usage(tenant_id,period_start,accepted_events) VALUES($1,$2,$3)
ON CONFLICT(tenant_id,period_start) DO UPDATE SET accepted_events=tenant_usage.accepted_events+excluded.accepted_events`, u.tenant, u.period, delta); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE worker_tenant_usage SET accepted_events=$1 WHERE worker_id=$2 AND tenant_id=$3 AND period_start=$4`, u.count, s.shared.config.WorkerID, u.tenant, u.period); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) refreshSharedIngestionPolicy(ctx context.Context) error {
	period, _ := tenantMonth(s.clock())
	columns := strings.Split(tenantColumns, ",")
	for i := range columns {
		columns[i] = "t." + columns[i]
	}
	rows, err := s.control.QueryContext(ctx, `SELECT `+strings.Join(columns, ",")+`,coalesce(u.accepted_events,0),coalesce(w.accepted_events,0)
FROM tenants t LEFT JOIN tenant_usage u ON u.tenant_id=t.id AND u.period_start=$1
LEFT JOIN worker_tenant_usage w ON w.tenant_id=t.id AND w.period_start=$1 AND w.worker_id=$2 ORDER BY t.id`, period.Format(time.RFC3339), s.shared.config.WorkerID)
	if err != nil {
		return err
	}
	type policy struct {
		tenant      Tenant
		global, own int64
	}
	var policies []policy
	for rows.Next() {
		var p policy
		var monthly sql.NullInt64
		t := &p.tenant
		err = rows.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.IngestionAllowed, &monthly, &t.Budgets.HotQueryMaxConcurrent, &t.Budgets.HotQueryTimeoutMS, &t.Budgets.HotQueryMaxRows, &t.Budgets.HotQueryMaxResultBytes, &p.global, &p.own)
		if err != nil {
			rows.Close()
			return err
		}
		if monthly.Valid {
			t.Budgets.IngestionMonthlyEvents = &monthly.Int64
		}
		policies = append(policies, p)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range policies {
		t := p.tenant
		_, err = tx.ExecContext(ctx, `INSERT INTO tenants(`+tenantColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT(id) DO UPDATE SET name=excluded.name,ingestion_allowed=excluded.ingestion_allowed,ingestion_monthly_events=excluded.ingestion_monthly_events,
hot_query_max_concurrent=excluded.hot_query_max_concurrent,hot_query_timeout_ms=excluded.hot_query_timeout_ms,hot_query_max_rows=excluded.hot_query_max_rows,hot_query_max_result_bytes=excluded.hot_query_max_result_bytes`,
			t.ID, t.Name, t.CreatedAt, t.IngestionAllowed, monthlyAllowanceValue(t.Budgets.IngestionMonthlyEvents), t.Budgets.HotQueryMaxConcurrent, t.Budgets.HotQueryTimeoutMS, t.Budgets.HotQueryMaxRows, t.Budgets.HotQueryMaxResultBytes)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO shared_tenant_cache(tenant_id,period_start,global_accepted,own_published,refreshed_at_ms) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(tenant_id,period_start) DO UPDATE SET global_accepted=excluded.global_accepted,own_published=excluded.own_published,refreshed_at_ms=excluded.refreshed_at_ms`, t.ID, period.Format(time.RFC3339), p.global, p.own, time.Now().UnixMilli())
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) sharedCookielessKey(ctx context.Context, tx *sql.Tx, day string) ([]byte, error) {
	var latest sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT max(day) FROM cookieless_keys`).Scan(&latest); err != nil {
		return nil, err
	}
	if latest.Valid && latest.String > day {
		return nil, errors.New("cookieless clock moved before the active identity day")
	}
	var secret []byte
	err := tx.QueryRowContext(ctx, `SELECT secret FROM cookieless_keys WHERE day=$1`, day).Scan(&secret)
	if err == nil {
		return secret, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	secret, err = (hostedCookielessKeys{s.control}).CookielessKey(ctx, day)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO cookieless_keys(day,secret) VALUES($1,$2)`, day, secret); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM cookieless_keys WHERE day<$1`, day); err != nil {
		return nil, err
	}
	return secret, nil
}

type hostedCookielessKeys struct{ db *sql.DB }

func (k hostedCookielessKeys) CookielessKey(ctx context.Context, day string) ([]byte, error) {
	var latest sql.NullString
	shared, err := k.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer shared.Rollback()
	if _, err = shared.ExecContext(ctx, `SELECT pg_advisory_xact_lock(1886352211,3)`); err != nil {
		return nil, err
	}
	if err = shared.QueryRowContext(ctx, `SELECT max(day) FROM shared_cookieless_keys`).Scan(&latest); err != nil {
		return nil, err
	}
	if latest.Valid && latest.String > day {
		return nil, errors.New("cookieless clock moved before the active identity day")
	}
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return nil, err
	}
	if _, err = shared.ExecContext(ctx, `INSERT INTO shared_cookieless_keys(day,secret) VALUES($1,$2) ON CONFLICT DO NOTHING`, day, secret); err != nil {
		return nil, err
	}
	if err = shared.QueryRowContext(ctx, `SELECT secret FROM shared_cookieless_keys WHERE day=$1`, day).Scan(&secret); err != nil {
		return nil, err
	}
	if _, err = shared.ExecContext(ctx, `DELETE FROM shared_cookieless_keys WHERE day<$1`, day); err != nil {
		return nil, err
	}
	if err = shared.Commit(); err != nil {
		return nil, err
	}
	return secret, nil
}

func (s *Service) startSharedIngestion() {
	if s.shared == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.sharedIngestion.cancel, s.sharedIngestion.done = cancel, make(chan struct{})
	go func() {
		defer close(s.sharedIngestion.done)
		timer := time.NewTicker(sharedIngestionRefreshInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				work, stop := context.WithTimeout(ctx, sharedIngestionRefreshInterval)
				err := s.syncSharedIngestion(work)
				stop()
				if err != nil {
					_, _ = s.db.ExecContext(ctx, `INSERT INTO metadata(key,value) VALUES('shared_ingestion_error','usage publication or policy refresh failed') ON CONFLICT(key) DO UPDATE SET value=excluded.value`)
				} else {
					_, _ = s.db.ExecContext(ctx, `DELETE FROM metadata WHERE key='shared_ingestion_error'`)
				}
			}
		}
	}()
}

func (s *Service) stopSharedIngestion() {
	if s.sharedIngestion == nil || s.sharedIngestion.cancel == nil {
		return
	}
	s.sharedIngestion.cancel()
	<-s.sharedIngestion.done
	ctx, cancel := context.WithTimeout(context.Background(), sharedIngestionRefreshInterval)
	defer cancel()
	_ = s.syncSharedIngestion(ctx)
}
