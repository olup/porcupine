package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
)

const defaultTenantID = "default"

// TenantBudgets are operator policy. A nil ingestion allowance is unlimited;
// query budgets are additionally bounded by the deployment's safety limits.
type TenantBudgets struct {
	IngestionMonthlyEvents *int64 `json:"ingestion_monthly_events" nullable:"true" minimum:"0" doc:"New canonical events accepted per UTC calendar month, including imports and identity controls. Null is unlimited; zero allows only retries of previously accepted events."`
	HotQueryMaxConcurrent  int    `json:"hot_query_max_concurrent" minimum:"1" maximum:"64" doc:"Interactive operations shared by all projects in this tenant, further bounded by deployment capacity."`
	HotQueryTimeoutMS      int    `json:"hot_query_timeout_ms" minimum:"1" maximum:"60000" doc:"Execution deadline in milliseconds, further bounded by the deployment ceiling."`
	HotQueryMaxRows        int    `json:"hot_query_max_rows" minimum:"1" maximum:"100000" doc:"Maximum rows returned by an interactive SQL query."`
	HotQueryMaxResultBytes int    `json:"hot_query_max_result_bytes" minimum:"256" maximum:"67108864" doc:"Maximum encoded interactive SQL result size, including metadata overhead."`
}

type Tenant struct {
	ID               string        `json:"id"`
	Name             string        `json:"name"`
	CreatedAt        string        `json:"created_at"`
	IngestionAllowed bool          `json:"ingestion_allowed" doc:"Operator switch for new events. Existing queued events continue delivery and duplicate retries remain valid."`
	Budgets          TenantBudgets `json:"budgets"`
}

const tenantColumns = `id,name,created_at,ingestion_allowed,ingestion_monthly_events,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes`

type tenantScanner interface{ Scan(...any) error }

func scanTenant(row tenantScanner) (Tenant, error) {
	var t Tenant
	var monthly sql.NullInt64
	err := row.Scan(&t.ID, &t.Name, &t.CreatedAt, &t.IngestionAllowed, &monthly, &t.Budgets.HotQueryMaxConcurrent, &t.Budgets.HotQueryTimeoutMS, &t.Budgets.HotQueryMaxRows, &t.Budgets.HotQueryMaxResultBytes)
	if monthly.Valid {
		t.Budgets.IngestionMonthlyEvents = &monthly.Int64
	}
	return t, err
}

func validateTenantBudgets(b TenantBudgets) error {
	if b.IngestionMonthlyEvents != nil && *b.IngestionMonthlyEvents < 0 || b.HotQueryMaxConcurrent < 1 || b.HotQueryMaxConcurrent > 64 || b.HotQueryTimeoutMS < 1 || b.HotQueryTimeoutMS > 60000 || b.HotQueryMaxRows < 1 || b.HotQueryMaxRows > 100000 || b.HotQueryMaxResultBytes < 256 || b.HotQueryMaxResultBytes > 64<<20 {
		return apierr.New(400, "invalid tenant budgets")
	}
	return nil
}

func tenantName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" || len(name) > 200 {
		return "", apierr.New(400, "tenant name must contain 1 to 200 UTF-8 bytes")
	}
	return name, nil
}

func (s *Service) initializeTenants(ctx context.Context) error {
	deleted, err := tenantDeletionExists(ctx, s.control, defaultTenantID)
	if err != nil || deleted {
		return err
	}
	b := s.defaultTenantBudgets()
	_, err = s.db.ExecContext(ctx, `INSERT INTO tenants(id,name,created_at,ingestion_allowed,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES($1,$2,$3,1,$4,$5,$6,$7) ON CONFLICT(id) DO NOTHING`, defaultTenantID, "Default", s.clock().UTC().Format(time.RFC3339Nano), b.HotQueryMaxConcurrent, b.HotQueryTimeoutMS, b.HotQueryMaxRows, b.HotQueryMaxResultBytes)
	return err
}

func (s *Service) loadTenant(ctx context.Context, id string) (Tenant, error) {
	if cleanup, _ := ctx.Value(projectCleanupContextKey{}).(bool); !cleanup {
		if err := requireTenantActive(ctx, s.control, id); err != nil {
			return Tenant{}, err
		}
	}
	t, err := scanTenant(s.control.QueryRowContext(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, apierr.New(404, "tenant not found")
	}
	if err != nil {
		return Tenant{}, apierr.New(503, "tenant storage unavailable")
	}
	return t, nil
}

func (s *Service) projectTenant(ctx context.Context, project string) (Tenant, error) {
	columns := strings.Split(tenantColumns, ",")
	for i := range columns {
		columns[i] = "t." + columns[i]
	}
	t, err := scanTenant(s.control.QueryRowContext(ctx, `SELECT `+strings.Join(columns, ",")+` FROM tenants t JOIN projects p ON p.tenant_id=t.id WHERE p.id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=p.id)`, project))
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, apierr.New(404, "project not found")
	}
	if err != nil {
		return Tenant{}, apierr.New(503, "tenant storage unavailable")
	}
	return t, nil
}

func tenantMonth(at time.Time) (start, end time.Time) {
	at = at.UTC()
	start = time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0)
}

func tenantPeriod(raw string) (time.Time, error) {
	at, err := time.Parse("2006-01", raw)
	if err != nil || at.Format("2006-01") != raw {
		return time.Time{}, apierr.New(400, "period must be a UTC calendar month in YYYY-MM format")
	}
	return at, nil
}

func monthlyAllowanceValue(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func (s *Service) defaultTenantBudgets() TenantBudgets {
	limits := s.queries.Limits()
	return TenantBudgets{HotQueryMaxConcurrent: limits.MaxConcurrent, HotQueryTimeoutMS: limits.TimeoutMS, HotQueryMaxRows: limits.MaxRows, HotQueryMaxResultBytes: limits.MaxResultBytes}
}

func (s *Service) insertTenant(ctx context.Context, name string, budgets TenantBudgets) (Tenant, error) {
	name, err := tenantName(name)
	if err != nil {
		return Tenant{}, err
	}
	if err = validateTenantBudgets(budgets); err != nil {
		return Tenant{}, err
	}
	t := Tenant{ID: eventapi.NewID(), Name: name, CreatedAt: s.clock().UTC().Format(time.RFC3339Nano), IngestionAllowed: true, Budgets: budgets}
	_, err = s.control.ExecContext(ctx, `INSERT INTO tenants(`+tenantColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, t.ID, t.Name, t.CreatedAt, t.IngestionAllowed, monthlyAllowanceValue(budgets.IngestionMonthlyEvents), budgets.HotQueryMaxConcurrent, budgets.HotQueryTimeoutMS, budgets.HotQueryMaxRows, budgets.HotQueryMaxResultBytes)
	if err != nil {
		return Tenant{}, apierr.New(503, "tenant creation unavailable")
	}
	return t, nil
}
