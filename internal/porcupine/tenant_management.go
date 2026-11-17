package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
)

type TenantListInput struct {
	Limit  int    `query:"limit" default:"100" minimum:"1" maximum:"200"`
	Cursor string `query:"cursor" maxLength:"128" doc:"Continue after the tenant ID returned as next_cursor."`
}
type TenantListOutput struct {
	Body struct {
		Tenants    []Tenant `json:"tenants"`
		NextCursor string   `json:"next_cursor,omitempty"`
	}
}
type TenantCreateInput struct {
	Body struct {
		Name    string         `json:"name" minLength:"1" maxLength:"200"`
		Budgets *TenantBudgets `json:"budgets,omitempty" doc:"Optional complete budget configuration. Omit for unlimited ingestion and deployment-default interactive limits."`
	}
}
type TenantIDInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
}
type TenantOutput struct {
	Body struct {
		Tenant Tenant `json:"tenant"`
	}
}
type TenantUpdateInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	Body     struct {
		Name             *string `json:"name,omitempty" minLength:"1" maxLength:"200"`
		IngestionAllowed *bool   `json:"ingestion_allowed,omitempty" doc:"Suspend only new acceptance. Delivery, queries, exports and deletion retain their existing permissions."`
	}
}
type TenantBudgetsInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	Body     TenantBudgets
}
type TenantUsageInput struct {
	TenantID string `path:"tenant_id" maxLength:"128"`
	Period   string `query:"period" maxLength:"7" doc:"UTC calendar month, YYYY-MM. Defaults to the current month. Prior months remain available; there is no manual reset operation."`
}
type TenantUsage struct {
	TenantID         string  `json:"tenant_id"`
	PeriodStart      string  `json:"period_start"`
	PeriodEnd        string  `json:"period_end"`
	AcceptedEvents   string  `json:"accepted_events" doc:"Unique canonical rows durably accepted during this UTC month, encoded as a decimal string. Includes accepted imports and identity controls; excludes duplicate, invalid and rejected rows. Edits and deletions never refund usage."`
	RemainingEvents  *string `json:"remaining_events" nullable:"true" doc:"Remaining monthly allowance, or null for unlimited. An operator may reduce the budget below consumption; the remaining allowance is then zero."`
	IngestionAllowed bool    `json:"ingestion_allowed" doc:"Effective permission for new events after applying operator suspension and this month's budget. Retries of accepted events remain valid."`
}
type TenantUsageOutput struct{ Body TenantUsage }

func (s *Service) registerTenants(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	operator := operationAccess{Permission: "instance:admin"}
	tenantAdmin := operationAccess{Permission: tenantAdminScope, TenantParameter: "tenant_id", TenantAdmin: true}
	huma.Register(api, op("list-tenants", "GET", "/api/v1/tenants", "List tenants administered by this caller; operators can list the deployment", operationAccess{Permission: tenantAdminScope, TenantAdmin: true}, 200), s.managementListTenants)
	huma.Register(api, op("create-tenant", "POST", "/api/v1/tenants", "Create a tenant with optional budgets; deployment operator only", operator, 201), s.managementCreateTenant)
	huma.Register(api, op("get-tenant", "GET", "/api/v1/tenants/{tenant_id}", "Read configuration of a tenant administered by this caller", tenantAdmin, 200), s.managementGetTenant)
	huma.Register(api, op("update-tenant", "PATCH", "/api/v1/tenants/{tenant_id}", "Rename this tenant; only instance operators may change ingestion_allowed", tenantAdmin, 200), s.managementUpdateTenant)
	huma.Register(api, op("update-tenant-budgets", "PUT", "/api/v1/tenants/{tenant_id}/budgets", "Replace monthly ingestion and interactive query budgets without resetting consumption; deployment operator only", operator, 200), s.managementUpdateTenantBudgets)
	usage := op("get-tenant-usage", "GET", "/api/v1/tenants/{tenant_id}/usage", "Read an administered tenant's UTC monthly acceptance counter and remaining allowance", tenantAdmin, 200)
	usage.Description = "Budgets belong to the tenant and aggregate all its projects. Calendar months start at midnight UTC. Counters are committed with durable acceptance, survive event/project deletion, and are retained across month boundaries. Imports are charged to their acceptance month rather than their occurrence month. Native/Segment quota refusals are explicit errors; PostHog compatibility uses a terminal quota_limited response. No reset API is exposed. Project readers discover their effective interactive limits through get-event-schema without gaining access to tenant-wide consumption."
	huma.Register(api, usage, s.managementTenantUsage)
}

func tenantOperator(ctx context.Context) error {
	if !principal(ctx).admin() {
		return apiError(403, "deployment operator required for tenant management")
	}
	return nil
}

func (s *Service) managementListTenants(ctx context.Context, input *TenantListInput) (*TenantListOutput, error) {
	limit := input.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 200 || len(input.Cursor) > 128 {
		return nil, apiError(400, "invalid tenant pagination")
	}
	args := []any{input.Cursor}
	filter := ""
	if !principal(ctx).admin() {
		admins := principal(ctx).tenantAdmins
		if len(admins) == 0 {
			return nil, apiError(403, "tenant administrator required")
		}
		markers := []string{}
		for _, id := range admins {
			args = append(args, id)
			markers = append(markers, fmt.Sprintf("$%d", len(args)))
		}
		filter = " AND id IN (" + strings.Join(markers, ",") + ")"
	}
	args = append(args, limit+1)
	rows, err := s.control.QueryContext(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id>$1`+filter+` ORDER BY id LIMIT `+fmt.Sprintf("$%d", len(args)), args...)
	if err != nil {
		return nil, apiError(503, "tenant storage unavailable")
	}
	defer rows.Close()
	out := &TenantListOutput{}
	out.Body.Tenants = []Tenant{}
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return nil, apiError(503, "tenant storage unavailable")
		}
		if len(out.Body.Tenants) == limit {
			out.Body.NextCursor = out.Body.Tenants[limit-1].ID
			break
		}
		out.Body.Tenants = append(out.Body.Tenants, t)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "tenant storage unavailable")
	}
	return out, nil
}

func (s *Service) managementCreateTenant(ctx context.Context, input *TenantCreateInput) (*TenantOutput, error) {
	if err := tenantOperator(ctx); err != nil {
		return nil, err
	}
	budgets := s.defaultTenantBudgets()
	if input.Body.Budgets != nil {
		budgets = *input.Body.Budgets
	}
	t, err := s.insertTenant(ctx, input.Body.Name, budgets)
	if err != nil {
		return nil, err
	}
	out := &TenantOutput{}
	out.Body.Tenant = t
	return out, nil
}

func (s *Service) managementGetTenant(ctx context.Context, input *TenantIDInput) (*TenantOutput, error) {
	if !principal(ctx).canAdminTenant(input.TenantID) {
		return nil, apiError(403, "tenant administrator required")
	}
	t, err := s.loadTenant(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	out := &TenantOutput{}
	out.Body.Tenant = t
	return out, nil
}

func (s *Service) managementUpdateTenant(ctx context.Context, input *TenantUpdateInput) (*TenantOutput, error) {
	if !principal(ctx).canAdminTenant(input.TenantID) {
		return nil, apiError(403, "tenant administrator required")
	}
	if input.Body.IngestionAllowed != nil && !principal(ctx).admin() {
		return nil, apiError(403, "only instance operators may change ingestion admission")
	}
	if s.shared == nil {
		if err := s.localAdmission.Lock(ctx); err != nil {
			return nil, err
		}
		defer s.localAdmission.Unlock()
	}
	if input.Body.Name == nil && input.Body.IngestionAllowed == nil {
		return nil, apiError(400, "name or ingestion_allowed is required")
	}
	var name any
	if input.Body.Name != nil {
		value, err := tenantName(*input.Body.Name)
		if err != nil {
			return nil, err
		}
		name = value
	}
	var allowed any
	if input.Body.IngestionAllowed != nil {
		allowed = *input.Body.IngestionAllowed
	}
	tx, err := s.beginTenantAdministration(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	t, err := scanTenant(tx.QueryRowContext(ctx, `UPDATE tenants SET name=coalesce($1,name),ingestion_allowed=coalesce($2,ingestion_allowed) WHERE id=$3 RETURNING `+tenantColumns, name, allowed, input.TenantID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "tenant not found")
	}
	if err != nil {
		return nil, apiError(503, "tenant storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "tenant storage unavailable")
	}
	out := &TenantOutput{}
	out.Body.Tenant = t
	return out, nil
}

func (s *Service) managementUpdateTenantBudgets(ctx context.Context, input *TenantBudgetsInput) (*TenantOutput, error) {
	if err := tenantOperator(ctx); err != nil {
		return nil, err
	}
	if s.shared == nil {
		if err := s.localAdmission.Lock(ctx); err != nil {
			return nil, err
		}
		defer s.localAdmission.Unlock()
	}
	b := input.Body
	if err := validateTenantBudgets(b); err != nil {
		return nil, err
	}
	t, err := scanTenant(s.control.QueryRowContext(ctx, `UPDATE tenants SET ingestion_monthly_events=$1,hot_query_max_concurrent=$2,hot_query_timeout_ms=$3,hot_query_max_rows=$4,hot_query_max_result_bytes=$5 WHERE id=$6 RETURNING `+tenantColumns, monthlyAllowanceValue(b.IngestionMonthlyEvents), b.HotQueryMaxConcurrent, b.HotQueryTimeoutMS, b.HotQueryMaxRows, b.HotQueryMaxResultBytes, input.TenantID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "tenant not found")
	}
	if err != nil {
		return nil, apiError(503, "tenant storage unavailable")
	}
	out := &TenantOutput{}
	out.Body.Tenant = t
	return out, nil
}

func (s *Service) managementTenantUsage(ctx context.Context, input *TenantUsageInput) (*TenantUsageOutput, error) {
	if !principal(ctx).canAdminTenant(input.TenantID) {
		return nil, apiError(403, "tenant administrator required")
	}
	if err := s.publishLocalUsage(ctx); err != nil {
		return nil, apiError(503, "tenant usage publication unavailable")
	}
	start, _ := tenantMonth(s.clock())
	if input.Period != "" {
		var err error
		start, err = tenantPeriod(input.Period)
		if err != nil {
			return nil, err
		}
	}
	// Read budget and counter from one statement so a concurrent policy change
	// cannot produce a remaining allowance from mismatched snapshots.
	var allowed bool
	var limit sql.NullInt64
	var used int64
	err := s.control.QueryRowContext(ctx, `SELECT t.ingestion_allowed,t.ingestion_monthly_events,coalesce(u.accepted_events,0) FROM tenants t LEFT JOIN tenant_usage u ON u.tenant_id=t.id AND u.period_start=$1 WHERE t.id=$2`, start.Format(time.RFC3339), input.TenantID).Scan(&allowed, &limit, &used)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "tenant not found")
	}
	if err != nil {
		return nil, apiError(503, "tenant usage unavailable")
	}
	usage := TenantUsage{TenantID: input.TenantID, PeriodStart: start.Format(time.RFC3339), PeriodEnd: start.AddDate(0, 1, 0).Format(time.RFC3339), AcceptedEvents: strconv.FormatInt(used, 10), IngestionAllowed: allowed}
	if limit.Valid {
		remaining := strconv.FormatInt(max(int64(0), limit.Int64-used), 10)
		usage.RemainingEvents = &remaining
		usage.IngestionAllowed = allowed && used < limit.Int64
	}
	return &TenantUsageOutput{Body: usage}, nil
}
