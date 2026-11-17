package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/olup/porcupine/internal/auth"
)

type TenantDeletion struct {
	TenantID          string `json:"tenant_id"`
	State             string `json:"state" enum:"deleting,logically_deleted,purged"`
	StartedAt         string `json:"started_at"`
	UpdatedAt         string `json:"updated_at"`
	ProjectsTotal     int    `json:"projects_total"`
	ProjectsRemaining int    `json:"projects_remaining"`
	ErrorCode         string `json:"error_code,omitempty" doc:"Constant recovery category. No secrets, raw storage errors or tenant payloads are retained."`
	PhysicalPurge     string `json:"physical_purge" doc:"pending_verification until managed objects, noncurrent versions and retention requirements have been verified. Logical deletion does not erase backups."`
}

type TenantDeletionOutput struct{ Body TenantDeletion }

var errRestoredArtifactsPending = errors.New("restored artifact ownership requires reconciliation")

const tenantNativeOwnedPrincipals = `SELECT principal_id FROM tenant_credentials WHERE tenant_id=$1
UNION SELECT principal_id FROM service_api_keys WHERE client_id IN (SELECT principal_id FROM tenant_credentials WHERE tenant_id=$1)`

func (s *Service) tenantOwnedPrincipals() string {
	query := tenantNativeOwnedPrincipals
	if !s.externalAuthEnabled() {
		query += ` UNION SELECT g.principal_id FROM oauth_grant_tenants t JOIN oauth_interactive_grants g ON g.id=t.grant_id WHERE t.tenant_id=$1`
	}
	return "(" + query + ")"
}

func (s *Service) registerTenantDeletion(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	operator := auth.OperationAccess{Permission: "instance:admin"}
	start := op("delete-tenant", "DELETE", "/api/v1/tenants/{tenant_id}", "Block this tenant immediately and start resumable deletion; deployment operator only", operator, 202)
	start.Description = "The durable fence blocks ingestion and resource access before cleanup. Retry the same tenant ID. Shared owner-local deployments remain pending until shared operational-state cleanup is available. Customer-owned published pipeline outputs are excluded. logically_deleted never claims that S3 object versions or backups have been purged."
	huma.Register(api, start, s.managementDeleteTenant)
	huma.Register(api, op("get-tenant-deletion", "GET", "/api/v1/tenants/{tenant_id}/deletion", "Read deletion progress after tenant removal; deployment operator only", operator, 200), s.managementTenantDeletion)
}

func (s *Service) tenantDeletionStatus(ctx context.Context, tenant string) (TenantDeletion, error) {
	var status TenantDeletion
	err := s.control.QueryRowContext(ctx, `SELECT tenant_id,state,started_at,updated_at,error_code FROM tenant_deletions WHERE tenant_id=$1`, tenant).Scan(&status.TenantID, &status.State, &status.StartedAt, &status.UpdatedAt, &status.ErrorCode)
	if errors.Is(err, sql.ErrNoRows) {
		return status, apiError(404, "tenant deletion not found")
	}
	if err != nil {
		return status, apiError(503, "tenant deletion status unavailable")
	}
	if err := s.control.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(CASE WHEN completed=0 THEN 1 ELSE 0 END),0) FROM project_deletions WHERE tenant_id=$1`, tenant).Scan(&status.ProjectsTotal, &status.ProjectsRemaining); err != nil {
		return status, apiError(503, "tenant deletion status unavailable")
	}
	status.PhysicalPurge = "pending_verification"
	if status.State == "purged" {
		status.PhysicalPurge = "verified"
	}
	return status, nil
}

func (s *Service) managementTenantDeletion(ctx context.Context, input *TenantIDInput) (*TenantDeletionOutput, error) {
	if err := tenantOperator(ctx); err != nil {
		return nil, err
	}
	status, err := s.tenantDeletionStatus(ctx, input.TenantID)
	if err != nil {
		return nil, err
	}
	return &TenantDeletionOutput{Body: status}, nil
}

func (s *Service) managementDeleteTenant(ctx context.Context, input *TenantIDInput) (*TenantDeletionOutput, error) {
	if err := tenantOperator(ctx); err != nil {
		return nil, err
	}
	if err := s.beginTenantDeletion(ctx, input.TenantID); err != nil {
		var managed *managementError
		if errors.As(err, &managed) {
			return nil, managed
		}
		return nil, apiError(503, "tenant deletion unavailable; retry the same tenant ID")
	}
	select {
	case s.deletionWake <- struct{}{}:
	default:
	}
	return s.managementTenantDeletion(ctx, input)
}

func (s *Service) beginTenantDeletion(ctx context.Context, tenant string) error {
	if s.shared == nil {
		if err := s.localAdmission.Lock(ctx); err != nil {
			return err
		}
		defer s.localAdmission.Unlock()
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "tenant deletion unavailable")
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	if err := s.requireExternalCurrent(ctx, tx); err != nil {
		return err
	}
	deleted, err := tenantDeletionExists(ctx, tx, tenant)
	if err != nil {
		return err
	}
	if deleted {
		return tx.Commit()
	}
	lock := ""
	if s.shared != nil {
		lock = " FOR UPDATE"
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT id FROM tenants WHERE id=$1`+lock, tenant).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "tenant not found")
	}
	if err != nil {
		return err
	}
	at := s.clock().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_deletions(tenant_id,state,started_at,updated_at) VALUES($1,'deleting',$2,$2)`, tenant, at); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET ingestion_allowed=0 WHERE id=$1`, tenant); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO project_deletions(project_id,tenant_id,completed)
SELECT id,tenant_id,0 FROM projects WHERE tenant_id=$1 ON CONFLICT(project_id) DO NOTHING`, tenant); err != nil {
		return err
	}
	owned := strings.ReplaceAll(s.tenantOwnedPrincipals(), "$1", "$2")
	if _, err := tx.ExecContext(ctx, `UPDATE access_keys SET revoked_at=coalesce(revoked_at,$1) WHERE id IN `+owned, at, tenant); err != nil {
		return err
	}
	if !s.externalAuthEnabled() {
		if _, err := tx.ExecContext(ctx, `UPDATE tenant_memberships SET revoked_at=coalesce(revoked_at,$1),updated_at=$1 WHERE tenant_id=$2`, at, tenant); err != nil {
			return err
		}
	}
	projects, err := tenantProjectIDs(ctx, tx, tenant)
	if err != nil {
		return err
	}
	for _, project := range projects {
		if err := s.purgeProjectGrants(ctx, tx, project); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func tenantProjectIDs(ctx context.Context, query oauthQueryer, tenant string) ([]string, error) {
	rows, err := query.QueryContext(ctx, `SELECT id FROM projects WHERE tenant_id=$1 ORDER BY id`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Service) resumeTenantDeletions(ctx context.Context) {
	rows, err := s.control.QueryContext(ctx, `SELECT tenant_id FROM tenant_deletions WHERE state='deleting' ORDER BY started_at,tenant_id`)
	if err != nil {
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) != nil {
			break
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		code := ""
		if err := s.finishTenantDeletion(ctx, id); err != nil {
			code = "cleanup_pending"
			if errors.Is(err, errRestoredArtifactsPending) {
				code = "restored_artifacts_pending"
			}
			switch {
			case s.sealHostedProject != nil:
				code = "hosted_cleanup_pending"
			case s.shared != nil:
				code = "shared_local_cleanup_pending"
			}
		}
		_, _ = s.control.ExecContext(ctx, `UPDATE tenant_deletions SET error_code=$1,updated_at=$2 WHERE tenant_id=$3`, code, s.clock().UTC().Format(time.RFC3339Nano), id)
	}
}

// The owner-local shared backend cannot prove erasure of another worker's
// journal/cache. Phase 5 replaces this limitation with shared operational state.
func (s *Service) finishTenantDeletion(ctx context.Context, tenant string) error {
	var remaining int
	if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM project_deletions WHERE tenant_id=$1 AND completed=0`, tenant).Scan(&remaining); err != nil {
		return err
	}
	if remaining != 0 || s.shared != nil || s.sealHostedProject != nil {
		return errors.New("tenant local cleanup pending")
	}
	var held bool
	if err := s.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM restored_artifact_holds WHERE tenant_id=$1)`, tenant).Scan(&held); err != nil {
		return err
	}
	if held {
		return errRestoredArtifactsPending
	}
	if err := s.localAdmission.Lock(ctx); err != nil {
		return err
	}
	defer s.localAdmission.Unlock()
	if err := s.publishLocalUsage(ctx); err != nil {
		return err
	}
	owned, err := s.control.QueryContext(ctx, `SELECT id FROM access_keys WHERE id IN `+s.tenantOwnedPrincipals(), tenant)
	if err != nil {
		return err
	}
	var anchors []string
	for owned.Next() {
		var id string
		if err := owned.Scan(&id); err != nil {
			owned.Close()
			return err
		}
		anchors = append(anchors, id)
	}
	if err := errors.Join(owned.Err(), owned.Close()); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, id := range anchors {
		if _, err := tx.ExecContext(ctx, `DELETE FROM access_keys WHERE id=$1`, id); err != nil {
			return err
		}
	}
	// The tenant FK removes its audit links. A global operator can still have
	// links or an in-flight job in another tenant, so never delete by subject alone.
	if _, err := tx.ExecContext(ctx, `DELETE FROM journal_actor_tenants WHERE tenant_id=$1`, tenant); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM access_keys WHERE id LIKE 'external:%'
AND NOT EXISTS(SELECT 1 FROM journal_actor_tenants a WHERE a.actor_id=access_keys.id)
AND NOT EXISTS(SELECT 1 FROM export_jobs j WHERE j.actor_id=access_keys.id)
AND NOT EXISTS(SELECT 1 FROM import_uploads j WHERE j.actor_id=access_keys.id)
AND NOT EXISTS(SELECT 1 FROM pipelines j WHERE j.actor_id=access_keys.id)`); err != nil {
		return err
	}
	for _, statement := range []string{
		`DELETE FROM shared_tenant_cache WHERE tenant_id=$1`,
		`DELETE FROM tenant_usage WHERE tenant_id=$1`,
		`DELETE FROM tenants WHERE id=$1`,
	} {
		if _, err := tx.ExecContext(ctx, statement, tenant); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.finishTenantControlDeletion(ctx, tenant)
}

func (s *Service) finishTenantControlDeletion(ctx context.Context, tenant string) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO tenant_deletion_usage(tenant_id,period_start,accepted_events)
SELECT tenant_id,period_start,accepted_events FROM tenant_usage WHERE tenant_id=$1
ON CONFLICT(tenant_id,project_id,period_start) DO UPDATE SET accepted_events=CASE WHEN tenant_deletion_usage.accepted_events<excluded.accepted_events THEN excluded.accepted_events ELSE tenant_deletion_usage.accepted_events END`, tenant); err != nil {
		return err
	}
	// These predicates follow explicit credential ownership and tenant-admin
	// consent ownership. Shared humans, OAuth apps and mixed project grants remain.
	owned := s.tenantOwnedPrincipals()
	grants := `(SELECT id FROM oauth_interactive_grants WHERE principal_id IN ` + owned + ` OR client_id IN ` + owned + `)`
	if !s.externalAuthEnabled() {
		for _, statement := range []string{
			`DELETE FROM oauth_authorization_requests WHERE client_id IN ` + owned,
			`DELETE FROM oauth_authorization_codes WHERE grant_id IN ` + grants,
			`DELETE FROM oauth_refresh_tokens WHERE grant_id IN ` + grants,
			`DELETE FROM oauth_tokens WHERE client_id IN ` + owned + ` OR interactive_grant_id IN ` + grants,
			`DELETE FROM oauth_interactive_access WHERE grant_id IN ` + grants,
			`DELETE FROM oauth_grant_tenants WHERE grant_id IN ` + grants + ` AND tenant_id<>$1`,
		} {
			if _, err := tx.ExecContext(ctx, statement, tenant); err != nil {
				return err
			}
		}
	}
	// Save owned principal IDs before removing the records used to select them.
	rows, err := tx.QueryContext(ctx, `SELECT id FROM access_keys WHERE id IN `+owned, tenant)
	if err != nil {
		return err
	}
	var principals []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		principals = append(principals, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	statements := []string{}
	if !s.externalAuthEnabled() {
		statements = append(statements, `DELETE FROM oauth_interactive_grants WHERE id IN `+grants, `DELETE FROM tenant_memberships WHERE tenant_id=$1`)
	}
	statements = append(statements, []string{
		`DELETE FROM service_api_keys WHERE principal_id IN ` + owned + ` OR client_id IN ` + owned,
		`DELETE FROM oauth_clients WHERE client_id IN ` + owned,
		`DELETE FROM tenant_credentials WHERE tenant_id=$1`,
		`DELETE FROM tenant_usage WHERE tenant_id=$1`,
		`DELETE FROM tenants WHERE id=$1`,
	}...)
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement, tenant); err != nil {
			return err
		}
	}
	for _, id := range principals {
		if _, err := tx.ExecContext(ctx, `DELETE FROM access_keys WHERE id=$1`, id); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE tenant_deletions SET state='logically_deleted',error_code='',updated_at=$2 WHERE tenant_id=$1 AND state='deleting'`, tenant, s.clock().UTC().Format(time.RFC3339Nano)); err != nil {
		return err
	}
	return tx.Commit()
}
