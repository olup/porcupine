package porcupine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

// Only foreign-key anchors are copied into the owner's local journal. Shared
// credentials are authenticated against control storage, never against these
// uncredentialed local actor records.
func (s *Service) ensureProjectLocal(ctx context.Context, project string, a auth.Principal) error {
	if s.shared != nil {
		owner, _, err := s.sharedProjectOwner(ctx, project)
		if err != nil {
			return err
		}
		if owner != s.shared.config.WorkerID {
			return apiError(503, "project journal belongs to another worker")
		}
	} else {
		var deleted bool
		if err := s.control.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)`, project).Scan(&deleted); err != nil {
			return apiError(503, "project metadata unavailable")
		}
		allowDeleted, _ := ctx.Value(sharedDeletedProjectContextKey{}).(bool)
		if deleted && !allowDeleted {
			return apiError(404, "project not found")
		}
	}
	var p Project
	err := s.control.QueryRowContext(ctx, `SELECT id,tenant_id,name,created_at,ingestion_mode FROM projects WHERE id=$1`, project).Scan(&p.ID, &p.TenantID, &p.Name, &p.CreatedAt, &p.IngestionMode)
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "project not found")
	}
	if err != nil {
		return apiError(503, "project metadata unavailable")
	}
	t, err := s.loadTenant(ctx, p.TenantID)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "local project journal unavailable")
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tenants(`+tenantColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT(id) DO UPDATE SET name=excluded.name,ingestion_allowed=excluded.ingestion_allowed,ingestion_monthly_events=excluded.ingestion_monthly_events,
hot_query_max_concurrent=excluded.hot_query_max_concurrent,hot_query_timeout_ms=excluded.hot_query_timeout_ms,hot_query_max_rows=excluded.hot_query_max_rows,hot_query_max_result_bytes=excluded.hot_query_max_result_bytes`, t.ID, t.Name, t.CreatedAt, t.IngestionAllowed, monthlyAllowanceValue(t.Budgets.IngestionMonthlyEvents), t.Budgets.HotQueryMaxConcurrent, t.Budgets.HotQueryTimeoutMS, t.Budgets.HotQueryMaxRows, t.Budgets.HotQueryMaxResultBytes)
	if err != nil {
		return apiError(503, "local tenant metadata unavailable")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO projects(id,tenant_id,name,created_at,ingestion_mode) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(id) DO UPDATE SET name=excluded.name,ingestion_mode=excluded.ingestion_mode WHERE projects.tenant_id=excluded.tenant_id`, p.ID, p.TenantID, p.Name, p.CreatedAt, p.IngestionMode)
	if err != nil {
		return apiError(503, "local project metadata unavailable")
	}
	if a.ID != "" {
		_, err = tx.ExecContext(ctx, `INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES($1,'local job owner',$2,'[]','[]',$3) ON CONFLICT(id) DO NOTHING`, a.ID, hash(token()), now())
		if err != nil {
			return apiError(503, "local job ownership unavailable")
		}
		if a.External != nil {
			// Native ownership lives in control storage. External subjects have only
			// this inert journal link, derived from the authoritative project row.
			if _, err = tx.ExecContext(ctx, `INSERT INTO journal_actor_tenants(actor_id,tenant_id) VALUES($1,$2) ON CONFLICT(actor_id,tenant_id) DO NOTHING`, a.ID, p.TenantID); err != nil {
				return apiError(503, "local audit ownership unavailable")
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return apiError(503, "local project metadata commit unavailable")
	}
	return nil
}

func projectOperationNeedsOwner(operation string) bool {
	switch operation {
	case "query-events", "get-event-catalog", "get-event-profile", "get-event-input-contract", "rename-project":
		return false
	default:
		return true
	}
}
