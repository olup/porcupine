package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"net/http"
	"time"

	"github.com/olup/porcupine/internal/apierr"
)

const exceptionBudgetExceeded = "exception_ingestion_budget_exceeded"

type tenantIngestionError struct {
	code      string
	message   string
	periodEnd string
}

func (e *tenantIngestionError) Error() string { return e.message }

func (e *tenantIngestionError) retryHint() string {
	if e.code == "ingestion_suspended" {
		return "Retry after the operator restores ingestion for this tenant."
	}
	return "Retry after the operator changes the budget or the next UTC calendar month begins."
}

// The caller owns the acceptance transaction. Zero-count retries must remain
// valid even when new ingestion is suspended or its allowance is exhausted.
func consumeTenantEventsTx(ctx context.Context, tx *sql.Tx, project string, acceptedAt time.Time, count, exceptions int64) error {
	if count < 0 || exceptions < 0 || exceptions > count {
		return errors.New("invalid tenant event consumption")
	}
	if count == 0 {
		return nil
	}
	start, end := tenantMonth(acceptedAt)
	period := start.Format(time.RFC3339)
	var tenant string
	var allowed bool
	var allowance, exceptionAllowance sql.NullInt64
	var used, usedExceptions int64
	err := tx.QueryRowContext(ctx, `SELECT t.id,t.ingestion_allowed,t.ingestion_monthly_events,coalesce(u.accepted_events,0),t.ingestion_monthly_exceptions,coalesce(u.accepted_exceptions,0)
FROM projects p JOIN tenants t ON t.id=p.tenant_id
LEFT JOIN tenant_usage u ON u.tenant_id=t.id AND u.period_start=$1 WHERE p.id=$2`, period, project).Scan(&tenant, &allowed, &allowance, &used, &exceptionAllowance, &usedExceptions)
	if err != nil {
		return err
	}
	if !allowed {
		return &tenantIngestionError{code: "ingestion_suspended", message: "tenant ingestion is suspended", periodEnd: end.Format(time.RFC3339)}
	}
	if !allowance.Valid || used < allowance.Int64 {
		if err := checkExceptionAllowance(exceptionAllowance, usedExceptions, exceptions, end); err != nil {
			return err
		}
	}
	if allowance.Valid && (used >= allowance.Int64 || count > allowance.Int64-used) {
		return &tenantIngestionError{code: "ingestion_budget_exceeded", message: "tenant monthly ingestion budget exceeded", periodEnd: end.Format(time.RFC3339)}
	}
	if count > math.MaxInt64-used {
		return errors.New("tenant event counter exhausted")
	}
	return addTenantEventsTx(ctx, tx, tenant, period, count, exceptions)
}

func checkExceptionAllowance(limit sql.NullInt64, used, requested int64, end time.Time) error {
	if requested > 0 && limit.Valid && (used >= limit.Int64 || requested > limit.Int64-used) {
		return &tenantIngestionError{code: exceptionBudgetExceeded, message: "tenant monthly exception ingestion budget exceeded", periodEnd: end.Format(time.RFC3339)}
	}
	return nil
}

func addTenantEventsTx(ctx context.Context, tx *sql.Tx, tenant, period string, count, exceptions int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO tenant_usage(tenant_id,period_start,accepted_events,accepted_exceptions) VALUES($1,$2,$3,$4)
ON CONFLICT(tenant_id,period_start) DO UPDATE SET accepted_events=tenant_usage.accepted_events+excluded.accepted_events, accepted_exceptions=tenant_usage.accepted_exceptions+excluded.accepted_exceptions`, tenant, period, count, exceptions)
	return err
}

func tenantIngestionProblem(err error) error {
	var failure *tenantIngestionError
	if !errors.As(err, &failure) {
		return err
	}
	return apierr.Detailed(http.StatusTooManyRequests, failure.code, failure.message,
		"The upload checkpoint is unchanged. "+failure.retryHint(), "")
}

func writeTenantIngestionError(w http.ResponseWriter, err error, nativeResponse, mixed bool) bool {
	var failure *tenantIngestionError
	if !errors.As(err, &failure) {
		return false
	}
	markIngestionRejection(w, "unavailable")
	quota := "events"
	hint := "New events were not accepted. " + failure.retryHint()
	if failure.code == exceptionBudgetExceeded {
		quota = "exceptionEvent"
		if mixed {
			hint = "The batch was rejected. Resend the non-exception events separately. " + failure.retryHint()
		}
	}
	if !nativeResponse && !(mixed && failure.code == exceptionBudgetExceeded) {
		write(w, http.StatusOK, map[string]any{"status": 1, "accepted": 0, "duplicates": 0, "identity_ignored": 0,
			"quota_limited": []string{quota}, "code": failure.code, "error": failure.message})
		return true
	}
	write(w, http.StatusTooManyRequests, map[string]any{"error": failure.message, "code": failure.code,
		"period_end": failure.periodEnd, "hint": hint})
	return true
}
