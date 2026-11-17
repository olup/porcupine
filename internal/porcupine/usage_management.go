package porcupine

import (
	"context"
	"errors"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/usage"
)

type UsageListInput struct {
	Start       string `query:"start" doc:"Inclusive UTC midnight; default current UTC month start."`
	End         string `query:"end" doc:"Exclusive UTC midnight; default tomorrow. At most 366 days."`
	TenantID    string `query:"tenant_id" doc:"Optional tenant ID; omitted includes all tenants."`
	Granularity string `query:"granularity" enum:"day,month" doc:"UTC calendar buckets; default day."`
	GroupBy     string `query:"group_by" enum:"project,tenant" doc:"Project rows or disjoint tenant aggregates; default project."`
	Limit       int    `query:"limit" minimum:"1" maximum:"1000" doc:"Rows per page; default 100. Keep unchanged across snapshot pages."`
	Cursor      string `query:"cursor" maxLength:"2048" doc:"Opaque continuation cursor; replayable for 15 minutes. Other filters may be omitted on subsequent pages."`
}
type UsageListOutput struct{ Body usage.UsagePage }

func (s *Service) registerUsage(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	operation := op("list-usage", "GET", "/api/v1/usage", "Read replayable numerical consumption by tenant/project and UTC day/month; deployment operator only", auth.OperationAccess{Permission: "instance:admin"}, 200)
	operation.Description = "Cumulative live/import acceptance, admitted interactive SQL/catalog/profile query count and observed native wall time, published encoded export/pipeline data bytes, unique successful webhook deliveries, and sampled current referenced Parquet bytes. No prices, SQL or event payload. A query's duration is a lower bound until its native execution actually returns; crashes leave unresolved work. Only durable published source totals are included, without a global sealed-through-time guarantee. Storage is sampled hourly, stale after two hours, and excludes obsolete objects and download traffic; missing coverage is unknown, never zero. Snapshots expire after 15 minutes; excessive periods/rows/bytes require narrowing the request. Deleted IDs retain numerical usage under the configured retention."
	huma.Register(api, operation, s.managementUsage)
}
func (s *Service) managementUsage(ctx context.Context, input *UsageListInput) (*UsageListOutput, error) {
	if err := tenantOperator(ctx); err != nil {
		return nil, err
	}
	if input.Cursor == "" {
		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		_ = s.synchronizeUsage(bounded)
		cancel()
	}
	page, err := s.usage.store.ReadSnapshot(ctx, usage.Query{Start: input.Start, End: input.End, TenantID: input.TenantID, Granularity: input.Granularity, GroupBy: input.GroupBy, Limit: input.Limit, Cursor: input.Cursor})
	if err != nil {
		switch {
		case errors.Is(err, usage.ErrExpired):
			return nil, apierr.Detailed(410, "usage_snapshot_expired", "usage snapshot expired", "Restart list-usage without the cursor.", "cursor")
		case errors.Is(err, usage.ErrCursor):
			return nil, apierr.Detailed(400, "usage_cursor_invalid", "invalid or mismatched usage pagination", "Use the cursor with its original filters and page size, or restart the read.", "cursor")
		case errors.Is(err, usage.ErrTooLarge):
			return nil, apierr.Detailed(413, "usage_snapshot_too_large", err.Error(), "Reduce the period or filter one tenant.", "")
		case errors.Is(err, usage.ErrCapacity):
			return nil, apierr.Detailed(503, "usage_snapshot_capacity", err.Error(), "Retry after the current construction or retained snapshots expire.", "")
		case errors.Is(err, usage.ErrQuery):
			return nil, apiError(400, err.Error())
		default:
			return nil, apiError(503, "usage storage unavailable; retry the read")
		}
	}
	return &UsageListOutput{Body: page}, nil
}
