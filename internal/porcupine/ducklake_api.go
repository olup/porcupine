package porcupine

import (
	"context"
	"encoding/json"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/nativequery"
)

func (s *Service) lakeAnalyticsSchema(ctx context.Context, project string) (*AnalyticsSchemaOutput, error) {
	delivery, err := s.deliveryStatus(ctx, project)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	columns := []AnalyticsColumn{
		{"id", "BIGINT", "Durable event ID, returned as a decimal string."},
		{"event_id", "BIGINT", "Alias of id."},
		{"uuid", "VARCHAR", "Stable event identifier within the project."},
		{"project_id", "VARCHAR", "Owning project, isolated in a separate analytical catalog."},
		{"event", "VARCHAR", "Event name."},
		{"event_type", "VARCHAR", "Canonical input category."},
		{"source_id", "VARCHAR", "Authenticated producer namespace."},
		{"message_id", "VARCHAR", "Original producer message ID."},
		{"distinct_id", "VARCHAR", "Actor identifier captured with the event."},
		{"person_id", "VARCHAR", "Current resolved project-local person."},
		{"person_properties", "VARCHAR", "Current traits encoded as JSON."},
		{"properties", "VARCHAR", "Custom JSON properties; use json_extract_string or JSON casts."},
		{"context", "VARCHAR", "Canonical JSON context: page.url, page.referrer, browser.name, browser.version, device.type, os.name, locale and session_id when supplied. Use json_extract_string(context,'$.browser.name')."},
		{"timestamp", "VARCHAR", "Original occurrence time in UTC RFC3339."},
		{"event_time", "TIMESTAMP WITH TIME ZONE", "Original occurrence time for temporal SQL."},
		{"timestamp_ms", "BIGINT", "Original occurrence time in Unix milliseconds."},
		{"received_at", "VARCHAR", "Server receipt time in UTC RFC3339."},
		{"received_at_ms", "BIGINT", "Server receipt time in Unix milliseconds."},
		{"revision", "UBIGINT", "Current revision for compare-and-swap mutations."},
		{"is_imported", "UTINYINT", "1 for historical imports; 0 for live ingestion."},
	}
	tenant, err := s.projectTenant(ctx, project)
	if err != nil {
		return nil, err
	}
	effective := effectiveInteractiveQueries(s.interactiveConfig, tenant.Budgets)
	limits := analyticsLimits()
	limits.MaxConcurrentQueries = effective.MaxConcurrent
	limits.MaxExecutionMS = effective.TimeoutMS
	limits.MaxExecutionSeconds = (effective.TimeoutMS + 999) / 1000
	limits.RequestTimeoutSeconds = limits.MaxExecutionSeconds + 5
	limits.MaxRows, limits.MaxResultBytes = effective.MaxRows, effective.MaxResultBytes
	limits.ObjectStorageRequestTimeoutSeconds = s.lakeS3RequestTimeoutSeconds
	limits.MetadataRequestTimeoutSeconds = s.lakeMetadataTimeoutSeconds
	limits.RequestTimeoutSeconds += s.lakeMetadataTimeoutSeconds
	if s.lakeS3RequestTimeoutSeconds > 0 {
		limits.RequestTimeoutSeconds += s.lakeS3RequestTimeoutSeconds
	}
	if s.lakeMemoryMB > 0 {
		limits.MaxMemoryBytes = s.lakeMemoryMB << 20
	}
	return &AnalyticsSchemaOutput{Body: AnalyticsSchema{Enabled: true, Tables: []AnalyticsTable{{Name: "events", Columns: columns}}, Limits: limits, TenantBudgets: tenant.Budgets, DeploymentLimits: s.interactiveConfig, Delivery: delivery,
		SQL: AnalyticsSQL{Dialect: "duckdb", Functions: nativequery.Functions(),
			Features:    []string{"SELECT", "GROUP BY", "CTEs", "subqueries", "joins", "UNION", "window functions", "scalar casts", "$name bound parameters"},
			Unsupported: []string{"writes", "qualified tables", "private columns", "external sources", "table functions", "recursive CTEs", "time travel", "per-query memory and temporary-disk quotas", "remote catalogs"}}}}, nil
}

func (s *Service) lakeAnalyticsQuery(ctx context.Context, input *AnalyticsQueryInput) (*AnalyticsQueryOutput, error) {
	native, limits, scope, err := s.beginUsageQuery(ctx, input.ID, "sql")
	if err != nil {
		return nil, err
	}
	defer scope.Finish()
	deadline, _ := native.Deadline()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	scope.Begin()
	result, err := s.interactiveReader().Query(native, input.ID, input.Body.SQL, input.Body.Parameters, limits.executionLimits())
	err = interactiveQueryResultError(native, scope.Finish(), err)
	if err != nil {
		return nil, err
	}
	delivery, err := s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	output := AnalyticsQuery{Result: result, Delivery: delivery}
	encoded, err := json.Marshal(output)
	if err != nil || len(encoded) > limits.MaxResultBytes {
		return nil, analyticsError(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
	}
	return &AnalyticsQueryOutput{Body: output}, nil
}

func (s *Service) lakeEventCatalog(ctx context.Context, input lakeapi.EventCatalogInput) (*EventCatalogOutput, error) {
	native, limits, scope, err := s.beginUsageQuery(ctx, input.ID, "catalog")
	if err != nil {
		return nil, err
	}
	defer scope.Finish()
	deadline, _ := native.Deadline()
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	scope.Begin()
	result, err := s.interactiveReader().Catalog(native, input)
	err = interactiveQueryResultError(native, scope.Finish(), err)
	if err != nil {
		return nil, err
	}
	if err := s.describeCatalogPage(ctx, input.ID, input.Event, &result); err != nil {
		return nil, apiError(503, "catalog descriptions unavailable")
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > limits.MaxResultBytes {
		return nil, apiError(429, "event catalog result byte limit reached")
	}
	return &EventCatalogOutput{Body: result}, nil
}
