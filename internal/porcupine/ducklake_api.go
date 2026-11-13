package porcupine

import (
	"context"
	"time"
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
	limits := analyticsLimits()
	limits.MaxReadRows, limits.MaxReadBytes = 0, 0
	if s.lakeMemoryMB > 0 {
		limits.MaxMemoryBytes = s.lakeMemoryMB << 20
	}
	return &AnalyticsSchemaOutput{Body: AnalyticsSchema{Enabled: true, Tables: []AnalyticsTable{{Name: "events", Columns: columns}}, Limits: limits, Delivery: delivery,
		SQL: AnalyticsSQL{Dialect: "duckdb", ResultCache: false, Functions: duckLakeSQLFunctions(),
			Features:    []string{"SELECT", "GROUP BY", "CTEs", "subqueries", "joins", "UNION", "window functions", "scalar casts", "$name bound parameters"},
			Unsupported: []string{"writes", "qualified tables", "private columns", "external sources", "table functions", "recursive CTEs", "time travel", "result cache", "physical scan row and byte quotas", "per-query memory and temporary-disk quotas", "remote catalogs", "analytical S3 storage"}}}}, nil
}

func (s *Service) lakeAnalyticsQuery(ctx context.Context, input *AnalyticsQueryInput) (*AnalyticsQueryOutput, error) {
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		return nil, apiError(429, "analytics query concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.lake.Query(ctx, input.ID, input.Body.SQL, input.Body.Parameters)
	if err != nil {
		return nil, err
	}
	delivery, err := s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	return &AnalyticsQueryOutput{Body: AnalyticsQuery{Result: result, Delivery: delivery}}, nil
}

func (s *Service) lakeEventCatalog(ctx context.Context, input EventCatalogInput) (*EventCatalogOutput, error) {
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		return nil, apiError(429, "analytics query concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	result, err := s.lake.Catalog(ctx, input)
	if err != nil {
		return nil, err
	}
	result.Delivery, err = s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	return &EventCatalogOutput{Body: result}, nil
}
