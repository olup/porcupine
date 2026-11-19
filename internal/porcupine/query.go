package porcupine

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/nativequery"
	"github.com/olup/porcupine/internal/query"
)

type AnalyticsSchemaInput struct {
	ID string `path:"id" doc:"Project identifier"`
}

type AnalyticsColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

type AnalyticsTable struct {
	Name    string            `json:"name"`
	Columns []AnalyticsColumn `json:"columns"`
}

type AnalyticsLimits struct {
	MaxSQLBytes                        int    `json:"max_sql_bytes"`
	MaxRows                            int    `json:"max_rows"`
	MaxResultBytes                     int    `json:"max_result_bytes"`
	MaxExecutionMS                     int    `json:"max_execution_ms" doc:"Effective tenant interactive execution deadline in milliseconds."`
	MaxExecutionSeconds                int    `json:"max_execution_seconds"`
	RequestTimeoutSeconds              int    `json:"request_timeout_seconds" doc:"Suggested client request timeout. Native object-storage I/O can finish after the SQL cancellation deadline."`
	ObjectStorageRequestTimeoutSeconds int    `json:"object_storage_request_timeout_seconds,omitempty" doc:"Native object-storage I/O timeout. Cancellation cannot immediately interrupt an in-flight HTTP request; it may extend the SQL execution deadline by this interval. Automatic HTTP retries are disabled."`
	MetadataRequestTimeoutSeconds      int    `json:"metadata_request_timeout_seconds,omitempty" doc:"Configured native PostgreSQL metadata I/O allowance. Shared execution permits renew until the call returns; failed renewal quarantines the project on this worker. Automatic expiry can overlap native work during a network partition."`
	MaxMemoryBytes                     int    `json:"max_memory_bytes" doc:"Configured memory budget shared by queries on each resident project engine. DuckDB allocations can exceed this; it is not a per-query limit or hard process RSS ceiling."`
	MemoryScope                        string `json:"memory_scope" doc:"Memory is configured per resident project engine, shared by its work; it is not an instance-wide or per-query bound."`
	MaxConcurrentQueries               int    `json:"max_concurrent_queries" doc:"Effective tenant interactive concurrency, shared across its projects and bounded by deployment capacity."`
}

type AnalyticsSchema struct {
	Enabled          bool                         `json:"enabled"`
	Tables           []AnalyticsTable             `json:"tables"`
	Limits           AnalyticsLimits              `json:"limits"`
	SQL              AnalyticsSQL                 `json:"sql"`
	TenantBudgets    TenantBudgets                `json:"tenant_budgets" doc:"Configured tenant policy; effective limits are bounded by deployment ceilings. No aggregate tenant consumption is included."`
	DeploymentLimits query.InteractiveQueryConfig `json:"deployment_limits" doc:"Worker safety ceilings, including analytical capacity shared with background work. Shared deployments additionally enforce tenant concurrency through PostgreSQL permits."`
	Delivery         lakeapi.DeliveryStatus       `json:"delivery" doc:"Project queue depth and last delivery success. A generic worker error is included only while this project has pending events."`
}

type AnalyticsSchemaOutput struct{ Body AnalyticsSchema }

type AnalyticsSQL struct {
	Dialect     string   `json:"dialect,omitempty" doc:"SQL dialect selected by this deployment."`
	Functions   []string `json:"functions"`
	Features    []string `json:"features"`
	Unsupported []string `json:"unsupported"`
}

type AnalyticsQueryInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		SQL        string                  `json:"sql" minLength:"1" maxLength:"16384" doc:"One SELECT against the project-scoped events table. Writes, external sources, table functions and private tables are forbidden."`
		Parameters map[string]string       `json:"parameters,omitempty" doc:"Bound DuckDB scalar parameters such as $event. Up to 32 identifier keys of at most 64 ASCII characters, each with a value up to 4096 bytes."`
		TimeRange  AnalyticsQueryTimeRange `json:"time_range,omitzero" required:"false" doc:"Restrict every events reference to this event occurrence time range. Omit for all time."`
	}
}

type AnalyticsQueryTimeRange struct {
	From    time.Time `json:"from" doc:"Inclusive event occurrence time in RFC3339."`
	To      time.Time `json:"to" doc:"Exclusive event occurrence time in RFC3339."`
	present bool
}

func (r *AnalyticsQueryTimeRange) UnmarshalJSON(data []byte) error {
	type bounds AnalyticsQueryTimeRange
	if string(data) == "null" {
		return errors.New("time_range must be an object")
	}
	if err := json.Unmarshal(data, (*bounds)(r)); err != nil {
		return err
	}
	r.present = true
	return nil
}

func (r AnalyticsQueryTimeRange) queryRange() (*lakeapi.QueryTimeRange, error) {
	if r == (AnalyticsQueryTimeRange{}) {
		return nil, nil
	}
	if !r.From.Before(r.To) {
		return nil, apierr.Detailed(400, "invalid_time_range", "Query time range start must be before its end", "Supply both RFC3339 bounds, with from earlier than to, or omit time_range.", "time_range")
	}
	return &lakeapi.QueryTimeRange{From: r.From, To: r.To}, nil
}

type AnalyticsQuery struct {
	Result   lakeapi.AnalyticsResult `json:"result"`
	Delivery lakeapi.DeliveryStatus  `json:"delivery" doc:"Project queue depth and last delivery success. A generic worker error is included only while this project has pending events."`
}

type AnalyticsQueryOutput struct{ Body AnalyticsQuery }

func (s *Service) analyticsProject(ctx context.Context, id string) error {
	var exists int
	if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)`, id).Scan(&exists); err != nil {
		return apierr.New(503, "project storage unavailable")
	}
	if exists == 0 {
		return apierr.New(404, "project not found")
	}
	return nil
}

func (s *Service) analyticsSchema(ctx context.Context, input *AnalyticsSchemaInput) (*AnalyticsSchemaOutput, error) {
	project := input.ID
	if err := s.analyticsProject(ctx, project); err != nil {
		return nil, err
	}
	delivery, err := s.deliveryStatus(ctx, project)
	if err != nil {
		return nil, apierr.New(503, "delivery status unavailable")
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
	deployment := s.queries.Limits()
	effective := deployment.Effective(query.InteractiveQueryConfig{
		MaxConcurrent: tenant.Budgets.HotQueryMaxConcurrent, TimeoutMS: tenant.Budgets.HotQueryTimeoutMS,
		MaxRows: tenant.Budgets.HotQueryMaxRows, MaxResultBytes: tenant.Budgets.HotQueryMaxResultBytes,
	})
	executionSeconds := (effective.TimeoutMS + 999) / 1000
	limits := AnalyticsLimits{
		MaxSQLBytes: lakeapi.MaxSQLBytes, MaxRows: effective.MaxRows, MaxResultBytes: effective.MaxResultBytes,
		MaxExecutionMS: effective.TimeoutMS, MaxExecutionSeconds: executionSeconds, MaxConcurrentQueries: effective.MaxConcurrent,
		RequestTimeoutSeconds:              executionSeconds + 5 + s.lakeMetadataTimeoutSeconds,
		ObjectStorageRequestTimeoutSeconds: s.lakeS3RequestTimeoutSeconds, MetadataRequestTimeoutSeconds: s.lakeMetadataTimeoutSeconds,
		MaxMemoryBytes: lakeapi.DefaultDuckLakeMemoryMB << 20, MemoryScope: "per_resident_project_engine",
	}
	if s.lakeS3RequestTimeoutSeconds > 0 {
		limits.RequestTimeoutSeconds += s.lakeS3RequestTimeoutSeconds
	}
	if s.lakeMemoryMB > 0 {
		limits.MaxMemoryBytes = s.lakeMemoryMB << 20
	}
	return &AnalyticsSchemaOutput{Body: AnalyticsSchema{Enabled: s.lake != nil, Tables: []AnalyticsTable{{Name: "events", Columns: columns}}, Limits: limits, TenantBudgets: tenant.Budgets, DeploymentLimits: deployment, Delivery: delivery,
		SQL: AnalyticsSQL{Dialect: "duckdb", Functions: nativequery.Functions(),
			Features:    []string{"SELECT", "GROUP BY", "CTEs", "subqueries", "joins", "UNION", "window functions", "scalar casts", "$name bound parameters"},
			Unsupported: []string{"writes", "qualified tables", "private columns", "external sources", "table functions", "recursive CTEs", "time travel", "per-query memory and temporary-disk quotas", "remote catalogs"}}}}, nil
}

func (s *Service) analyticsQuery(ctx context.Context, input *AnalyticsQueryInput) (*AnalyticsQueryOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if err := validateQueryParameters(input.Body.Parameters); err != nil {
		return nil, err
	}
	timeRange, err := input.Body.TimeRange.queryRange()
	if err != nil {
		return nil, err
	}
	if s.lake == nil {
		return nil, apierr.New(503, "analytics storage is not configured")
	}
	result, bounds, err := s.queries.SQL(ctx, s.interactiveReader(), input.ID, input.Body.SQL, input.Body.Parameters, timeRange)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithDeadline(ctx, bounds.Deadline)
	defer cancel()
	delivery, err := s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apierr.New(503, "delivery status unavailable")
	}
	output := AnalyticsQuery{Result: result, Delivery: delivery}
	encoded, err := json.Marshal(output)
	if err != nil || len(encoded) > bounds.MaxBytes {
		return nil, apierr.Detailed(429, "result_byte_limit", "analytics result byte limit reached", "Select fewer columns or use create-event-export for a larger result.", "")
	}
	return &AnalyticsQueryOutput{Body: output}, nil
}

var analyticsParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validateQueryParameters(parameters map[string]string) error {
	if len(parameters) > 32 {
		return apierr.Detailed(400, "invalid_parameter", "too many analytics query parameters", "Supply at most 32 bound scalar parameters.", "")
	}
	for name, value := range parameters {
		if !analyticsParameterName.MatchString(name) || len(value) > 4096 {
			return apierr.Detailed(400, "invalid_parameter", "invalid analytics query parameter", "Use identifier keys of at most 64 ASCII characters and string values of at most 4096 bytes.", "")
		}
	}
	return nil
}
