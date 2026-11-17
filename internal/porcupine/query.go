package porcupine

import (
	"context"
	"encoding/json"
	"regexp"

	"github.com/olup/porcupine/internal/nativequery"
)

const (
	maxSQLBytes       = nativequery.MaxSQLBytes
	analyticsMaxRows  = 1000
	analyticsMaxBytes = 1 << 20
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
	MetadataRequestTimeoutSeconds      int    `json:"metadata_request_timeout_seconds,omitempty" doc:"Configured native PostgreSQL metadata I/O allowance. Shared workers enforce a fail-stop watchdog before an abandoned query permit can be reused."`
	MaxMemoryBytes                     int    `json:"max_memory_bytes" doc:"Configured memory budget shared by queries on each resident project engine. DuckDB allocations can exceed this; it is not a per-query limit or hard process RSS ceiling."`
	MemoryScope                        string `json:"memory_scope" doc:"Memory is configured per resident project engine, shared by its work; it is not an instance-wide or per-query bound."`
	MaxReadRows                        int    `json:"max_read_rows" doc:"Physical row scan quota; zero means unavailable for this engine."`
	MaxReadBytes                       int    `json:"max_read_bytes" doc:"Physical byte scan quota; zero means unavailable for this engine."`
	MaxConcurrentQueries               int    `json:"max_concurrent_queries" doc:"Effective tenant interactive concurrency, shared across its projects and bounded by deployment capacity."`
}

type AnalyticsSchema struct {
	Enabled          bool                   `json:"enabled"`
	Tables           []AnalyticsTable       `json:"tables"`
	Limits           AnalyticsLimits        `json:"limits"`
	SQL              AnalyticsSQL           `json:"sql"`
	TenantBudgets    TenantBudgets          `json:"tenant_budgets" doc:"Configured tenant policy; effective limits are bounded by deployment ceilings. No aggregate tenant consumption is included."`
	DeploymentLimits InteractiveQueryConfig `json:"deployment_limits" doc:"Worker safety ceilings, including analytical capacity shared with background work. Shared deployments additionally enforce tenant concurrency through PostgreSQL permits."`
	Delivery         DeliveryStatus         `json:"delivery" doc:"Project queue depth and last delivery success. A generic worker error is included only while this project has pending events."`
}

type AnalyticsSchemaOutput struct{ Body AnalyticsSchema }

type AnalyticsSQL struct {
	Dialect     string   `json:"dialect,omitempty" doc:"SQL dialect selected by this deployment."`
	ResultCache bool     `json:"result_cache" doc:"Whether this engine supports the documented result cache."`
	Functions   []string `json:"functions"`
	Features    []string `json:"features"`
	Unsupported []string `json:"unsupported"`
}

type AnalyticsQueryInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		SQL        string            `json:"sql" minLength:"1" maxLength:"16384" doc:"One SELECT against the project-scoped events table. FORMAT, SETTINGS, external sources and mutations are forbidden."`
		Parameters map[string]string `json:"parameters,omitempty" doc:"Bound DuckDB scalar parameters such as $event. Up to 32 identifier keys of at most 64 ASCII characters, each with a value up to 4096 bytes."`
		UseCache   *bool             `json:"use_cache,omitempty" doc:"DuckDB always executes fresh queries. This preference has no effect while get-event-schema reports result_cache=false."`
	}
}

type AnalyticsResultColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type AnalyticsStatistics struct {
	Elapsed float64 `json:"elapsed"`
}

type AnalyticsResult struct {
	Meta                   []AnalyticsResultColumn      `json:"meta"`
	Data                   []map[string]json.RawMessage `json:"data" doc:"Typed SQL JSON values. 64-bit integers are represented as strings to preserve precision."`
	Rows                   uint64                       `json:"rows"`
	Statistics             AnalyticsStatistics          `json:"statistics"`
	RowsBeforeLimitAtLeast *uint64                      `json:"rows_before_limit_at_least,omitempty"`
}

type AnalyticsQuery struct {
	Result   AnalyticsResult `json:"result"`
	Delivery DeliveryStatus  `json:"delivery" doc:"Project queue depth and last delivery success. A generic worker error is included only while this project has pending events."`
}

type AnalyticsQueryOutput struct{ Body AnalyticsQuery }

func analyticsLimits() AnalyticsLimits {
	return AnalyticsLimits{
		MaxSQLBytes: maxSQLBytes, MaxRows: analyticsMaxRows, MaxResultBytes: analyticsMaxBytes,
		MaxExecutionMS: 5000, MaxExecutionSeconds: 5, RequestTimeoutSeconds: 10, MaxMemoryBytes: defaultDuckLakeMemoryMB << 20,
		MemoryScope: "per_resident_project_engine", MaxConcurrentQueries: 2,
	}
}

func (s *Service) analyticsProject(ctx context.Context, id string) error {
	var exists int
	if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1)`, id).Scan(&exists); err != nil {
		return apiError(503, "project storage unavailable")
	}
	if exists == 0 {
		return apiError(404, "project not found")
	}
	return nil
}

func (s *Service) analyticsSchema(ctx context.Context, input *AnalyticsSchemaInput) (*AnalyticsSchemaOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	schema, err := s.lakeAnalyticsSchema(ctx, input.ID)
	if err != nil {
		return nil, err
	}
	schema.Body.Enabled = s.lake != nil
	return schema, nil
}

func (s *Service) analyticsQuery(ctx context.Context, input *AnalyticsQueryInput) (*AnalyticsQueryOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if err := validateQueryParameters(input.Body.Parameters); err != nil {
		return nil, err
	}
	if s.lake == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	return s.lakeAnalyticsQuery(ctx, input)
}

var analyticsParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validateQueryParameters(parameters map[string]string) error {
	if len(parameters) > 32 {
		return analyticsError(400, "invalid_parameter", "too many analytics query parameters", "Supply at most 32 bound scalar parameters.", "")
	}
	for name, value := range parameters {
		if !analyticsParameterName.MatchString(name) || len(value) > 4096 {
			return analyticsError(400, "invalid_parameter", "invalid analytics query parameter", "Use identifier keys of at most 64 ASCII characters and string values of at most 4096 bytes.", "")
		}
	}
	return nil
}
