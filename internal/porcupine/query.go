package porcupine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	analyticsTimeout  = 10 * time.Second
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
	MaxSQLBytes           int `json:"max_sql_bytes"`
	MaxRows               int `json:"max_rows"`
	MaxResultBytes        int `json:"max_result_bytes"`
	MaxExecutionSeconds   int `json:"max_execution_seconds"`
	RequestTimeoutSeconds int `json:"request_timeout_seconds"`
	MaxMemoryBytes        int `json:"max_memory_bytes"`
	MaxReadRows           int `json:"max_read_rows"`
	MaxReadBytes          int `json:"max_read_bytes"`
	MaxConcurrentQueries  int `json:"max_concurrent_queries" doc:"Instance-wide query concurrency limit"`
}

type AnalyticsSchema struct {
	Enabled  bool             `json:"enabled"`
	Tables   []AnalyticsTable `json:"tables"`
	Limits   AnalyticsLimits  `json:"limits"`
	SQL      AnalyticsSQL     `json:"sql"`
	Delivery DeliveryStatus   `json:"delivery" doc:"Project queue depth and last delivery success. A generic worker error is included only while this project has pending events."`
}

type AnalyticsSchemaOutput struct{ Body AnalyticsSchema }

type AnalyticsSQL struct {
	Functions   []string `json:"functions"`
	Features    []string `json:"features"`
	Unsupported []string `json:"unsupported"`
}

type AnalyticsQueryInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		SQL        string            `json:"sql" minLength:"1" maxLength:"16384" doc:"One SELECT against the project-scoped events table. FORMAT, SETTINGS, external sources and mutations are forbidden."`
		Parameters map[string]string `json:"parameters,omitempty" doc:"Typed ClickHouse placeholders such as {event:String}. Up to 32 identifier keys of at most 64 ASCII characters, each with a value up to 4096 bytes."`
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
	Data                   []map[string]json.RawMessage `json:"data" doc:"Typed ClickHouse JSON values. 64-bit integers are represented as strings to preserve precision."`
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
		MaxExecutionSeconds: 5, RequestTimeoutSeconds: 10, MaxMemoryBytes: 268435456,
		MaxReadRows: 5000000, MaxReadBytes: 1073741824, MaxConcurrentQueries: 2,
	}
}

func (s *Service) analyticsProject(ctx context.Context, id string) error {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM projects WHERE id=?`, id).Scan(&exists); err != nil {
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
	delivery, err := s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	columns := []AnalyticsColumn{
		{"id", "UInt64", "Durable event ID. JSON queries return this as a string."},
		{"revision", "UInt64", "Current event revision as a decimal string. Use it as expected_revision for an edit or deletion."},
		{"project_id", "String", "Owning project, enforced by ClickHouse row policies and the project view."},
		{"uuid", "String", "SDK event identifier, unique within this project."},
		{"event", "String", "Event name."},
		{"distinct_id", "String", "Event distinct ID; cookieless IDs rotate each UTC day."},
		{"properties", "String", "JSON object encoded as a string. Use JSONExtractString and related functions."},
		{"timestamp", "String", "Event timestamp in UTC RFC3339 format."},
		{"timestamp_ms", "Int64", "Event timestamp in Unix milliseconds."},
		{"event_time", "DateTime64(3, 'UTC')", "UTC event timestamp at millisecond precision for SQL date and time aggregation."},
		{"received_at", "String", "Server request arrival time in UTC RFC3339 format."},
		{"received_at_ms", "Int64", "Server request arrival time in Unix milliseconds."},
	}
	functions := append([]string(nil), sqlFunctionNames...)
	sort.Strings(functions)
	return &AnalyticsSchemaOutput{Body: AnalyticsSchema{
		Enabled: s.ch != nil, Tables: []AnalyticsTable{{Name: "events", Columns: columns}},
		Limits: analyticsLimits(), Delivery: delivery,
		SQL: AnalyticsSQL{Functions: functions,
			Features:    []string{"SELECT", "WHERE", "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "sequential CTEs", "subqueries", "joins", "UNION", "typed scalar parameters"},
			Unsupported: []string{"writes", "qualified tables", "external sources", "table functions", "system metadata", "FORMAT", "SETTINGS", "windows", "lambdas", "CAST", "recursive CTEs"}},
	}}, nil
}

func (s *Service) analyticsQuery(ctx context.Context, input *AnalyticsQueryInput) (*AnalyticsQueryOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if err := validateQueryParameters(input.Body.Parameters); err != nil {
		return nil, err
	}
	if err := validateSQLParameters(input.Body.SQL, input.Body.Parameters); err != nil {
		return nil, apiError(400, "invalid analytics query parameters")
	}
	canonical, err := validateSQL(input.Body.SQL)
	if err != nil {
		return nil, apiError(400, "unsupported analytics SQL")
	}
	if s.ch == nil {
		return nil, apiError(503, "analytics storage is not configured")
	}
	select {
	case s.querySlots <- struct{}{}:
		defer func() { <-s.querySlots }()
	default:
		return nil, apiError(429, "analytics query concurrency limit reached")
	}
	ctx, cancel := context.WithTimeout(ctx, analyticsTimeout)
	defer cancel()
	namespace, reader, password, err := s.analyticsReader(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "analytics reader unavailable")
	}
	data, err := s.ch.query(ctx, reader, password, namespace, canonical, input.Body.Parameters)
	if err != nil {
		return nil, s.analyticsStorageError(ctx, input.ID, err)
	}
	var result AnalyticsResult
	if json.Unmarshal(data, &result) != nil || result.Meta == nil || result.Data == nil || len(result.Data) > analyticsMaxRows || result.Rows != uint64(len(result.Data)) {
		return nil, apiError(503, "invalid analytics storage response")
	}
	delivery, err := s.deliveryStatus(ctx, input.ID)
	if err != nil {
		return nil, apiError(503, "delivery status unavailable")
	}
	return &AnalyticsQueryOutput{Body: AnalyticsQuery{Result: result, Delivery: delivery}}, nil
}

var analyticsParameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)

func validateQueryParameters(parameters map[string]string) error {
	if len(parameters) > 32 {
		return apiError(400, "too many analytics query parameters")
	}
	for name, value := range parameters {
		if !analyticsParameterName.MatchString(name) || len(value) > 4096 {
			return apiError(400, "invalid analytics query parameter")
		}
	}
	return nil
}

func analyticsQueryError(err error) error {
	var failure *clickHouseError
	if !errors.As(err, &failure) {
		return apiError(503, "analytics storage unavailable")
	}
	switch failure.Code {
	case 497, 164, 192:
		return apiError(403, "analytics query access denied")
	case 158, 159, 160, 161, 202, 241, 396:
		return apiError(429, "analytics query resource limit exceeded")
	case 6, 10, 16, 19, 20, 27, 36, 41, 42, 43, 44, 46, 47, 48, 60, 62, 63, 69, 70, 72, 117, 184, 215, 386, 407:
		return apiError(400, "invalid analytics query")
	default:
		return apiError(503, "analytics storage unavailable")
	}
}

func (s *Service) analyticsStorageError(ctx context.Context, projectID string, err error) error {
	var failure *clickHouseError
	if errors.As(err, &failure) && (failure.Code == 516 || failure.Code == 81 || failure.Code == 60) {
		if lockErr := s.queryMu.Lock(ctx); lockErr == nil {
			delete(s.queryReady, projectID)
			s.queryMu.Unlock()
		}
		return apiError(503, "analytics reader unavailable; retry after storage recovery")
	}
	return analyticsQueryError(err)
}

func (s *Service) analyticsReader(ctx context.Context, projectID string) (namespace, reader, password string, err error) {
	database := s.ch.config.Database
	digest := sha256.Sum256([]byte(database + "\x00" + projectID))
	namespace = "pc_" + hex.EncodeToString(digest[:16])
	reader = namespace + "_reader"
	mac := hmac.New(sha256.New, s.querySecret)
	mac.Write([]byte(projectID + "\x00" + database))
	password = hex.EncodeToString(mac.Sum(nil))
	if err = s.queryMu.Lock(ctx); err != nil {
		return
	}
	defer s.queryMu.Unlock()
	if err = ctx.Err(); err != nil {
		return
	}
	if s.queryReady[projectID] {
		return
	}
	if err = s.ch.initialize(ctx); err != nil {
		return
	}
	passwordHash := sha256.Sum256([]byte(password))
	authentication := " IDENTIFIED WITH sha256_hash BY '" + hex.EncodeToString(passwordHash[:]) + "'"
	settings := []string{
		"readonly=1 READONLY", "allow_ddl=0 READONLY", "allow_introspection_functions=0 READONLY", "final=1 READONLY", "optimize_move_to_prewhere_if_final=0 READONLY",
		"max_execution_time=5 READONLY", "timeout_overflow_mode='throw' READONLY",
		"max_memory_usage=268435456 READONLY", "max_rows_to_read=5000000 READONLY", "max_bytes_to_read=1073741824 READONLY",
		"read_overflow_mode='throw' READONLY", "max_result_rows=1000 READONLY", "max_result_bytes=1048576 READONLY",
		"result_overflow_mode='throw' READONLY", "max_threads=2 READONLY",
		"output_format_json_quote_64bit_integers=1 READONLY",
		"session_timezone='UTC' READONLY", "use_query_cache=0 READONLY",
	}
	// The project literal is hex encoded so no supplied identifier becomes SQL syntax.
	statements := []string{
		"CREATE DATABASE IF NOT EXISTS " + namespace,
		"CREATE USER IF NOT EXISTS " + reader + authentication,
		"ALTER USER " + reader + authentication + " DEFAULT ROLE NONE SETTINGS " + strings.Join(settings, ", "),
		"REVOKE ALL ON *.* FROM " + reader,
		"CREATE ROW POLICY OR REPLACE " + namespace + "_admin ON " + database + "." + eventTable + " USING 1 TO " + quoteAnalyticsIdentifier(s.ch.config.User),
		"CREATE ROW POLICY OR REPLACE " + namespace + "_scope ON " + database + "." + eventTable + " USING project_id=unhex('" + hex.EncodeToString([]byte(projectID)) + "') TO " + reader,
		"GRANT SELECT(event_id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,received_at,received_at_ms,revision) ON " + database + "." + eventTable + " TO " + reader,
		"CREATE OR REPLACE VIEW " + namespace + ".events SQL SECURITY INVOKER AS " +
			"SELECT event_id AS id,project_id,uuid,event,distinct_id,properties,timestamp,timestamp_ms,revision," +
			"fromUnixTimestamp64Milli(timestamp_ms,'UTC') AS event_time,received_at,received_at_ms FROM " + database +
			"." + eventTable + " FINAL WHERE project_id=unhex('" + hex.EncodeToString([]byte(projectID)) + "')",
		"GRANT SELECT ON " + namespace + ".events TO " + reader,
	}
	for _, statement := range statements {
		if _, err = s.ch.admin(ctx, statement); err != nil {
			return
		}
	}
	s.queryReady[projectID] = true
	return
}

func quoteAnalyticsIdentifier(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return "`" + strings.ReplaceAll(value, "`", "\\`") + "`"
}
