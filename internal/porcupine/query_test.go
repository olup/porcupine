package porcupine

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/apierr"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func queryInput(projectID, sql string) *AnalyticsQueryInput {
	input := &AnalyticsQueryInput{ID: projectID}
	input.Body.SQL = sql
	return input
}
func queryStatus(err error) int {
	if failure, ok := err.(*apierr.Error); ok {
		return failure.GetStatus()
	}
	return 0
}
func TestAnalyticsValidationSchemaAndDisabledStorage(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "pending"), 200)
	schema, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: id})
	if err != nil || schema.Body.Enabled || schema.Body.Delivery.Pending != 1 || schema.Body.Tables[0].Name != "events" || schema.Body.Limits.MaxRows != 1000 {
		t.Fatalf("incorrect disabled discovery: %#v %v", schema, err)
	}
	limits := schema.Body.Limits
	if schema.Body.SQL.Dialect != "duckdb" || limits.MaxMemoryBytes != lakeapi.DefaultDuckLakeMemoryMB<<20 || limits.MemoryScope != "per_resident_project_engine" {
		t.Fatal("native discovery advertised unsupported guarantees", schema.Body)
	}
	if _, err = s.analyticsQuery(context.Background(), queryInput(id, "SELECT count(*) FROM events")); queryStatus(err) != 503 {
		t.Fatal("disabled analytics must return 503")
	}
	for _, call := range []func() error{
		func() error {
			_, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: "missing"})
			return err
		},
		func() error {
			_, err := s.analyticsQuery(context.Background(), queryInput("missing", "SELECT count(*) FROM events"))
			return err
		},
	} {
		if queryStatus(call()) != 404 {
			t.Fatal("missing project must return 404")
		}
	}
}
func TestAnalyticsScalarParameterBudgets(t *testing.T) {
	t.Parallel()
	for _, parameters := range []map[string]string{
		{"bad-name": "unsafe"}, {"name": strings.Repeat("x", 4097)}, {strings.Repeat("x", 65): "value"},
	} {
		err := validateQueryParameters(parameters)
		if failure, ok := err.(*apierr.Error); !ok || failure.GetStatus() != 400 || failure.Code != "invalid_parameter" || failure.Message != "invalid analytics query parameter" {
			t.Fatalf("unexpected scalar parameter error: %#v", err)
		}
	}
	parameters := map[string]string{strings.Repeat("x", 64): strings.Repeat("é", 2048)}
	for i := range 31 {
		parameters[fmt.Sprintf("p%d", i)] = "value"
	}
	if err := validateQueryParameters(parameters); err != nil {
		t.Fatalf("maximum count, name and value sizes should be accepted: %v", err)
	}
	parameters["bad-name"] = "value"
	if err := validateQueryParameters(parameters); err == nil || err.Error() != "too many analytics query parameters" {
		t.Fatalf("parameter count must be checked before entries: %v", err)
	}
	if err := validateQueryParameters(map[string]string{"unicode": "雪 ' ; DROP TABLE events", "name": strings.Repeat("x", 4096)}); err != nil {
		t.Fatal("valid bound scalar rejected", err)
	}
}

func TestAnalyticsQueryTimeRangeRequests(t *testing.T) {
	t.Parallel()
	s := service(t, 20)
	id, _ := project(t, s)
	_, token := key(t, s, []string{"events:read"}, []string{id})
	path := "/api/v1/projects/" + id + "/query"
	for _, tc := range []struct {
		name, rangeJSON string
		status          int
	}{
		{"both bounds required", `{}`, 422},
		{"missing end", `{"from":"2026-11-01T00:00:00Z"}`, 422},
		{"missing start", `{"to":"2026-11-02T00:00:00Z"}`, 422},
		{"malformed bound", `{"from":"yesterday","to":"2026-11-02T00:00:00Z"}`, 422},
		{"bad type", `[]`, 422},
		{"null range", `null`, 422},
		{"null bound", `{"from":null,"to":"2026-11-02T00:00:00Z"}`, 422},
		{"empty range", `{"from":"2026-11-01T01:00:00+01:00","to":"2026-11-01T00:00:00Z"}`, 400},
		{"reversed range", `{"from":"2026-11-02T00:00:00Z","to":"2026-11-01T00:00:00Z"}`, 400},
		{"valid range reaches storage", `{"from":"2026-11-01T00:00:00Z","to":"2026-11-02T00:00:00Z"}`, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := `{"sql":"SELECT count(*) FROM events","time_range":` + tc.rangeJSON + `}`
			response := request(t, s, "POST", path, token, body, tc.status)
			if tc.status == 400 && response["code"] != "invalid_time_range" {
				t.Fatal(response)
			}
		})
	}
	request(t, s, "POST", path, token, `{"sql":"SELECT count(*) FROM events"}`, 503)
}

func TestAnalyticsQueryTimeRangeThroughHTTP(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	id, _ := project(t, s)
	otherID, _ := project(t, s)
	_, token := key(t, s, []string{"events:read"}, []string{id})
	events := []lakeapi.EventProjection{
		lakeStorageProjection(t, id, "before", 1),
		lakeStorageProjection(t, id, "inclusive", 2),
		lakeStorageProjection(t, id, "exclusive", 3),
		lakeStorageProjection(t, otherID, "foreign", 4),
	}
	events[0].Timestamp = "2026-11-01T00:59:59Z"
	events[1].Timestamp = "2026-11-01T01:00:00Z"
	events[2].Timestamp = "2026-11-01T02:00:00Z"
	events[3].Timestamp = "2026-11-01T01:00:00Z"
	if err := s.lake.Apply(t.Context(), events); err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/projects/" + id + "/query"
	sql := `"sql":"SELECT count(*) AS n FROM events a CROSS JOIN events b"`
	window := `"time_range":{"from":"2026-11-01T02:00:00+01:00","to":"2026-11-01T03:00:00+01:00"}`
	scoped := request(t, s, "POST", path, token, "{"+sql+","+window+"}", 200)
	if rows := scoped["result"].(map[string]any)["data"].([]any); len(rows) != 1 || rows[0].(map[string]any)["n"] != "1" {
		t.Fatalf("unexpected ranged result %v", rows)
	}
	unbounded := request(t, s, "POST", path, token, "{"+sql+"}", 200)
	if rows := unbounded["result"].(map[string]any)["data"].([]any); len(rows) != 1 || rows[0].(map[string]any)["n"] != "9" {
		t.Fatalf("unexpected all-time result %v", rows)
	}
	request(t, s, "POST", "/api/v1/projects/"+otherID+"/query", token, "{"+sql+","+window+"}", 403)
}
