package porcupine

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func queryInput(projectID, sql string) *AnalyticsQueryInput {
	input := &AnalyticsQueryInput{ID: projectID}
	input.Body.SQL = sql
	return input
}
func queryStatus(err error) int {
	if failure, ok := err.(*managementError); ok {
		return failure.GetStatus()
	}
	return 0
}
func TestAnalyticsValidationSchemaAndDisabledStorage(t *testing.T) {
	s := service(t, 20)
	id, ingest := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(ingest, "pending"), 200)
	schema, err := s.analyticsSchema(context.Background(), &AnalyticsSchemaInput{ID: id})
	if err != nil || schema.Body.Enabled || schema.Body.Delivery.Pending != 1 || schema.Body.Tables[0].Name != "events" || schema.Body.Limits.MaxRows != 1000 {
		t.Fatalf("incorrect disabled discovery: %#v %v", schema, err)
	}
	limits := schema.Body.Limits
	if schema.Body.SQL.Dialect != "duckdb" || limits.MaxMemoryBytes != defaultDuckLakeMemoryMB<<20 || limits.MemoryScope != "per_resident_project_engine" {
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
	for _, parameters := range []map[string]string{
		{"bad-name": "unsafe"}, {"name": strings.Repeat("x", 4097)}, {strings.Repeat("x", 65): "value"},
	} {
		if queryStatus(validateQueryParameters(parameters)) != 400 {
			t.Fatal("invalid scalar parameters accepted")
		}
	}
	tooMany := map[string]string{}
	for i := range 33 {
		tooMany[fmt.Sprintf("p%d", i)] = "value"
	}
	if queryStatus(validateQueryParameters(tooMany)) != 400 {
		t.Fatal("too many parameters accepted")
	}
	if err := validateQueryParameters(map[string]string{"unicode": "雪 ' ; DROP TABLE events", "name": strings.Repeat("x", 4096)}); err != nil {
		t.Fatal("valid bound scalar rejected", err)
	}
}
