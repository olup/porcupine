package porcupine

import (
	"testing"

	"github.com/olup/porcupine/internal/output"
)

func testS3Spec() output.S3PipelineSpec {
	return output.S3PipelineSpec{SQL: "SELECT event, count() AS n FROM events GROUP BY event", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-08T12:00:00Z", Destination: output.S3PipelineDestination{Bucket: "client-bucket", Region: "us-east-1", Prefix: "private/exports"}}
}
func TestPipelineS3SpecValidation(t *testing.T) {
	t.Parallel()
	s := duckLakeJourneyService(t)
	enableTestPipelines(t, s)
	projectID, _ := project(t, s)
	good := testS3Spec()
	good.SQL = "SELECT count() FROM events WHERE received_at_ms >= CAST($window_start AS BIGINT) AND received_at_ms < CAST($window_end AS BIGINT) AND event=$name"
	good.Parameters = map[string]string{"name": "東京"}
	good.Destination.Endpoint = "http://127.0.0.1:9000"
	got, err := s.pipelines.ValidateS3Spec(t.Context(), projectID, good)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Destination.UsePathStyle {
		t.Fatal("custom endpoint must stay at allowlisted origin")
	}
	withoutPrefix := testS3Spec()
	withoutPrefix.Destination.Prefix = ""
	normalized, err := s.pipelines.ValidateS3Spec(t.Context(), projectID, withoutPrefix)
	if err != nil || normalized.Destination.Prefix != "porcupine" {
		t.Fatal("missing default S3 prefix", err)
	}
	changes := []func(*output.S3PipelineSpec){func(c *output.S3PipelineSpec) { c.StartAt = "2026-11-08T12:00:01Z" }, func(c *output.S3PipelineSpec) { c.Destination.Prefix = "../public" }, func(c *output.S3PipelineSpec) {
		c.SQL = "SELECT * FROM events WHERE received_at_ms>CAST($window_start AS 'WRONG')"
	}, func(c *output.S3PipelineSpec) { c.Parameters = map[string]string{"window_start": "0"} }, func(c *output.S3PipelineSpec) { c.Parameters = map[string]string{"unused": "v"} }, func(c *output.S3PipelineSpec) { c.SQL = "SELECT * FROM other.events" }, func(c *output.S3PipelineSpec) { c.IntervalSeconds = 0 }, func(c *output.S3PipelineSpec) { c.SQL = "SELECT 1" }}
	for i, change := range changes {
		c := testS3Spec()
		change(&c)
		if _, err := s.pipelines.ValidateS3Spec(t.Context(), projectID, c); err == nil {
			t.Errorf("invalid spec %d accepted", i)
		}
	}
}
