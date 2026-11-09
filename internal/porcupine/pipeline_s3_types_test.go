package porcupine

import (
	"strings"
	"testing"
)

func testS3Spec() S3PipelineSpec {
	return S3PipelineSpec{SQL: "SELECT event, count() AS n FROM events GROUP BY event", Format: "jsonl", IntervalSeconds: 60, StartAt: "2026-11-08T12:00:00Z", Destination: S3PipelineDestination{Bucket: "client-bucket", Region: "us-east-1", Prefix: "private/exports"}}
}
func TestPipelineS3WindowSources(t *testing.T) {
	queries := []struct {
		sql     string
		sources int
	}{
		{"SELECT events.event FROM events", 1},
		{"SELECT '東京 events' AS label, uuid FROM events WHERE uuid IN (SELECT uuid FROM events)", 2},
		{"WITH q AS (SELECT uuid FROM events) SELECT a.uuid FROM events a JOIN q b ON a.uuid=b.uuid UNION ALL SELECT uuid FROM events", 3},
		{"SELECT e.event FROM events AS e", 1},
		{"SELECT a.uuid FROM events a INNER JOIN events b ON a.uuid=b.uuid", 2},
		{"WITH x AS (SELECT event FROM events) SELECT event FROM x", 1},
		{"SELECT event FROM events UNION ALL SELECT event FROM events", 2},
		{"SELECT event FROM (SELECT event FROM events) AS e", 1},
	}
	for _, q := range queries {
		t.Run(q.sql, func(t *testing.T) {
			sql, err := pipelineWindowSQL(q.sql, "pq_scope")
			if err != nil {
				t.Fatal(err)
			}
			if strings.Count(sql, "FROM pq_scope.events AS _porcupine_window WHERE _porcupine_window.received_at_ms >= {window_start:Int64} AND _porcupine_window.received_at_ms < {window_end:Int64} AND _porcupine_window.is_imported=0") != q.sources {
				t.Fatalf("window sources: %s", sql)
			}
			if strings.Contains(sql, "AS events AS") {
				t.Fatalf("double alias: %s", sql)
			}
		})
	}
	for _, q := range []string{"SELECT * FROM other.events", "WITH events AS (SELECT * FROM events) SELECT * FROM events", "SELECT * FROM remote('x',events)", "SELECT 1"} {
		if _, err := pipelineWindowSQL(q, "pq_scope"); err == nil {
			t.Errorf("accepted unsafe/no-source query %s", q)
		}
	}
}
func TestPipelineS3SpecValidation(t *testing.T) {
	good := testS3Spec()
	good.SQL = "SELECT count() FROM events WHERE received_at_ms >= {window_start:Int64} AND received_at_ms < {window_end:Int64} AND event={name:String}"
	good.Parameters = map[string]string{"name": "東京"}
	good.Destination.Endpoint = "http://127.0.0.1:9000"
	got, err := validateS3PipelineSpec(good)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Destination.UsePathStyle {
		t.Fatal("custom endpoint must stay at allowlisted origin")
	}
	withoutPrefix := testS3Spec()
	withoutPrefix.Destination.Prefix = ""
	normalized, err := validateS3PipelineSpec(withoutPrefix)
	if err != nil || normalized.Destination.Prefix != "porcupine" {
		t.Fatal("missing default S3 prefix", err)
	}
	changes := []func(*S3PipelineSpec){func(c *S3PipelineSpec) { c.StartAt = "2026-11-08T12:00:01Z" }, func(c *S3PipelineSpec) { c.Destination.Prefix = "../public" }, func(c *S3PipelineSpec) { c.SQL = "SELECT * FROM events WHERE received_at_ms>{window_start:String}" }, func(c *S3PipelineSpec) { c.Parameters = map[string]string{"window_start": "0"} }, func(c *S3PipelineSpec) { c.Parameters = map[string]string{"unused": "v"} }, func(c *S3PipelineSpec) { c.SQL = "SELECT * FROM other.events" }, func(c *S3PipelineSpec) { c.IntervalSeconds = 0 }, func(c *S3PipelineSpec) { c.SQL = "SELECT 1" }}
	for i, change := range changes {
		c := testS3Spec()
		change(&c)
		if _, err := validateS3PipelineSpec(c); err == nil {
			t.Errorf("invalid spec %d accepted", i)
		}
	}
	if validateS3PipelineSecret(S3PipelineSecret{}) == nil || validateS3PipelineSecret(S3PipelineSecret{AccessKeyID: "a"}) == nil {
		t.Fatal("default IAM credentials must never be used")
	}
}
