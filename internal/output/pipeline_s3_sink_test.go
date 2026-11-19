package output

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestScheduledSinkEmptyWindowAndCompressedLimit(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	fake := &exportTestS3{}
	server := httptest.NewServer(fake)
	defer server.Close()
	s := isolatedPipelines(t, nil, server.URL)
	s.config.ExportLimits.MaxOutputBytes = 12
	start := s.clock().UTC().Truncate(time.Minute).Add(-time.Minute)
	spec := S3PipelineSpec{
		SQL: "SELECT event FROM events", Format: "csv", IntervalSeconds: 60, StartAt: start.Format(time.RFC3339),
		Destination: S3PipelineDestination{Bucket: "private-reports", Region: "us-east-1", Endpoint: server.URL},
	}
	p, err := s.Define(ctx, "project", PipelineRequest{Name: "Report", Kind: "s3", S3: &spec,
		Credentials: PipelineCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}}, auth.Principal{ID: "actor"})
	if err != nil {
		t.Fatal(err)
	}
	work, err := s.nextPipelineS3(ctx)
	if err != nil || work == nil {
		t.Fatal("schedule did not produce a window", err)
	}
	work.attemptID, work.claimID = "attempt", "claim"
	if _, err := s.db.Exec(`UPDATE pipeline_s3_runs SET state='running',attempt_id=?,claim_id=? WHERE id=?`, work.attemptID, work.claimID, work.id); err != nil {
		t.Fatal(err)
	}
	store, err := s.pipelineS3Store(ctx, work.pipeline, work.spec)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	columns := []lakeapi.AnalyticsResultColumn{{Name: "event", Type: "VARCHAR"}}
	sink := newPipelineS3Sink(ctx, s, work, nil, store)
	if err := sink.setColumns(columns); err != nil {
		t.Fatal(err)
	}
	manifest, err := sink.finishManifest()
	if err != nil || manifest.PipelineID != p.ID || manifest.Rows != 0 || manifest.Files == nil || len(manifest.Files) != 0 || len(manifest.Columns) != 1 {
		t.Fatal("empty window did not keep its columns without creating a CSV part", manifest, err)
	}

	sink = newPipelineS3Sink(ctx, s, work, nil, store)
	if err := sink.setColumns(columns); err != nil {
		t.Fatal(err)
	}
	if err := sink.writeRow([]json.RawMessage{json.RawMessage(`"x"`)}); err != nil {
		t.Fatal("the CSV row fits the decoded byte limit", err)
	}
	if _, err := sink.finishManifest(); !errors.Is(err, errExportOutputLimit) {
		t.Fatal("gzip overhead should exceed the compressed byte limit", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.seq != 1 || fake.completes != 0 {
		t.Fatal("empty or oversized scheduled file was published")
	}
}
