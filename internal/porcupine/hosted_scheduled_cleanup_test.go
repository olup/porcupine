package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedScheduledCleanup(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	lake := lakeapi.DuckLakeConfig{
		PostgresURL: dsn, S3: &fixture.storage, Threads: 1, MemoryLimitMB: 128, MaxOpenProjects: 2,
		MaintenanceInterval: time.Hour, CleanupGrace: 3 * time.Second,
		RestoreWindow: time.Second, RestoreMaxReaderTime: time.Second, RestoreSafetyMargin: time.Second,
	}
	customerEndpoint := fixture.storage.Endpoint
	if fixture.storage.CACertFile != "" {
		upstream, err := url.Parse(customerEndpoint)
		if err != nil {
			t.Fatal(err)
		}
		proxy := httputil.NewSingleHostReverseProxy(upstream)
		proxy.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
		host := httptest.NewServer(proxy)
		t.Cleanup(host.Close)
		customerEndpoint = host.URL
	}
	base := Config{Profile: ProfileHosted, AcceptanceMode: AcceptanceModeBuffered, BootstrapToken: adminToken,
		Shared: &SharedConfig{DatabaseURL: dsn}, DuckLake: &lake, allowUnreleasedHostedFixture: true,
		Pipelines: &output.PipelineConfig{Key: bytes.Repeat([]byte{2}, 32), AllowedS3Endpoints: []string{customerEndpoint}},
	}
	s, host, _ := hostedCleanupWorker(t, base)
	s.pipelines.Stop()
	project, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "scheduled cleanup")
	neighbor, _ := sharedJourneyProject(t, host.URL, defaultTenantID, "scheduled neighbor")

	// Model a worker that completed one object and opened another attempt
	// upload before its lease and task were interrupted.
	destination := fixture
	destination.storage.Bucket += "-customer-" + eventapi.NewID()[:8]
	if _, err := destination.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(destination.storage.Bucket)}); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{Bucket: aws.String(destination.storage.Bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	prefix := fixture.storage.Prefix + "/customer"
	pipeline, attempt, run, incarnation, revision := eventapi.NewID(), eventapi.NewID(), eventapi.NewID(), eventapi.NewID(), eventapi.NewID()
	key := fmt.Sprintf("%s/0/%s/part-000001.jsonl.gz", pipeline, attempt)
	unowned := prefix + "/" + key + "-neighbor"
	deliveredAttempt, deliveredRun := eventapi.NewID(), eventapi.NewID()
	deliveredKey := fmt.Sprintf("%s/60000/%s/part-000001.jsonl.gz", pipeline, deliveredAttempt)
	markerKey := prefix + "/" + pipeline + "/60000/manifest.json"
	checkpoint := fmt.Sprintf(`{"project_id":%q,"pipeline_id":%q,"run_id":%q,"window_start_ms":60000,"window_end_ms":120000,"files":[{"key":%q,"bytes":24}]}`, project, pipeline, deliveredRun, prefix+"/"+deliveredKey)
	destination.put(t, prefix+"/"+key, "completed attempt bytes")
	destination.put(t, unowned, "keep neighbor bytes")
	destination.put(t, prefix+"/"+deliveredKey, "published customer bytes")
	destination.put(t, markerKey, "unrecognized customer marker")
	upload, err := destination.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(destination.storage.Bucket), Key: aws.String(prefix + "/" + key),
	})
	if err != nil {
		t.Fatal(err)
	}
	config, _ := json.Marshal(output.S3PipelineSpec{Format: "jsonl", Destination: output.S3PipelineDestination{
		Bucket: destination.storage.Bucket, Region: destination.storage.Region, Prefix: prefix, Endpoint: customerEndpoint, UsePathStyle: true,
	}})
	private, _ := json.Marshal(output.S3PipelineSecret{AccessKeyID: destination.storage.AccessKeyID, SecretAccessKey: destination.storage.SecretAccessKey})
	binding, _ := json.Marshal([4]string{defaultTenantID, pipeline, incarnation, revision})
	sealed, err := s.pipelines.SealSecret(project, string(binding), private)
	if err != nil {
		t.Fatal(err)
	}
	install := func(tx *sql.Tx) error {
		if err := s.pipelines.EnsureHostedPipelineCapacityTx(ctx, tx, defaultTenantID, project); err != nil {
			return err
		}
		for _, statement := range []struct {
			query string
			args  []any
		}{
			{`INSERT INTO pipeline_effect_definitions VALUES($1,$2,$3,$4,'rule',$5,'creator','active',$6,'','sha',1)`,
				[]any{defaultTenantID, project, pipeline, incarnation, revision, "scheduled-query/control-rule/" + pipeline + "/rule"}},
			{`INSERT INTO hosted_pipeline_configs VALUES($1,$2,$3,'scheduled',$4,$5,0,'s3')`, []any{project, pipeline, defaultTenantID, string(config), sealed}},
			{`INSERT INTO hosted_s3_pipeline_state(project_id,pipeline_id,cursor_ms,interval_ms) VALUES($1,$2,60000,60000)`, []any{project, pipeline}},
			{`INSERT INTO hosted_s3_pipeline_runs(id,project_id,pipeline_id,window_start_ms,window_end_ms,snapshot_catalog_id,snapshot_version,
state,attempt_id,destination_revision,secret,created_at_ms,reserved_bytes) VALUES($1,$2,$3,0,60000,'catalog',0,'canceled',$4,$5,$6,0,512)`, []any{run, project, pipeline, attempt, revision, sealed}},
			{`INSERT INTO hosted_s3_pipeline_runs(id,project_id,pipeline_id,window_start_ms,window_end_ms,snapshot_catalog_id,snapshot_version,
state,attempt_id,destination_revision,secret,created_at_ms,reserved_bytes,manifest) VALUES($1,$2,$3,60000,120000,'catalog',0,'failed',$4,$5,$6,0,512,$7)`, []any{deliveredRun, project, pipeline, deliveredAttempt, revision, sealed, checkpoint}},
			{`INSERT INTO hosted_s3_pipeline_files(run_id,object_key,upload_id) VALUES($1,$2,$3),($4,$5,'')`, []any{run, key, aws.ToString(upload.UploadId), deliveredRun, deliveredKey}},
			{`UPDATE pipeline_capacity SET used_rows=used_rows+258,used_artifacts=used_artifacts+$3,used_proofs=used_proofs+2
WHERE scope_kind='instance' OR (scope_kind='tenant' AND scope_id=$1) OR (scope_kind='project' AND scope_id=$2)`, []any{defaultTenantID, project, 2 * (512 + exportstore.MaxManifestBytes)}},
		} {
			if _, err := tx.ExecContext(ctx, statement.query, statement.args...); err != nil {
				return err
			}
		}
		return nil
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := install(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := beginHostedProjectActivity(ctx, s.control, project, "scheduled-worker"); err != nil {
		t.Fatal(err)
	}
	if err := s.beginControlProjectDeletion(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := s.hosted.flusher.sealProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanHostedProject(ctx, project); !errors.Is(err, errHostedProjectBusy) {
		t.Fatalf("cleanup crossed the active attempt: %v", err)
	}
	if err := endHostedProjectActivity(ctx, s.control, project, "scheduled-worker"); err != nil {
		t.Fatal(err)
	}
	if err := s.cleanHostedProject(ctx, project); err == nil {
		t.Fatal("cleanup accepted the unrecognized customer marker")
	}
	destination.put(t, markerKey, checkpoint)
	if err := s.cleanHostedProject(ctx, project); err != nil {
		t.Fatal(err)
	}
	var artifacts int
	if err := s.control.QueryRowContext(ctx, `SELECT count(*) FROM hosted_project_cleanup_objects WHERE project_id=$1 AND kind=$2`, project, output.HostedDeletionScheduled).Scan(&artifacts); err != nil || artifacts != 2 {
		t.Fatalf("provider version and upload inventory count=%d err=%v", artifacts, err)
	}
	if err := s.cleanHostedProject(ctx, project); !errors.Is(err, errHostedRecoveryFloor) {
		t.Fatalf("cleanup crossed the recovery floor: %v", err)
	}
	if _, err := s.control.ExecContext(ctx, `UPDATE hosted_project_cleanup SET inventory_at=clock_timestamp()-interval '1 minute' WHERE project_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	s.hosted.recoveryFloor = func(context.Context) (time.Time, error) { return time.Now().Add(-time.Second), nil }
	for range 4 {
		if err := s.cleanHostedProject(ctx, project); err != nil {
			t.Fatal(err)
		}
	}
	var completed, neighborPresent, publishedBytes int
	if err := s.control.QueryRowContext(ctx, `SELECT (SELECT completed FROM project_deletions WHERE project_id=$1),
(SELECT count(*) FROM projects WHERE id=$2),
(SELECT coalesce(sum(pipeline_bytes),0) FROM usage_rollups WHERE project_id=$1)`, project, neighbor).
		Scan(&completed, &neighborPresent, &publishedBytes); err != nil || completed != 1 || neighborPresent != 1 || publishedBytes != 24 {
		t.Fatalf("cleanup completion=%d neighbor=%d published bytes=%d err=%v", completed, neighborPresent, publishedBytes, err)
	}
	expected := []string{unowned, markerKey, prefix + "/" + deliveredKey}
	slices.Sort(expected)
	if remaining := destination.keys(t, prefix+"/"); !slices.Equal(remaining, expected) {
		t.Fatalf("unexpected customer keys after exact cleanup: %v", remaining)
	}
	ownedKey := prefix + "/" + key
	versions, err := destination.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
		Bucket: aws.String(destination.storage.Bucket), Prefix: aws.String(ownedKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, version := range versions.Versions {
		if aws.ToString(version.Key) == ownedKey {
			t.Fatal("completed unpublished object version survived")
		}
	}
	uploads, err := destination.client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(destination.storage.Bucket), Prefix: aws.String(ownedKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, active := range uploads.Uploads {
		if aws.ToString(active.Key) == ownedKey {
			t.Fatal("unpublished multipart upload survived")
		}
	}

}
