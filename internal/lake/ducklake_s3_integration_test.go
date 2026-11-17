package lake

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDuckLakeS3CleanupPreservesReaderThenReclaimsPayload(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	config := DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1}
	lake, err := NewDuckLake(config)
	s3FixtureError(t, fixture, "open S3 cleanup store", err)
	store := lake.(*DuckLakeStore)
	defer store.Close()
	ctx := context.Background()
	deleted := lakeStorageProjection(t, "project", "deleted", 1)
	deleted.Properties = json.RawMessage(`{"marker":"obsolete-S3-cleanup-payload"}`)
	live := lakeStorageProjection(t, "project", "live", 2)
	s3FixtureError(t, fixture, "seed S3 cleanup events", store.Apply(ctx, []EventProjection{deleted, live}))
	fixture.flush(t, store, "project")
	oldFiles := cleanupEventFiles(t, store, "project")
	project, release, err := store.LeaseProject(ctx, "project")
	s3FixtureError(t, fixture, "lease S3 snapshot", err)
	defer release()
	transaction, err := project.DB.BeginTx(ctx, nil)
	s3FixtureError(t, fixture, "begin old S3 reader", err)
	defer transaction.Rollback()
	var count int
	if err := transaction.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil || count != 2 {
		t.Fatal("old S3 reader did not observe the initial snapshot")
	}
	deleted.Revision, deleted.IsDeleted = 1, true
	s3FixtureError(t, fixture, "apply S3 deletion", store.Apply(ctx, []EventProjection{deleted}))
	future := time.Now().Add(10 * 24 * time.Hour)
	s3FixtureError(t, fixture, "cleanup with active S3 reader", store.CleanupDue(ctx, future))
	if err := transaction.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil || count != 2 {
		t.Fatal("native S3 cleanup broke an active reader")
	}
	prefix := fixture.projectPrefix(t, store, "project")
	keys := fixture.keys(t, prefix)
	for _, uri := range oldFiles {
		key := strings.TrimPrefix(uri, "s3://"+fixture.storage.Bucket+"/")
		if !containsS3Key(keys, key) {
			t.Fatal("S3 cleanup deleted a file held by the old reader")
		}
	}
	s3FixtureError(t, fixture, "finish old S3 reader", transaction.Commit())
	release()
	for _, advance := range []time.Duration{2 * time.Hour, 3 * 24 * time.Hour} {
		s3FixtureError(t, fixture, "reclaim obsolete S3 files", store.CleanupDue(ctx, future.Add(advance)))
	}
	keys = fixture.keys(t, prefix)
	for _, uri := range oldFiles {
		key := strings.TrimPrefix(uri, "s3://"+fixture.storage.Bucket+"/")
		if containsS3Key(keys, key) {
			t.Fatal("released historical S3 data file was not reclaimed")
		}
	}
	if lakeStorageCount(t, store, "project", "events") != 1 {
		t.Fatal("S3 cleanup removed a surviving event")
	}
	downloads := t.TempDir()
	for _, key := range keys {
		if !strings.HasSuffix(key, ".parquet") {
			continue
		}
		object, err := fixture.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(key)})
		s3FixtureError(t, fixture, "read remaining S3 Parquet", err)
		file, err := os.OpenFile(filepath.Join(downloads, filepath.Base(key)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			object.Body.Close()
			t.Fatal("cannot prepare downloaded test Parquet")
		}
		_, copyErr := io.Copy(file, io.LimitReader(object.Body, 32<<20))
		file.Close()
		object.Body.Close()
		if copyErr != nil {
			t.Fatal("cannot download remaining S3 Parquet")
		}
	}
	assertCleanupPayloadAbsent(t, downloads, "obsolete-S3-cleanup-payload")
	s3FixtureError(t, fixture, "retry original S3 event", store.Apply(ctx, []EventProjection{lakeStorageProjection(t, "project", "deleted", 1)}))
	tombstone, err := store.Load(ctx, "project", "deleted")
	s3FixtureError(t, fixture, "read preserved S3 tombstone", err)
	if !tombstone.IsDeleted || tombstone.OriginalSnapshot != "" {
		t.Fatal("S3 cleanup lost the payload-free retry tombstone")
	}
}

func TestDuckLakeS3SlowProjectDeletionLeavesOtherProjectsAvailable(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	lake, err := NewDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 2})
	s3FixtureError(t, fixture, "open slow-delete fixture", err)
	store := lake.(*DuckLakeStore)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for index, id := range []string{"a", "b"} {
		s3FixtureError(t, fixture, "seed slow-delete fixture", store.Apply(ctx, []EventProjection{lakeStorageProjection(t, id, id, int64(index+1))}))
		fixture.flush(t, store, id)
	}
	options := store.Remote.client.Options()
	paused := &pausedS3HTTPClient{underlying: options.HTTPClient, prefix: fixture.projectPrefix(t, store, "a"), entered: make(chan struct{}), resume: make(chan struct{})}
	options.HTTPClient = paused
	store.Remote.client = s3.New(options)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); done <- store.DeleteProject(ctx, "a") }()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(paused.resume) }) }
	defer func() { release(); cancel(); <-finished }()
	select {
	case <-paused.entered:
	case <-ctx.Done():
		t.Fatal("project deletion did not reach S3")
	}
	query, stop := context.WithTimeout(ctx, time.Second)
	result, err := store.Query(query, "b", "SELECT uuid FROM events", nil, DefaultQueryExecutionLimits())
	stop()
	if err != nil || len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"b"` {
		t.Fatal("one project's remote deletion blocked another project's analytical query")
	}
	release()
	s3FixtureError(t, fixture, "complete slow project deletion", <-done)
	if len(fixture.keys(t, paused.prefix)) != 0 || lakeStorageCount(t, store, "b", "events") != 1 {
		t.Fatal("slow S3 deletion changed the wrong project")
	}
}

func TestDuckLakeS3CredentialChainUsesOnlyIsolatedEnvironment(t *testing.T) {
	fixture := nativeS3Fixture(t)
	credentialsFile := filepath.Join(t.TempDir(), "empty-aws-config")
	if err := os.WriteFile(credentialsFile, nil, 0600); err != nil {
		t.Fatal("cannot prepare isolated AWS configuration")
	}
	for name, value := range map[string]string{
		"AWS_ACCESS_KEY_ID": fixture.storage.AccessKeyID, "AWS_SECRET_ACCESS_KEY": fixture.storage.SecretAccessKey,
		"AWS_SESSION_TOKEN": "", "AWS_REGION": fixture.storage.Region, "AWS_DEFAULT_REGION": fixture.storage.Region,
		"AWS_PROFILE": "", "AWS_CONFIG_FILE": credentialsFile, "AWS_SHARED_CREDENTIALS_FILE": credentialsFile,
		"AWS_EC2_METADATA_DISABLED": "true", "AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
		"AWS_WEB_IDENTITY_TOKEN_FILE": "", "AWS_ROLE_ARN": "", "AWS_ENDPOINT_URL": "", "AWS_ENDPOINT_URL_S3": "",
	} {
		t.Setenv(name, value)
	}
	storage := fixture.storage
	storage.AccessKeyID, storage.SecretAccessKey, storage.SessionToken = "", "", ""
	lake, err := NewDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1})
	s3FixtureError(t, fixture, "open native credential-chain store", err)
	store := lake.(*DuckLakeStore)
	defer store.Close()
	ctx := context.Background()
	s3FixtureError(t, fixture, "ingest with the native credential chain", store.Apply(ctx, []EventProjection{lakeStorageProjection(t, "chain", "credential-chain", 1)}))
	fixture.flush(t, store, "chain")
	prefix := fixture.projectPrefix(t, store, "chain")
	if len(fixture.keys(t, prefix)) == 0 {
		t.Fatal("native credential chain produced no remote analytical objects")
	}
	result, err := store.Query(ctx, "chain", "SELECT uuid FROM events", nil, DefaultQueryExecutionLimits())
	s3FixtureError(t, fixture, "query native credential-chain S3 files after configuration lock", err)
	if len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"credential-chain"` {
		t.Fatal("native credential-chain query lost its event")
	}
	s3FixtureError(t, fixture, "delete through Go SDK credential chain", store.DeleteProject(ctx, "chain"))
	if len(fixture.keys(t, prefix)) != 0 {
		t.Fatal("Go SDK credential chain did not remove the target's objects")
	}
}
