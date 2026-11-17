package porcupine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func checkpointCurrentFileFixture(t *testing.T, store *lakeapi.DuckLakeStore, project, kind string) string {
	t.Helper()
	ctx := context.Background()
	original := lakeStorageProjection(t, project, "checkpoint-original", 1)
	survivor := lakeStorageProjection(t, project, "checkpoint-survivor", 2)
	if err := store.Apply(ctx, []lakeapi.EventProjection{original, survivor}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, store, project)
	original.Revision = 1
	original.Event.Event = "checkpoint-edited"
	if err := store.Apply(ctx, []lakeapi.EventProjection{original}); err != nil {
		t.Fatal(err)
	}
	flushCleanupProject(t, store, project)
	p, release, err := store.LeaseProject(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	column := "data_file"
	if kind == "delete" {
		column = "delete_file"
	}
	var path string
	if err := p.DB.QueryRowContext(ctx, "SELECT "+column+" FROM ducklake_list_files('lake','current_events') WHERE "+column+" IS NOT NULL LIMIT 1").Scan(&path); err != nil {
		t.Fatalf("fixture did not produce a current %s file: %v", kind, err)
	}
	return path
}

func TestDuckLakeCheckpointMissingCurrentFilePreservesHealthyBackup(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"data", "delete"} {
		t.Run(kind, func(t *testing.T) {
			service, config := nativeBackupService(t)
			project, _ := project(t, service)
			store := service.lake.(*lakeapi.DuckLakeStore)
			missing := checkpointCurrentFileFixture(t, store, project, kind)
			ctx := context.Background()
			if err := service.createNativeBackup(ctx); err != nil {
				t.Fatal(err)
			}
			first := service.nativeBackupStatus().LatestBundle
			if err := os.Remove(missing); err != nil {
				t.Fatal(err)
			}
			if err := service.createNativeBackup(ctx); err == nil {
				t.Fatal("missing current analytical file was published as a healthy replacement")
			}
			if service.nativeBackupStatus().LatestBundle != first {
				t.Fatal("failed source capture replaced the last healthy backup")
			}
			if _, err := lakeapi.VerifyNativeBackup(ctx, filepath.Join(config.NativeBackup.Directory, first)); err != nil {
				t.Fatal("failed source capture pruned the last healthy backup", err)
			}
		})
	}
}

func TestDuckLakeS3CheckpointMissingCurrentFilePreservesHealthyBackup(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"data", "delete"} {
		t.Run(kind, func(t *testing.T) {
			fixture := nativeS3Fixture(t)
			backup := fixture.storage
			backup.Prefix += "-backup"
			root := t.TempDir()
			config := Config{DBPath: filepath.Join(root, "queue.db"), BootstrapToken: adminToken,
				DuckLake:     &lakeapi.DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour},
				NativeBackup: &lakeapi.NativeBackupConfig{S3: &backup, Retain: 1}}
			service, err := Open(config)
			s3FixtureError(t, fixture, "open incomplete-source backup fixture", err)
			defer service.Close()
			service.stopNativeBackups()
			service.lakeMaintenanceCancel()
			<-service.lakeMaintenanceDone
			project, _ := project(t, service)
			store := service.lake.(*lakeapi.DuckLakeStore)
			missing := checkpointCurrentFileFixture(t, store, project, kind)
			ctx := context.Background()
			s3FixtureError(t, fixture, "create healthy backup before source damage", service.createNativeBackup(ctx))
			first := service.nativeBackupStatus().LatestBundle
			source, err := store.SourceID(ctx)
			s3FixtureError(t, fixture, "read source identity", err)
			reader, err := lakeapi.NewNativeBackupS3(ctx, backup, source)
			s3FixtureError(t, fixture, "open healthy backup reader", err)
			defer reader.Close()
			key := strings.TrimPrefix(missing, "s3://"+fixture.storage.Bucket+"/")
			if key == missing || !strings.HasPrefix(key, store.Remote.ProjectPrefix(project)) {
				t.Fatal("fixture current file escaped its project prefix")
			}
			_, err = fixture.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(key)})
			s3FixtureError(t, fixture, "remove current source analytical file", err)
			if err := service.createNativeBackup(ctx); err == nil {
				t.Fatal("missing current S3 analytical file was published as a healthy replacement")
			}
			if service.nativeBackupStatus().LatestBundle != first {
				t.Fatal("incomplete source replaced the last healthy S3 backup")
			}
			manifest, err := reader.Manifest(ctx, first)
			s3FixtureError(t, fixture, "read preserved healthy manifest", err)
			for _, file := range manifest.Files {
				s3FixtureError(t, fixture, "verify preserved healthy backup file", reader.Verify(ctx, first, file))
			}
		})
	}
}
