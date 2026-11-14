//go:build ducklake

package porcupine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestDuckLakeS3BackupCompleteVerifiedInventoryAndFailure(t *testing.T) {
	fixture := nativeS3Fixture(t)
	upstream, _ := url.Parse(fixture.storage.Endpoint)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var fail atomic.Bool
	var corrupt atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() && r.Method == "PUT" && strings.HasSuffix(r.URL.Path, ".parquet") {
			http.Error(w, "injected upload failure", http.StatusServiceUnavailable)
			return
		}
		if corrupt.Load() && r.Method == "GET" && strings.HasSuffix(r.URL.Path, ".parquet") {
			w.Header().Set("Content-Length", "4")
			io.WriteString(w, "evil")
			return
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	backup := fixture.storage
	backup.Prefix += "-backup"
	backup.Endpoint = proxy.URL
	backup.CACertFile = ""
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaintenanceInterval: 24 * time.Hour}, NativeBackup: &NativeBackupConfig{S3: &backup, Retain: 1}}
	s, err := Open(config)
	s3FixtureError(t, fixture, "open backup source", err)
	defer s.Close()
	s.stopNativeBackups()
	projectID, ingest := project(t, s)
	other, _ := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"offhost-copy","actor_id":"user","name":"original"}`, 200)
	duckLakeJourneyDrain(t, s)
	store := s.lake.(*duckLakeStore)
	fixture.flush(t, store, projectID)
	fixture.flush(t, store, other)
	// Historical, currently unreferenced files are part of the conservative source-prefix copy.
	historical := fixture.projectPrefix(t, store, projectID) + "old-snapshot.parquet"
	fixture.put(t, historical, "historical snapshot bytes")
	outside := fixture.storage.Prefix + "/outside-sentinel"
	fixture.put(t, outside, "untouched")
	otherSource := backup.Prefix + "/native-backups/00000000-0000-0000-0000-000000000000/pc_backup_other/sentinel"
	fixture.put(t, otherSource, "other source")
	ctx := context.Background()
	err = s.createNativeBackup(ctx)
	s3FixtureError(t, fixture, "complete offhost backup", err)
	source, err := s.lake.SourceID(ctx)
	s3FixtureError(t, fixture, "source ID", err)
	reader, err := newNativeBackupS3(ctx, backup, source)
	s3FixtureError(t, fixture, "backup reader", err)
	defer reader.Close()
	first := s.nativeBackupStatus().LatestBundle
	m, err := reader.Manifest(ctx, first)
	s3FixtureError(t, fixture, "read completed manifest", err)
	if len(m.Catalogs) != 2 || m.SourceID != source || m.SourceStorage.Mode != "s3" || m.SourceStorage.Bucket != fixture.storage.Bucket || m.SourceStorage.Prefix != fixture.storage.Prefix {
		t.Fatalf("missing backup catalog descriptors: %+v", m.Catalogs)
	}
	historicalFound := false
	for _, file := range m.Files {
		body, err := reader.Open(ctx, first, file)
		s3FixtureError(t, fixture, "open copied file", err)
		h := sha256.New()
		n, err := io.Copy(h, body)
		body.Close()
		s3FixtureError(t, fixture, "read copied file", err)
		if n != file.Bytes || hex.EncodeToString(h.Sum(nil)) != file.SHA256 {
			t.Fatal("copied file is not exact")
		}
		historicalFound = historicalFound || strings.HasSuffix(file.Path, "/files/old-snapshot.parquet")
	}
	if !historicalFound {
		t.Fatal("historical source objects absent from completed inventory")
	}
	var writer string
	if err := s.db.QueryRow("SELECT value FROM metadata WHERE key='native_backup_writer_id'").Scan(&writer); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE metadata SET value=? WHERE key='native_backup_writer_id'", newID()); err != nil {
		t.Fatal(err)
	}
	if err := s.createNativeBackup(ctx); err == nil {
		t.Fatal("restored writer claimed live source backup namespace")
	}
	if _, err := reader.Manifest(ctx, first); err != nil {
		t.Fatal("owner refusal damaged original backup", err)
	}
	if _, err := s.db.Exec("UPDATE metadata SET value=? WHERE key='native_backup_writer_id'", writer); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := s.createNativeBackup(ctx); err == nil {
		t.Fatal("partial upload was published as complete")
	}
	fail.Store(false)
	if s.nativeBackupStatus().LatestBundle != first {
		t.Fatal("failure replaced last known successful backup")
	}
	if _, err := reader.Manifest(ctx, first); err != nil {
		t.Fatal("failed attempt damaged prior completed backup", err)
	}
	keys := fixture.keys(t, reader.namespace)
	for _, key := range keys {
		if key != reader.namespace+"owner.json" && !strings.HasPrefix(key, reader.namespace+first+"/") {
			t.Fatal("failed attempt was not removed", key)
		}
	}
	corrupt.Store(true)
	if err := s.createNativeBackup(ctx); err == nil {
		t.Fatal("destination corruption escaped verification")
	}
	corrupt.Store(false)
	// Retention protects the newly published ID even if the previous manifest's clock is later.
	m.CreatedAt = "2030-01-01T00:00:00Z"
	if err := reader.publish(ctx, m); err != nil {
		t.Fatal(err)
	}
	// A process crash may leave uploaded data without a manifest. The next attempt takes a fresh checkpoint.
	fixture.put(t, reader.namespace+"pc_backup_crashed/acceptance.sqlite", "abandoned")
	s3FixtureError(t, fixture, "retry backup after failures", s.createNativeBackup(ctx))
	second := s.nativeBackupStatus().LatestBundle
	if second == first {
		t.Fatal("completed backup reused an identity")
	}
	if _, err := reader.Manifest(ctx, second); err != nil {
		t.Fatal("new backup pruned after rollback", err)
	}
	for _, key := range fixture.keys(t, reader.namespace) {
		if key != reader.namespace+"owner.json" && !strings.HasPrefix(key, reader.namespace+second+"/") {
			t.Fatal("retention or incomplete cleanup failed", key)
		}
	}
	for _, key := range []string{historical, outside, otherSource} {
		out, err := fixture.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(key)})
		s3FixtureError(t, fixture, "unrelated object preserved", err)
		out.Body.Close()
	}
	if lakeStorageCount(t, store, other, "events") != 0 {
		t.Fatal("second project changed during backup")
	}
}

func TestDuckLakeS3BackupLocalSourceStreamsOffhost(t *testing.T) {
	fixture := nativeS3Fixture(t)
	s, _ := nativeBackupService(t)
	projectID, ingest := project(t, s)
	nativeRequest(t, s, ingest, `{"type":"track","message_id":"local-offhost","actor_id":"u","name":"kept"}`, 200)
	duckLakeJourneyDrain(t, s)
	flushCleanupProject(t, s.lake.(*duckLakeStore), projectID)
	s.backups.config.Directory = ""
	s.backups.config.S3 = &fixture.storage
	s3FixtureError(t, fixture, "local source offhost backup", s.createNativeBackup(context.Background()))
	source, err := s.lake.SourceID(context.Background())
	s3FixtureError(t, fixture, "source ID", err)
	reader, err := newNativeBackupS3(context.Background(), fixture.storage, source)
	s3FixtureError(t, fixture, "offhost reader", err)
	defer reader.Close()
	m, err := reader.Manifest(context.Background(), s.nativeBackupStatus().LatestBundle)
	s3FixtureError(t, fixture, "local offhost manifest", err)
	if m.SourceStorage != (nativeBackupStorageIdentity{Mode: "local"}) {
		t.Fatal("local source storage identity missing", m.SourceStorage)
	}
	parquet := false
	for _, file := range m.Files {
		parquet = parquet || strings.HasSuffix(file.Path, ".parquet")
		if err := reader.verify(context.Background(), m.ID, file); err != nil {
			t.Fatal(err)
		}
	}
	if !parquet {
		t.Fatal("local Parquet inventory was not copied")
	}
}
