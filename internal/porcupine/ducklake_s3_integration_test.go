//go:build ducklake

package porcupine

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var duckLakeS3ExtensionOnce sync.Once
var duckLakeS3ExtensionError error

type duckLakeS3Fixture struct {
	storage DuckLakeS3Config
	client  *s3.Client
}

func nativeS3Fixture(t *testing.T) duckLakeS3Fixture {
	t.Helper()
	endpoint := os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("run make ducklake-s3-test for the isolated native S3 fixture")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || !net.ParseIP(parsed.Hostname()).IsLoopback() {
		t.Fatal("native S3 integration tests require a loopback-only endpoint")
	}
	bucket := os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_BUCKET")
	if !strings.HasPrefix(bucket, "porcupine-ducklake-test-") {
		t.Fatal("native S3 integration tests require their dedicated fixture bucket")
	}
	duckLakeS3ExtensionOnce.Do(func() {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			duckLakeS3ExtensionError = err
			return
		}
		defer db.Close()
		for _, extension := range []string{"httpfs", "aws"} {
			if _, err := db.Exec("LOAD " + extension); err != nil {
				if _, err := db.Exec("INSTALL " + extension); err != nil {
					duckLakeS3ExtensionError = err
					return
				}
				if _, err := db.Exec("LOAD " + extension); err != nil {
					duckLakeS3ExtensionError = err
					return
				}
			}
		}
	})
	if duckLakeS3ExtensionError != nil {
		t.Fatal("cannot prepare the native S3 test extensions")
	}
	config := DuckLakeS3Config{Bucket: bucket, Endpoint: endpoint, Region: os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_REGION"),
		Prefix:      path.Join(os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_PREFIX"), t.Name()),
		AccessKeyID: os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_SECRET_ACCESS_KEY"),
		UsePathStyle: true, CACertFile: os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_CA_CERT_FILE")}
	if config.AccessKeyID == "" || config.SecretAccessKey == "" {
		t.Fatal("isolated native S3 credentials are missing")
	}
	transport := &http.Transport{}
	if config.CACertFile != "" {
		cert, err := os.ReadFile(config.CACertFile)
		if err != nil {
			t.Fatal("cannot read the native S3 fixture CA")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(cert) {
			t.Fatal("invalid native S3 fixture CA")
		}
		transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(client.CloseIdleConnections)
	return duckLakeS3Fixture{storage: config, client: s3.New(s3.Options{Region: config.Region, BaseEndpoint: aws.String(endpoint), UsePathStyle: true,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, "")), HTTPClient: client, RetryMaxAttempts: 2})}
}

func s3FixtureError(t *testing.T, fixture duckLakeS3Fixture, operation string, err error) {
	t.Helper()
	if err == nil {
		return
	}
	for _, secret := range []string{fixture.storage.AccessKeyID, fixture.storage.SecretAccessKey} {
		if secret != "" && strings.Contains(err.Error(), secret) {
			t.Fatal("native S3 failure exposed an operator credential")
		}
	}
	t.Fatalf("%s: %v", operation, err)
}

func (f duckLakeS3Fixture) keys(t *testing.T, prefix string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var keys []string
	pager := s3.NewListObjectsV2Paginator(f.client, &s3.ListObjectsV2Input{Bucket: aws.String(f.storage.Bucket), Prefix: aws.String(prefix)})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		s3FixtureError(t, f, "list fixture objects", err)
		for _, object := range page.Contents {
			keys = append(keys, aws.ToString(object.Key))
		}
	}
	return keys
}

func (f duckLakeS3Fixture) put(t *testing.T, key, payload string) {
	t.Helper()
	_, err := f.client.PutObject(context.Background(), &s3.PutObjectInput{Bucket: aws.String(f.storage.Bucket), Key: aws.String(key), Body: strings.NewReader(payload)})
	s3FixtureError(t, f, "write fixture sentinel", err)
}

func (f duckLakeS3Fixture) projectPrefix(t *testing.T, store *duckLakeStore, project string) string {
	t.Helper()
	source, err := store.SourceID(context.Background())
	s3FixtureError(t, f, "read native source identity", err)
	digest := sha256.Sum256([]byte(project))
	return path.Join(f.storage.Prefix, source, hex.EncodeToString(digest[:])) + "/"
}

func (f duckLakeS3Fixture) flush(t *testing.T, store *duckLakeStore, id string) {
	t.Helper()
	p, release, err := store.leaseProject(context.Background(), id)
	s3FixtureError(t, f, "lease project before native flush", err)
	defer release()
	s3FixtureError(t, f, "flush native S3 Parquet", p.maintenanceCommand(context.Background(), "CALL ducklake_flush_inlined_data('lake')", &duckLakeMaintenanceStats{}))
}

func nativeS3Service(t *testing.T, fixture duckLakeS3Fixture) (*Service, Config) {
	t.Helper()
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "acceptance.db"), BootstrapToken: adminToken,
		DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 2, MaintenanceInterval: 24 * time.Hour}}
	s, err := Open(config)
	s3FixtureError(t, fixture, "open native S3 service", err)
	s.lakeMaintenanceCancel()
	<-s.lakeMaintenanceDone
	s.deliveryCancel()
	<-s.deliveryDone
	return s, config
}

func TestDuckLakeS3HTTPJourneyRestartAndProjectDeletion(t *testing.T) {
	fixture := nativeS3Fixture(t)
	s, config := nativeS3Service(t, fixture)
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	a, ingestA := project(t, s)
	b, ingestB := project(t, s)
	_, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	_, manager := key(t, s, []string{"events:read", "events:edit", "events:delete"}, []string{a})
	page, _ := json.Marshal(map[string]any{"api_key": ingestA, "uuid": "s3-page", "event": "$pageview", "distinct_id": "browser", "properties": map[string]any{"$session_id": "s3-session"}})
	request(t, s, "POST", "/capture/", "", string(page), 200)
	purchase := `{"type":"track","message_id":"s3-purchase","actor_id":"person","name":"Purchase","properties":{"amount":42}}`
	first := receiptFrom(t, nativeRequest(t, s, ingestA, purchase, 200), 0)
	removed := receiptFrom(t, nativeRequest(t, s, ingestA, `{"type":"track","message_id":"s3-remove","actor_id":"browser","name":"Remove"}`, 200), 0)
	nativeRequest(t, s, ingestA, `{"type":"identify","message_id":"s3-identify","anonymous_id":"browser","user_id":"person","set":{"plan":"team"}}`, 200)
	request(t, s, "POST", "/capture/", "", captureBody(ingestB, "s3-other-project"), 200)
	duckLakeJourneyDrain(t, s)
	store := s.lake.(*duckLakeStore)
	for _, id := range []string{a, b} {
		fixture.flush(t, store, id)
	}
	prefixA, prefixB := fixture.projectPrefix(t, store, a), fixture.projectPrefix(t, store, b)
	if len(fixture.keys(t, prefixA)) == 0 || len(fixture.keys(t, prefixB)) == 0 {
		t.Fatal("native flush did not create isolated S3 project objects")
	}
	if len(cleanupParquetFiles(t, config.DuckLake.Directory)) != 0 {
		t.Fatal("S3-backed analytical Parquet unexpectedly remained on local disk")
	}
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT uuid,event_id,person_id,person_properties FROM events ORDER BY id", nil)
	if len(rows) != 3 {
		t.Fatalf("unexpected S3 logical events: %d", len(rows))
	}
	for _, row := range rows {
		if row.(map[string]any)["person_id"] != "person" || !strings.Contains(row.(map[string]any)["person_properties"].(string), `"plan":"team"`) {
			t.Fatal("identify did not resolve on S3-backed events")
		}
	}
	edit := `{"expected_revision":"0","idempotency_key":"s3-correction","snapshot":{"event":"Corrected purchase","distinct_id":"person","properties":{"amount":99},"timestamp":"2026-11-14T00:00:00Z"}}`
	request(t, s, "PATCH", "/api/v1/projects/"+a+"/events/"+first["uuid"].(string), manager, edit, 202)
	request(t, s, "DELETE", "/api/v1/projects/"+a+"/events/"+removed["uuid"].(string), manager, `{"expected_revision":"0","idempotency_key":"s3-delete"}`, 202)
	duckLakeJourneyDrain(t, s)
	retry := nativeRequest(t, s, ingestA, purchase, 200)
	if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != first["event_id"] {
		t.Fatal("S3 delivery changed native retry identity")
	}
	rows = duckLakeJourneyQuery(t, s, a, reader, "SELECT event_id,event,properties FROM events WHERE event='Corrected purchase'", nil)
	if len(rows) != 1 || rows[0].(map[string]any)["event_id"] != first["event_id"] {
		t.Fatal("S3 correction lost the durable event ID")
	}
	request(t, s, "POST", "/api/v1/projects/"+b+"/query", reader, `{"sql":"SELECT * FROM events"}`, 403)
	for _, sql := range []string{
		"SELECT * FROM lake.current_events", "SELECT * FROM duckdb_secrets()", "SELECT * FROM duckdb_settings()",
		"SELECT * FROM read_parquet('s3://" + fixture.storage.Bucket + "/" + prefixB + "*.parquet')",
		"SELECT * FROM read_json_auto('http://127.0.0.1:1/private')", "SELECT * FROM other_project.events",
	} {
		body, _ := json.Marshal(map[string]any{"sql": sql})
		request(t, s, "POST", "/api/v1/projects/"+a+"/query", reader, string(body), 400)
	}
	source, err := store.SourceID(context.Background())
	s3FixtureError(t, fixture, "record S3 source", err)
	s3FixtureError(t, fixture, "close S3 service", s.Close())
	s = nil
	s, err = Open(config)
	s3FixtureError(t, fixture, "reopen S3 service", err)
	s.deliveryCancel()
	<-s.deliveryDone
	s.lakeMaintenanceCancel()
	<-s.lakeMaintenanceDone
	store = s.lake.(*duckLakeStore)
	reopened, err := store.SourceID(context.Background())
	s3FixtureError(t, fixture, "reopened source identity", err)
	if reopened != source {
		t.Fatal("S3 source identity changed across restart")
	}
	retry = nativeRequest(t, s, ingestA, purchase, 200)
	if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != first["event_id"] {
		t.Fatal("S3 restart lost native retry identity")
	}
	deletedRetry := nativeRequest(t, s, ingestA, `{"type":"track","message_id":"s3-remove","actor_id":"browser","name":"Remove"}`, 200)
	if deletedRetry["duplicates"] != float64(1) || receiptFrom(t, deletedRetry, 0)["event_id"] != removed["event_id"] {
		t.Fatal("S3 restart lost deletion retry identity")
	}
	rows = duckLakeJourneyQuery(t, s, a, reader, "SELECT event_id,event FROM events ORDER BY id", nil)
	if len(rows) != 2 {
		t.Fatal("S3 restart resurrected a deleted event or lost live history")
	}
	// Deletion may remove only the selected project's exact prefix. Keep both
	// another project's data and operator-owned neighboring objects untouched.
	sentinelOutside := path.Join(fixture.storage.Prefix, "operator-sentinel")
	sentinelNeighbor := strings.TrimSuffix(prefixA, "/") + "-neighbor/sentinel"
	fixture.put(t, sentinelOutside, "outside project")
	fixture.put(t, sentinelNeighbor, "neighbor project")
	beforeB := strings.Join(fixture.keys(t, prefixB), "\n")
	request(t, s, "DELETE", "/api/v1/projects/"+a, adminToken, "", 204)
	request(t, s, "DELETE", "/api/v1/projects/"+a, adminToken, "", 204)
	if len(fixture.keys(t, prefixA)) != 0 || strings.Join(fixture.keys(t, prefixB), "\n") != beforeB {
		t.Fatal("S3 project deletion retained target objects or changed another project")
	}
	for _, sentinel := range []string{sentinelOutside, sentinelNeighbor} {
		if keys := fixture.keys(t, sentinel); len(keys) != 1 || keys[0] != sentinel {
			t.Fatal("S3 deletion removed an object outside its exact project prefix")
		}
	}
	request(t, s, "POST", "/capture/", "", captureBody(ingestA, "after-delete"), 401)
	rows = duckLakeJourneyQuery(t, s, b, adminToken, "SELECT count(*) AS total FROM events", nil)
	if rows[0].(map[string]any)["total"] != "1" {
		t.Fatal("S3 project deletion changed unrelated analytics")
	}
}

func TestDuckLakeS3CleanupPreservesReaderThenReclaimsPayload(t *testing.T) {
	fixture := nativeS3Fixture(t)
	config := DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1}
	lake, err := newDuckLake(config)
	s3FixtureError(t, fixture, "open S3 cleanup store", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx := context.Background()
	deleted := lakeStorageProjection(t, "project", "deleted", 1)
	deleted.Properties = json.RawMessage(`{"marker":"obsolete-S3-cleanup-payload"}`)
	live := lakeStorageProjection(t, "project", "live", 2)
	s3FixtureError(t, fixture, "seed S3 cleanup events", store.Apply(ctx, []eventProjection{deleted, live}))
	fixture.flush(t, store, "project")
	oldFiles := cleanupEventFiles(t, store, "project")
	project, release, err := store.leaseProject(ctx, "project")
	s3FixtureError(t, fixture, "lease S3 snapshot", err)
	defer release()
	transaction, err := project.db.BeginTx(ctx, nil)
	s3FixtureError(t, fixture, "begin old S3 reader", err)
	defer transaction.Rollback()
	var count int
	if err := transaction.QueryRow("SELECT count(*) FROM events").Scan(&count); err != nil || count != 2 {
		t.Fatal("old S3 reader did not observe the initial snapshot")
	}
	deleted.Revision, deleted.IsDeleted = 1, true
	s3FixtureError(t, fixture, "apply S3 deletion", store.Apply(ctx, []eventProjection{deleted}))
	future := time.Now().Add(10 * 24 * time.Hour)
	s3FixtureError(t, fixture, "cleanup with active S3 reader", store.cleanupDue(ctx, future))
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
		s3FixtureError(t, fixture, "reclaim obsolete S3 files", store.cleanupDue(ctx, future.Add(advance)))
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
	s3FixtureError(t, fixture, "retry original S3 event", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, "project", "deleted", 1)}))
	tombstone, err := store.Load(ctx, "project", "deleted")
	s3FixtureError(t, fixture, "read preserved S3 tombstone", err)
	if !tombstone.IsDeleted || tombstone.OriginalSnapshot != "" {
		t.Fatal("S3 cleanup lost the payload-free retry tombstone")
	}
}

func containsS3Key(keys []string, wanted string) bool {
	for _, key := range keys {
		if key == wanted {
			return true
		}
	}
	return false
}

func TestDuckLakeS3RejectsIncompleteLocalBackups(t *testing.T) {
	fixture := nativeS3Fixture(t)
	root := t.TempDir()
	config := Config{DBPath: filepath.Join(root, "acceptance.db"), BootstrapToken: adminToken,
		DuckLake: &DuckLakeConfig{Directory: filepath.Join(root, "lake"), S3: &fixture.storage}, NativeBackup: &NativeBackupConfig{Directory: filepath.Join(root, "backups")}}
	service, err := Open(config)
	if err == nil {
		service.Close()
		t.Fatal("local backup scheduler accepted remote analytical files without a complete backup protocol")
	}
	config.NativeBackup = nil
	service, err = Open(config)
	s3FixtureError(t, fixture, "open S3 store without local backup", err)
	defer service.Close()
	checkpoint, err := service.lake.stageCheckpoint(context.Background(), func(context.Context, string) error { return nil })
	s3FixtureError(t, fixture, "capture empty S3 checkpoint", err)
	if len(checkpoint.Objects) != 0 {
		t.Fatal("empty S3 checkpoint included foreign objects")
	}
	s3FixtureError(t, fixture, "release empty S3 checkpoint", checkpoint.Close())
}

type pausedS3HTTPClient struct {
	underlying interface {
		Do(*http.Request) (*http.Response, error)
	}
	prefix  string
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *pausedS3HTTPClient) Do(request *http.Request) (*http.Response, error) {
	if request.Method == "GET" && request.URL.Query().Get("prefix") == c.prefix {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.resume:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}
	return c.underlying.Do(request)
}

func TestDuckLakeS3SlowProjectDeletionLeavesOtherProjectsAvailable(t *testing.T) {
	fixture := nativeS3Fixture(t)
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 2})
	s3FixtureError(t, fixture, "open slow-delete fixture", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for index, id := range []string{"a", "b"} {
		s3FixtureError(t, fixture, "seed slow-delete fixture", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, id, id, int64(index+1))}))
		fixture.flush(t, store, id)
	}
	options := store.remote.client.Options()
	paused := &pausedS3HTTPClient{underlying: options.HTTPClient, prefix: fixture.projectPrefix(t, store, "a"), entered: make(chan struct{}), resume: make(chan struct{})}
	options.HTTPClient = paused
	store.remote.client = s3.New(options)
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
	result, err := store.Query(query, "b", "SELECT uuid FROM events", nil)
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

func TestDuckLakeS3CanceledObjectReadRecovers(t *testing.T) {
	fixture := nativeS3Fixture(t)
	upstream, _ := url.Parse(fixture.storage.Endpoint)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var paused atomic.Bool
	entered := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if paused.Load() && r.Method == "GET" && strings.HasSuffix(r.URL.Path, ".parquet") {
			once.Do(func() { close(entered) })
			select {
			case <-resume:
			case <-r.Context().Done():
				return
			case <-time.After(3 * time.Second):
			}
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	storage := fixture.storage
	storage.Endpoint, storage.CACertFile = proxy.URL, ""
	storage.RequestTimeoutSeconds = 1
	config := DuckLakeConfig{Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1}
	lake, err := newDuckLake(config)
	s3FixtureError(t, fixture, "open S3 cancellation fixture", err)
	store := lake.(*duckLakeStore)
	defer func() { store.Close() }()
	ctx := context.Background()
	s3FixtureError(t, fixture, "seed S3 cancellation fixture", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, "a", "kept", 1)}))
	fixture.flush(t, store, "a")
	s3FixtureError(t, fixture, "close S3 cancellation seed", store.Close())
	lake, err = newDuckLake(config)
	s3FixtureError(t, fixture, "reopen S3 cancellation fixture", err)
	store = lake.(*duckLakeStore)
	paused.Store(true)
	limited, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	start := time.Now()
	_, err = store.Query(limited, "a", "SELECT uuid FROM events", nil)
	cancel()
	elapsed := time.Since(start)
	paused.Store(false)
	close(resume)
	select {
	case <-entered:
	default:
		t.Fatal("cancellation fixture did not exercise an actual remote Parquet read")
	}
	if err == nil || elapsed > 2*time.Second {
		t.Fatalf("S3 query cancellation was not bounded: error=%t elapsed=%s", err != nil, elapsed)
	}
	result, err := store.Query(ctx, "a", "SELECT uuid FROM events", nil)
	s3FixtureError(t, fixture, "query recovered S3 endpoint", err)
	if len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"kept"` {
		t.Fatal("native S3 connection did not recover after cancellation")
	}
}

func TestDuckLakeS3ColdOpenLeavesResidentProjectAvailable(t *testing.T) {
	fixture := nativeS3Fixture(t)
	upstream, _ := url.Parse(fixture.storage.Endpoint)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var blockPrefix atomic.Value
	blockPrefix.Store("")
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if prefix := blockPrefix.Load().(string); prefix != "" && strings.HasPrefix(r.URL.Path, prefix) && (r.Method == "HEAD" || r.Method == "GET") {
			once.Do(func() { close(entered) })
			select {
			case <-resume:
			case <-r.Context().Done():
				return
			}
		}
		forward.ServeHTTP(w, r)
	}))
	defer proxy.Close()
	storage := fixture.storage
	storage.Endpoint, storage.CACertFile = proxy.URL, ""
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 2})
	s3FixtureError(t, fixture, "open cold S3 fixture", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for index, id := range []string{"a", "b"} {
		s3FixtureError(t, fixture, "seed cold S3 fixture", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, id, id, int64(index+1))}))
		fixture.flush(t, store, id)
	}
	_, releaseB, err := store.leaseProject(ctx, "b")
	s3FixtureError(t, fixture, "hold resident S3 project", err)
	defer releaseB()
	s3FixtureError(t, fixture, "evict the cold S3 project", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, "c", "c", 3)}))
	if store.projects["a"] != nil {
		t.Fatal("cold-open fixture did not evict its target")
	}
	blockPrefix.Store("/" + fixture.storage.Bucket + "/" + fixture.projectPrefix(t, store, "a"))
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_, err := store.Query(ctx, "a", "SELECT uuid FROM events", nil)
		done <- err
	}()
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(resume) }) }
	defer func() { release(); cancel(); <-finished }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("cold opening did not exercise remote catalog-identity data")
	}
	limited, stop := context.WithTimeout(ctx, time.Second)
	result, err := store.Query(limited, "b", "SELECT uuid FROM events", nil)
	stop()
	if err != nil || len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"b"` {
		t.Fatal("cold-project S3 access blocked an already resident project")
	}
	release()
	s3FixtureError(t, fixture, "finish cold S3 opening", <-done)
	if len(store.projects) > 2 {
		t.Fatal("cold S3 admission exceeded the resident-engine budget")
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
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1})
	s3FixtureError(t, fixture, "open native credential-chain store", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx := context.Background()
	s3FixtureError(t, fixture, "ingest with the native credential chain", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, "chain", "credential-chain", 1)}))
	fixture.flush(t, store, "chain")
	prefix := fixture.projectPrefix(t, store, "chain")
	if len(fixture.keys(t, prefix)) == 0 {
		t.Fatal("native credential chain produced no remote analytical objects")
	}
	result, err := store.Query(ctx, "chain", "SELECT uuid FROM events", nil)
	s3FixtureError(t, fixture, "query native credential-chain S3 files after configuration lock", err)
	if len(result.Data) != 1 || string(result.Data[0]["uuid"]) != `"credential-chain"` {
		t.Fatal("native credential-chain query lost its event")
	}
	s3FixtureError(t, fixture, "delete through Go SDK credential chain", store.DeleteProject(ctx, "chain"))
	if len(fixture.keys(t, prefix)) != 0 {
		t.Fatal("Go SDK credential chain did not remove the target's objects")
	}
}

func TestDuckLakeS3RejectsUntrustedTLSCertificate(t *testing.T) {
	fixture := nativeS3Fixture(t)
	if !strings.HasPrefix(fixture.storage.Endpoint, "https://") {
		t.Skip("negative certificate assertion uses the private TLS fixture")
	}
	wrongAuthority := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := wrongAuthority.Certificate()
	wrongAuthority.Close()
	wrongCA := filepath.Join(t.TempDir(), "untrusted.pem")
	if err := os.WriteFile(wrongCA, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0600); err != nil {
		t.Fatal("cannot prepare untrusted fixture CA")
	}
	for _, chain := range []bool{false, true} {
		t.Run(map[bool]string{false: "static", true: "credential-chain"}[chain], func(t *testing.T) {
			storage := fixture.storage
			storage.CACertFile = wrongCA
			if chain {
				storage.AccessKeyID, storage.SecretAccessKey = "", ""
			}
			config := DuckLakeConfig{Directory: t.TempDir(), S3: &storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 1}
			lake, err := newDuckLake(config)
			s3FixtureError(t, fixture, "open untrusted S3 configuration", err)
			store := lake.(*duckLakeStore)
			defer func() { store.Close() }()
			ctx := context.Background()
			original := lakeStorageProjection(t, "tls", "kept", 1)
			s3FixtureError(t, fixture, "accept inline event before TLS assertion", store.Apply(ctx, []eventProjection{original}))
			project, release, err := store.leaseProject(ctx, "tls")
			s3FixtureError(t, fixture, "lease untrusted S3 store", err)
			flushErr := project.maintenanceCommand(ctx, "CALL ducklake_flush_inlined_data('lake')", &duckLakeMaintenanceStats{})
			release()
			if flushErr == nil {
				t.Fatal("native S3 upload accepted a certificate outside its trust bundle")
			}
			for _, secret := range []string{fixture.storage.AccessKeyID, fixture.storage.SecretAccessKey} {
				if strings.Contains(flushErr.Error(), secret) {
					t.Fatal("native TLS failure exposed an operator credential")
				}
			}
			s3FixtureError(t, fixture, "close failed TLS configuration", store.Close())
			storage.CACertFile = fixture.storage.CACertFile
			lake, err = newDuckLake(config)
			s3FixtureError(t, fixture, "reopen S3 store with the correct CA", err)
			store = lake.(*duckLakeStore)
			s3FixtureError(t, fixture, "retry accepted event after correcting TLS", store.Apply(ctx, []eventProjection{original}))
			fixture.flush(t, store, "tls")
			rows, err := store.Query(ctx, "tls", "SELECT uuid FROM events", nil)
			s3FixtureError(t, fixture, "query after correcting TLS", err)
			if len(rows.Data) != 1 || string(rows.Data[0]["uuid"]) != `"kept"` {
				t.Fatal("TLS configuration correction lost the accepted event")
			}
		})
	}
}
