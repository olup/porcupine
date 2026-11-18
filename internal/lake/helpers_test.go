package lake

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/testfixtures"
)

type duckLakeS3Fixture struct {
	storage DuckLakeS3Config
	client  *s3.Client
}

func nativeS3Fixture(t *testing.T) duckLakeS3Fixture {
	t.Helper()
	endpoint := os.Getenv("PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT")
	if endpoint == "" {
		if err := testfixtures.CheckSelection(t.Name(), testfixtures.S3); err != nil {
			t.Fatal(err)
		}
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

func (f duckLakeS3Fixture) projectPrefix(t *testing.T, store *DuckLakeStore, project string) string {
	t.Helper()
	source, err := store.SourceID(context.Background())
	s3FixtureError(t, f, "read native source identity", err)
	digest := sha256.Sum256([]byte(project))
	return path.Join(f.storage.Prefix, source, hex.EncodeToString(digest[:])) + "/"
}

func (f duckLakeS3Fixture) flush(t *testing.T, store *DuckLakeStore, id string) {
	t.Helper()
	p, release, err := store.LeaseProject(context.Background(), id)
	s3FixtureError(t, f, "lease project before native flush", err)
	defer release()
	s3FixtureError(t, f, "flush native S3 Parquet", p.MaintenanceCommand(context.Background(), "CALL ducklake_flush_inlined_data('lake')", &DuckLakeMaintenanceStats{}))
}

func containsS3Key(keys []string, wanted string) bool {
	for _, key := range keys {
		if key == wanted {
			return true
		}
	}
	return false
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

func queryStatus(err error) int {
	if failure, ok := err.(*apierr.Error); ok {
		return failure.GetStatus()
	}
	return 0
}

func cleanupParquetFiles(t *testing.T, directory string) []string {
	t.Helper()
	var paths []string
	if err := filepath.WalkDir(directory, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".parquet") {
			paths = append(paths, path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return paths
}

// Inspect decoded Parquet rows, not compressed file bytes, to prove that the
// deleted payload is no longer in any managed data file.
func assertCleanupPayloadAbsent(t *testing.T, directory, marker string) {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	files := cleanupParquetFiles(t, directory)
	if len(files) == 0 {
		t.Fatal("expected surviving Parquet data")
	}
	for _, path := range files {
		var found int
		if err := db.QueryRow("SELECT count(*) FROM read_parquet(?) t WHERE contains(to_json(t)::VARCHAR,?)", path, marker).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found != 0 {
			t.Fatalf("deleted payload still in %s", filepath.Base(path))
		}
	}
}

func cleanupEventFiles(t *testing.T, s *DuckLakeStore, id string) []string {
	t.Helper()
	p, release, err := s.LeaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	rows, err := p.DB.Query("SELECT data_file FROM ducklake_list_files('lake','current_events')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("expected event data file")
	}
	return paths
}

func flushCleanupProject(t *testing.T, s *DuckLakeStore, id string) {
	t.Helper()
	p, release, err := s.LeaseProject(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if err := p.MaintenanceCommand(context.Background(), "CALL ducklake_flush_inlined_data('lake')", &DuckLakeMaintenanceStats{}); err != nil {
		t.Fatal(err)
	}
}

// skipInShort keeps `make check` fast. `make check-full` and CI run every test.
func skipInShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("slow journey; run make check-full")
	}
}

func lakeMaintenanceFileCount(ctx context.Context, p *ProjectLake, table string) (int, error) {
	var count int
	err := p.DB.QueryRowContext(ctx, "SELECT count(*) FROM ducklake_list_files('lake',?)", table).Scan(&count)
	return count, err
}

// sharedPostgresDSN gives each test its own disposable database. Absence of the
// integration setting skips this suite; a configured but unreachable DB fails.
func sharedPostgresDSN(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("PORCUPINE_TEST_POSTGRES_DSN")
	if raw == "" {
		if err := testfixtures.CheckSelection(t.Name(), testfixtures.Postgres); err != nil {
			t.Fatal(err)
		}
		t.Skip("set PORCUPINE_TEST_POSTGRES_DSN or run make shared-postgres-test")
	}
	dsn, err := url.Parse(raw)
	if err != nil || (dsn.Scheme != "postgres" && dsn.Scheme != "postgresql") || dsn.Host == "" {
		t.Fatal("PORCUPINE_TEST_POSTGRES_DSN must be a PostgreSQL URL")
	}
	admin, err := sql.Open("pgx", raw)
	if err != nil {
		t.Fatal("cannot open the configured PostgreSQL fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		admin.Close()
		t.Fatal("configured PostgreSQL fixture is unavailable")
	}
	var suffix [12]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	name := "porcupine_test_" + hex.EncodeToString(suffix[:])
	// Only this generated ASCII identifier is interpolated; caller input is never
	// used in a CREATE/DROP DATABASE statement.
	if _, err := admin.ExecContext(ctx, `CREATE DATABASE "`+name+`"`); err != nil {
		admin.Close()
		t.Fatal("cannot create an isolated PostgreSQL test database", err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if _, err := admin.ExecContext(cleanup, `DROP DATABASE "`+name+`" WITH (FORCE)`); err != nil {
			t.Error("cannot remove the isolated PostgreSQL test database", err)
		}
	})
	dsn.Path, dsn.RawPath = "/"+name, ""
	return dsn.String()
}

func metadataProperties(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

var duckLakeS3ExtensionOnce sync.Once

var duckLakeS3ExtensionError error
