//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeS3StoragePinAndCredentialRotation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	database, err := sql.Open("sqlite", filepath.Join(root, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("CREATE TABLE catalogs(project_id TEXT)"); err != nil {
		t.Fatal(err)
	}
	original := validLakeS3Config()
	if err := pinDuckLakeStorage(ctx, database, root, &original); err != nil {
		t.Fatal(err)
	}
	rotated := original
	rotated.AccessKeyID = "new-access"
	rotated.SecretAccessKey = "new-secret"
	rotated.UsePathStyle = false
	rotated.Region = "us-east-1"
	rotated.RequestTimeoutSeconds = 1
	if err := pinDuckLakeStorage(ctx, database, root, &rotated); err != nil {
		t.Fatal("credential rotation changed storage identity", err)
	}
	for _, modified := range []*DuckLakeS3Config{nil, func() *DuckLakeS3Config { c := original; c.Prefix = "other"; return &c }(), func() *DuckLakeS3Config { c := original; c.Bucket = "other-bucket"; return &c }(), func() *DuckLakeS3Config { c := original; c.Endpoint = "https://other.example"; return &c }()} {
		if err := pinDuckLakeStorage(ctx, database, root, modified); err == nil {
			t.Fatal("archive silently changed object namespace")
		}
	}
	local, err := sql.Open("sqlite", filepath.Join(root, "local.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer local.Close()
	if _, err := local.Exec("CREATE TABLE catalogs(project_id TEXT);INSERT INTO catalogs VALUES('existing')"); err != nil {
		t.Fatal(err)
	}
	if err := pinDuckLakeStorage(ctx, local, root, &original); err == nil {
		t.Fatal("legacy local archive silently switched to S3")
	}
}

func TestDuckLakeS3StoragePinPreservesAWSPartition(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	database, err := sql.Open("sqlite", filepath.Join(root, "aws-registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec("CREATE TABLE catalogs(project_id TEXT)"); err != nil {
		t.Fatal(err)
	}
	original := validLakeS3Config()
	original.Endpoint = ""
	if err := pinDuckLakeStorage(ctx, database, root, &original); err != nil {
		t.Fatal(err)
	}
	changed := original
	changed.Region = "us-east-1"
	if err := pinDuckLakeStorage(ctx, database, root, &changed); err != nil {
		t.Fatal("same-partition signing-region correction rejected", err)
	}
	for _, region := range []string{"cn-north-1", "us-gov-west-1"} {
		changed.Region = region
		if err := pinDuckLakeStorage(ctx, database, root, &changed); err == nil {
			t.Fatal("AWS partition switch changed the object namespace", region)
		}
	}
}

func TestDuckLakeS3LocalBackupsFailClosed(t *testing.T) {
	c := validLakeS3Config()
	lake := DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), S3: &c}
	if _, err := prepareNativeBackup(&NativeBackupConfig{Directory: t.TempDir()}, "queue.db", &lake); err == nil {
		t.Fatal("local-only backups accepted remote data")
	}
	store := openLakeStorageTest(t, t.TempDir(), 1)
	store.config.S3 = &c
	called := false
	if _, err := store.stageCheckpoint(context.Background(), func(context.Context, string) error { called = true; return nil }); err == nil || called {
		t.Fatal("remote checkpoint published an incomplete local snapshot")
	}
}

func TestDuckLakeOpeningReservationsDoNotHoldGlobalMutex(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 2)
	ctx := context.Background()
	_, release, err := store.leaseProject(ctx, "hot")
	if err != nil {
		t.Fatal(err)
	}
	release()
	// Block only the cold project's extension/engine opening using its reserved
	// slot. A hot project must remain usable while the reservation is present.
	pending := &projectOpening{done: make(chan struct{})}
	if err := store.mu.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	store.opening["cold"] = pending
	store.mu.Unlock()
	if _, err := store.Query(ctx, "hot", "SELECT count(*) AS total FROM events", nil); err != nil {
		t.Fatal("opening reservation blocked the hot project", err)
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	if _, err := store.stageCheckpoint(deadline, func(context.Context, string) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatal("checkpoint bypassed pending catalog opening", err)
	}
	cancel()
	if err := store.Close(); err == nil {
		t.Fatal("active opening released the writer lifetime lock")
	}
	if err := store.DeleteProject(ctx, "cold"); err == nil {
		t.Fatal("deletion raced with an opening")
	}
	if err := store.mu.Lock(ctx); err != nil {
		t.Fatal(err)
	}
	delete(store.opening, "cold")
	pending.err = errors.New("injected opening failure")
	close(pending.done)
	store.mu.Unlock()
}

type secretErrorDriver struct{ reached *bool }

func (d secretErrorDriver) Open(string) (driver.Conn, error) {
	return &secretErrorConnection{reached: d.reached}, nil
}

type secretErrorConnection struct{ reached *bool }

func (c *secretErrorConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.ErrUnsupported
}
func (c *secretErrorConnection) Close() error              { return nil }
func (c *secretErrorConnection) Begin() (driver.Tx, error) { return nil, errors.ErrUnsupported }
func (c *secretErrorConnection) ExecContext(_ context.Context, statement string, _ []driver.NamedValue) (driver.Result, error) {
	if strings.HasPrefix(statement, "CREATE SECRET") {
		*c.reached = true
		return nil, fmt.Errorf("native bind failure containing private statement: %s", statement)
	}
	return driver.RowsAffected(0), nil
}

func TestDuckLakeS3SecretBindErrorsCannotDiscloseCredentials(t *testing.T) {
	reached := false
	name := "s3-secret-error-" + newID()
	sql.Register(name, secretErrorDriver{reached: &reached})
	database, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	c := validLakeS3Config()
	c.RequestTimeoutSeconds = 5
	objects := duckLakeObjectStore{config: c, namespace: "tests/source/"}
	_, err = objects.configure(context.Background(), database, "project")
	if err == nil || !reached {
		t.Fatal("fixture did not exercise the native secret bind failure")
	}
	for _, secret := range []string{c.AccessKeyID, c.SecretAccessKey, "CREATE SECRET"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatal("native bind error disclosed the secret statement")
		}
	}
}

func TestDuckLakeS3SettingsReachAllConnections(t *testing.T) {
	database, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	database.SetMaxOpenConns(3)
	first, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	c := validLakeS3Config()
	c.Endpoint = "https://storage.example"
	c.RequestTimeoutSeconds = 5
	for _, candidate := range []string{"/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem"} {
		if _, err := os.Stat(candidate); err == nil {
			c.CACertFile = candidate
			break
		}
	}
	objects := duckLakeObjectStore{config: c, namespace: "tests/source/"}
	if _, err := objects.configure(context.Background(), database, "project"); err != nil {
		t.Fatal(err)
	}
	expected, err := os.Stat(c.CACertFile)
	if err != nil {
		t.Fatal(err)
	}
	for _, connection := range []*sql.Conn{first, second} {
		var verifiesTLS bool
		if err := connection.QueryRowContext(context.Background(), "SELECT current_setting('enable_server_cert_verification')").Scan(&verifiesTLS); err != nil || !verifiesTLS {
			t.Fatal("pooled connection disabled TLS certificate verification", err)
		}
		var actualPath string
		if err := connection.QueryRowContext(context.Background(), "SELECT current_setting('ca_cert_file')").Scan(&actualPath); err != nil {
			t.Fatal(err)
		}
		actual, err := os.Stat(actualPath)
		if err != nil || !os.SameFile(actual, expected) {
			t.Fatal("pooled connection did not receive the configured TLS trust bundle", err)
		}
		for setting, wanted := range map[string]uint64{"http_timeout": 5, "http_retries": 0} {
			var actual uint64
			if err := connection.QueryRowContext(context.Background(), "SELECT current_setting(?)", setting).Scan(&actual); err != nil || actual != wanted {
				t.Fatalf("pooled connection %s=%d, wanted %d: %v", setting, actual, wanted, err)
			}
		}
	}
}
