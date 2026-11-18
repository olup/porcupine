package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
	_ "modernc.org/sqlite"
)

func fixtureActor() auth.Principal {
	return auth.Principal{ID: "reader", Permissions: []string{"events:read"}, Projects: []string{"*"}}
}
func newExportWorkerFixture(t *testing.T, fake *exportTestS3) *ExportJobs {
	t.Helper()
	pragmas := url.Values{"_pragma": {"journal_mode(WAL)", "synchronous(FULL)", "foreign_keys(ON)", "busy_timeout(5000)"}}
	dsn := (&url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "journal.sqlite")}).String() + "?" + pragmas.Encode()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	schema, err := os.ReadFile("../porcupine/journal_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(string(schema) + `;
 ALTER TABLE project_deletions ADD COLUMN tenant_id TEXT NOT NULL DEFAULT '';
 INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES('default','default','now',4,10000,1000,1048576);
 INSERT INTO projects(id,tenant_id,name,created_at) VALUES('project','default','project','now');
 INSERT INTO access_keys VALUES('reader','reader','hash','["events:read"]','["*"]',NULL,'now');
 INSERT INTO metadata VALUES('initialized','1');
 CREATE TABLE project_deletion_files(project_id TEXT,object_key TEXT,upload_id TEXT,storage_id TEXT,PRIMARY KEY(project_id,object_key,upload_id));
 ALTER TABLE export_files ADD COLUMN storage_id TEXT NOT NULL DEFAULT '';
 CREATE TABLE export_external_authority(job_id TEXT PRIMARY KEY REFERENCES export_jobs(id) ON DELETE CASCADE,tenant_id TEXT NOT NULL,issuer TEXT NOT NULL,expires_at_ms INTEGER NOT NULL CHECK(expires_at_ms>0));
 ` + usage.JournalSchema)
	if err != nil {
		t.Fatal(err)
	}
	controlDSN := (&url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "control.sqlite")}).String() + "?" + pragmas.Encode()
	control, err := sql.Open("sqlite", controlDSN)
	if err != nil {
		t.Fatal(err)
	}
	control.SetMaxOpenConns(1)
	t.Cleanup(func() { control.Close() })
	if err := control.Ping(); err != nil {
		t.Fatal(err)
	}
	controlSchema, err := os.ReadFile("../porcupine/control_schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Exec(string(controlSchema) + `;
 INSERT INTO tenants(id,name,created_at,hot_query_max_concurrent,hot_query_timeout_ms,hot_query_max_rows,hot_query_max_result_bytes) VALUES('default','default','now',4,10000,1000,1048576);
 INSERT INTO projects(id,tenant_id,name,created_at) VALUES('project','default','project','now');
 INSERT INTO access_keys(id,name,token_hash,permissions,projects,created_at) VALUES('reader','reader','hash','["events:read"]','["*"]','now');
 CREATE TABLE managed_artifact_ownership(tenant_id TEXT,project_id TEXT,kind TEXT,storage_id TEXT,object_key TEXT,upload_id TEXT,PRIMARY KEY(tenant_id,project_id,kind,storage_id,object_key,upload_id));
 `); err != nil {
		t.Fatal(err)
	}
	reader, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), MemoryLimitMB: 256})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	config := ExportConfig{BandwidthBytesPerSecond: 100 << 20, FileBytes: 1024, MaxExecutionSeconds: 10, S3: exportstore.Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}}
	d := ExportDependencies{Journal: db, Control: control, Reader: reader, Clock: time.Now, Usage: usage.NewRecorder(db, "output-test", time.Now), LoadActor: func(ctx context.Context, id string) (auth.Principal, error) {
		var revoked sql.NullString
		err := control.QueryRowContext(ctx, `SELECT revoked_at FROM access_keys WHERE id=?`, id).Scan(&revoked)
		if revoked.Valid {
			return auth.Principal{}, sql.ErrNoRows
		}
		return fixtureActor(), err
	}, RequireActive: func(context.Context, string) error { return nil }, BeginActivity: func(ctx context.Context, _ string) (context.Context, func(), bool) { return ctx, func() {}, true }, Acquire: func(context.Context) (func(), bool) { return func() {}, true }}

	s, err := NewSQLiteExports(d, &config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

func exportWorkerFixture(t *testing.T, fake *exportTestS3) (*ExportJobs, storedExport) {
	t.Helper()
	s := newExportWorkerFixture(t, fake)
	created, err := s.Submit(t.Context(), "project", ExportRequest{IdempotencyKey: "fixture", SQL: "SELECT uuid,event,json_extract_string(properties,'$.amount') AS amount FROM events WHERE event=$name", Parameters: map[string]string{"name": "purchase"}}, fixtureActor())
	if err != nil {
		t.Fatal(err)
	}
	queued, err := s.loadExport(t.Context(), "project", created.ID)
	if err != nil {
		t.Fatal(err)
	}
	job, err := s.claimExport(t.Context(), queued)
	if err != nil {
		t.Fatal(err)
	}
	return s, job
}
func nativeExportWorkerFixture(t *testing.T, fake *exportTestS3) (*ExportJobs, storedExport) {
	s, job := exportWorkerFixture(t, fake)
	applyExportEvent(t, s, job.ProjectID, "native-export", "short")
	return s, job
}
func applyExportEvent(t *testing.T, s *ExportJobs, project, id, padding string) {
	t.Helper()
	props, _ := json.Marshal(map[string]any{"amount": 42, "padding": padding})
	at := time.Now().UTC().Format(time.RFC3339Nano)
	err := s.lake.(*lakeapi.DuckLakeStore).Apply(t.Context(), []lakeapi.EventProjection{{Event: eventapi.Event{ID: 1, ProjectID: project, UUID: id, Event: "purchase", DistinctID: "user", Properties: props, Timestamp: at, ReceivedAt: at}, Revision: 1}})
	if err != nil {
		t.Fatal(err)
	}
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }
