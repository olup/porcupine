package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

type exportSnapshotFixture struct {
	snapshot lakeapi.ExportSnapshot
	read     func(lakeapi.ExportSnapshot, lakeapi.ExportStreamCallbacks) error
}

func (f exportSnapshotFixture) CaptureExportSnapshot(context.Context, string) (lakeapi.ExportSnapshot, error) {
	return f.snapshot, nil
}

func (f exportSnapshotFixture) ExportAtSnapshot(_ context.Context, _, _ string, _ map[string]string, snapshot lakeapi.ExportSnapshot, _ int64, callbacks lakeapi.ExportStreamCallbacks) (lakeapi.ExportStreamResult, error) {
	cols := []lakeapi.AnalyticsResultColumn{{Name: "value", Type: "VARCHAR"}}
	if err := callbacks.Columns(cols); err != nil {
		return lakeapi.ExportStreamResult{}, err
	}
	if f.read != nil {
		if err := f.read(snapshot, callbacks); err != nil {
			return lakeapi.ExportStreamResult{}, err
		}
	}
	return lakeapi.ExportStreamResult{Columns: cols, Rows: 1}, nil
}

func TestSharedPostgresExportRetryFencesPreviousAttemptAndAccountsOnce(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", pipelinePostgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	_, err = db.ExecContext(ctx, `
CREATE TABLE tenants(id TEXT PRIMARY KEY);
CREATE TABLE projects(id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id));
CREATE TABLE tenant_deletions(tenant_id TEXT PRIMARY KEY);
CREATE TABLE project_deletions(project_id TEXT PRIMARY KEY,tenant_id TEXT);
CREATE TABLE access_keys(id TEXT PRIMARY KEY, revoked_at TEXT);
CREATE TABLE test_export_activity(project_id TEXT,attempt_id TEXT PRIMARY KEY);
CREATE TABLE managed_artifact_ownership(tenant_id TEXT,project_id TEXT,kind TEXT,storage_id TEXT,object_key TEXT,upload_id TEXT,PRIMARY KEY(tenant_id,project_id,kind,storage_id,object_key,upload_id));
INSERT INTO tenants VALUES('tenant');
INSERT INTO projects VALUES ('a','tenant'),('b','tenant');
INSERT INTO access_keys VALUES('reader',NULL);
`+ExportPostgresSchemaV1+usage.ControlSchema)
	if err != nil {
		t.Fatal(err)
	}
	fake := &exportTestS3{}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	config := ExportConfig{MaxExecutionSeconds: 20, S3: exportstore.Config{Region: "us-east-1", Bucket: "private-exports", Prefix: "isolated", Endpoint: server.URL, AccessKeyID: "key", SecretAccessKey: "secret", UsePathStyle: true}}
	load := func(context.Context, string) (auth.Principal, error) { return fixtureActor(), nil }
	d := ExportDependencies{
		Control: db, Clock: time.Now, Usage: usage.NewRecorder(db, "test-hosted-export", time.Now),
		SnapshotCut: func(ctx context.Context, project string, commit func(context.Context, ExportControlTx) error) error {
			var tickets int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM test_export_activity WHERE project_id=$1`, project).Scan(&tickets); err != nil {
				return err
			}
			if tickets != 1 {
				t.Errorf("native capture activity count=%d", tickets)
			}
			run := func(ctx context.Context, work func(context.Context, *sql.Tx) error) error {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				defer tx.Rollback()
				if err = work(ctx, tx); err != nil {
					return err
				}
				return tx.Commit()
			}
			if err := commit(ctx, run); err != nil {
				return err
			}
			var jobs int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM export_jobs WHERE project_id=$1`, project).Scan(&jobs); err != nil {
				return err
			}
			if jobs != 1 {
				t.Errorf("snapshot cut released before job was persisted: %d", jobs)
			}
			return nil
		},
		LoadActor: load, LoadActorTx: func(ctx context.Context, _ *sql.Tx, id string) (auth.Principal, error) { return load(ctx, id) },
		CurrentActorTx: func(_ context.Context, _ *sql.Tx, actor auth.Principal) (auth.Principal, error) { return actor, nil },
		RequireActive:  func(context.Context, string) error { return nil },
		RequireOwner:   func(context.Context, string) error { t.Error("hosted export consulted local ownership"); return nil },
		BeginActivity:  func(ctx context.Context, _ string) (context.Context, func(), bool) { return ctx, func() {}, true },
		Acquire:        func(context.Context) (func(), bool) { return func() {}, true },
		BeginAttemptTx: func(ctx context.Context, tx *sql.Tx, project, attempt string) error {
			if _, err := lockActiveExportProject(ctx, tx, project); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO test_export_activity VALUES($1,$2)`, project, attempt)
			return err
		},
		EndAttempt: func(ctx context.Context, project, attempt string) error {
			_, err := db.ExecContext(ctx, `DELETE FROM test_export_activity WHERE project_id=$1 AND attempt_id=$2`, project, attempt)
			return err
		},
	}
	admitted := lakeapi.ExportSnapshot{CatalogID: "catalog-a", Version: 8}
	a, err := NewPostgresExports(d, exportSnapshotFixture{snapshot: admitted}, &config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Close)
	b, err := NewPostgresExports(d, exportSnapshotFixture{
		snapshot: lakeapi.ExportSnapshot{CatalogID: admitted.CatalogID, Version: admitted.Version + 1},
		read: func(snapshot lakeapi.ExportSnapshot, cb lakeapi.ExportStreamCallbacks) error {
			if snapshot != admitted {
				t.Errorf("retry used snapshot %+v, want %+v", snapshot, admitted)
			}
			return cb.Row([]json.RawMessage{json.RawMessage(`"data"`)})
		},
	}, &config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	request := ExportRequest{IdempotencyKey: "same-request", SQL: "SELECT value FROM events"}
	job, err := a.Submit(ctx, "a", request, fixtureActor())
	if err != nil || job.Consistency != retainedExportConsistency {
		t.Fatalf("submit: %+v %v", job, err)
	}
	if same, err := b.Submit(ctx, "a", request, fixtureActor()); err != nil || same.ID != job.ID {
		t.Fatalf("cross-worker idempotency: %+v %v", same, err)
	}
	if _, err := b.Get(ctx, "b", job.ID); err == nil {
		t.Fatal("another project could read the export")
	}
	queued, ok := a.nextQueuedExport(ctx)
	if !ok {
		t.Fatal("queued export missing")
	}
	first, err := a.claimExport(ctx, queued)
	if err != nil {
		t.Fatal(err)
	}
	if worked, err := b.RunOne(ctx); worked || err != nil {
		t.Fatalf("live attempt reclaimed: worked=%t err=%v", worked, err)
	}
	sink := newExportSink(ctx, a, first)
	if err = sink.setColumns([]lakeapi.AnalyticsResultColumn{{Name: "value"}}); err != nil {
		t.Fatal(err)
	}
	if err = sink.writeRow([]json.RawMessage{json.RawMessage(`"old attempt"`)}); err != nil {
		t.Fatal(err)
	}
	if err = sink.finish(); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE export_jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	if worked, err := b.RunOne(ctx); !worked || err != nil {
		t.Fatalf("reclaim: worked=%t err=%v", worked, err)
	}
	current, err := b.loadExport(ctx, "a", job.ID)
	if err != nil || current.Attempts != 2 || current.State != ExportSucceeded || current.AttemptID == first.AttemptID || current.Snapshot != admitted {
		t.Fatalf("reclaimed job: %+v %v", current, err)
	}
	if err = a.publishExport(ctx, first, lakeapi.ExportStreamResult{}, sink); !errors.Is(err, errExportCanceled) {
		t.Fatalf("stale attempt published: %v", err)
	}
	var published int64
	if err = db.QueryRowContext(ctx, `SELECT sum(export_bytes) FROM usage_rollups WHERE project_id='a'`).Scan(&published); err != nil || published <= 0 || published != current.Bytes {
		t.Fatalf("usage=%d export=%d: %v", published, current.Bytes, err)
	}
	var tickets int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM test_export_activity`).Scan(&tickets); err != nil || tickets != 1 {
		t.Fatalf("stale actual-return ticket count=%d: %v", tickets, err)
	}
	a.finishExportActivity(first.ProjectID, first.AttemptID)
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM test_export_activity`).Scan(&tickets); err != nil || tickets != 0 {
		t.Fatalf("activity ticket count after return=%d: %v", tickets, err)
	}
	fake.mu.Lock()
	objects, deletes := len(fake.objects), fake.deletes
	fake.mu.Unlock()
	if objects != 1 || deletes != 1 {
		t.Fatalf("published objects=%d cleanup deletes=%d", objects, deletes)
	}

	expired, err := a.Submit(ctx, "b", request, fixtureActor())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO export_external_authority(job_id,tenant_id,issuer,expires_at_ms) VALUES($1,'tenant','https://issuer.example',$2)`, expired.ID, time.Now().Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if worked, err := b.RunOne(ctx); worked || err != nil {
		t.Fatalf("expired authority executed: worked=%t err=%v", worked, err)
	}
	expired, err = b.Get(ctx, "b", expired.ID)
	if err != nil || expired.State != ExportExpired || expired.Error != "authorization_expired" || expired.Attempts != 0 {
		t.Fatalf("external authorization expiration: %+v %v", expired, err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = StageExportProjectCleanupTx(ctx, tx, "a"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = b.CleanupProject(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	var holds int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM managed_artifact_ownership WHERE project_id='a'`).Scan(&holds); err != nil || holds != 2 {
		t.Fatalf("retained artifact ownership=%d: %v", holds, err)
	}
	if _, err = b.Get(ctx, "b", expired.ID); err != nil {
		t.Fatalf("targeted deletion affected another project: %v", err)
	}
	if _, err = db.ExecContext(ctx, `UPDATE export_jobs SET retire_after=clock_timestamp()-interval '1 second' WHERE id=$1`, expired.ID); err != nil {
		t.Fatal(err)
	}
	if worked, err := b.RunOne(ctx); worked || err != nil {
		t.Fatalf("unexpected work while retiring metadata: worked=%t err=%v", worked, err)
	}
	if _, err = b.Get(ctx, "b", expired.ID); err == nil {
		t.Fatal("settled export persisted after the idempotency horizon")
	}
	_, err = db.ExecContext(ctx, `INSERT INTO export_jobs(id,project_id,actor_id,sql,parameters,format,idempotency_hash,request_hash,state,snapshot_catalog_id,snapshot_version,created_at)
SELECT 'owned-'||slot,'b','reader','','{}','jsonl','owned-'||slot,'fixture','succeeded','catalog-b',1,'now'
FROM generate_series(1,$1::integer) AS slot`, maxHostedProjectOwnedExports)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Submit(ctx, "b", request, fixtureActor()); err == nil || err.Error() != "retained export capacity is full" {
		t.Fatalf("retained export capacity permitted another job: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.objects) != 0 || fake.deletes != 2 {
		t.Fatalf("deleted project objects=%d cleanup deletes=%d", len(fake.objects), fake.deletes)
	}
}
