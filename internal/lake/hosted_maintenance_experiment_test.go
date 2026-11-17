package lake

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/event"
)

// This opt-in probe bypasses the production refusal only on disposable fixtures.
// It records the native commit and fencing behavior of each candidate.
func TestDuckLakeS3SharedPostgresHostedMaintenanceExperiment(t *testing.T) {
	t.Parallel()
	if os.Getenv("PORCUPINE_TEST_HOSTED_MAINTENANCE_EXPERIMENT") != "1" {
		t.Skip("opt-in native maintenance safety experiment")
	}
	probe, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	if _, err := probe.ExecContext(t.Context(), "LOAD ducklake"); err != nil {
		t.Fatal(err)
	}
	var version, engine string
	if err := probe.QueryRowContext(t.Context(), `SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := probe.QueryRowContext(t.Context(), `SELECT version()`).Scan(&engine); err != nil {
		t.Fatal(err)
	}
	t.Logf("engine=%s ducklake=%s", engine, version)
	for _, candidate := range []struct {
		kind, mode string
	}{
		{"merge", "autocommit"}, {"rewrite", "autocommit"},
		{"merge", "snapshot-no-retry"}, {"rewrite", "snapshot-no-retry"},
		{"merge", "snapshot-default-retry"}, {"rewrite", "snapshot-default-retry"},
		{"merge", "snapshot-no-retry-healthy"}, {"rewrite", "snapshot-no-retry-healthy"},
		{"merge", "production-no-retry"}, {"rewrite", "production-no-retry"},
		{"merge", "production-no-retry-healthy"}, {"rewrite", "production-no-retry-healthy"},
	} {
		t.Run(candidate.kind+"/"+candidate.mode, func(t *testing.T) {
			kind := candidate.kind
			ctx, a, b, scope, pause := hostedMaintenanceExperiment(t)
			control, coordinator := nativeFenceControl(t)
			owner := acquireFenceSession(t, coordinator, scope.Key)
			fence := owner.Fence(scope.StreamID)
			if err := b.AdvanceWriteFence(ctx, fence); err != nil {
				t.Fatal(err)
			}
			pb := b.Projects[scope.Key.ProjectID]
			for batch := 0; batch < 4; batch++ {
				tx, err := pb.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := pb.acquireHostedNativeWrite(ctx, tx, fence); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO lake.heads SELECT CAST(? AS VARCHAR) || '-' || CAST(i AS VARCHAR),1,0,true,'2026-11-17T00:00:00Z' FROM range(4) t(i)`, batch); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if kind == "rewrite" {
				tx, err := pb.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := pb.acquireHostedNativeWrite(ctx, tx, fence); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if _, err := tx.ExecContext(ctx, `DELETE FROM lake.heads WHERE uuid='0-0'`); err != nil {
					tx.Rollback()
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := a.LookupHostedProjectExisting(ctx, scope.Key); err != nil {
				t.Fatal(err)
			}
			pa := a.Projects[scope.Key.ProjectID]
			statement := "CALL ducklake_merge_adjacent_files('lake','heads',max_compacted_files=>1,max_file_size=>1048576)"
			if kind == "rewrite" {
				statement = "CALL ducklake_rewrite_data_files('lake','heads',delete_threshold=>0,max_compacted_files=>1)"
			}
			before := maintenanceExperimentFiles(t, ctx, pa)
			pathsBefore := maintenanceExperimentPaths(t, ctx, pa)
			if before < 2 {
				t.Fatal("missing native file fixture", before)
			}
			var generation int64
			if err := pa.DB.QueryRowContext(ctx, `SELECT generation FROM lake.hosted_write_guard`).Scan(&generation); err != nil {
				t.Fatal(err)
			}
			tx, err := pa.DB.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := pa.acquireHostedNativeWrite(ctx, tx, fence); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			var stats DuckLakeMaintenanceStats
			callErr := runLakeMaintenanceCommand(ctx, tx, statement, &stats)
			if callErr != nil {
				tx.Rollback()
				t.Fatalf("CALL failed before testing atomic commit: %v", callErr)
			}
			if stats.FilesProcessed == 0 || stats.FilesCreated == 0 {
				tx.Rollback()
				t.Fatal("CALL did no native compaction", stats)
			}
			commitErr := tx.Commit()
			if commitErr == nil || !strings.Contains(commitErr.Error(), "Transactions can either make changes OR perform compaction - not both") {
				t.Fatal("pinned engine behavior changed; reassess hosted maintenance", commitErr)
			}
			t.Logf("guard+%s CALL processed=%d created=%d; COMMIT rejected: %v", kind, stats.FilesProcessed, stats.FilesCreated, commitErr)
			var afterGeneration int64
			if err := pa.DB.QueryRowContext(ctx, `SELECT generation FROM lake.hosted_write_guard`).Scan(&afterGeneration); err != nil || afterGeneration != generation {
				t.Fatal("failed commit changed guard", generation, afterGeneration, err)
			}
			if after := maintenanceExperimentFiles(t, ctx, pa); after != before {
				t.Fatal("failed commit replaced committed files", before, after)
			}
			if maintenanceExperimentPaths(t, ctx, pa) != pathsBefore {
				t.Fatal("failed commit changed committed file paths")
			}
			if strings.Contains(candidate.mode, "no-retry") {
				reopenMaintenanceExperimentEngine(t, ctx, pb, scope)
			}
			call := func(nativeCtx context.Context) error {
				bounded, cancel := context.WithTimeout(context.WithoutCancel(nativeCtx), 20*time.Second)
				defer cancel()
				if strings.HasPrefix(candidate.mode, "production-") {
					var measured DuckLakeMaintenanceStats
					return pb.hostedMaintenanceAttempt(bounded, fence, statement, &measured)
				}
				if candidate.mode != "autocommit" {
					return maintenanceSnapshotExperiment(t, bounded, pb, fence, statement, candidate.mode != "snapshot-default-retry")
				}
				var measured DuckLakeMaintenanceStats
				err := runLakeMaintenanceCommand(bounded, pb.DB, statement, &measured)
				if err == nil && (measured.FilesProcessed == 0 || measured.FilesCreated == 0) {
					return errors.New("delayed CALL did no compaction")
				}
				return err
			}
			if strings.HasSuffix(candidate.mode, "healthy") {
				if err := owner.RunNative(ctx, call); err != nil {
					t.Fatal("fresh valid owner failed native snapshot compaction", err)
				}
				if maintenanceExperimentPaths(t, ctx, pa) == pathsBefore {
					t.Fatal("fresh valid owner changed no committed native files")
				}
				t.Log("fresh valid owner read fence and committed compaction in one transaction with retries0")
				return
			}

			// A blocked output PUT occurs inside a real native CALL, regardless of
			// whether its input reads were satisfied by the native file cache.
			pause.armed.Store(true)
			finished := make(chan error, 1)
			returned := make(chan struct{})
			go func() {
				defer close(returned)
				finished <- owner.RunNative(ctx, call)
			}()
			defer func() { pause.release(); <-returned }()
			select {
			case <-pause.entered:
			case err := <-finished:
				t.Fatal("CALL returned before S3 pause", err)
			case <-ctx.Done():
				t.Fatal("native CALL did not reach S3", ctx.Err())
			}
			terminateFenceSession(t, control, owner)
			successor := acquireFenceSession(t, coordinator, scope.Key)
			next := successor.Fence(scope.StreamID)
			if err := a.AdvanceWriteFence(ctx, next); err != nil {
				t.Fatal("takeover failed while native CALL paused", err)
			}
			sealed, err := a.SealHostedProject(ctx, next)
			if err != nil {
				t.Fatal("seal failed while native CALL paused", err)
			}
			select {
			case err := <-finished:
				t.Fatal("control loss unexpectedly completed paused CALL", err)
			default:
			}
			atSeal := maintenanceExperimentFiles(t, ctx, pa)
			filesAtSeal := maintenanceExperimentPaths(t, ctx, pa)
			pause.release()
			select {
			case err := <-finished:
				if strings.Contains(candidate.mode, "no-retry") {
					if err == nil || !strings.Contains(err.Error(), "maximum retry count of 0") {
						t.Fatal("old snapshot unexpectedly committed or returned another failure", err)
					}
					t.Logf("snapshot guard with retries0 rejected old native COMMIT: %v", err)
				} else if err != nil {
					t.Fatal("split-transaction candidate no longer reproduces stale compaction", err)
				}
			case <-ctx.Done():
				t.Fatal("delayed CALL did not return", ctx.Err())
			}
			after := maintenanceExperimentFiles(t, ctx, pa)
			if strings.Contains(candidate.mode, "no-retry") {
				if maintenanceExperimentPaths(t, ctx, pa) != filesAtSeal {
					t.Fatal("rejected old snapshot changed committed files")
				}
				var count int
				want := 16
				if kind == "rewrite" {
					want--
				}
				if err := pa.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.heads`).Scan(&count); err != nil || count != want {
					t.Fatal("rejected old snapshot lost committed rows", count, err)
				}
				key := strings.TrimPrefix(pause.outputPath, "/"+a.Config.S3.Bucket+"/")
				objects, err := a.Remote.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(a.Config.S3.Bucket), Prefix: aws.String(key)})
				if err != nil || len(objects.Contents) != 0 {
					t.Fatal("failed native attempt did not clean its own output", err)
				}
				t.Logf("successor fence+seal retained committed files %d; old actual CALL returned with no retry", after)
				return
			}
			if maintenanceExperimentPaths(t, ctx, pa) == filesAtSeal {
				t.Fatal("delayed CALL did not replace any committed native file")
			}
			if kind == "merge" && after >= atSeal {
				t.Fatal("delayed compaction did not replace file set after seal", atSeal, after)
			}
			var state string
			var epoch int64
			if err := pa.DB.QueryRowContext(ctx, `SELECT state,fence_epoch FROM lake.hosted_write_guard`).Scan(&state, &epoch); err != nil || state != "sealed" || epoch != next.Epoch {
				t.Fatal("delayed compaction changed fence", state, epoch, err)
			}
			expected := 16
			if kind == "rewrite" {
				expected--
			}
			var rows int
			if err := pa.DB.QueryRowContext(ctx, `SELECT count(*) FROM lake.heads`).Scan(&rows); err != nil || rows != expected {
				t.Fatal("compaction changed retained rows", rows, err)
			}
			t.Logf("unsafe split guard/CALL: old epoch=%d, successor=%d, seal=%d; CALL remained running after control loss and committed after seal; native files %d -> %d; rows=%d", fence.Epoch, next.Epoch, sealed.FinalSequence, atSeal, after, rows)
			if err := pa.MaintenanceCommand(ctx, statement, &DuckLakeMaintenanceStats{}); !errors.Is(err, ErrHostedNativeGuardRequired) {
				t.Fatal("production hosted maintenance gate opened", err)
			}
		})
	}
}

func maintenanceSnapshotExperiment(t *testing.T, ctx context.Context, p *ProjectLake, fence ProjectWriteFence, statement string, noRetry bool) error {
	conn, err := p.DB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if noRetry {
		var retries int
		if err := conn.QueryRowContext(ctx, `SELECT current_setting('ducklake_max_retry_count')`).Scan(&retries); err != nil {
			return err
		}
		if retries != 0 {
			return errors.New("native commit retries were not disabled on the maintenance connection")
		}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var owned bool
	if err := tx.QueryRowContext(ctx, `SELECT count(*)=1 FROM lake.hosted_write_guard
 WHERE id=1 AND source_id=? AND project_id=? AND stream_id=? AND state='open' AND identity_ready AND fence_epoch=? AND fence_nonce=?`, fence.Key.SourceID, fence.Key.ProjectID, fence.StreamID, fence.Epoch, fence.AcquisitionID).Scan(&owned); err != nil {
		return err
	}
	if !owned {
		return ErrProjectWriteFenceStale
	}
	var before, after int64
	if err := tx.QueryRowContext(ctx, `SELECT id FROM ducklake_current_snapshot('lake')`).Scan(&before); err != nil {
		return err
	}
	var stats DuckLakeMaintenanceStats
	if err := runLakeMaintenanceCommand(ctx, tx, statement, &stats); err != nil {
		return err
	}
	if stats.FilesProcessed == 0 || stats.FilesCreated == 0 {
		return errors.New("snapshot CALL did no native compaction")
	}
	if err := tx.QueryRowContext(ctx, `SELECT id FROM ducklake_current_snapshot('lake')`).Scan(&after); err != nil {
		return err
	}
	if before != after {
		return errors.New("CALL refreshed the guarded native snapshot")
	}
	t.Logf("native snapshot after fence read=%d after CALL=%d", before, after)
	return tx.Commit()
}

// Reopen one fixture engine through the production existing-only maintenance
// opener, keeping two engines in the race and ordinary connection policy intact.
func reopenMaintenanceExperimentEngine(t *testing.T, ctx context.Context, p *ProjectLake, scope HostedProjectScope) {
	t.Helper()
	if err := p.DB.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := p.sharedStore.openProjectEngine(ctx, p.projectID, nil, true, true)
	if err != nil {
		t.Fatal(err)
	}
	p.DB = next.DB
	if got, err := readHostedProjectScope(ctx, p.DB, scope.Key); err != nil || got != scope {
		t.Fatal(got, err)
	}
}

func maintenanceExperimentFiles(t *testing.T, ctx context.Context, p *ProjectLake) int {
	t.Helper()
	count, err := lakeMaintenanceFileCount(ctx, p, "heads")
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func maintenanceExperimentPaths(t *testing.T, ctx context.Context, p *ProjectLake) string {
	t.Helper()
	var paths string
	if err := p.DB.QueryRowContext(ctx, `SELECT string_agg(data_file,'|' ORDER BY data_file) FROM ducklake_list_files('lake','heads')`).Scan(&paths); err != nil {
		t.Fatal(err)
	}
	return paths
}

type maintenanceS3Pause struct {
	armed      atomic.Bool
	entered    chan struct{}
	resume     chan struct{}
	once       sync.Once
	outputPath string
}

func (p *maintenanceS3Pause) release() { p.once.Do(func() { close(p.resume) }) }

func hostedMaintenanceExperiment(t *testing.T) (context.Context, *DuckLakeStore, *DuckLakeStore, HostedProjectScope, *maintenanceS3Pause) {
	t.Helper()
	fixture := nativeS3Fixture(t)
	target, err := url.Parse(fixture.storage.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if target.Scheme != "http" {
		t.Skip("paused native S3 probe uses the isolated HTTP fixture")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	pause := &maintenanceS3Pause{entered: make(chan struct{}), resume: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, ".parquet") && pause.armed.CompareAndSwap(true, false) {
			pause.outputPath = r.URL.Path
			close(pause.entered)
			select {
			case <-pause.resume:
			case <-r.Context().Done():
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(pause.release)
	fixture.storage.Endpoint = server.URL
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	config := DuckLakeConfig{Directory: t.TempDir(), PostgresURL: sharedPostgresDSN(t), SharedSourceID: event.NewID(), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 1, DisableCleanup: true}
	open := func() *DuckLakeStore {
		config.Directory = t.TempDir()
		store, err := NewDuckLake(config)
		if err != nil {
			t.Fatal(err)
		}
		s := store.(*DuckLakeStore)
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Error(err)
			}
		})
		return s
	}
	a, b := open(), open()
	key := ProjectWriteKey{config.SharedSourceID, "maintenance"}
	scope, err := a.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, a, b, scope, pause
}
