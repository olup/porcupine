//go:build ducklake

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type checkpointPausedHTTPClient struct {
	underlying interface {
		Do(*http.Request) (*http.Response, error)
	}
	match   func(*http.Request) bool
	entered chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (c *checkpointPausedHTTPClient) Do(request *http.Request) (*http.Response, error) {
	if c.match(request) {
		c.once.Do(func() { close(c.entered) })
		select {
		case <-c.resume:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
	}
	return c.underlying.Do(request)
}

func pauseCheckpointHTTP(store *duckLakeStore, match func(*http.Request) bool) *checkpointPausedHTTPClient {
	options := store.remote.client.Options()
	paused := &checkpointPausedHTTPClient{underlying: options.HTTPClient, match: match, entered: make(chan struct{}), resume: make(chan struct{})}
	options.HTTPClient = paused
	store.remote.client = s3.New(options)
	return paused
}

func checkpointAcceptanceFixture(t *testing.T) func(context.Context, string) error {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "acceptance.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec("CREATE TABLE accepted(id INTEGER PRIMARY KEY); INSERT INTO accepted VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context, destination string) error { return snapshotSQLite(ctx, db, destination) }
}

func assertCheckpointPins(t *testing.T, store *duckLakeStore, ctx context.Context, project string) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	var detail *managementError
	if err := store.DeleteProject(deadline, project); !errors.As(err, &detail) || detail.Code != "backup_in_progress" || detail.status != 503 {
		t.Fatalf("checkpoint did not return a retryable deletion error: %v", err)
	}
	cleanup, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	defer stop()
	if err := store.cleanupDue(cleanup, time.Now()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cleanup bypassed the checkpoint protection: %v", err)
	}
	if err := store.Close(); err == nil {
		t.Fatal("checkpoint released the live source writer lock")
	}
}

func TestDuckLakeS3CheckpointPinsObjectsWithoutBlockingReadsOrWrites(t *testing.T) {
	fixture := nativeS3Fixture(t)
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1, MaxOpenProjects: 2})
	s3FixtureError(t, fixture, "open checkpoint fixture", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	for index, id := range []string{"a", "b"} {
		s3FixtureError(t, fixture, "seed checkpoint catalog", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, id, id, int64(index+1))}))
		fixture.flush(t, store, id)
	}
	prefix := fixture.projectPrefix(t, store, "a")
	fixture.put(t, prefix+"checkpoint-sentinel", "first-payload")
	pausedList := pauseCheckpointHTTP(store, func(r *http.Request) bool { return r.Method == "GET" && r.URL.Query().Get("prefix") == prefix })
	var resumeList sync.Once
	releaseList := func() { resumeList.Do(func() { close(pausedList.resume) }) }
	defer releaseList()
	type captureResult struct {
		checkpoint *nativeCheckpoint
		err        error
	}
	captured := make(chan captureResult, 1)
	acceptance := checkpointAcceptanceFixture(t)
	go func() {
		checkpoint, err := store.stageCheckpoint(ctx, acceptance)
		captured <- captureResult{checkpoint, err}
	}()
	select {
	case <-pausedList.entered:
	case <-ctx.Done():
		t.Fatal("checkpoint did not reach remote inventory")
	}
	assertCheckpointPins(t, store, ctx, "a")
	query, stopQuery := context.WithTimeout(ctx, 4*time.Second)
	if _, err := store.Query(query, "b", "SELECT uuid FROM events", nil); err != nil {
		stopQuery()
		t.Fatal("slow checkpoint LIST held the global mutex", err)
	}
	stopQuery()
	write, stopWrite := context.WithTimeout(ctx, 4*time.Second)
	err = store.Apply(write, []eventProjection{lakeStorageProjection(t, "a", "during-list", 3)})
	stopWrite()
	s3FixtureError(t, fixture, "write during checkpoint inventory", err)
	releaseList()
	result := <-captured
	s3FixtureError(t, fixture, "capture native S3 checkpoint", result.err)
	checkpoint := result.checkpoint
	defer checkpoint.Close()
	if len(checkpoint.Catalogs) != 2 || len(checkpoint.Objects) == 0 || checkpoint.SourceID == "" || checkpoint.CapturedAt == "" {
		t.Fatal("checkpoint omitted its native catalogs or object inventory")
	}
	if err := validateBackupSQLite(ctx, filepath.Join(checkpoint.Directory, "acceptance.sqlite")); err != nil {
		t.Fatal("checkpoint omitted the real acceptance snapshot", err)
	}
	var sentinel nativeCheckpointObject
	var payload nativeCheckpointObject
	for _, object := range checkpoint.Objects {
		if strings.HasSuffix(object.RelativePath, "/checkpoint-sentinel") {
			sentinel = object
		}
		if strings.HasSuffix(object.RelativePath, ".parquet") && object.ProjectID == "a" {
			payload = object
		}
	}
	if sentinel.ETag == "" || payload.Bytes == 0 {
		t.Fatal("checkpoint omitted exact remote prefix contents")
	}
	bad := payload
	bad.RelativePath = "ducklake/../outside"
	if body, err := checkpoint.OpenObject(ctx, bad); err == nil {
		body.Close()
		t.Fatal("checkpoint opened an object outside its captured inventory")
	}
	bad = payload
	bad.Bytes++
	if body, err := checkpoint.OpenObject(ctx, bad); err == nil {
		body.Close()
		t.Fatal("checkpoint accepted altered captured metadata")
	}
	// The transfer context controls long source streams; the analytical HTTP
	// client's short timeout remains unchanged by the per-operation override.
	options := store.remote.client.Options()
	standard, ok := pausedList.underlying.(*http.Client)
	if !ok {
		t.Fatal("checkpoint fixture lost the standard analytical HTTP client")
	}
	short := *standard
	short.Timeout = time.Nanosecond
	longRead := options
	longRead.HTTPClient = &short
	store.remote.client = s3.New(longRead)
	body, err := checkpoint.OpenObject(ctx, payload)
	s3FixtureError(t, fixture, "read checkpoint using its total deadline", err)
	_, err = io.Copy(io.Discard, body)
	s3FixtureError(t, fixture, "stream checkpoint using its total deadline", errors.Join(err, body.Close()))
	if short.Timeout != time.Nanosecond {
		t.Fatal("checkpoint changed the analytical client's timeout")
	}
	store.remote.client = s3.New(options)
	pausedGet := pauseCheckpointHTTP(store, func(r *http.Request) bool {
		return r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/"+strings.TrimPrefix(payload.RelativePath, "ducklake/"+projectLakeDigest("a")+"/files/")) && r.URL.Query().Get("list-type") == ""
	})
	var resumeGet sync.Once
	releaseGet := func() { resumeGet.Do(func() { close(pausedGet.resume) }) }
	defer releaseGet()
	readDone := make(chan error, 1)
	go func() {
		body, err := checkpoint.OpenObject(ctx, payload)
		if err == nil {
			var copied int64
			copied, err = io.Copy(io.Discard, body)
			err = errors.Join(err, body.Close())
			if err == nil && copied != payload.Bytes {
				err = errors.New("checkpoint object size changed")
			}
		}
		readDone <- err
	}()
	select {
	case <-pausedGet.entered:
	case <-ctx.Done():
		t.Fatal("checkpoint did not reach remote object read")
	}
	assertCheckpointPins(t, store, ctx, "a")
	query, stopQuery = context.WithTimeout(ctx, 4*time.Second)
	rows, err := store.Query(query, "a", "SELECT uuid FROM events", nil)
	stopQuery()
	if err != nil || len(rows.Data) != 2 {
		t.Fatal("slow checkpoint GET blocked source reads", err)
	}
	write, stopWrite = context.WithTimeout(ctx, 4*time.Second)
	err = store.Apply(write, []eventProjection{lakeStorageProjection(t, "a", "during-get", 4)})
	stopWrite()
	s3FixtureError(t, fixture, "write during pinned object read", err)
	releaseGet()
	s3FixtureError(t, fixture, "read captured immutable payload", <-readDone)
	fixture.put(t, prefix+"checkpoint-sentinel", "other-payload")
	if body, err := checkpoint.OpenObject(ctx, sentinel); err == nil {
		body.Close()
		t.Fatal("checkpoint accepted an object whose ETag changed")
	}
	stage := checkpoint.Directory
	s3FixtureError(t, fixture, "release checkpoint", checkpoint.Close())
	s3FixtureError(t, fixture, "release checkpoint twice", checkpoint.Close())
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatal("checkpoint retained its abandoned local stage", err)
	}
	if body, err := checkpoint.OpenObject(ctx, payload); err == nil {
		body.Close()
		t.Fatal("closed checkpoint read an unprotected source object")
	}
	s3FixtureError(t, fixture, "cleanup after checkpoint release", store.cleanupDue(ctx, time.Now()))
	s3FixtureError(t, fixture, "delete released checkpoint project", store.DeleteProject(ctx, "a"))
	if len(fixture.keys(t, prefix)) != 0 || lakeStorageCount(t, store, "b", "events") != 1 {
		t.Fatal("checkpoint release retained project objects or affected another project")
	}
}

func TestDuckLakeS3CheckpointCanceledInventoryReleasesProtections(t *testing.T) {
	fixture := nativeS3Fixture(t)
	lake, err := newDuckLake(DuckLakeConfig{Directory: t.TempDir(), S3: &fixture.storage, MemoryLimitMB: 256, Threads: 1})
	s3FixtureError(t, fixture, "open canceled checkpoint fixture", err)
	store := lake.(*duckLakeStore)
	defer store.Close()
	ctx := context.Background()
	s3FixtureError(t, fixture, "seed canceled checkpoint", store.Apply(ctx, []eventProjection{lakeStorageProjection(t, "a", "a", 1)}))
	fixture.flush(t, store, "a")
	prefix := fixture.projectPrefix(t, store, "a")
	paused := pauseCheckpointHTTP(store, func(r *http.Request) bool { return r.Method == "GET" && r.URL.Query().Get("prefix") == prefix })
	var resume sync.Once
	release := func() { resume.Do(func() { close(paused.resume) }) }
	defer release()
	capture, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	acceptance := checkpointAcceptanceFixture(t)
	go func() { _, err := store.stageCheckpoint(capture, acceptance); done <- err }()
	select {
	case <-paused.entered:
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatal("checkpoint did not reach cancelable inventory")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal("checkpoint inventory ignored cancellation", err)
	}
	stages, err := filepath.Glob(filepath.Join(store.root, ".checkpoint-*"))
	if err != nil || len(stages) != 0 {
		t.Fatal("canceled remote checkpoint leaked its stage", err)
	}
	deadline, stop := context.WithTimeout(ctx, 4*time.Second)
	defer stop()
	if err := store.cleanup.Lock(deadline); err != nil {
		t.Fatal("canceled inventory leaked the cleanup guard", err)
	}
	store.cleanup.Unlock()
	// The paused LIST adapter also covers deletion. Release it before checking
	// that the project pin was removed by the failed capture.
	release()
	s3FixtureError(t, fixture, "delete after canceled checkpoint", store.DeleteProject(deadline, "a"))
}
