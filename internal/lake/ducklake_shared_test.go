package lake

import (
	"context"
	"errors"
	"fmt"
	"github.com/olup/porcupine/internal/event"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSharedNativeConfiguration(t *testing.T) {
	t.Parallel()
	source := event.NewID()
	for _, config := range []DuckLakeConfig{
		{PostgresURL: "postgres://localhost/lake"},
		{SharedSourceID: source},
		{PostgresURL: "https://localhost/lake", SharedSourceID: source, S3: &DuckLakeS3Config{}},
		{PostgresURL: "postgres://localhost/lake", SharedSourceID: "../../bad", S3: &DuckLakeS3Config{}},
	} {
		if _, err := NewDuckLake(config); err == nil {
			t.Fatal("accepted incomplete shared analytical configuration")
		}
	}
	schema := SharedNativeSchema(SharedNativeCatalogID(source, "project/with/untrusted chars"))
	if len(schema) > 63 {
		t.Fatal("shared schema exceeds PostgreSQL identifier limit")
	}
	if SharedNativeCatalogID(source, "a") == SharedNativeCatalogID(source, "b") {
		t.Fatal("project catalog identities overlap")
	}
}

func sharedNativeStores(t *testing.T) (context.Context, *DuckLakeStore, *DuckLakeStore, DuckLakeConfig) {
	t.Helper()
	dsn := sharedPostgresDSN(t)
	fixture := nativeS3Fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	config := DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "first"), PostgresURL: dsn, SharedSourceID: event.NewID(), S3: &fixture.storage, MemoryLimitMB: 128, Threads: 1, MaxOpenProjects: 2, DisableCleanup: true}
	first, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	secondConfig := config
	secondConfig.Directory = filepath.Join(t.TempDir(), "second")
	second, err := NewDuckLake(secondConfig)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	a, b := first.(*DuckLakeStore), second.(*DuckLakeStore)
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return ctx, a, b, config
}

func TestDuckLakeS3SharedPostgresIndependentEngines(t *testing.T) {
	t.Parallel()
	ctx, a, b, config := sharedNativeStores(t)
	duplicate := lakeStorageProjection(t, "project-a", "duplicate", 1)
	var wait sync.WaitGroup
	problems := make(chan error, 12)
	// Cold creation and replay happen independently on two workers.
	for i := 0; i < 12; i++ {
		unique := lakeStorageProjection(t, "project-a", fmt.Sprintf("unique-%d", i), int64(i+2))
		engine := a
		if i%2 != 0 {
			engine = b
		}
		wait.Add(1)
		go func() { defer wait.Done(); problems <- engine.Apply(ctx, []EventProjection{duplicate, unique}) }()
	}
	wait.Wait()
	close(problems)
	for err := range problems {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := lakeStorageCount(t, a, "project-a", "events"); got != 13 {
		t.Fatalf("cross-worker retries produced %d rows", got)
	}
	if got := lakeStorageCount(t, b, "project-a", "lake.heads"); got != 13 {
		t.Fatalf("cross-worker retries produced %d heads", got)
	}
	var inlined int
	schema := SharedNativeSchema(SharedNativeCatalogID(config.SharedSourceID, "project-a"))
	if err := a.SharedCatalog.QueryRowContext(ctx, "SELECT count(*) FROM "+schema+".ducklake_inlined_data_tables").Scan(&inlined); err != nil || inlined != 0 {
		t.Fatal("shared analytical payloads were inlined into PostgreSQL", inlined, err)
	}
	if _, err := os.Stat(filepath.Join(a.ProjectDirectory("project-a"), "catalog.ducklake")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("shared catalog unexpectedly lives on worker disk", err)
	}
	foreign := lakeStorageProjection(t, "project-b", "duplicate", 1)
	if err := b.Apply(ctx, []EventProjection{foreign}); err != nil {
		t.Fatal(err)
	}
	if got := lakeStorageCount(t, a, "project-b", "events"); got != 1 {
		t.Fatal("project catalogs leaked rows", got)
	}
	if err := a.Maintain(ctx); err != nil {
		t.Fatal("shared guarded maintenance failed", err)
	}
	a.Config.DisableCleanup = false
	if err := a.CleanupDue(ctx, time.Now()); err != nil {
		t.Fatal("shared guarded cleanup failed", err)
	}
	status, err := a.CleanupStatus(ctx)
	if err != nil || status.ObsoleteFileGraceSeconds < int64(sharedNativeReaderHorizon/time.Second) || status.SnapshotRetentionSeconds < int64(sharedNativeReaderHorizon/time.Second) {
		t.Fatal("shared cleanup did not report the remote reader protection horizon", status, err)
	}
	wrong := duplicate
	wrong.ID = 999
	if err := b.Apply(ctx, []EventProjection{wrong}); err == nil {
		t.Fatal("shared replay replaced its canonical ID")
	}
	older := lakeStorageProjection(t, "project-a", "identity-old", 100)
	newer := lakeStorageProjection(t, "project-a", "identity-new", 101)
	older.IdentitySnapshot = `{"version":"2","identified_id":"person","traits":{"plan":"free"},"links":[{"anonymous_id":"anonymous","identified_id":"person","version":"2"}]}`
	newer.IdentitySnapshot = `{"version":"3","identified_id":"person","traits":{"plan":"paid"},"links":[{"anonymous_id":"anonymous","identified_id":"person","version":"3"}]}`
	wait.Add(2)
	go func() {
		defer wait.Done()
		if err := a.Apply(ctx, []EventProjection{newer}); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wait.Done()
		if err := b.Apply(ctx, []EventProjection{older}); err != nil {
			t.Error(err)
		}
	}()
	wait.Wait()
	query, err := b.Query(ctx, "project-a", "SELECT person_id,person_properties FROM events WHERE uuid='duplicate'", nil, DefaultQueryExecutionLimits())
	if err != nil || len(query.Data) != 1 || string(query.Data[0]["person_id"]) != `"person"` || !strings.Contains(string(query.Data[0]["person_properties"]), "paid") {
		t.Fatal("cross-worker identity failed", query, err)
	}
	edit := duplicate
	edit.Revision = 5
	edit.Event.Event = "corrected"
	deleted := duplicate
	deleted.Revision = 6
	deleted.IsDeleted = true
	wait.Add(2)
	go func() {
		defer wait.Done()
		if err := a.Apply(ctx, []EventProjection{edit}); err != nil {
			t.Error(err)
		}
	}()
	go func() {
		defer wait.Done()
		if err := b.Apply(ctx, []EventProjection{deleted, duplicate}); err != nil {
			t.Error(err)
		}
	}()
	wait.Wait()
	current, err := a.Load(ctx, "project-a", duplicate.UUID)
	if err != nil || !current.IsDeleted || current.Revision != 6 || current.ID != duplicate.ID {
		t.Fatalf("shared projections resurrected event: %+v %v", current, err)
	}
	if got := lakeStorageCount(t, b, "project-a", "lake.identity_profiles"); got != 1 {
		t.Fatal("shared identity versions duplicated profiles", got)
	}
	if got := lakeStorageCount(t, b, "project-a", "lake.identity_links"); got != 1 {
		t.Fatal("shared identity versions duplicated links", got)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewDuckLake(config)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := lakeStorageCount(t, reopened.(*DuckLakeStore), "project-a", "events"); got != 12 {
		t.Fatal("worker restart lost shared analytical state", got)
	}
	if _, err := reopened.StageCheckpoint(ctx, func(context.Context, string) error { t.Fatal("shared checkpoint started local snapshot"); return nil }); err == nil {
		t.Fatal("published incomplete local backup for shared catalogs")
	}
}

func TestDuckLakeS3SharedPostgresGuardAndDeletion(t *testing.T) {
	t.Parallel()
	ctx, a, b, _ := sharedNativeStores(t)
	event := lakeStorageProjection(t, "project", "event", 1)
	if err := a.Apply(ctx, []EventProjection{event}); err != nil {
		t.Fatal(err)
	}
	var inlined int
	schema := SharedNativeSchema(SharedNativeCatalogID(a.Config.SharedSourceID, "project"))
	if err := a.SharedCatalog.QueryRowContext(ctx, "SELECT count(*) FROM "+schema+".ducklake_inlined_data_tables").Scan(&inlined); err != nil || inlined != 0 {
		t.Fatal("one-event shared write retained payload in PostgreSQL", inlined, err)
	}
	pa, releaseA, err := a.LeaseProject(ctx, "project")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()
	pb, releaseB, err := b.LeaseProject(ctx, "project")
	if err != nil {
		t.Fatal(err)
	}
	defer releaseB()
	first, err := pa.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Rollback()
	second, err := pb.DB.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Rollback()
	if err := pa.acquireNativeWrite(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := pb.acquireNativeWrite(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(); !isSharedNativeConflict(err) {
		t.Fatal("stale transaction did not fail its native guard", err)
	}
	releaseA()
	releaseB()
	if err := a.DeleteProject(ctx, "project"); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(ctx, []EventProjection{event}); err == nil {
		t.Fatal("stale worker resurrected a deleted project")
	}
	if _, err := b.Load(ctx, "project", "event"); err == nil {
		t.Fatal("cached worker served a deleted project")
	}
	if err := b.DeleteProject(ctx, "project"); err != nil {
		t.Fatal("deletion retry failed", err)
	}
}

func TestDuckLakeS3SharedPostgresStorageIdentity(t *testing.T) {
	t.Parallel()
	ctx, a, _, config := sharedNativeStores(t)
	if _, err := a.SourceID(ctx); err != nil {
		t.Fatal(err)
	}
	changed := config
	changed.Directory = t.TempDir()
	storage := *config.S3
	storage.Prefix += "-changed"
	changed.S3 = &storage
	if _, err := NewDuckLake(changed); err == nil {
		t.Fatal("shared source was redirected to another S3 namespace")
	}
}
