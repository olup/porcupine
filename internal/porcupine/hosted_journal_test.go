package porcupine

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/syncx"
)

func TestHostedJournalExclusiveAndBoundToSource(t *testing.T) {
	t.Parallel()
	if !syncx.FileLockSupported() {
		t.Skip("worker journal requires flock")
	}
	path := filepath.Join(t.TempDir(), "worker", "queue.sqlite")
	db, unlock, err := openHostedJournal(t.Context(), path, "first")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := openHostedJournal(t.Context(), path, "first"); err == nil {
		t.Fatal("a second worker acquired the same journal")
	}
	for pragma, expected := range map[string]string{"journal_mode": "wal", "synchronous": "2"} {
		var value string
		if err := db.QueryRow("PRAGMA " + pragma).Scan(&value); err != nil || value != expected {
			t.Errorf("%s=%q, want %q: %v", pragma, value, expected, err)
		}
	}
	_ = db.Close()
	unlock()
	if _, _, err := openHostedJournal(t.Context(), path, "different"); err == nil {
		t.Fatal("journal reopened against another shared source")
	}
	db, unlock, err = openHostedJournal(t.Context(), path, "first")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unlock)
	t.Cleanup(func() { _ = db.Close() })
}

func TestHostedJournalRestoresSplitBatches(t *testing.T) {
	t.Parallel()
	if !syncx.FileLockSupported() {
		t.Skip("worker journal requires flock")
	}
	path := filepath.Join(t.TempDir(), "queue.sqlite")
	cfg := acceptance.DefaultBufferConfig()
	cfg.ProjectMaxEvents = 9
	cfg.FlushInterval = time.Hour
	start := func(ctx context.Context, write acceptance.FlushFunc) *acceptance.Buffer {
		t.Helper()
		db, unlock, err := openHostedJournal(ctx, path, "source")
		if err != nil {
			t.Fatal(err)
		}
		buffer, err := acceptance.NewJournalBuffer(ctx, ctx, cfg, write, nil, nil, db, unlock)
		if err != nil {
			_ = db.Close()
			unlock()
			t.Fatal(err)
		}
		return buffer
	}
	ctx, stop := context.WithCancel(t.Context())
	defer stop()
	started := make(chan acceptance.FlushBatch, 1)
	first := start(ctx, func(ctx context.Context, batch acceptance.FlushBatch) error {
		if batch.CanSplit() {
			return acceptance.ErrBatchTooLarge
		}
		started <- batch
		<-ctx.Done()
		return ctx.Err()
	})
	properties := `{ "section": "welcome" }`
	for _, id := range []string{"one", "two", "three"} {
		batch := acceptance.Batch{ProjectID: "project", Events: []acceptance.Input{
			{Event: eventapi.Event{ProjectID: "project", UUID: id, Event: "click", Properties: []byte(properties)}},
		}}
		if _, err := first.Accept(t.Context(), batch, acceptance.Peer{}, true); err != nil {
			t.Fatal(err)
		}
	}
	var left acceptance.FlushBatch
	select {
	case left = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("did not split the journaled requests")
	}
	stop()
	deadline, finish := context.WithTimeout(t.Context(), 5*time.Second)
	defer finish()
	if err := first.Drain(deadline); err == nil || deadline.Err() != nil {
		t.Fatalf("expected a stopped worker with pending batches: %v", err)
	}
	replayed := make(chan acceptance.FlushBatch, 3)
	second := start(deadline, func(_ context.Context, batch acceptance.FlushBatch) error {
		replayed <- batch
		return nil
	})
	if err := second.Drain(deadline); err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 {
		t.Fatal("recovery did not preserve the two sealed children")
	}
	firstChild, secondChild := <-replayed, <-replayed
	if firstChild.ID != left.ID || secondChild.ID == firstChild.ID {
		t.Fatal("recovery changed sealed batch identity")
	}
	var order []string
	for _, batch := range []acceptance.FlushBatch{firstChild, secondChild} {
		for _, ev := range batch.Events {
			order = append(order, ev.Input.Event.UUID)
			if string(ev.Input.Event.Properties) != properties {
				t.Fatal("recovery changed raw properties")
			}
		}
	}
	if got := strings.Join(order, ","); got != "one,two,three" {
		t.Fatal("recovery changed request order or count", got)
	}
}

func TestDuckLakeS3SharedPostgresHostedJournalCrashRecovery(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	dsn := sharedPostgresDSN(t)
	server := filepath.Join(t.TempDir(), "porcupine-server")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", server, "./cmd/porcupine")
	build.Dir, build.Env = "../..", append(os.Environ(), "CGO_ENABLED=1")
	if result, err := build.CombinedOutput(); err != nil {
		t.Fatal("build server", err, string(result))
	}
	journal := filepath.Join(t.TempDir(), "worker.sqlite")
	launch := func(interval string) *sharedJourneyProcess {
		t.Helper()
		return startHostedProfileBinary(t, server, dsn, t.TempDir(), fixture, func(config map[string]any) {
			config["acceptance"] = map[string]string{"mode": "durable", "flush_interval": interval}
			config["storage"].(map[string]string)["journal"] = journal
		})
	}
	first := launch("30s")
	project, ingest := sharedJourneyProject(t, first.origin, defaultTenantID, "persistent worker")
	other, otherIngest := sharedJourneyProject(t, first.origin, defaultTenantID, "other project")
	_, reader := sharedJourneyKey(t, first.origin, project, []string{"events:read"})
	_, otherReader := sharedJourneyKey(t, first.origin, other, []string{"events:read"})

	input := `{"type":"track","message_id":"replay","name":"journal event","actor_id":"test","timestamp":"2020-01-01T12:00:00Z"}`
	schema := sharedJourneyHTTP(t, first.origin, "GET", "/ingest/v1/schema", "", nil, nil, 200)
	description := schema["x-porcupine-acknowledgment"].(map[string]any)
	if description["durability"] != "durable" || len(description["receipt_fields"].([]any)) != 2 {
		t.Fatal("native discovery did not describe hosted durable admission", description)
	}
	for _, key := range []string{ingest, ingest, otherIngest} {
		ack := sharedJourneyNative(t, first.origin, key, input, 200)
		if ack["durability"] != "durable" || ack["received"] != float64(1) || ack["duplicates"] != nil ||
			len(ack["receipts"].([]any)[0].(map[string]any)) != 2 {
			t.Fatal("hosted acknowledgment was not journaled", ack)
		}
	}
	first.stopProcess(t, true)

	db, err := openLocalSQLite(journal)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var rows int
	if err := db.QueryRow("SELECT count(*) FROM requests WHERE batch_id IS NULL").Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("after SIGKILL before native flush, pending rows = %d: %v", rows, err)
	}
	// Hold the journal at the exact boundary after publication.
	if _, err := db.Exec(`CREATE TRIGGER hold_ack BEFORE DELETE ON requests BEGIN SELECT RAISE(FAIL, 'test crash boundary'); END`); err != nil {
		t.Fatal(err)
	}

	second := launch("1s")
	control, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	usage := func(project string) int {
		t.Helper()
		var n int
		if err := control.QueryRow(`SELECT coalesce(sum(live_events),0) FROM usage_rollups WHERE project_id=$1`, project).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	wait := func(description string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(25 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatal(description)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	wait("recovered lots were not published", func() bool { return usage(project) == 1 && usage(other) == 1 })
	var sealed int
	if err := db.QueryRow("SELECT count(DISTINCT batch_id) FROM requests").Scan(&sealed); err != nil || sealed != 2 {
		t.Fatalf("published lots were not retained for replay: %d: %v", sealed, err)
	}
	second.stopProcess(t, true)
	if _, err := db.Exec("DROP TRIGGER hold_ack"); err != nil {
		t.Fatal(err)
	}

	third := launch("1s")
	wait("replayed lots were not removed after native publication", func() bool {
		if err := db.QueryRow("SELECT count(*) FROM requests").Scan(&rows); err != nil {
			t.Fatal(err)
		}
		return rows == 0
	})
	for _, scope := range []struct{ project, key string }{{project, reader}, {other, otherReader}} {
		result := sharedJourneyQuery(t, third.origin, scope.project, scope.key, "SELECT count(*) AS n FROM events WHERE event='journal event'", 200)
		data := result["result"].(map[string]any)["data"].([]any)
		if len(data) != 1 || data[0].(map[string]any)["n"] != "1" || usage(scope.project) != 1 {
			t.Fatalf("replay lost or duplicated an event or its usage for %s: %v", scope.project, data)
		}
	}
}
