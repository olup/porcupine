//go:build ducklake

package porcupine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDuckLakeStoppedVolumeRestore(t *testing.T) {
	root := t.TempDir()
	live, backup := filepath.Join(root, "live"), filepath.Join(root, "backup")
	config := Config{DBPath: filepath.Join(live, "queue.db"), BootstrapToken: adminToken,
		DuckLake: &DuckLakeConfig{Directory: filepath.Join(live, "lake"), MemoryLimitMB: 128, Threads: 1}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve an accepted, undelivered event in the stopped checkpoint.
	s.deliveryCancel()
	<-s.deliveryDone
	a, ingest := project(t, s)
	b, _ := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	first := `{"type":"track","message_id":"restored-history","name":"page_viewed","actor_id":"visitor"}`
	second := `{"type":"track","message_id":"restored-queue","name":"signup_completed","actor_id":"visitor"}`
	firstID := receiptFrom(t, nativeRequest(t, s, ingest, first, 200), 0)["event_id"]
	duckLakeJourneyDrain(t, s)
	source, err := s.lake.SourceID(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.lake.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondID := receiptFrom(t, nativeRequest(t, s, ingest, second, 200), 0)["event_id"]
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(backup, os.DirFS(live)); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(live); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(live, os.DirFS(backup)); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	duckLakeJourneyDrain(t, s)
	restoredSource, err := s.lake.SourceID(context.Background())
	if err != nil || restoredSource != source {
		t.Fatalf("archive identity changed: %v", err)
	}
	rows := duckLakeJourneyQuery(t, s, a, reader, "SELECT event_id,event FROM events ORDER BY id", nil)
	if len(rows) != 2 {
		t.Fatalf("restored history and queue: %v", rows)
	}
	expected := []struct {
		id    any
		event string
	}{{firstID, "page_viewed"}, {secondID, "signup_completed"}}
	for i, want := range expected {
		got := rows[i].(map[string]any)
		if got["event_id"] != want.id || got["event"] != want.event {
			t.Fatalf("restored event changed: %v", got)
		}
	}
	for i, payload := range []string{first, second} {
		retry := nativeRequest(t, s, ingest, payload, 200)
		if retry["duplicates"] != float64(1) || receiptFrom(t, retry, 0)["event_id"] != expected[i].id {
			t.Fatal("restored retry changed stable ID")
		}
	}
	request(t, s, "POST", "/api/v1/projects/"+b+"/query", reader, `{"sql":"SELECT count(*) FROM events"}`, 403)
}
