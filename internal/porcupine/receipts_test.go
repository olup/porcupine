package porcupine

import (
	"context"
	"path/filepath"
	"testing"
)

func (s *Service) dbPathForReceiptTest(t *testing.T) string {
	t.Helper()
	var seq int
	var name, path string
	if err := s.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPermanentReceiptsKeepNativeIDsAcrossRestart(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	config := Config{DBPath: filepath.Join(directory, "queue.db"), BootstrapToken: adminToken, DuckLake: &DuckLakeConfig{Directory: filepath.Join(directory, "lake"), MemoryLimitMB: 128, Threads: 1}}
	s, err := Open(config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	stopCoreDelivery(s)
	projectID, writer := project(t, s)
	first := nativeRequest(t, s, writer, `{"type":"track","message_id":"permanent","actor_id":"a","name":"purchase","timestamp":"2020-01-01T01:02:03.123456789Z","properties":{"value":1}}`, 200)
	id := receiptFrom(t, first, 0)["event_id"]
	for _, key := range []string{"later-a", "later-b", "later-c"} {
		nativeRequest(t, s, writer, `{"type":"track","message_id":"`+key+`","name":"later"}`, 200)
	}
	duckLakeJourneyDrain(t, s)
	if receiptCount(t, s) != 4 {
		t.Fatal("native delivery did not keep permanent receipt facts")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(config)
	if err != nil {
		t.Fatal(err)
	}
	stopCoreDelivery(s)
	replay := nativeRequest(t, s, writer, `{"type":"track","message_id":"permanent","name":"changed","properties":{"value":999}}`, 200)
	if receiptFrom(t, replay, 0)["event_id"] != id || replay["duplicates"] != float64(1) {
		t.Fatal("permanent receipt changed stable ID or replayed a payload", replay)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM events WHERE project_id=?`, projectID).Scan(&count); err != nil || count != 0 {
		t.Fatal("duplicate re-entered queue", err)
	}
}

func TestReceiptAndHeadNumericIDRemainAtomic(t *testing.T) {
	t.Parallel()
	s := service(t, 10)
	projectID, writer := project(t, s)
	first := nativeRequest(t, s, writer, `{"type":"track","message_id":"one","name":"one"}`, 200)
	uuid := receiptFrom(t, first, 0)["uuid"].(string)
	var id int64
	if err := s.db.QueryRow(`SELECT id FROM events WHERE project_id=? AND uuid=?`, projectID, uuid).Scan(&id); err != nil {
		t.Fatal(err)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.recordReceipt(context.Background(), tx, projectID, uuid, id); err != nil {
		t.Fatal(err)
	}
	tx.Rollback()
	if receiptCount(t, s) != 0 {
		t.Fatal("uncommitted receipt escaped its owning acknowledgment transaction")
	}
}

func attachCoreLake(t *testing.T, s *Service) *DuckLakeConfig {
	t.Helper()
	config := &DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), MemoryLimitMB: 128, Threads: 1}
	lake, err := newDuckLake(*config)
	if err != nil {
		t.Fatal(err)
	}
	s.lake = lake
	s.imports.deliveryEnabled = true
	s.lakeMemoryMB = 128
	if err := s.initializeLakeSource(context.Background()); err != nil {
		t.Fatal(err)
	}
	return config
}
func nativeCoreService(t *testing.T) (*Service, Config) {
	t.Helper()
	s := service(t, 100)
	config := attachCoreLake(t, s)
	return s, Config{DBPath: s.dbPathForReceiptTest(t), BootstrapToken: adminToken, DuckLake: config}
}
func stopCoreDelivery(s *Service) {
	if s.deliveryCancel != nil {
		s.deliveryCancel()
		<-s.deliveryDone
		s.deliveryCancel = nil
		s.deliveryDone = nil
	}
}
