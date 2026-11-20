package porcupine

import (
	"database/sql"
	"errors"
	"io"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/importer"
)

func TestStandaloneDeliveryWakesAndSweeps(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := service(t, 100)
		id, token := project(t, s)
		_, editKey := key(t, s, []string{"events:read", "events:edit", "events:delete", "imports:write", "events:write"}, []string{id})
		attachCoreLake(t, s)
		capture := func(uuid string) {
			request(t, s, "POST", "/capture/", "", captureBody(token, uuid), 200)
		}
		expectReceipts := func(want int) {
			t.Helper()
			synctest.Wait()
			if got := receiptCount(t, s); got != want {
				t.Fatalf("got %d acknowledged originals, want %d", got, want)
			}
		}
		waitForIdle := func() {
			time.Sleep(40 * time.Second)
			synctest.Wait()
		}

		capture("repair")
		capture("startup")
		queued, err := s.deliveryBatch(t.Context())
		if err != nil || len(queued) != 2 {
			t.Fatalf("seeded journal queue: %v, %v", queued, err)
		}
		retained := queued[0]
		if changed, err := s.quarantineOriginal(t.Context(), retained, 321); !changed || err != nil {
			t.Fatalf("seeded quarantine: %t, %v", changed, err)
		}
		s.startDelivery()
		expectReceipts(1)

		waitForIdle()
		capture("wake")
		expectReceipts(2)
		capture("batch-1")
		time.Sleep(deliveryInterval / 2)
		capture("batch-2")
		expectReceipts(2)
		time.Sleep(deliveryInterval / 2)
		expectReceipts(4)

		waitForIdle()
		data := importTestRow("historical")
		job := importTestCreate(t, s, id, editKey, "delivery-wake", []importer.ImportUploadFileSpec{{
			Path: "events.jsonl", Kind: "events", SizeBytes: int64(len(data)), SHA256: importTestSHA(data),
		}})
		importTestPut(t, s, id, job, job.Files[0], editKey, 0, data, 200)
		expectReceipts(5)

		waitForIdle()
		eventPath := "/api/v1/projects/" + id + "/events/startup"
		mutation := request(t, s, "DELETE", eventPath, editKey, `{"expected_revision":"0","idempotency_key":"delivery-wake"}`, 202)
		synctest.Wait()
		statusPath := "/api/v1/projects/" + id + "/event-mutations/" + mutation["mutation_id"].(string)
		if status := request(t, s, "GET", statusPath, editKey, "", 200); status["state"] != "applied" {
			t.Fatalf("mutation did not wake delivery: %v", status)
		}

		waitForIdle()
		retryPath := "/api/v1/projects/" + id + "/quarantined-events/" + strconv.FormatInt(retained.ID, 10) + "/retry"
		request(t, s, "POST", retryPath, editKey, "", 202)
		expectReceipts(6)

		waitForIdle()
		// Simulate a durable writer without an in-process wake signal.
		writer := *s.acceptance.(*durableAcceptance)
		writer.wakeDelivery = nil
		batch, err := prepareLiveBatch(id, "", eventBatch{{UUID: "silent", Event: "purchase", DistinctID: "user"}}, time.Now(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Accept(t.Context(), batch, acceptance.Peer{}, false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(deliveryInterval)
		expectReceipts(6)
		time.Sleep(deliveryMaxDelay)
		expectReceipts(7)

		waitForIdle()
		if _, err := s.db.Exec(`CREATE TRIGGER lose_worker_delivery_ack BEFORE DELETE ON events
			WHEN OLD.uuid='retry' BEGIN SELECT RAISE(FAIL,'simulated lost acknowledgment'); END`); err != nil {
			t.Fatal(err)
		}
		capture("retry")
		expectReceipts(7)
		status, err := s.deliveryStatus(t.Context(), id)
		if err != nil || status.Error != "DuckLake delivery failed" || status.Pending != 1 {
			t.Fatalf("lost acknowledgment did not retain durable work and report its health: %v, %v", status, err)
		}
		if _, err := s.db.Exec(`DROP TRIGGER lose_worker_delivery_ack`); err != nil {
			t.Fatal(err)
		}
		time.Sleep(deliveryInterval)
		expectReceipts(8)
	})
}

func TestDeliveryIdleSweepSkipsProjectionGuard(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	if s.projectionGuard == nil {
		t.Skip("projection locks are unavailable")
	}
	s.projectionGuard.path = t.TempDir() // opening a directory as a lock file fails
	if mutations, originals, err := s.deliverPending(t.Context()); mutations || originals || err != nil {
		t.Fatalf("idle sweep: %t, %t, %v", mutations, originals, err)
	}
	_, token := project(t, s)
	request(t, s, "POST", "/capture/", "", captureBody(token, "queued"), 200)
	if _, _, err := s.deliverPending(t.Context()); err == nil {
		t.Fatal("pending original bypassed the projection guard")
	}
}

func TestDeliveryResultWritesOnChange(t *testing.T) {
	t.Parallel()
	s := service(t, 100)
	if _, err := s.db.Exec(`
		INSERT INTO metadata(key,value) VALUES('last_delivery_error','persisted error');
		CREATE TABLE delivery_error_updates(key TEXT);
		CREATE TRIGGER count_delivery_error_update AFTER UPDATE ON metadata
		WHEN NEW.key='last_delivery_error' BEGIN
			INSERT INTO delivery_error_updates VALUES(NEW.key);
		END;
	`); err != nil {
		t.Fatal(err)
	}
	readError := func(want string) {
		t.Helper()
		var value string
		err := s.db.QueryRow(`SELECT value FROM metadata WHERE key='last_delivery_error'`).Scan(&value)
		if want == "" && errors.Is(err, sql.ErrNoRows) {
			return
		}
		if err != nil || value != want {
			t.Fatalf("delivery error = %q, %v, want %q", value, err, want)
		}
	}

	s.recordDeliveryResult(t.Context(), nil)
	readError("")
	s.recordDeliveryResult(t.Context(), io.EOF)
	readError("DuckLake delivery failed")
	s.recordDeliveryResult(t.Context(), io.ErrUnexpectedEOF)
	var updates int
	if err := s.db.QueryRow(`SELECT count(*) FROM delivery_error_updates`).Scan(&updates); err != nil || updates != 0 {
		t.Fatalf("unchanged failure caused %d metadata updates, %v", updates, err)
	}
	s.recordDeliveryResult(t.Context(), nil)
	readError("")
}
