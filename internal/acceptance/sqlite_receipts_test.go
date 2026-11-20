package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/syncx"
	_ "modernc.org/sqlite"
)

func TestReceiptProjectGateCancellationReleasesReferences(t *testing.T) {
	t.Parallel()
	var gates receiptProjectGates
	release, err := gates.lock(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gates.lock(canceled, "a"); err != context.Canceled {
		t.Fatalf("canceled project gate returned %v", err)
	}
	release()
	if len(gates.projects) != 0 {
		t.Fatal("canceled waiter leaked an idle project gate")
	}
}

func TestSQLiteReceiptAdmissionUsesMarkedHistoryAndReleasesAfterFailure(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "receipts.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT);
		INSERT INTO metadata VALUES('receipts_pruned:pruned', '1')`); err != nil {
		t.Fatal(err)
	}

	var admission syncx.ContextMutex
	var history ReceiptLookup
	receipts := NewSQLiteReceipts(db, &admission, time.Now, func() ReceiptLookup { return history })
	if err := receipts.Prune(t.Context(), time.Hour); err != nil {
		t.Fatal("prune without analytical history", err)
	}
	uuids := []string{"a", "b", "a"}
	known, release, err := receipts.BeginAdmission(t.Context(), "unpruned", uuids)
	if err != nil {
		t.Fatal("unpruned admission without analytical history", err)
	}
	release()
	if len(known) != 0 {
		t.Fatalf("unpruned receipt lookup returned %v", known)
	}

	if _, _, err := receipts.BeginAdmission(t.Context(), "pruned", uuids); !errors.Is(err, ErrReceiptLookup) {
		t.Fatalf("admission with missing analytical history returned %v", err)
	}

	var lookupContext context.Context
	history = func(ctx context.Context, project string, ids []string) (map[string]int64, error) {
		lookupContext = ctx
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > receiptLookupTimeout {
			t.Error("analytical lookup has no bounded deadline")
		}
		if project != "pruned" || !slices.Equal(ids, []string{"a", "b"}) {
			t.Errorf("looked up %v for project %q", ids, project)
		}
		if err := admission.Lock(ctx); err != nil {
			return nil, err
		}
		admission.Unlock()
		return map[string]int64{"a": 42}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*receiptLookupTimeout)
	defer cancel()
	known, release, err = receipts.BeginAdmission(ctx, "pruned", uuids)
	if err != nil {
		t.Fatal("retry with restored analytical history", err)
	}
	release()
	if known["a"] != 42 || len(known) != 1 {
		t.Fatalf("recovered receipt lookup returned %v", known)
	}
	if lookupContext == nil || !errors.Is(lookupContext.Err(), context.Canceled) {
		t.Fatal("admission retained its lookup context")
	}

	wait, stop := context.WithCancel(t.Context())
	defer stop()
	history = func(context.Context, string, []string) (map[string]int64, error) {
		stop()
		return nil, nil
	}
	if _, _, err := receipts.BeginAdmission(wait, "pruned", uuids); err != context.Canceled {
		t.Fatalf("admission mutex cancellation returned %v", err)
	}
	if len(receipts.gates.projects) != 0 {
		t.Fatal("canceled admission retained a project gate")
	}
}
