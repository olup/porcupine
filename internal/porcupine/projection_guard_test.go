package porcupine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProjectionGuardCancellationAndNestedCalls(t *testing.T) {
	if !projectionLockSupported() {
		t.Skip("online checkpoint locks are unavailable on this platform")
	}
	guard, err := openProjectionGuard(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer guard.close()
	external, err := os.OpenFile(guard.path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = projectionFileLock(external, true); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	if _, _, err = guard.shared(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("checkpoint wait ignored cancellation", err)
	}
	external.Close()
	ctx, release, err := guard.shared(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, nestedRelease, err := guard.shared(ctx)
	if err != nil {
		t.Fatal("nested projection guard acquisition failed", err)
	}
	nestedRelease()
	external, err = os.OpenFile(guard.path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	if err = projectionFileLock(external, true); !projectionLockBusy(err) {
		t.Fatal("nested release unlocked its caller's projection", err)
	}
	release()
	if err = projectionFileLock(external, true); err != nil {
		t.Fatal("released projection retained its checkpoint lock", err)
	}
}

func TestProjectionGuardLifetimeAllowsServicesAndExcludesRestore(t *testing.T) {
	if !projectionLockSupported() {
		t.Skip("online checkpoint locks are unavailable on this platform")
	}
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := openProjectionGuard(path)
	if err != nil {
		t.Fatal(err)
	}
	second, err := openProjectionGuard(path)
	if err != nil {
		t.Fatal("normal concurrent Service callers were rejected", err)
	}
	external, err := os.OpenFile(path+".writer.lock", os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	for _, closeGuard := range []func() error{first.close, second.close} {
		if err = projectionFileLock(external, true); !projectionLockBusy(err) {
			t.Fatal("offline restore could bypass a live service", err)
		}
		closeGuard()
	}
	if err = projectionFileLock(external, true); err != nil {
		t.Fatal("stopped services left a stale restore lease", err)
	}
	if _, err = openProjectionGuard(path); err == nil {
		t.Fatal("application startup bypassed an offline restore")
	}
	external.Close()
	if err = os.WriteFile(path+".restore-incomplete", []byte("incomplete"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = openProjectionGuard(path); err == nil {
		t.Fatal("application startup ignored the interrupted restore marker")
	}
}
