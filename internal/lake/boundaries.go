package lake

import (
	"os"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/syncx"
)

type managementError = apierr.Error

type contextMutex = syncx.ContextMutex

func apiError(status int, message string) error { return apierr.New(status, message) }

func analyticsError(status int, code, message, hint, field string) error {
	return apierr.Detailed(status, code, message, hint, field)
}

func projectionLockSupported() bool                          { return syncx.FileLockSupported() }
func projectionFileLock(file *os.File, exclusive bool) error { return syncx.FileLock(file, exclusive) }

// lockWriter takes the exclusive writer lock. A process that is forking may
// briefly share the previous owner's lock description, so a busy lock is
// retried for a short time before another writer is assumed.
func lockWriter(owner *os.File) error {
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := projectionFileLock(owner, true)
		if err == nil || !syncx.FileLockBusy(err) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}
