package porcupine

import (
	"os"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/syncx"
)

// Local names for types and helpers that moved to lower internal packages.

type managementError = apierr.Error

type contextMutex = syncx.ContextMutex

func apiError(status int, message string) error { return apierr.New(status, message) }

func analyticsError(status int, code, message, hint, field string) error {
	return apierr.Detailed(status, code, message, hint, field)
}

func projectionLockSupported() bool                          { return syncx.FileLockSupported() }
func projectionFileLock(file *os.File, exclusive bool) error { return syncx.FileLock(file, exclusive) }
func projectionLockBusy(err error) bool                      { return syncx.FileLockBusy(err) }
