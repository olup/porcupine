//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package syncx

import (
	"errors"
	"os"
)

// FileLockSupported reports whether advisory file locks work on this platform.
func FileLockSupported() bool { return false }

// FileLock takes a non-blocking shared or exclusive advisory lock.
func FileLock(*os.File, bool) error {
	return errors.New("online backup guard unavailable on this platform")
}

// FileLockBusy reports whether err means another holder has the lock.
func FileLockBusy(error) bool { return false }
