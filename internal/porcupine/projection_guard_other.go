//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package porcupine

import (
	"errors"
	"os"
)

func projectionLockSupported() bool { return false }
func projectionFileLock(*os.File, bool) error {
	return errors.New("online backup guard unavailable on this platform")
}
func projectionLockBusy(error) bool { return false }
