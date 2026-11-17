//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package syncx

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// FileLockSupported reports whether advisory file locks work on this platform.
func FileLockSupported() bool { return true }

// FileLock takes a non-blocking shared or exclusive advisory lock.
func FileLock(file *os.File, exclusive bool) error {
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	return unix.Flock(int(file.Fd()), mode|unix.LOCK_NB)
}

// FileLockBusy reports whether err means another holder has the lock.
func FileLockBusy(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
