//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package porcupine

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func projectionLockSupported() bool { return true }
func projectionFileLock(file *os.File, exclusive bool) error {
	mode := unix.LOCK_SH
	if exclusive {
		mode = unix.LOCK_EX
	}
	return unix.Flock(int(file.Fd()), mode|unix.LOCK_NB)
}
func projectionLockBusy(err error) bool {
	return errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)
}
