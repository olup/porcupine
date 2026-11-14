//go:build !ducklake

package porcupine

import (
	"context"
	"errors"
)

func RestoreNativeBackup(context.Context, NativeRestoreConfig) (NativeRestoreResult, error) {
	return NativeRestoreResult{}, errors.New("offline native restore requires a server built with the ducklake build tag")
}
