package main

import (
	"testing"
	"time"
)

func TestNativeBackupConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_BACKUP_DIR", "PORCUPINE_BACKUP_INTERVAL", "PORCUPINE_BACKUP_TIMEOUT", "PORCUPINE_BACKUP_RETAIN"} {
		t.Setenv(name, "")
	}
	config, err := nativeBackupConfiguration()
	if err != nil || config != nil {
		t.Fatal("native backup must be opt-in")
	}
	t.Setenv("PORCUPINE_BACKUP_INTERVAL", "24h")
	if _, err := nativeBackupConfiguration(); err == nil {
		t.Fatal("ignored partial backup configuration")
	}
	t.Setenv("PORCUPINE_BACKUP_DIR", t.TempDir())
	t.Setenv("PORCUPINE_BACKUP_TIMEOUT", "2m")
	t.Setenv("PORCUPINE_BACKUP_RETAIN", "3")
	config, err = nativeBackupConfiguration()
	if err != nil || config.Interval != 24*time.Hour || config.Timeout != 2*time.Minute || config.Retain != 3 {
		t.Fatal("native backup policy not parsed", err)
	}
	for _, field := range []struct{ name, value string }{{"PORCUPINE_BACKUP_INTERVAL", "0s"}, {"PORCUPINE_BACKUP_TIMEOUT", "2h"}, {"PORCUPINE_BACKUP_RETAIN", "0"}, {"PORCUPINE_BACKUP_RETAIN", "1001"}} {
		t.Run(field.name+field.value, func(t *testing.T) {
			t.Setenv(field.name, field.value)
			if _, err := nativeBackupConfiguration(); err == nil {
				t.Fatal("invalid backup configuration accepted")
			}
		})
	}
}
