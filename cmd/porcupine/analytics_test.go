package main

import (
	"strings"
	"testing"
	"time"
)

func TestAnalyticsConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_DB", "PORCUPINE_CLICKHOUSE_URL", "PORCUPINE_DUCKLAKE_DIR", "PORCUPINE_DUCKLAKE_MEMORY_MB", "PORCUPINE_DUCKLAKE_THREADS", "PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", "PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "PORCUPINE_HOT_STORAGE_DAYS", "PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION", "PORCUPINE_DUCKLAKE_CLEANUP_GRACE", "PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL", "PORCUPINE_DUCKLAKE_CLEANUP_ENABLED"} {
		t.Setenv(name, "")
	}
	t.Run("build default", func(t *testing.T) {
		t.Setenv("PORCUPINE_DB", "/srv/analytics/queue.db")
		lake, err := analyticsConfiguration()
		expected := defaultAnalyticsDirectory("/srv/analytics/queue.db")
		if err != nil || lake == nil || lake.Directory != expected {
			t.Fatalf("unexpected default: lake=%v err=%v", lake, err)
		}
	})
	t.Run("retired engine", func(t *testing.T) {
		t.Setenv("PORCUPINE_CLICKHOUSE_URL", "http://clickhouse:8123")
		lake, err := analyticsConfiguration()
		if err == nil || lake != nil {
			t.Fatal("retired engine configuration was silently ignored")
		}
	})
	t.Run("ambiguous engine", func(t *testing.T) {
		t.Setenv("PORCUPINE_CLICKHOUSE_URL", "http://clickhouse:8123")
		t.Setenv("PORCUPINE_DUCKLAKE_DIR", "/srv/analytics/lake")
		if _, err := analyticsConfiguration(); err == nil {
			t.Fatal("ambiguous storage configuration accepted")
		}
	})
	for _, field := range []struct{ name, value string }{
		{"PORCUPINE_DUCKLAKE_MEMORY_MB", "63"}, {"PORCUPINE_DUCKLAKE_MEMORY_MB", "32769"},
		{"PORCUPINE_DUCKLAKE_THREADS", "5"}, {"PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", "0"},
		{"PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION", "-1h"},
		{"PORCUPINE_DUCKLAKE_CLEANUP_GRACE", "500ms"},
		{"PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL", "invalid"},
		{"PORCUPINE_DUCKLAKE_CLEANUP_ENABLED", "invalid"},
		{"PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "0s"}, {"PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "25h"},
	} {
		t.Run(field.name+field.value, func(t *testing.T) {
			t.Setenv("PORCUPINE_DUCKLAKE_DIR", t.TempDir())
			t.Setenv(field.name, field.value)
			if _, err := analyticsConfiguration(); err == nil {
				t.Fatal("invalid native budget accepted")
			}
		})
	}
}

func TestAnalyticsCleanupConfiguration(t *testing.T) {
	t.Setenv("PORCUPINE_CLICKHOUSE_URL", "")
	t.Setenv("PORCUPINE_DUCKLAKE_DIR", t.TempDir())
	t.Setenv("PORCUPINE_DUCKLAKE_SNAPSHOT_RETENTION", "240h")
	t.Setenv("PORCUPINE_DUCKLAKE_CLEANUP_GRACE", "72h")
	t.Setenv("PORCUPINE_DUCKLAKE_CLEANUP_INTERVAL", "2h")
	t.Setenv("PORCUPINE_DUCKLAKE_CLEANUP_ENABLED", "false")
	lake, err := analyticsConfiguration()
	if err != nil || lake.SnapshotRetention != 240*time.Hour || lake.CleanupGrace != 72*time.Hour || lake.CleanupInterval != 2*time.Hour || !lake.DisableCleanup {
		t.Fatalf("cleanup policy not parsed: %+v %v", lake, err)
	}
}

func TestAnalyticsRejectsRetiredEnvironmentWithoutExposingValues(t *testing.T) {
	for _, name := range []string{"PORCUPINE_CLICKHOUSE_URL", "PORCUPINE_CLICKHOUSE_DATABASE", "PORCUPINE_CLICKHOUSE_USER", "PORCUPINE_CLICKHOUSE_PASSWORD", "PORCUPINE_CLICKHOUSE_STORAGE_POLICY", "PORCUPINE_CLICKHOUSE_COLD_VOLUME", "PORCUPINE_HOT_STORAGE_DAYS", "PORCUPINE_HOT_RECEIPT_LIMIT"} {
		t.Run(name, func(t *testing.T) {
			const private = "retired-private-value"
			t.Setenv(name, private)
			config, err := analyticsConfiguration()
			if err == nil || config != nil || !strings.Contains(err.Error(), name) || strings.Contains(err.Error(), private) {
				t.Fatalf("retired configuration was accepted or exposed: %v", err)
			}
		})
	}
}
