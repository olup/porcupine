package main

import "testing"

func TestAnalyticsConfiguration(t *testing.T) {
	for _, name := range []string{"PORCUPINE_DB", "PORCUPINE_CLICKHOUSE_URL", "PORCUPINE_DUCKLAKE_DIR", "PORCUPINE_DUCKLAKE_MEMORY_MB", "PORCUPINE_DUCKLAKE_THREADS", "PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", "PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "PORCUPINE_HOT_STORAGE_DAYS"} {
		t.Setenv(name, "")
	}
	t.Run("build default", func(t *testing.T) {
		t.Setenv("PORCUPINE_DB", "/srv/analytics/queue.db")
		ch, lake, err := analyticsConfiguration()
		expected := defaultAnalyticsDirectory("/srv/analytics/queue.db")
		if err != nil || ch != nil || expected != "" && (lake == nil || lake.Directory != expected) || expected == "" && lake != nil {
			t.Fatalf("unexpected default: ch=%v lake=%v err=%v", ch, lake, err)
		}
	})
	t.Run("explicit ClickHouse", func(t *testing.T) {
		t.Setenv("PORCUPINE_CLICKHOUSE_URL", "http://clickhouse:8123")
		ch, lake, err := analyticsConfiguration()
		if err != nil || ch == nil || lake != nil {
			t.Fatalf("explicit engine rejected: %v", err)
		}
	})
	t.Run("ambiguous engine", func(t *testing.T) {
		t.Setenv("PORCUPINE_CLICKHOUSE_URL", "http://clickhouse:8123")
		t.Setenv("PORCUPINE_DUCKLAKE_DIR", "/srv/analytics/lake")
		if _, _, err := analyticsConfiguration(); err == nil {
			t.Fatal("ambiguous storage configuration accepted")
		}
	})
	for _, field := range []struct{ name, value string }{
		{"PORCUPINE_DUCKLAKE_MEMORY_MB", "63"}, {"PORCUPINE_DUCKLAKE_MEMORY_MB", "32769"},
		{"PORCUPINE_DUCKLAKE_THREADS", "5"}, {"PORCUPINE_DUCKLAKE_MAX_OPEN_PROJECTS", "0"},
		{"PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "0s"}, {"PORCUPINE_DUCKLAKE_MAINTENANCE_INTERVAL", "25h"},
	} {
		t.Run(field.name+field.value, func(t *testing.T) {
			t.Setenv("PORCUPINE_DUCKLAKE_DIR", t.TempDir())
			t.Setenv(field.name, field.value)
			if _, _, err := analyticsConfiguration(); err == nil {
				t.Fatal("invalid native budget accepted")
			}
		})
	}
}
