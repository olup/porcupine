package porcupine

import (
	"testing"
	"time"
)

func TestDuckLakeUnsupportedConfigurationFailsBeforeOpeningStorage(t *testing.T) {
	for _, c := range []Config{
		{DuckLake: &DuckLakeConfig{}, ClickHouse: &ClickHouseConfig{}},
		{DuckLake: &DuckLakeConfig{}, HotReceiptLimit: 1},
		{DuckLake: &DuckLakeConfig{}, Exports: &ExportConfig{}},
		{DuckLake: &DuckLakeConfig{MaintenanceInterval: -time.Second}},
		{DuckLake: &DuckLakeConfig{SnapshotRetention: -time.Second}},
		{DuckLake: &DuckLakeConfig{CleanupGrace: time.Millisecond}},
		{DuckLake: &DuckLakeConfig{CleanupInterval: -time.Second}},
	} {
		s, err := Open(c)
		if s != nil {
			s.Close()
		}
		if s != nil || err == nil {
			t.Fatal("unsupported experimental configuration accepted")
		}
	}
}
