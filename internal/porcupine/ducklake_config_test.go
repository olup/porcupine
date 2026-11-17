package porcupine

import (
	"testing"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeUnsupportedConfigurationFailsBeforeOpeningStorage(t *testing.T) {
	t.Parallel()
	for _, c := range []Config{
		{DuckLake: &lakeapi.DuckLakeConfig{}, Exports: &output.ExportConfig{}},
		{DuckLake: &lakeapi.DuckLakeConfig{MaintenanceInterval: -time.Second}},
		{DuckLake: &lakeapi.DuckLakeConfig{SnapshotRetention: -time.Second}},
		{DuckLake: &lakeapi.DuckLakeConfig{CleanupGrace: time.Millisecond}},
		{DuckLake: &lakeapi.DuckLakeConfig{CleanupInterval: -time.Second}},
	} {
		s, err := Open(c)
		if s != nil {
			s.Close()
		}
		if s != nil || err == nil {
			t.Fatal("invalid native configuration accepted")
		}
	}
}
