package lake

import (
	"testing"
	"time"
)

func TestDuckLakeRestorePolicy(t *testing.T) {
	t.Parallel()
	valid := DuckLakeConfig{
		PostgresURL:          "postgres://localhost/catalogs",
		S3:                   &DuckLakeS3Config{},
		CleanupGrace:         30 * 24 * time.Hour,
		RestoreWindow:        21 * 24 * time.Hour,
		RestoreMaxReaderTime: 7 * 24 * time.Hour,
		RestoreSafetyMargin:  2 * 24 * time.Hour,
	}
	for _, tc := range []struct {
		name      string
		change    func(*DuckLakeConfig)
		wantError bool
	}{
		{"exact grace", func(*DuckLakeConfig) {}, false},
		{"short grace", func(c *DuckLakeConfig) { c.CleanupGrace -= time.Second }, true},
		{"missing reader bound", func(c *DuckLakeConfig) { c.RestoreMaxReaderTime = 0 }, true},
		{"negative margin", func(c *DuckLakeConfig) { c.RestoreSafetyMargin = -time.Second }, true},
		{"local catalog", func(c *DuckLakeConfig) { c.PostgresURL = "" }, true},
		{"no S3", func(c *DuckLakeConfig) { c.S3 = nil }, true},
		{"overflowing sum", func(c *DuckLakeConfig) { c.RestoreWindow = time.Duration(1<<63 - 1) }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := valid
			tc.change(&config)
			if err := NormalizeDuckLakeCleanup(&config); (err != nil) != tc.wantError {
				t.Fatalf("want error %t, got %v", tc.wantError, err)
			}
		})
	}
}
