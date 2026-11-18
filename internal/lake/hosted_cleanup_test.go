package lake

import (
	"errors"
	"testing"
	"time"
)

func TestHostedCleanupHorizonGuard(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)
	policy := DuckLakeConfig{SnapshotRetention: 7 * 24 * time.Hour, CleanupGrace: 240 * time.Hour,
		RestoreWindow: 168 * time.Hour, RestoreMaxReaderTime: time.Hour, RestoreSafetyMargin: 24 * time.Hour}
	recent, old := now.Add(-time.Hour), now.Add(-30*24*time.Hour)
	for _, test := range []struct {
		name   string
		change func(*DuckLakeConfig)
		floor  time.Time
		file   time.Time
	}{
		{name: "grace limits deletion", floor: recent, file: now.Add(-240 * time.Hour)},
		{name: "older floor limits deletion", floor: old, file: old},
		{name: "grace below window, readers and margin", change: func(c *DuckLakeConfig) { c.CleanupGrace = 192 * time.Hour }, floor: recent},
		{name: "missing restore window", change: func(c *DuckLakeConfig) { c.RestoreWindow = 0 }, floor: recent},
		{name: "missing reader time", change: func(c *DuckLakeConfig) { c.RestoreMaxReaderTime = 0 }, floor: recent},
		{name: "missing safety margin", change: func(c *DuckLakeConfig) { c.RestoreSafetyMargin = 0 }, floor: recent},
		{name: "cleanup disabled", change: func(c *DuckLakeConfig) { c.DisableCleanup = true }, floor: recent},
		{name: "unverified floor"},
		{name: "future floor", floor: now.Add(time.Second)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			config := policy
			if test.change != nil {
				test.change(&config)
			}
			snapshots, files, err := HostedCleanupHorizon(config, now, test.floor)
			if test.file.IsZero() {
				if !errors.Is(err, ErrHostedCleanupGuard) || !snapshots.IsZero() || !files.IsZero() {
					t.Fatalf("guard permitted cleanup: %v %v %v", snapshots, files, err)
				}
				return
			}
			if err != nil || !files.Equal(test.file) || !snapshots.Equal(now.Add(-config.SnapshotRetention)) {
				t.Fatalf("horizon snapshots=%v files=%v err=%v, want files %v", snapshots, files, err, test.file)
			}
		})
	}
}
