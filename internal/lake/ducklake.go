package lake

import (
	"errors"
	"time"
)

const DefaultDuckLakeMemoryMB = 512

// DuckLakeConfig selects native resource limits, a local working directory and
// optional shared PostgreSQL metadata with S3 analytical data.
type DuckLakeConfig struct {
	Directory            string
	MemoryLimitMB        int
	Threads              int
	MaxOpenProjects      int
	MaintenanceInterval  time.Duration
	SnapshotRetention    time.Duration
	CleanupGrace         time.Duration
	CleanupInterval      time.Duration
	RestoreWindow        time.Duration
	RestoreMaxReaderTime time.Duration
	RestoreSafetyMargin  time.Duration
	DisableCleanup       bool
	// Hosted selects the fenced history cleanup in CleanupHostedDue. The
	// unfenced CleanupDue path never runs for a hosted store.
	Hosted                 bool
	S3                     *DuckLakeS3Config
	PostgresURL            string
	SharedSourceID         string
	MetadataTimeoutSeconds int
}

type AnalyticalWindow struct {
	StartMS int64
	EndMS   int64
}

// DuckLakeCleanupStatus describes scheduled history/file reclamation, not event retention.
type DuckLakeCleanupStatus struct {
	Enabled                  bool   `json:"enabled"`
	SnapshotRetentionSeconds int64  `json:"snapshot_retention_seconds"`
	ObsoleteFileGraceSeconds int64  `json:"obsolete_file_grace_seconds"`
	IntervalSeconds          int64  `json:"interval_seconds"`
	RegisteredCatalogs       int    `json:"registered_catalogs"`
	CompletedCatalogs        int    `json:"completed_catalogs" doc:"Catalogs with at least one completed cleanup cycle; not proof that all obsolete files have been removed."`
	FailedCatalogs           int    `json:"failed_catalogs"`
	RunningCatalogs          int    `json:"running_catalogs"`
	LastAttempt              string `json:"last_attempt,omitempty"`
	LastSuccess              string `json:"last_success,omitempty"`
	SnapshotsExpired         int64  `json:"snapshots_expired,omitempty" doc:"Hosted profile: snapshots expired by completed passes in registered catalogs."`
	FilesDeleted             int64  `json:"files_deleted,omitempty" doc:"Hosted profile: obsolete DuckLake files deleted by completed passes in registered catalogs."`
	LastError                string `json:"last_error,omitempty" doc:"Hosted profile: stable code (restore_guard, failed or interrupted) of the most recent unsuccessful pass that no later pass of the same catalog has cleared. restore_guard means the restore policy or recovery floor did not permit deletion."`
}

func NormalizeDuckLakeCleanup(c *DuckLakeConfig) error {
	if c.SnapshotRetention == 0 {
		c.SnapshotRetention = 7 * 24 * time.Hour
	}
	if c.CleanupGrace == 0 {
		c.CleanupGrace = 48 * time.Hour
	}
	if c.CleanupInterval == 0 {
		c.CleanupInterval = time.Hour
	}
	if c.SnapshotRetention < time.Second || c.CleanupGrace < time.Second || c.CleanupInterval < time.Second {
		return errors.New("DuckLake cleanup durations must be at least one second")
	}
	// An explicit policy checks the local deletion grace. Provider PITR and
	// object lifecycle settings are independent and require a provider drill.
	if c.RestoreWindow != 0 || c.RestoreMaxReaderTime != 0 || c.RestoreSafetyMargin != 0 {
		if c.PostgresURL == "" || c.S3 == nil {
			return errors.New("DuckLake restore policy requires PostgreSQL metadata and S3")
		}
		if c.RestoreWindow < time.Second || c.RestoreMaxReaderTime < time.Second || c.RestoreSafetyMargin < time.Second {
			return errors.New("DuckLake restore window, maximum reader time and safety margin must each be at least one second")
		}
		if !restoreGraceCovered(*c) {
			return errors.New("DuckLake cleanup grace must cover the restore window, maximum reader time and safety margin")
		}
	}
	return nil
}

// restoreGraceCovered reports cleanup_grace >= restore_window +
// restore_max_reader_time + restore_safety_margin with each term set.
func restoreGraceCovered(c DuckLakeConfig) bool {
	if c.CleanupGrace < c.RestoreWindow || c.RestoreWindow < time.Second || c.RestoreMaxReaderTime < time.Second || c.RestoreSafetyMargin < time.Second {
		return false
	}
	// Subtract from the grace so large durations cannot wrap the sum.
	remaining := c.CleanupGrace - c.RestoreWindow
	return remaining >= c.RestoreMaxReaderTime && remaining-c.RestoreMaxReaderTime >= c.RestoreSafetyMargin
}
