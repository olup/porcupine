package lake

import (
	"context"
	"errors"
	"time"
)

const DefaultDuckLakeMemoryMB = 512

// DuckLakeConfig selects native resource limits, a local working directory and
// optional shared PostgreSQL metadata with S3 analytical data.
type DuckLakeConfig struct {
	Directory              string
	MemoryLimitMB          int
	Threads                int
	MaxOpenProjects        int
	MaintenanceInterval    time.Duration
	SnapshotRetention      time.Duration
	CleanupGrace           time.Duration
	CleanupInterval        time.Duration
	DisableCleanup         bool
	S3                     *DuckLakeS3Config
	PostgresURL            string
	SharedSourceID         string
	MetadataTimeoutSeconds int
}

type LakeStore interface {
	SourceID(context.Context) (string, error)
	Apply(context.Context, []EventProjection) error
	Load(context.Context, string, string) (EventProjection, error)
	Query(context.Context, string, string, map[string]string, QueryExecutionLimits) (AnalyticsResult, error)
	ValidateExport(context.Context, string, string, map[string]string, bool) (string, error)
	Export(context.Context, string, string, map[string]string, *AnalyticalWindow, int64, ExportStreamCallbacks) (ExportStreamResult, error)
	Catalog(context.Context, EventCatalogInput) (EventCatalog, error)
	Profile(context.Context, string, EventProfileOptions, time.Time) (EventProfileRead, error)
	DeleteProject(context.Context, string) error
	Maintain(context.Context) error
	CleanupStatus(context.Context) (DuckLakeCleanupStatus, error)
	StageCheckpoint(context.Context, func(context.Context, string) error) (*NativeCheckpoint, error)
	Close() error
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
	return nil
}
