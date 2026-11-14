package porcupine

import (
	"context"
	"errors"
	"time"
)

const defaultDuckLakeMemoryMB = 512

// DuckLakeConfig selects the local analytical store. The native
// engine is available only in builds made with the ducklake build tag.
type DuckLakeConfig struct {
	Directory           string
	MemoryLimitMB       int
	Threads             int
	MaxOpenProjects     int
	MaintenanceInterval time.Duration
	SnapshotRetention   time.Duration
	CleanupGrace        time.Duration
	CleanupInterval     time.Duration
	DisableCleanup      bool
}

type lakeStore interface {
	SourceID(context.Context) (string, error)
	Apply(context.Context, []eventProjection) error
	Load(context.Context, string, string) (eventProjection, error)
	Query(context.Context, string, string, map[string]string) (AnalyticsResult, error)
	ValidateExport(context.Context, string, string, map[string]string, bool) (string, error)
	Export(context.Context, string, string, map[string]string, *analyticalWindow, int64, exportStreamCallbacks) (exportStreamResult, error)
	Catalog(context.Context, EventCatalogInput) (EventCatalog, error)
	DeleteProject(context.Context, string) error
	Maintain(context.Context) error
	CleanupStatus(context.Context) (DuckLakeCleanupStatus, error)
	stageCheckpoint(context.Context, func(context.Context, string) error) (string, error)
	Close() error
}

type analyticalWindow struct {
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

func normalizeDuckLakeCleanup(c *DuckLakeConfig) error {
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
