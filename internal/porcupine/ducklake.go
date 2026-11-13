package porcupine

import (
	"context"
	"time"
)

// DuckLakeConfig selects the local experimental analytical store. The native
// engine is available only in builds made with the ducklake build tag.
type DuckLakeConfig struct {
	Directory           string
	MemoryLimitMB       int
	Threads             int
	MaxOpenProjects     int
	MaintenanceInterval time.Duration
}

type lakeStore interface {
	SourceID(context.Context) (string, error)
	Apply(context.Context, []eventProjection) error
	Load(context.Context, string, string) (eventProjection, error)
	Query(context.Context, string, string, map[string]string) (AnalyticsResult, error)
	ValidateExport(context.Context, string, string, map[string]string, bool) (string, error)
	Export(context.Context, string, string, map[string]string, *analyticalWindow, int64, exportStreamCallbacks) (exportStreamResult, error)
	Catalog(context.Context, string, string, int, int) (EventCatalog, error)
	DeleteProject(context.Context, string) error
	Maintain(context.Context) error
	Close() error
}

type analyticalWindow struct {
	StartMS int64
	EndMS   int64
}
