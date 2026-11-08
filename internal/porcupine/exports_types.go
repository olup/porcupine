package porcupine

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

// ExportConfig is deployment-only; no HTTP or MCP operation can raise its budgets.
type ExportConfig struct {
	S3                      exportstore.Config
	MaxExecutionSeconds     int
	MaxReadBytes            int64
	MaxOutputBytes          int64
	FileBytes               int64
	MaxRowBytes             int64
	MemoryBytes             int64
	BandwidthBytesPerSecond int64
	TemporaryBytes          int64
	QueueThreshold          int64
	MaxQueuedJobs           int
	RetentionHours          int
}

type ExportState string

const (
	ExportQueued    ExportState = "queued"
	ExportRunning   ExportState = "running"
	ExportSucceeded ExportState = "succeeded"
	ExportFailed    ExportState = "failed"
	ExportCanceled  ExportState = "canceled"
	ExportExpired   ExportState = "expired"
)

type ExportJob struct {
	ID                     string                  `json:"id"`
	ProjectID              string                  `json:"project_id"`
	State                  ExportState             `json:"state"`
	Format                 string                  `json:"format"`
	Rows                   int64                   `json:"rows"`
	Bytes                  int64                   `json:"bytes"`
	DecodedBytes           int64                   `json:"decoded_bytes"`
	FileCount              int                     `json:"file_count"`
	Attempts               int                     `json:"attempts"`
	CreatedAt              string                  `json:"created_at"`
	StartedAt              string                  `json:"started_at,omitempty"`
	CompletedAt            string                  `json:"completed_at,omitempty"`
	ExpiresAt              string                  `json:"expires_at,omitempty"`
	Error                  string                  `json:"error,omitempty"`
	Columns                []AnalyticsResultColumn `json:"columns,omitempty"`
	Consistency            string                  `json:"consistency"`
	Totals                 json.RawMessage         `json:"totals,omitempty"`
	Extremes               json.RawMessage         `json:"extremes,omitempty"`
	RowsBeforeLimitAtLeast *uint64                 `json:"rows_before_limit_at_least,omitempty"`
}

type storedExport struct {
	ExportJob
	ActorID           string
	SQL               string
	Parameters        map[string]string
	AttemptID         string
	QueryID           string
	EventWatermark    int64
	MutationWatermark int64
	CancelRequested   bool
	ExpiresAtMS       int64
	CleanupPending    bool
}

type exportArtifact struct {
	Index        int
	Key          string
	UploadID     string
	Complete     bool
	Rows         int64
	Bytes        int64
	DecodedBytes int64
	SHA256       string
}

type ExportFile struct {
	Index        int    `json:"index"`
	Name         string `json:"name"`
	Rows         int64  `json:"rows"`
	Bytes        int64  `json:"bytes"`
	DecodedBytes int64  `json:"decoded_bytes"`
	SHA256       string `json:"sha256"`
	URL          string `json:"url"`
	ExpiresAt    string `json:"expires_at"`
}

type exportRuntime struct {
	config       ExportConfig
	store        *exportstore.Store
	cancel       context.CancelFunc
	done         chan struct{}
	wake         chan struct{}
	activeMu     sync.Mutex
	activeJob    string
	activeCancel context.CancelFunc
}

type exportStreamResult struct {
	Columns                []AnalyticsResultColumn
	Rows                   int64
	Statistics             AnalyticsStatistics
	Totals                 json.RawMessage
	Extremes               json.RawMessage
	RowsBeforeLimitAtLeast *uint64
}

type exportStreamCallbacks struct {
	Columns func([]AnalyticsResultColumn) error
	Row     func([]json.RawMessage) error
}

const exportLinkTTL = 5 * time.Minute
