package output

import (
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

// ExportConfig is deployment-only; no HTTP or MCP operation can raise its budgets.
type ExportConfig struct {
	S3                      exportstore.Config
	MaxExecutionSeconds     int
	MaxOutputBytes          int64
	FileBytes               int64
	MaxRowBytes             int64
	BandwidthBytesPerSecond int64
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
	ID           string                          `json:"id"`
	ProjectID    string                          `json:"project_id"`
	State        ExportState                     `json:"state"`
	Format       string                          `json:"format"`
	Rows         int64                           `json:"rows"`
	Bytes        int64                           `json:"bytes"`
	DecodedBytes int64                           `json:"decoded_bytes"`
	FileCount    int                             `json:"file_count"`
	Attempts     int                             `json:"attempts"`
	CreatedAt    string                          `json:"created_at"`
	StartedAt    string                          `json:"started_at,omitempty"`
	CompletedAt  string                          `json:"completed_at,omitempty"`
	ExpiresAt    string                          `json:"expires_at,omitempty"`
	Error        string                          `json:"error,omitempty"`
	Columns      []lakeapi.AnalyticsResultColumn `json:"columns,omitempty"`
	Consistency  string                          `json:"consistency"`
}

type storedExport struct {
	ExportJob
	ActorID             string
	SQL                 string
	Parameters          map[string]string
	AttemptID           string
	EventWatermark      int64
	MutationWatermark   int64
	Snapshot            lakeapi.ExportSnapshot
	CancelRequested     bool
	ExpiresAtMS         int64
	CleanupPending      bool
	ExternalTenantID    string
	ExternalIssuer      string
	ExternalExpiresAtMS int64
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

const exportLinkTTL = 5 * time.Minute
