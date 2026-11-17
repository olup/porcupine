package importer

const MaxChunkBytes = 256 << 10
const MaxRowBytes = 1 << 20
const MaxChunkRows = 1000
const MaxFiles = 4096
const MaxActiveJobs = 4
const MaxSamples = 20

// Import progress is metadata only. The one incomplete row and hash state are
// bounded private resumability state, removed on completion or cancellation.
type ImportCounts struct {
	Rows       int64 `json:"rows"`
	Accepted   int64 `json:"accepted"`
	Duplicates int64 `json:"duplicates"`
	Ignored    int64 `json:"ignored"`
	Unresolved int64 `json:"unresolved"`
	Invalid    int64 `json:"invalid"`
}

type ImportUploadLimits struct {
	MaxChunkBytes         int `json:"max_chunk_bytes"`
	MaxRowsPerChunk       int `json:"max_rows_per_chunk"`
	MaxRowBytes           int `json:"max_row_bytes"`
	MaxFiles              int `json:"max_files"`
	MaxActiveJobs         int `json:"max_active_jobs"`
	MaxSamples            int `json:"max_samples"`
	MaxConcurrentRequests int `json:"max_concurrent_requests"`
}

type ImportRowSample struct {
	FileID    string `json:"file_id"`
	RowNumber int64  `json:"row_number"`
	Reason    string `json:"reason"`
}

type ImportUploadFileSpec struct {
	Path      string `json:"path" maxLength:"1024"`
	Kind      string `json:"kind" enum:"events,persons"`
	SizeBytes int64  `json:"size_bytes" minimum:"0"`
	SHA256    string `json:"sha256" minLength:"64" maxLength:"64"`
}

type ImportUploadFile struct {
	ID          string       `json:"id"`
	Path        string       `json:"path"`
	Kind        string       `json:"kind"`
	SizeBytes   int64        `json:"size_bytes"`
	OffsetBytes int64        `json:"offset_bytes"`
	SHA256      string       `json:"sha256"`
	State       string       `json:"state"`
	Counts      ImportCounts `json:"counts"`
	Error       string       `json:"error,omitempty"`
}

type ImportDelivery struct {
	Enabled     bool  `json:"enabled"`
	Pending     int64 `json:"pending" doc:"Undelivered project events up to the import acceptance cursor, including earlier work and quarantine. Later live events do not delay this count."`
	Quarantined int64 `json:"quarantined"`
	Ready       bool  `json:"ready"`
}

type ImportUploadJob struct {
	Format      string             `json:"format"`
	Delivery    *ImportDelivery    `json:"delivery,omitempty"`
	ID          string             `json:"id"`
	ProjectID   string             `json:"project_id"`
	SourceID    string             `json:"source_id"`
	State       string             `json:"state"`
	Counts      ImportCounts       `json:"counts"`
	Files       []ImportUploadFile `json:"files,omitempty"`
	Samples     []ImportRowSample  `json:"samples,omitempty"`
	Limits      ImportUploadLimits `json:"limits"`
	Error       string             `json:"error,omitempty"`
	CreatedAt   string             `json:"created_at"`
	UpdatedAt   string             `json:"updated_at"`
	CompletedAt string             `json:"completed_at,omitempty"`
}

func Limits() ImportUploadLimits {
	return ImportUploadLimits{MaxChunkBytes, MaxChunkRows, MaxRowBytes, MaxFiles, MaxActiveJobs, MaxSamples, 2}
}
