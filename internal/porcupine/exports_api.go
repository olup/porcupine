package porcupine

import (
	"context"
	"strings"

	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/output"
)

type ExportCreateBody struct {
	IdempotencyKey string            `json:"idempotency_key" minLength:"1" maxLength:"200"`
	SQL            string            `json:"sql" minLength:"1" maxLength:"16384" doc:"One validated SELECT against the project events table, with the same SQL grammar as query-events. The delivery barrier does not create a point-in-time snapshot."`
	Parameters     map[string]string `json:"parameters,omitempty"`
	Format         string            `json:"format,omitempty" enum:"jsonl,csv" default:"jsonl"`
}

type ExportCreateInput struct {
	ID   string `path:"id"`
	Body ExportCreateBody
}

type ExportGetInput struct {
	ID       string `path:"id"`
	ExportID string `path:"export_id"`
}

type ExportJobOutput struct{ Body output.ExportJob }

type ExportListInput struct {
	ID     string `path:"id"`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0"`
}

type ExportLimits struct {
	MaxConcurrentJobs       int    `json:"max_concurrent_jobs"`
	MaxQueuedJobs           int    `json:"max_queued_jobs"`
	MaxExecutionSeconds     int    `json:"max_execution_seconds"`
	MaxOutputBytes          int64  `json:"max_output_bytes"`
	FileBytes               int64  `json:"file_bytes"`
	MaxRowBytes             int64  `json:"max_row_bytes"`
	MemoryScope             string `json:"memory_scope" doc:"per_resident_project_engine: shared engine budget, not a per-query quota or process cap."`
	MemoryBytes             int64  `json:"memory_bytes" doc:"DuckDB shared per-resident-project engine budget. Not an export-specific or hard process RSS limit."`
	BandwidthBytesPerSecond int64  `json:"bandwidth_bytes_per_second"`
	QueueThreshold          int64  `json:"queue_threshold"`
	RetentionHours          int    `json:"retention_hours"`
}

type ExportList struct {
	Enabled    bool               `json:"enabled"`
	Limits     *ExportLimits      `json:"limits,omitempty"`
	Jobs       []output.ExportJob `json:"jobs"`
	Limit      int                `json:"limit"`
	Offset     int                `json:"offset"`
	NextOffset *int               `json:"next_offset,omitempty"`
}

type ExportListOutput struct{ Body ExportList }

type ExportFilesInput struct {
	ID       string `path:"id"`
	ExportID string `path:"export_id"`
	Limit    int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset   int    `query:"offset" default:"0" minimum:"0"`
}

type ExportFiles struct {
	Files      []output.ExportFile `json:"files"`
	Limit      int                 `json:"limit"`
	Offset     int                 `json:"offset"`
	NextOffset *int                `json:"next_offset,omitempty"`
}

type ExportFilesOutput struct{ Body ExportFiles }

func (s *Service) registerExports(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	access := auth.OperationAccess{Permission: "events:read", ProjectParameter: "id"}
	base := "/api/v1/projects/{id}/exports"
	huma.Register(api, op("create-event-export", "POST", base, "Queue a private streamed SQL export with idempotency and a delivery barrier", access, 202), s.createEventExport)
	huma.Register(api, op("list-event-exports", "GET", base, "List project export jobs and operator resource limits", access, 200), s.listEventExports)
	huma.Register(api, op("get-event-export", "GET", base+"/{export_id}", "Read export status without SQL, storage keys or credentials", access, 200), s.getEventExport)
	huma.Register(api, op("cancel-event-export", "DELETE", base+"/{export_id}", "Request cancellation; restricted to the creator or an instance administrator", access, 202), s.cancelEventExport)
	huma.Register(api, op("list-event-export-files", "GET", base+"/{export_id}/files", "Get a bounded page of fresh five-minute download URLs after full export success", access, 200), s.eventExportFiles)
}

func (s *Service) createEventExport(ctx context.Context, input *ExportCreateInput) (*ExportJobOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if !s.exports.Enabled() {
		return nil, apiError(503, "exports are not configured")
	}
	if strings.TrimSpace(input.Body.IdempotencyKey) == "" || len(input.Body.IdempotencyKey) > 200 {
		return nil, apiError(400, "idempotency_key must contain 1 to 200 characters")
	}
	if err := validateQueryParameters(input.Body.Parameters); err != nil {
		return nil, err
	}
	canonical, err := s.lake.ValidateExport(ctx, input.ID, input.Body.SQL, input.Body.Parameters, false)
	if err != nil {
		return nil, err
	}
	job, err := s.exports.Submit(ctx, input.ID, output.ExportRequest{IdempotencyKey: input.Body.IdempotencyKey, SQL: canonical, Parameters: input.Body.Parameters, Format: input.Body.Format}, principal(ctx))
	if err != nil {
		return nil, err
	}
	return &ExportJobOutput{Body: job}, nil
}
func (s *Service) listEventExports(ctx context.Context, input *ExportListInput) (*ExportListOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	page, err := s.exports.List(ctx, input.ID, input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}
	result := ExportList{Enabled: s.exports.Enabled(), Jobs: page.Jobs, Limit: input.Limit, Offset: input.Offset, NextOffset: page.NextOffset}
	if s.exports.Enabled() {
		c := s.exports.Config()
		result.Limits = &ExportLimits{MaxConcurrentJobs: 1, MaxQueuedJobs: min(c.MaxQueuedJobs, 100), MaxExecutionSeconds: c.MaxExecutionSeconds, MaxOutputBytes: c.MaxOutputBytes, FileBytes: c.FileBytes, MaxRowBytes: c.MaxRowBytes, MemoryScope: "per_resident_project_engine", MemoryBytes: int64(s.lakeMemoryMB) << 20, BandwidthBytesPerSecond: c.BandwidthBytesPerSecond, QueueThreshold: c.QueueThreshold, RetentionHours: c.RetentionHours}
	}
	return &ExportListOutput{Body: result}, nil
}
func (s *Service) getEventExport(ctx context.Context, input *ExportGetInput) (*ExportJobOutput, error) {
	job, err := s.exports.Get(ctx, input.ID, input.ExportID)
	if err != nil {
		return nil, err
	}
	return &ExportJobOutput{Body: job}, nil
}
func (s *Service) cancelEventExport(ctx context.Context, input *ExportGetInput) (*ExportJobOutput, error) {
	job, err := s.exports.Cancel(ctx, input.ID, input.ExportID, principal(ctx))
	if err != nil {
		return nil, err
	}
	return &ExportJobOutput{Body: job}, nil
}
func (s *Service) eventExportFiles(ctx context.Context, input *ExportFilesInput) (*ExportFilesOutput, error) {
	page, err := s.exports.Files(ctx, input.ID, input.ExportID, input.Limit, input.Offset, principal(ctx))
	if err != nil {
		return nil, err
	}
	return &ExportFilesOutput{Body: ExportFiles{Files: page.Files, Limit: page.Limit, Offset: page.Offset, NextOffset: page.NextOffset}}, nil
}
