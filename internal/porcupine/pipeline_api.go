package porcupine

import (
	"context"

	"github.com/danielgtaylor/huma/v2"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"

	"github.com/olup/porcupine/internal/output"
)

type PipelineCreateBody struct {
	Name        string                      `json:"name" minLength:"1" maxLength:"200"`
	Kind        string                      `json:"kind" enum:"webhook,s3"`
	Webhook     *output.WebhookPipelineSpec `json:"webhook,omitempty"`
	S3          *output.S3PipelineSpec      `json:"s3,omitempty"`
	Credentials output.PipelineCredentials  `json:"credentials" writeOnly:"true" doc:"Write-only destination credentials. Never returned."`
}
type PipelineCreateInput struct {
	ID   string `path:"id"`
	Body PipelineCreateBody
}
type PipelineGetInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
}
type PipelineOutput struct{ Body output.Pipeline }
type PipelineListInput struct {
	ID     string `path:"id"`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0"`
}
type PipelineList struct {
	Enabled    bool              `json:"enabled"`
	Pipelines  []output.Pipeline `json:"pipelines"`
	Limit      int               `json:"limit"`
	Offset     int               `json:"offset"`
	NextOffset *int              `json:"next_offset,omitempty"`
}
type PipelineListOutput struct{ Body PipelineList }
type PipelineRotateInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
	Body       struct {
		Webhook     *output.WebhookPipelineSpec `json:"webhook,omitempty"`
		S3          *output.S3PipelineSpec      `json:"s3,omitempty"`
		Credentials output.PipelineCredentials  `json:"credentials" writeOnly:"true"`
	}
}
type PipelineRunsInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
	Limit      int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset     int    `query:"offset" default:"0" minimum:"0"`
}
type PipelineRunInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
	RunID      string `path:"run_id"`
}
type PipelineRuns struct {
	Runs       []output.PipelineRun `json:"runs"`
	Limit      int                  `json:"limit"`
	Offset     int                  `json:"offset"`
	NextOffset *int                 `json:"next_offset,omitempty"`
}
type PipelineRunsOutput struct{ Body PipelineRuns }
type PipelineRunOutput struct{ Body output.PipelineRun }
type PipelineRetry struct {
	Requeued bool `json:"requeued"`
}
type PipelineRetryOutput struct{ Body PipelineRetry }

func (s *Service) registerPipelines(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	read := auth.OperationAccess{Permission: "pipelines:read", AdditionalPermissions: []string{"events:read"}, ProjectParameter: "id"}
	write := auth.OperationAccess{Permission: "pipelines:write", AdditionalPermissions: []string{"events:read"}, ProjectParameter: "id"}
	base := "/api/v1/projects/{id}/pipelines"
	huma.Register(api, op("create-pipeline", "POST", base, "Create a project egress pipeline; credentials are encrypted and never returned", write, 201), s.createPipeline)
	huma.Register(api, op("list-pipelines", "GET", base, "List project pipeline metadata without destination URLs or credentials", read, 200), s.listPipelines)
	huma.Register(api, op("get-pipeline", "GET", base+"/{pipeline_id}", "Read project pipeline metadata", read, 200), s.getPipeline)
	huma.Register(api, op("pause-pipeline", "POST", base+"/{pipeline_id}/pause", "Pause new webhook enqueueing and hold pending work; no retrospective backfill", write, 200), s.pausePipeline)
	huma.Register(api, op("resume-pipeline", "POST", base+"/{pipeline_id}/resume", "Resume future and pending work, subject to creator authority", write, 200), s.resumePipeline)
	huma.Register(api, op("delete-pipeline", "DELETE", base+"/{pipeline_id}", "Delete a pipeline and its retained delivery metadata", write, 204), s.deletePipeline)
	huma.Register(api, op("replace-pipeline-credentials", "PUT", base+"/{pipeline_id}/credentials", "Replace encrypted credentials and destination; pending deliveries to the old destination are retired", write, 200), s.rotatePipeline)
	huma.Register(api, op("list-pipeline-runs", "GET", base+"/{pipeline_id}/runs", "List bounded delivery status metadata without event payloads or destination secrets", read, 200), s.listPipelineRuns)
	huma.Register(api, op("get-pipeline-run", "GET", base+"/{pipeline_id}/runs/{run_id}", "Read one retained delivery status", read, 200), s.getPipelineRun)
	huma.Register(api, op("retry-pipeline-run", "POST", base+"/{pipeline_id}/runs/{run_id}/retry", "Retry a failed run; delivery remains subject to active pipeline and creator grants", write, 202), s.retryPipelineRun)
}

func (s *Service) createPipeline(ctx context.Context, input *PipelineCreateInput) (*PipelineOutput, error) {
	if s.externalAuthEnabled() && (!principal(ctx).ServiceKey || principal(ctx).External != nil) {
		return nil, apierr.Detailed(403, "execution_credential_required", "permanent pipelines require a native machine key in external authentication mode", "Authenticate this request with a team machine key that has pipelines:write and events:read on the project.", "")
	}
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	result, err := s.pipelines.Define(ctx, input.ID, output.PipelineRequest(input.Body), principal(ctx))
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: *result}, nil
}

func (s *Service) listPipelines(ctx context.Context, input *PipelineListInput) (*PipelineListOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	result, err := s.pipelines.List(ctx, input.ID, input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}
	return &PipelineListOutput{Body: PipelineList(*result)}, nil
}

func (s *Service) getPipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	result, err := s.pipelines.Get(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: *result}, nil
}

func (s *Service) rotatePipeline(ctx context.Context, input *PipelineRotateInput) (*PipelineOutput, error) {
	result, err := s.pipelines.Replace(ctx, input.ID, input.PipelineID, output.PipelineChange{Webhook: input.Body.Webhook, S3: input.Body.S3, Credentials: input.Body.Credentials})
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: *result}, nil
}

func (s *Service) listPipelineRuns(ctx context.Context, input *PipelineRunsInput) (*PipelineRunsOutput, error) {
	result, err := s.pipelines.Runs(ctx, input.ID, input.PipelineID, input.Limit, input.Offset)
	if err != nil {
		return nil, err
	}
	return &PipelineRunsOutput{Body: PipelineRuns(*result)}, nil
}

func (s *Service) getPipelineRun(ctx context.Context, input *PipelineRunInput) (*PipelineRunOutput, error) {
	result, err := s.pipelines.Run(ctx, input.ID, input.PipelineID, input.RunID)
	if err != nil {
		return nil, err
	}
	return &PipelineRunOutput{Body: *result}, nil
}

func (s *Service) retryPipelineRun(ctx context.Context, input *PipelineRunInput) (*PipelineRetryOutput, error) {
	result, err := s.pipelines.Retry(ctx, input.ID, input.PipelineID, input.RunID)
	if err != nil {
		return nil, err
	}
	return &PipelineRetryOutput{Body: PipelineRetry{Requeued: result}}, nil
}

func (s *Service) pausePipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	r, err := s.pipelines.SetState(ctx, input.ID, input.PipelineID, "paused")
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: *r}, nil
}
func (s *Service) resumePipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	r, err := s.pipelines.SetState(ctx, input.ID, input.PipelineID, "active")
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: *r}, nil
}
func (s *Service) deletePipeline(ctx context.Context, input *PipelineGetInput) (*struct{}, error) {
	return nil, s.pipelines.Delete(ctx, input.ID, input.PipelineID)
}
