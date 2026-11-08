package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
)

type PipelineCreateBody struct {
	Name        string               `json:"name" minLength:"1" maxLength:"200"`
	Kind        string               `json:"kind" enum:"webhook,s3"`
	Webhook     *WebhookPipelineSpec `json:"webhook,omitempty"`
	S3          *S3PipelineSpec      `json:"s3,omitempty"`
	Credentials PipelineCredentials  `json:"credentials" writeOnly:"true" doc:"Write-only destination credentials. Never returned."`
}
type PipelineCreateInput struct {
	ID   string `path:"id"`
	Body PipelineCreateBody
}
type PipelineGetInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
}
type PipelineOutput struct{ Body Pipeline }
type PipelineListInput struct {
	ID     string `path:"id"`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0"`
}
type PipelineList struct {
	Enabled    bool       `json:"enabled"`
	Pipelines  []Pipeline `json:"pipelines"`
	Limit      int        `json:"limit"`
	Offset     int        `json:"offset"`
	NextOffset *int       `json:"next_offset,omitempty"`
}
type PipelineListOutput struct{ Body PipelineList }
type PipelineRotateInput struct {
	ID         string `path:"id"`
	PipelineID string `path:"pipeline_id"`
	Body       struct {
		Webhook     *WebhookPipelineSpec `json:"webhook,omitempty"`
		S3          *S3PipelineSpec      `json:"s3,omitempty"`
		Credentials PipelineCredentials  `json:"credentials" writeOnly:"true"`
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
	Runs       []PipelineRun `json:"runs"`
	Limit      int           `json:"limit"`
	Offset     int           `json:"offset"`
	NextOffset *int          `json:"next_offset,omitempty"`
}
type PipelineRunsOutput struct{ Body PipelineRuns }
type PipelineRunOutput struct{ Body PipelineRun }
type PipelineRetry struct {
	Requeued bool `json:"requeued"`
}
type PipelineRetryOutput struct{ Body PipelineRetry }

func (s *Service) initializePipelineSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS pipelines (
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), actor_id TEXT NOT NULL REFERENCES access_keys(id),
 kind TEXT NOT NULL CHECK(kind IN ('webhook','s3')), name TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('active','paused')), config TEXT NOT NULL, secret BLOB NOT NULL, created_at_ms INTEGER NOT NULL);
 CREATE INDEX IF NOT EXISTS pipelines_project ON pipelines(project_id,created_at_ms,id);`)
	return err
}

func (s *Service) registerPipelines(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	read := operationAccess{Permission: "pipelines:read", AdditionalPermissions: []string{"events:read"}, ProjectParameter: "id"}
	write := operationAccess{Permission: "pipelines:write", AdditionalPermissions: []string{"events:read"}, ProjectParameter: "id"}
	base := "/api/v1/projects/{id}/pipelines"
	huma.Register(api, op("create-pipeline", "POST", base, "Create a project egress pipeline; credentials are encrypted and never returned", write, 201), s.createPipeline)
	huma.Register(api, op("list-pipelines", "GET", base, "List project pipeline metadata without destination URLs or credentials", read, 200), s.listPipelines)
	huma.Register(api, op("get-pipeline", "GET", base+"/{pipeline_id}", "Read project pipeline metadata", read, 200), s.getPipeline)
	huma.Register(api, op("pause-pipeline", "POST", base+"/{pipeline_id}/pause", "Pause new webhook enqueueing and hold pending work; no retrospective backfill", write, 200), s.pausePipeline)
	huma.Register(api, op("resume-pipeline", "POST", base+"/{pipeline_id}/resume", "Resume future and pending work, subject to creator authority", write, 200), s.resumePipeline)
	huma.Register(api, op("delete-pipeline", "DELETE", base+"/{pipeline_id}", "Delete a pipeline and its retained delivery metadata", write, 204), s.deletePipeline)
	huma.Register(api, op("replace-pipeline-credentials", "PUT", base+"/{pipeline_id}/credentials", "Replace encrypted credentials and destination; pending deliveries use the new destination", write, 200), s.rotatePipeline)
	huma.Register(api, op("list-pipeline-runs", "GET", base+"/{pipeline_id}/runs", "List bounded delivery status metadata without event payloads or destination secrets", read, 200), s.listPipelineRuns)
	huma.Register(api, op("get-pipeline-run", "GET", base+"/{pipeline_id}/runs/{run_id}", "Read one retained delivery status", read, 200), s.getPipelineRun)
	huma.Register(api, op("retry-pipeline-run", "POST", base+"/{pipeline_id}/runs/{run_id}/retry", "Retry a failed run; delivery remains subject to active pipeline and creator grants", write, 202), s.retryPipelineRun)
}

const pipelineColumns = `id,project_id,actor_id,kind,name,state,config,secret,created_at_ms`

func scanPipeline(row interface{ Scan(...any) error }) (storedPipeline, error) {
	var p storedPipeline
	var config string
	err := row.Scan(&p.ID, &p.ProjectID, &p.ActorID, &p.Kind, &p.Name, &p.State, &config, &p.Secret, &p.CreatedAtMS)
	p.Config = json.RawMessage(config)
	return p, err
}
func (s *Service) loadPipeline(ctx context.Context, projectID, pipelineID string) (storedPipeline, error) {
	return scanPipeline(s.db.QueryRowContext(ctx, `SELECT `+pipelineColumns+` FROM pipelines WHERE project_id=? AND id=?`, projectID, pipelineID))
}
func pipelineLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "pipeline not found")
	}
	return apiError(503, "pipeline storage unavailable")
}

func (s *Service) encodePipelineDestination(projectID, pipelineID, kind string, webhook *WebhookPipelineSpec, s3 *S3PipelineSpec, credentials PipelineCredentials) ([]byte, []byte, error) {
	var config, secret []byte
	if kind == "webhook" {
		if webhook == nil || s3 != nil {
			return nil, nil, apiError(400, "webhook pipelines require only a webhook specification")
		}
		if credentials.AccessKeyID != "" || credentials.SecretAccessKey != "" || credentials.SessionToken != "" {
			return nil, nil, apiError(400, "unexpected S3 credentials")
		}
		if webhook.DeliveryMode == "" {
			webhook.DeliveryMode = "event"
		}
		if webhook.DeliveryMode != "event" && webhook.DeliveryMode != "batch" {
			return nil, nil, apiError(400, "invalid webhook delivery mode")
		}
		if webhook.DeliveryMode == "event" && (webhook.IntervalSeconds != 0 || webhook.StartAt != "") {
			return nil, nil, apiError(400, "event delivery has no time window")
		}
		if webhook.DeliveryMode == "batch" {
			if webhook.IntervalSeconds < 60 || webhook.IntervalSeconds > 86400 {
				return nil, nil, apiError(400, "invalid webhook batch interval")
			}
			if webhook.StartAt == "" {
				webhook.StartAt = time.Unix(s.clock().Unix()/webhook.IntervalSeconds*webhook.IntervalSeconds, 0).UTC().Format(time.RFC3339)
			}
			start, err := time.Parse(time.RFC3339, webhook.StartAt)
			if err != nil || start.Unix() < 0 || start.Nanosecond() != 0 || start.Unix()%webhook.IntervalSeconds != 0 {
				return nil, nil, apiError(400, "invalid webhook batch start")
			}
			webhook.StartAt = start.UTC().Format(time.RFC3339)
		}
		if validateWebhookEndpoint(webhook.Endpoint) != nil {
			return nil, nil, apiError(400, "invalid webhook endpoint")
		}
		for _, events := range [][]string{webhook.IncludeEvents, webhook.ExcludeEvents} {
			if len(events) > 100 {
				return nil, nil, apiError(400, "too many event filters")
			}
			for _, event := range events {
				if strings.TrimSpace(event) == "" || len(event) > 500 {
					return nil, nil, apiError(400, "invalid event filter")
				}
			}
		}
		headers := map[string]string{}
		for key, value := range credentials.Headers {
			headers[key] = value
		}
		if credentials.BearerToken != "" {
			for key := range headers {
				if strings.EqualFold(key, "Authorization") {
					return nil, nil, apiError(400, "duplicate authorization credential")
				}
			}
			headers["Authorization"] = "Bearer " + credentials.BearerToken
		}
		if validateWebhookHeaders(headers) != nil {
			return nil, nil, apiError(400, "invalid webhook credentials")
		}
		filters := *webhook
		filters.Endpoint = ""
		config, _ = json.Marshal(filters)
		secret, _ = json.Marshal(WebhookPipelineSecret{Endpoint: webhook.Endpoint, Headers: headers})
	} else if kind == "s3" {
		if s3 == nil || webhook != nil {
			return nil, nil, apiError(400, "S3 pipelines require only an S3 specification")
		}
		if len(credentials.Headers) > 0 || credentials.BearerToken != "" {
			return nil, nil, apiError(400, "unexpected webhook credentials")
		}
		normalized, err := validateS3PipelineSpec(*s3)
		if err != nil {
			return nil, nil, apiError(400, "invalid S3 pipeline specification")
		}
		if s.validatePipelineS3Endpoint(normalized.Destination.Endpoint) != nil {
			return nil, nil, apiError(400, "S3 endpoint is not allowed")
		}
		private := S3PipelineSecret{AccessKeyID: credentials.AccessKeyID, SecretAccessKey: credentials.SecretAccessKey, SessionToken: credentials.SessionToken}
		if validateS3PipelineSecret(private) != nil {
			return nil, nil, apiError(400, "invalid S3 credentials")
		}
		config, _ = json.Marshal(normalized)
		secret, _ = json.Marshal(private)
	} else {
		return nil, nil, apiError(400, "unsupported pipeline kind")
	}
	sealed, err := s.sealPipelineSecret(projectID, pipelineID, secret)
	if err != nil {
		return nil, nil, apiError(503, "pipeline credentials unavailable")
	}
	return config, sealed, nil
}

func (s *Service) createPipeline(ctx context.Context, input *PipelineCreateInput) (*PipelineOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if s.pipelines == nil {
		return nil, apiError(503, "pipelines are not configured")
	}
	if input.Body.Kind == "s3" && s.ch == nil {
		return nil, apiError(503, "S3 pipelines require ClickHouse")
	}
	if strings.TrimSpace(input.Body.Name) == "" || len(input.Body.Name) > 200 {
		return nil, apiError(400, "invalid pipeline name")
	}
	p := Pipeline{ID: newID(), ProjectID: input.ID, Kind: input.Body.Kind, Name: input.Body.Name, State: "active", CreatedAtMS: s.clock().UnixMilli()}
	config, secret, err := s.encodePipelineDestination(p.ProjectID, p.ID, p.Kind, input.Body.Webhook, input.Body.S3, input.Body.Credentials)
	if err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	var projectCount, totalCount int
	if tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE project_id=?) FROM pipelines`, p.ProjectID).Scan(&totalCount, &projectCount) != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	if projectCount >= 64 || totalCount >= 512 {
		return nil, apiError(409, "pipeline limit reached")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pipelines(`+pipelineColumns+`) VALUES(?,?,?,?,?,?,?,?,?)`, p.ID, p.ProjectID, principal(ctx).ID, p.Kind, p.Name, p.State, string(config), secret, p.CreatedAtMS)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	if p.Kind == "s3" {
		var spec S3PipelineSpec
		if json.Unmarshal(config, &spec) != nil {
			return nil, apiError(503, "pipeline configuration unavailable")
		}
		if err = s.createS3PipelineState(ctx, tx, p.ID, spec); err != nil {
			return nil, apiError(503, "pipeline scheduling unavailable")
		}
	}
	if tx.Commit() != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	s.pipelineWake()
	public, err := publicPipeline(storedPipeline{Pipeline: p, Config: config})
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: public}, nil
}

func (s *Service) listPipelines(ctx context.Context, input *PipelineListInput) (*PipelineListOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid pipeline pagination")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,project_id,kind,name,state,created_at_ms FROM pipelines WHERE project_id=? ORDER BY created_at_ms DESC,id LIMIT ? OFFSET ?`, input.ID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	defer rows.Close()
	result := PipelineList{Enabled: s.pipelines != nil, Pipelines: []Pipeline{}, Limit: input.Limit, Offset: input.Offset}
	for rows.Next() {
		var p Pipeline
		if rows.Scan(&p.ID, &p.ProjectID, &p.Kind, &p.Name, &p.State, &p.CreatedAtMS) != nil {
			return nil, apiError(503, "pipeline storage unavailable")
		}
		result.Pipelines = append(result.Pipelines, p)
	}
	if rows.Err() != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	if len(result.Pipelines) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		result.Pipelines = result.Pipelines[:input.Limit]
	}
	return &PipelineListOutput{Body: result}, nil
}
func (s *Service) getPipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: public}, nil
}
func (s *Service) setPipelineState(ctx context.Context, input *PipelineGetInput, state string) (*PipelineOutput, error) {
	if state == "active" && s.pipelines == nil {
		return nil, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE pipelines SET state=? WHERE project_id=? AND id=?`, state, input.ID, input.PipelineID)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, apiError(404, "pipeline not found")
	}
	p.State = state
	s.pipelineWake()
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: public}, nil
}
func (s *Service) pausePipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	return s.setPipelineState(ctx, input, "paused")
}
func (s *Service) resumePipeline(ctx context.Context, input *PipelineGetInput) (*PipelineOutput, error) {
	return s.setPipelineState(ctx, input, "active")
}
func (s *Service) deletePipeline(ctx context.Context, input *PipelineGetInput) (*struct{}, error) {
	p, lookupErr := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if lookupErr != nil {
		return nil, pipelineLookupError(lookupErr)
	}
	if p.Kind == "s3" {
		if err := s.deleteS3Pipeline(ctx, p.ProjectID, p.ID); err != nil {
			return nil, apiError(503, "pipeline cleanup unavailable")
		}
		s.pipelineWake()
		return nil, nil
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE project_id=? AND id=?`, input.ID, input.PipelineID)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, apiError(404, "pipeline not found")
	}
	s.pipelineWake()
	return nil, nil
}
func (s *Service) rotatePipeline(ctx context.Context, input *PipelineRotateInput) (*PipelineOutput, error) {
	if s.pipelines == nil {
		return nil, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	if p.Kind == "webhook" && input.Body.Webhook == nil && input.Body.S3 == nil {
		var spec WebhookPipelineSpec
		var private WebhookPipelineSecret
		plaintext, openErr := s.openPipelineSecret(p.ProjectID, p.ID, p.Secret)
		if openErr != nil || json.Unmarshal(plaintext, &private) != nil || json.Unmarshal(p.Config, &spec) != nil {
			return nil, apiError(503, "pipeline credentials unavailable")
		}
		spec.Endpoint = private.Endpoint
		input.Body.Webhook = &spec
	}
	if p.Kind == "s3" && input.Body.S3 == nil && input.Body.Webhook == nil {
		var spec S3PipelineSpec
		if json.Unmarshal(p.Config, &spec) != nil {
			return nil, apiError(503, "pipeline configuration unavailable")
		}
		input.Body.S3 = &spec
	}
	config, secret, err := s.encodePipelineDestination(p.ProjectID, p.ID, p.Kind, input.Body.Webhook, input.Body.S3, input.Body.Credentials)
	if err != nil {
		return nil, err
	}
	// Scheduling and filters are immutable here. Rotation can replace only the destination and its credentials.
	if p.Kind == "s3" {
		if string(config) != string(p.Config) {
			return nil, apiError(409, "S3 specification cannot change during credential rotation")
		}
	} else {
		var oldSpec, newSpec WebhookPipelineSpec
		if json.Unmarshal(p.Config, &oldSpec) != nil || json.Unmarshal(config, &newSpec) != nil {
			return nil, apiError(503, "pipeline configuration unavailable")
		}
		a, _ := json.Marshal(oldSpec)
		b, _ := json.Marshal(newSpec)
		if string(a) != string(b) {
			return nil, apiError(409, "event filters cannot change during credential rotation")
		}
	}
	result, err := s.db.ExecContext(ctx, `UPDATE pipelines SET secret=? WHERE project_id=? AND id=?`, secret, p.ProjectID, p.ID)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, apiError(404, "pipeline not found")
	}
	s.pipelineWake()
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &PipelineOutput{Body: public}, nil
}

func (s *Service) pipelineRunPage(ctx context.Context, p storedPipeline, limit, offset int) ([]PipelineRun, error) {
	if p.Kind == "s3" {
		return s.s3PipelineRunPage(ctx, p.ProjectID, p.ID, limit, offset)
	}
	return s.webhookPipelineRunPage(ctx, p.ProjectID, p.ID, limit, offset)
}
func (s *Service) listPipelineRuns(ctx context.Context, input *PipelineRunsInput) (*PipelineRunsOutput, error) {
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid pipeline pagination")
	}
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	runs, err := s.pipelineRunPage(ctx, p, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "pipeline run metadata unavailable")
	}
	result := PipelineRuns{Runs: []PipelineRun{}, Limit: input.Limit, Offset: input.Offset}
	if len(runs) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		runs = runs[:input.Limit]
	}
	for _, run := range runs {
		run.Error = publicPipelineError(run.Error)
		result.Runs = append(result.Runs, run)
	}
	return &PipelineRunsOutput{Body: result}, nil
}
func publicPipelineError(raw string) string {
	if raw == "" {
		return ""
	}
	switch raw {
	case "authority_revoked", "creator_unauthorized", "destination_rejected", "invalid_destination", "http_rejected", "request_failed", "invalid_response", "network_error", "timeout", "attempt_failed", "attempts_exhausted", "interrupted", "pipeline_paused", "invalid_credentials", "storage_unavailable", "query_failed", "output_limit", "canceled":
		return raw
	default:
		return "delivery_failed"
	}
}
func (s *Service) getPipelineRun(ctx context.Context, input *PipelineRunInput) (*PipelineRunOutput, error) {
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	var run PipelineRun
	if p.Kind == "s3" {
		run, err = s.getS3PipelineRun(ctx, p.ProjectID, p.ID, input.RunID)
	} else {
		run, err = s.getWebhookPipelineRun(ctx, p.ProjectID, p.ID, input.RunID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "pipeline run not found")
	}
	if err != nil {
		return nil, apiError(503, "pipeline run metadata unavailable")
	}
	run.Error = publicPipelineError(run.Error)
	return &PipelineRunOutput{Body: run}, nil
}
func (s *Service) retryPipelineRun(ctx context.Context, input *PipelineRunInput) (*PipelineRetryOutput, error) {
	if s.pipelines == nil {
		return nil, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, input.ID, input.PipelineID)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	if p.State != "active" {
		return nil, apiError(409, "pipeline is paused")
	}
	var requeued bool
	if p.Kind == "s3" {
		requeued, err = s.retryS3PipelineRun(ctx, p.ProjectID, p.ID, input.RunID)
	} else {
		requeued, err = s.retryWebhookPipelineRun(ctx, p.ProjectID, p.ID, input.RunID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "pipeline run not found")
	}
	if err != nil {
		return nil, apiError(503, "pipeline retry unavailable")
	}
	if !requeued {
		return nil, apiError(409, "only failed pipeline runs can be retried")
	}
	s.pipelineWake()
	return &PipelineRetryOutput{Body: PipelineRetry{Requeued: true}}, nil
}

func publicPipeline(p storedPipeline) (Pipeline, error) {
	result := p.Pipeline
	if p.Kind == "webhook" {
		var spec WebhookPipelineSpec
		if json.Unmarshal(p.Config, &spec) != nil {
			return Pipeline{}, apiError(503, "pipeline configuration unavailable")
		}
		mode := spec.DeliveryMode
		if mode == "" {
			mode = "event"
		}
		result.Webhook = &WebhookPipelineRule{IncludeEvents: spec.IncludeEvents, ExcludeEvents: spec.ExcludeEvents, DeliveryMode: mode, IntervalSeconds: spec.IntervalSeconds, StartAt: spec.StartAt}
	} else if p.Kind == "s3" {
		var spec S3PipelineSpec
		if json.Unmarshal(p.Config, &spec) != nil {
			return Pipeline{}, apiError(503, "pipeline configuration unavailable")
		}
		result.S3 = &S3PipelineRule{SQL: spec.SQL, Parameters: spec.Parameters, Format: spec.Format, IntervalSeconds: spec.IntervalSeconds, StartAt: spec.StartAt, Destination: S3PipelinePublicDestination{Bucket: spec.Destination.Bucket, Region: spec.Destination.Region, Prefix: spec.Destination.Prefix, UsePathStyle: spec.Destination.UsePathStyle}}
	}
	return result, nil
}
