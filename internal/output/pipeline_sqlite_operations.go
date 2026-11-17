package output

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
)

type PipelineRequest struct {
	Name, Kind  string
	Webhook     *WebhookPipelineSpec
	S3          *S3PipelineSpec
	Credentials PipelineCredentials
}
type PipelineChange struct {
	Webhook     *WebhookPipelineSpec
	S3          *S3PipelineSpec
	Credentials PipelineCredentials
}
type PipelinePage struct {
	Enabled       bool
	Pipelines     []Pipeline
	Limit, Offset int
	NextOffset    *int
}
type PipelineRunPage struct {
	Runs          []PipelineRun
	Limit, Offset int
	NextOffset    *int
}

const pipelineColumns = `id,project_id,actor_id,kind,name,state,config,secret,created_at_ms`

func scanPipeline(row interface{ Scan(...any) error }) (storedPipeline, error) {
	var p storedPipeline
	var config string
	err := row.Scan(&p.ID, &p.ProjectID, &p.ActorID, &p.Kind, &p.Name, &p.State, &config, &p.Secret, &p.CreatedAtMS)
	p.Config = json.RawMessage(config)
	return p, err
}
func (s *PipelineJobs) loadPipeline(ctx context.Context, projectID, pipelineID string) (storedPipeline, error) {
	return scanPipeline(s.db.QueryRowContext(ctx, `SELECT `+pipelineColumns+` FROM pipelines WHERE project_id=$1 AND id=$2`, projectID, pipelineID))
}
func pipelineLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "pipeline not found")
	}
	return apiError(503, "pipeline storage unavailable")
}

func (s *PipelineJobs) encodePipelineDestination(ctx context.Context, projectID, pipelineID, kind string, webhook *WebhookPipelineSpec, s3 *S3PipelineSpec, credentials PipelineCredentials) ([]byte, []byte, error) {
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
		if delivery.ValidateWebhookEndpoint(webhook.Endpoint) != nil {
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
		if delivery.ValidateWebhookHeaders(headers) != nil {
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
		normalized, err := s.ValidateS3Spec(ctx, projectID, *s3)
		if err != nil {
			var status interface{ GetStatus() int }
			if errors.As(err, &status) {
				return nil, nil, err
			}
			return nil, nil, apiError(400, "invalid S3 pipeline specification")
		}
		if s.ValidateS3Endpoint(normalized.Destination.Endpoint) != nil {
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
	sealed, err := s.SealSecret(projectID, pipelineID, secret)
	if err != nil {
		return nil, nil, apiError(503, "pipeline credentials unavailable")
	}
	return config, sealed, nil
}

func (s *PipelineJobs) Define(ctx context.Context, project string, request PipelineRequest, caller auth.Principal) (*Pipeline, error) {
	if !s.Enabled() {
		return nil, apiError(503, "pipelines are not configured")
	}
	if request.Kind == "s3" && s.lake == nil {
		return nil, apiError(503, "S3 pipelines require an analytical store")
	}
	if strings.TrimSpace(request.Name) == "" || len(request.Name) > 200 {
		return nil, apiError(400, "invalid pipeline name")
	}
	p := Pipeline{ID: eventapi.NewID(), ProjectID: project, Kind: request.Kind, Name: request.Name, State: "active", CreatedAtMS: s.clock().UnixMilli()}
	config, secret, err := s.encodePipelineDestination(ctx, p.ProjectID, p.ID, p.Kind, request.Webhook, request.S3, request.Credentials)
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
	if tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE project_id=$1) FROM (SELECT project_id FROM pipelines UNION ALL SELECT project_id FROM pipeline_retired_destinations)`, p.ProjectID).Scan(&totalCount, &projectCount) != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	if projectCount >= 64 || totalCount >= 512 {
		return nil, apiError(409, "pipeline limit reached")
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pipelines(`+pipelineColumns+`) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, p.ID, p.ProjectID, caller.ID, p.Kind, p.Name, p.State, string(config), secret, p.CreatedAtMS)
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
	s.Wake()
	public, err := publicPipeline(storedPipeline{Pipeline: p, Config: config})
	if err != nil {
		return nil, err
	}
	return &public, nil
}

func (s *PipelineJobs) List(ctx context.Context, project string, limit, offset int) (*PipelinePage, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, apiError(400, "invalid pipeline pagination")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,project_id,kind,name,state,created_at_ms FROM pipelines WHERE project_id=$1 ORDER BY created_at_ms DESC,id LIMIT $2 OFFSET $3`, project, limit+1, offset)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	defer rows.Close()
	result := PipelinePage{Enabled: s.Enabled(), Pipelines: []Pipeline{}, Limit: limit, Offset: offset}
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
	if len(result.Pipelines) > limit {
		next := offset + limit
		result.NextOffset = &next
		result.Pipelines = result.Pipelines[:limit]
	}
	return &result, nil
}
func (s *PipelineJobs) Get(ctx context.Context, project, id string) (*Pipeline, error) {
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &public, nil
}
func (s *PipelineJobs) SetState(ctx context.Context, project, id string, state string) (*Pipeline, error) {
	if state == "active" && !s.Enabled() {
		return nil, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	if p.Kind == "s3" && state == "active" {
		if s.lake == nil {
			return nil, apiError(503, "S3 pipelines require an analytical store")
		}
		release, err := s.lockS3Lifecycle(p.ID)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	result, err := s.db.ExecContext(ctx, `UPDATE pipelines SET state=$1 WHERE project_id=$2 AND id=$3`, state, project, id)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, apiError(404, "pipeline not found")
	}
	p.State = state
	s.Wake()
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &public, nil
}
func (s *PipelineJobs) Delete(ctx context.Context, project, id string) error {
	p, lookupErr := s.loadPipeline(ctx, project, id)
	if lookupErr != nil {
		return pipelineLookupError(lookupErr)
	}
	if p.Kind == "s3" {
		if err := s.deleteS3Pipeline(ctx, p.ProjectID, p.ID); err != nil {
			return err
		}
		s.Wake()
		return nil
	}
	result, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE project_id=$1 AND id=$2`, project, id)
	if err != nil {
		return apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return apiError(404, "pipeline not found")
	}
	s.Wake()
	return nil
}
func (s *PipelineJobs) Replace(ctx context.Context, project, id string, change PipelineChange) (*Pipeline, error) {
	if s.Enabled() {
		release, err := s.lockS3Lifecycle(id)
		if err != nil {
			return nil, err
		}
		defer release()
	}
	if !s.Enabled() {
		return nil, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	if p.Kind == "webhook" && change.Webhook == nil && change.S3 == nil {
		var spec WebhookPipelineSpec
		var private WebhookPipelineSecret
		plaintext, openErr := s.OpenSecret(p.ProjectID, p.ID, p.Secret)
		if openErr != nil || json.Unmarshal(plaintext, &private) != nil || json.Unmarshal(p.Config, &spec) != nil {
			return nil, apiError(503, "pipeline credentials unavailable")
		}
		spec.Endpoint = private.Endpoint
		change.Webhook = &spec
	}
	if p.Kind == "s3" && change.S3 == nil && change.Webhook == nil {
		var spec S3PipelineSpec
		if json.Unmarshal(p.Config, &spec) != nil {
			return nil, apiError(503, "pipeline configuration unavailable")
		}
		change.S3 = &spec
	}
	config, secret, err := s.encodePipelineDestination(ctx, p.ProjectID, p.ID, p.Kind, change.Webhook, change.S3, change.Credentials)
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
	oldPlain, oldErr := s.OpenSecret(p.ProjectID, p.ID, p.Secret)
	newPlain, newErr := s.OpenSecret(p.ProjectID, p.ID, secret)
	if oldErr == nil && newErr == nil && bytes.Equal(oldPlain, newPlain) {
		public, err := publicPipeline(p)
		if err != nil {
			return nil, err
		}
		return &public, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return nil, err
	}
	if p.Kind == "s3" {
		if err = s.retireS3Tx(ctx, tx, p); err != nil {
			return nil, err
		}
	} else {
		if _, err = tx.ExecContext(ctx, `UPDATE webhook_deliveries SET state='canceled',payload='',bytes=0,error_code='destination_replaced',completed_at_ms=$1 WHERE pipeline_id=$2 AND state IN('queued','sending','failed')`, s.clock().UnixMilli(), p.ID); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_members WHERE pipeline_id=$1`, p.ID); err != nil {
			return nil, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM webhook_cooldowns WHERE pipeline_id=$1`, p.ID); err != nil {
			return nil, err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE pipelines SET secret=$1 WHERE project_id=$2 AND id=$3 AND secret=$4`, secret, p.ProjectID, p.ID, p.Secret)
	if err != nil {
		return nil, apiError(503, "pipeline storage unavailable")
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return nil, apiError(409, "pipeline destination changed; retry replacement")
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	s.Wake()
	public, err := publicPipeline(p)
	if err != nil {
		return nil, err
	}
	return &public, nil
}

func (s *PipelineJobs) pipelineRunPage(ctx context.Context, p storedPipeline, limit, offset int) ([]PipelineRun, error) {
	if p.Kind == "s3" {
		return s.s3PipelineRunPage(ctx, p.ProjectID, p.ID, limit, offset)
	}
	return s.webhookPipelineRunPage(ctx, p.ProjectID, p.ID, limit, offset)
}
func (s *PipelineJobs) Runs(ctx context.Context, project, id string, limit, offset int) (*PipelineRunPage, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, apiError(400, "invalid pipeline pagination")
	}
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	runs, err := s.pipelineRunPage(ctx, p, limit+1, offset)
	if err != nil {
		return nil, apiError(503, "pipeline run metadata unavailable")
	}
	result := PipelineRunPage{Runs: []PipelineRun{}, Limit: limit, Offset: offset}
	if len(runs) > limit {
		next := offset + limit
		result.NextOffset = &next
		runs = runs[:limit]
	}
	for _, run := range runs {
		run.Error = PublicPipelineError(run.Error)
		result.Runs = append(result.Runs, run)
	}
	return &result, nil
}
func (s *PipelineJobs) Run(ctx context.Context, project, id, runID string) (*PipelineRun, error) {
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return nil, pipelineLookupError(err)
	}
	var run PipelineRun
	if p.Kind == "s3" {
		run, err = s.getS3PipelineRun(ctx, p.ProjectID, p.ID, runID)
	} else {
		run, err = s.getWebhookPipelineRun(ctx, p.ProjectID, p.ID, runID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "pipeline run not found")
	}
	if err != nil {
		return nil, apiError(503, "pipeline run metadata unavailable")
	}
	run.Error = PublicPipelineError(run.Error)
	return &run, nil
}
func (s *PipelineJobs) Retry(ctx context.Context, project, id, runID string) (bool, error) {
	if !s.Enabled() {
		return false, apiError(503, "pipelines are not configured")
	}
	p, err := s.loadPipeline(ctx, project, id)
	if err != nil {
		return false, pipelineLookupError(err)
	}
	if p.State != "active" {
		return false, apiError(409, "pipeline is paused")
	}
	var requeued bool
	if p.Kind == "s3" {
		requeued, err = s.retryS3PipelineRun(ctx, p.ProjectID, p.ID, runID)
	} else {
		requeued, err = s.retryWebhookPipelineRun(ctx, p.ProjectID, p.ID, runID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, apiError(404, "pipeline run not found")
	}
	if err != nil {
		return false, apiError(503, "pipeline retry unavailable")
	}
	if !requeued {
		return false, apiError(409, "only failed pipeline runs can be retried")
	}
	s.Wake()
	return true, nil
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
func PublicPipelineError(raw string) string {
	if raw == "" {
		return ""
	}
	switch raw {
	case "authority_revoked", "creator_unauthorized", "destination_rejected", "invalid_destination", "http_rejected", "request_failed", "invalid_response", "network_error", "timeout", "attempt_failed", "attempts_exhausted", "interrupted", "pipeline_paused", "invalid_credentials", "storage_unavailable", "query_failed", "output_limit", "canceled", "destination_replaced":
		return raw
	default:
		return "delivery_failed"
	}
}
