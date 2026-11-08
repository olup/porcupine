package porcupine

import "encoding/json"

// Pipeline exposes rule metadata without endpoints or credentials.
type Pipeline struct {
	ID          string               `json:"id"`
	ProjectID   string               `json:"project_id"`
	Kind        string               `json:"kind" enum:"webhook,s3"`
	Name        string               `json:"name"`
	State       string               `json:"state" enum:"active,paused"`
	CreatedAtMS int64                `json:"created_at_ms"`
	Webhook     *WebhookPipelineRule `json:"webhook,omitempty"`
	S3          *S3PipelineRule      `json:"s3,omitempty"`
}

type storedPipeline struct {
	Pipeline
	ActorID string
	Config  json.RawMessage
	Secret  []byte
}

type WebhookPipelineSpec struct {
	Endpoint        string   `json:"endpoint" writeOnly:"true" minLength:"1" maxLength:"4096" doc:"HTTPS destination; stored encrypted and never returned."`
	IncludeEvents   []string `json:"include_events,omitempty" maxItems:"100"`
	ExcludeEvents   []string `json:"exclude_events,omitempty" maxItems:"100"`
	DeliveryMode    string   `json:"delivery_mode,omitempty" enum:"event,batch" default:"event"`
	IntervalSeconds int64    `json:"interval_seconds,omitempty"`
	StartAt         string   `json:"start_at,omitempty"`
}

type WebhookPipelineSecret struct {
	Endpoint string            `json:"endpoint"`
	Headers  map[string]string `json:"headers,omitempty" writeOnly:"true"`
}

type PipelineCredentials struct {
	Headers         map[string]string `json:"headers,omitempty" writeOnly:"true" doc:"Webhook headers; write-only and encrypted."`
	BearerToken     string            `json:"bearer_token,omitempty" writeOnly:"true" doc:"Webhook Bearer credential; write-only and encrypted."`
	AccessKeyID     string            `json:"access_key_id,omitempty" writeOnly:"true"`
	SecretAccessKey string            `json:"secret_access_key,omitempty" writeOnly:"true"`
	SessionToken    string            `json:"session_token,omitempty" writeOnly:"true"`
}

type PipelineRun struct {
	ID            string `json:"id"`
	PipelineID    string `json:"pipeline_id"`
	State         string `json:"state"`
	Attempts      int    `json:"attempts"`
	CreatedAtMS   int64  `json:"created_at_ms"`
	CompletedAtMS int64  `json:"completed_at_ms,omitempty"`
	Error         string `json:"error,omitempty" doc:"Sanitized machine-readable error code only."`
	LastStatus    int    `json:"last_status,omitempty"`
	WindowStartMS int64  `json:"window_start_ms,omitempty"`
	WindowEndMS   int64  `json:"window_end_ms,omitempty"`
	EventCount    int64  `json:"event_count,omitempty"`
	Rows          int64  `json:"rows,omitempty"`
	Bytes         int64  `json:"bytes,omitempty"`
}

type WebhookPipelineRule struct {
	IncludeEvents   []string `json:"include_events,omitempty"`
	ExcludeEvents   []string `json:"exclude_events,omitempty"`
	DeliveryMode    string   `json:"delivery_mode"`
	IntervalSeconds int64    `json:"interval_seconds,omitempty"`
	StartAt         string   `json:"start_at,omitempty"`
}
type S3PipelineRule struct {
	SQL             string                      `json:"sql"`
	Parameters      map[string]string           `json:"parameters,omitempty"`
	Format          string                      `json:"format"`
	IntervalSeconds int64                       `json:"interval_seconds"`
	StartAt         string                      `json:"start_at"`
	Destination     S3PipelinePublicDestination `json:"destination"`
}
type S3PipelinePublicDestination struct {
	Bucket       string `json:"bucket"`
	Region       string `json:"region"`
	Prefix       string `json:"prefix"`
	UsePathStyle bool   `json:"use_path_style"`
}
