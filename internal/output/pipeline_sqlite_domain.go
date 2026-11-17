package output

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/olup/porcupine/internal/auth"

	"github.com/olup/porcupine/internal/usage"
)

// PipelineReader holds native leases and permits until the call actually returns.
type PipelineReader interface {
	ExportReader
	ValidateExport(context.Context, string, string, map[string]string, bool) (string, error)
}

// PipelineDependencies supplies resource authority and shared native admission.
// Queue state, encryption, scheduling and output publication belong to PipelineJobs.
type PipelineDependencies struct {
	Journal, Control            *sql.DB
	Clock                       func() time.Time
	Reader                      PipelineReader
	Usage                       *usage.Recorder
	LoadActor                   func(context.Context, string) (auth.Principal, error)
	CredentialMissing           func(error) bool
	RequireActive, RequireOwner func(context.Context, string) error
	BeginActivity               func(context.Context, string) (context.Context, func(), bool)
	Acquire                     func(context.Context) (func(), bool)
	WebhookClient               *http.Client
}

func NewSQLitePipelines(d PipelineDependencies, config *PipelineConfig) (*PipelineJobs, error) {
	s, err := newSQLitePipelineRuntime(config)
	if err != nil {
		return nil, err
	}
	if s == nil {
		s = &PipelineJobs{}
	}
	s.db, s.control, s.clock, s.lake, s.usage = d.Journal, d.Control, d.Clock, d.Reader, d.Usage
	s.loadActor, s.credentialMissing = d.LoadActor, d.CredentialMissing
	s.requireActive, s.requireOwner = d.RequireActive, d.RequireOwner
	s.beginActivity, s.acquire = d.BeginActivity, d.Acquire
	if s.clock == nil {
		s.clock = time.Now
	}
	if s.credentialMissing == nil {
		s.credentialMissing = func(error) bool { return false }
	}
	if s.Enabled() {
		c := s.config
		s.webhooks = newWebhookQueue(s.db, &webhookLimits{MaxQueuedEvents: c.WebhookMaxQueuedEvents, MaxQueuedBytes: c.WebhookMaxQueuedBytes, ProjectMaxQueuedEvents: c.WebhookProjectMaxQueuedEvents, ProjectMaxQueuedBytes: c.WebhookProjectMaxQueuedBytes, MaxAttempts: c.WebhookMaxAttempts, TimeoutSeconds: c.WebhookTimeoutSeconds}, s.clock, s.AuthorizeCreator, s.OpenSecret, s.beginActivity)
		s.webhooks.Usage = s.usage
		if d.WebhookClient != nil {
			s.webhooks.Client = d.WebhookClient
		}
	}
	return s, nil
}
func (s *PipelineJobs) Enabled() bool          { return s != nil && s.aead != nil }
func (s *PipelineJobs) Config() PipelineConfig { return s.config }
func (s *PipelineJobs) Enqueuer() *WebhookEnqueuer {
	if !s.Enabled() {
		return nil
	}
	return s.webhooks.Enqueuer()
}
func (s *PipelineJobs) RunWebhook(ctx context.Context) (bool, error) {
	if !s.Enabled() {
		return false, nil
	}
	return s.webhooks.Step(ctx)
}
func (s *PipelineJobs) RunOne(ctx context.Context) (bool, error) {
	worked, err := s.RunWebhook(ctx)
	if worked || err != nil {
		return worked, err
	}
	return s.RunS3(ctx)
}
func (s *PipelineJobs) Close() {
	s.Stop()
	if s != nil && s.webhooks != nil {
		s.webhooks.Client.CloseIdleConnections()
	}
}
