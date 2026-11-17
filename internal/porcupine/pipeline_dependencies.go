package porcupine

import (
	"errors"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	"github.com/olup/porcupine/internal/output"
)

func (s *Service) configurePipelines(c *output.PipelineConfig) error {
	if s.pipelineClient == nil {
		s.pipelineClient = delivery.NewWebhookHTTPClient()
	}
	d := output.PipelineDependencies{Journal: s.db, Control: s.control, Clock: func() time.Time { return s.clock() }, LoadActor: s.loadCredentialActor, CredentialMissing: func(err error) bool { return errors.Is(err, errInteractiveGrant) }, RequireActive: s.requireProjectActive, RequireOwner: s.requireSharedProjectOwner, BeginActivity: s.activity.begin, Acquire: s.acquirePipelineExport, WebhookClient: s.pipelineClient}
	if s.lake != nil {
		d.Reader = s.lake
	}
	if s.usage != nil {
		d.Usage = s.usage.recorder
	}
	p, err := output.NewSQLitePipelines(d, c)
	if err != nil {
		return err
	}
	s.pipelines = p
	if a, ok := s.acceptance.(*durableAcceptance); ok {
		a.enqueueWebhooks = nil
		if p.Enabled() {
			a.enqueueWebhooks = p.Enqueuer().Enqueue
		}
	}
	return nil
}
