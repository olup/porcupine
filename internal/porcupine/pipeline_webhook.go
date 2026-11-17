package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/delivery"
)

func (s *Service) pipelineCreatorAuthorized(ctx context.Context, id, project string) (bool, error) {
	if err := s.requireSharedProjectOwner(ctx, project); err != nil {
		return false, err
	}
	a, err := s.loadCredentialActor(ctx, id)
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errInteractiveGrant) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return auth.Authorize(a, "pipelines:write", project) && auth.Authorize(a, "events:read", project), nil
}

type webhookHost struct{ service *Service }

func (h webhookHost) AuthorizeWebhook(ctx context.Context, actor, project string) (bool, error) {
	return h.service.pipelineCreatorAuthorized(ctx, actor, project)
}
func (h webhookHost) OpenWebhookSecret(project, pipeline string, blob []byte) ([]byte, error) {
	return h.service.openPipelineSecret(project, pipeline, blob)
}
func (h webhookHost) BeginWebhookActivity(ctx context.Context, project string) (context.Context, func(), bool) {
	return h.service.activity.begin(ctx, project)
}

func configureWebhooks(s *Service, acceptance *durableAcceptance, runtime *pipelineRuntime) {
	acceptance.enqueueWebhooks = nil
	if runtime == nil {
		return
	}
	c := runtime.config
	limits := &delivery.WebhookLimits{
		MaxQueuedEvents:        c.WebhookMaxQueuedEvents,
		MaxQueuedBytes:         c.WebhookMaxQueuedBytes,
		ProjectMaxQueuedEvents: c.WebhookProjectMaxQueuedEvents,
		ProjectMaxQueuedBytes:  c.WebhookProjectMaxQueuedBytes,
		MaxAttempts:            c.WebhookMaxAttempts,
		TimeoutSeconds:         c.WebhookTimeoutSeconds,
	}
	clock := &s.eventRules.clock
	now := func() time.Time { return (*clock)() }
	runtime.webhooks = delivery.NewWebhookQueue(s.db, limits, now, webhookHost{s})
	acceptance.enqueueWebhooks = runtime.webhooks.Enqueuer().Enqueue
}
