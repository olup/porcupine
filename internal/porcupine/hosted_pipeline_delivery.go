package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

type hostedPipelineDelivery struct {
	flusher        *hostedLiveFlusher
	destination    func(context.Context, *sql.Tx, string, string, string, string, string) (output.WebhookPipelineSecret, error)
	limits         output.HostedWebhookLimits
	client         *http.Client
	deleteGate     func(context.Context, *sql.Tx, lakeapi.HostedProjectScope, string) (bool, error)
	beginAttemptTx func(context.Context, *sql.Tx, string, string) error
	endAttempt     func(context.Context, string, string) error
}

func newHostedPipelineDelivery(
	flusher *hostedLiveFlusher,
	destination func(context.Context, *sql.Tx, string, string, string, string, string) (output.WebhookPipelineSecret, error),
	limits output.HostedWebhookLimits,
	client *http.Client,
	deleteGate func(context.Context, *sql.Tx, lakeapi.HostedProjectScope, string) (bool, error),
	beginAttemptTx func(context.Context, *sql.Tx, string, string) error,
	endAttempt func(context.Context, string, string) error,
) *hostedPipelineDelivery {
	return &hostedPipelineDelivery{
		flusher:        flusher,
		destination:    destination,
		limits:         limits,
		client:         client,
		deleteGate:     deleteGate,
		beginAttemptTx: beginAttemptTx,
		endAttempt:     endAttempt,
	}
}

type frozenHostedWebhook struct {
	claim   output.PipelineClaim
	payload []byte
	secret  output.WebhookPipelineSecret
}

func (w *hostedPipelineDelivery) Step(ctx context.Context) (bool, error) {
	if w.deleteGate == nil || w.beginAttemptTx == nil || w.endAttempt == nil {
		return false, output.ErrPipelineAuthority
	}
	f := w.flusher
	ctx, cancel := f.incarnation.writeContext(ctx)
	defer cancel()
	p := f.publisher
	var skipped []string
	var failures error
	for range 8 {
		d, err := p.effects.NextDeliveryOutsideProjects(ctx, p.control, f.native.Config.SharedSourceID, skipped)
		if errors.Is(err, sql.ErrNoRows) {
			break
		}
		if err != nil {
			return false, err
		}
		progress, err := w.stepDelivery(ctx, d)
		failures = errors.Join(failures, err)
		if progress {
			return true, failures
		}
		skipped = append(skipped, d.Claim.Scope.ProjectID)
	}
	progress, err := w.assembleNext(ctx)
	return progress, errors.Join(failures, err)
}

func (w *hostedPipelineDelivery) stepDelivery(ctx context.Context, d output.PipelineDelivery) (progress bool, resultErr error) {
	f := w.flusher
	c := d.Claim
	key := lakeapi.ProjectWriteKey{SourceID: c.Scope.SourceID, ProjectID: c.Scope.ProjectID}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return false, err
	}
	attemptID := eventapi.NewID()
	var frozen frozenHostedWebhook
	var ticket bool
	err = f.runActiveHostedProject(ctx, session, key.ProjectID, func(ctx context.Context) error {
		var err error
		frozen, progress, ticket, err = w.freeze(ctx, session, key, d, attemptID)
		if progress || ticket || ctx.Err() != nil {
			return err
		}
		return errors.Join(err, w.deferWork(ctx, session, c))
	})
	releaseErr := session.Release()
	if ticket {
		defer func() { resultErr = errors.Join(resultErr, w.finishTicket(ctx, key.ProjectID, attemptID)) }()
	}
	if err = errors.Join(err, releaseErr); err != nil || frozen.claim.Attempt == "" {
		return progress, err
	}
	return true, w.deliver(ctx, key, d, frozen)
}

func (w *hostedPipelineDelivery) deferWork(ctx context.Context, session *lakeapi.ProjectWriteSession, c output.PipelineClaim) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return w.flusher.publisher.effects.DeferPipelineWorkTx(ctx, tx, c)
	})
}

func (w *hostedPipelineDelivery) finishTicket(ctx context.Context, project, attempt string) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return w.endAttempt(cleanup, project, attempt)
}

func (w *hostedPipelineDelivery) freeze(ctx context.Context, session *lakeapi.ProjectWriteSession, key lakeapi.ProjectWriteKey, d output.PipelineDelivery, attemptID string) (frozenHostedWebhook, bool, bool, error) {
	var out frozenHostedWebhook
	f := w.flusher
	p := f.publisher
	scope, err := f.native.LookupHostedProjectExisting(ctx, key)
	if err != nil {
		return out, false, false, err
	}
	c := d.Claim
	if pipelineScope(scope) != c.Scope {
		return out, false, false, output.ErrPipelineConflict
	}
	fence := session.Fence(scope.StreamID)
	if err := f.native.AdvanceRecoveryFence(ctx, fence); err != nil {
		return out, false, false, err
	}
	if err := f.reconcileHostedWrites(ctx, session, scope, fence, c.TenantID); err != nil {
		return out, false, false, err
	}
	cut, _, err := p.checkpoint(ctx, scope, c.TenantID)
	if err != nil {
		return out, false, false, err
	}
	members, err := p.effects.DeliveryMembers(ctx, p.control, c)
	if err != nil {
		return out, false, false, err
	}
	for _, m := range members {
		head, err := f.native.Load(ctx, key.ProjectID, m.UUID)
		if err != nil {
			return out, false, false, err
		}
		if head.IsDeleted {
			err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return p.effects.CancelHostedWebhookEventTx(ctx, tx, c.Scope, c.TenantID, m.UUID)
			})
			return out, err == nil, false, err
		}
	}
	payload, err := p.objects.ReadImmutableEffectObject(ctx, scope, nativeRef(d.Body))
	if err != nil {
		return out, false, false, err
	}
	var ticket bool
	err = session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		blocked, err := w.deleteGate(ctx, tx, scope, c.TenantID)
		if err != nil || blocked {
			return err
		}
		claim, send, err := p.effects.FreezeDeliveryTx(ctx, tx, d, attemptID, cut, w.limits)
		if err != nil || !send {
			return err
		}
		secret, err := w.destination(ctx, tx, c.TenantID, key.ProjectID, d.Target.ID, c.Incarnation, d.Target.DestinationRevision)
		if err != nil {
			return err
		}
		if err := w.beginAttemptTx(ctx, tx, key.ProjectID, attemptID); err != nil {
			return err
		}
		ticket = true
		out = frozenHostedWebhook{claim: claim, payload: payload, secret: secret}
		return nil
	})
	return out, ticket && err == nil, ticket, err
}

func (w *hostedPipelineDelivery) deliver(ctx context.Context, key lakeapi.ProjectWriteKey, d output.PipelineDelivery, frozen frozenHostedWebhook) error {
	f := w.flusher
	checkedAt := time.Now()
	remaining, err := f.publisher.effects.DeliveryLeaseRemaining(ctx, f.publisher.control, frozen.claim)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	attemptDeadline := checkedAt.Add(min(remaining, w.limits.Timeout))
	if !time.Now().Before(attemptDeadline) {
		return nil
	}

	status, code := 0, "invalid_destination"
	secret := frozen.secret
	if delivery.ValidateWebhookEndpoint(secret.Endpoint) == nil && delivery.ValidateWebhookHeaders(secret.Headers) == nil {
		attempt, cancel := context.WithDeadline(ctx, attemptDeadline)
		status, code = delivery.SendWebhook(attempt, w.client, delivery.WebhookMessage{Endpoint: secret.Endpoint, Headers: secret.Headers, DeliveryID: d.Claim.ID, Payload: string(frozen.payload)})
		cancel()
	}
	retry := code != "" && (status == 0 && code != "invalid_destination" || status == 408 || status == 425 || status == 429 || status >= 500)
	completion, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	session, err := f.coordinator.Acquire(completion, key)
	if err != nil {
		return err
	}
	defer session.Release()
	err = session.RunControlTx(completion, func(ctx context.Context, tx *sql.Tx) error {
		return f.publisher.effects.CompleteDeliveryTx(ctx, tx, d, frozen.claim, status, code, retry, w.limits.MaxAttempts)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil // Cancellation or reclaim changed the winning attempt.
	}
	return err
}
