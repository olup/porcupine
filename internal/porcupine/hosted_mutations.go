package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

const hostedMutationStagingLifetime = 15 * time.Minute

type hostedMutationAuthority func(context.Context, *sql.Tx, string, string, string) error

func (s *Service) authorizeHostedMutation(ctx context.Context, tx *sql.Tx, project, tenant, operation string) error {
	a := principal(ctx)
	allows := func(actor auth.Principal) bool {
		if operation == "delete-events" {
			return auth.Authorize(actor, "events:read", project) && auth.Authorize(actor, "events:delete", project)
		}
		return auth.Authorize(actor, "events:"+operation, project)
	}
	if !allows(a) {
		return apierr.New(403, "credential cannot mutate this project")
	}
	if a.External == nil {
		var locked string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM access_keys WHERE id=$1 AND revoked_at IS NULL FOR UPDATE`, a.ID).Scan(&locked); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return apierr.New(403, "credential is no longer authorized")
			}
			return err
		}
		// Lock inherited authority before loading its current restrictions.
		rows, err := tx.QueryContext(ctx, `SELECT k.id FROM access_keys k WHERE k.id IN (
SELECT client_id FROM service_api_keys WHERE principal_id=$1
UNION SELECT client_id FROM oauth_interactive_grants WHERE principal_id=$1
UNION SELECT human_id FROM oauth_interactive_grants WHERE principal_id=$1)
ORDER BY k.id FOR UPDATE`, a.ID)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err := rows.Scan(&locked); err != nil {
				rows.Close()
				return err
			}
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return err
		}
	}
	current, err := s.currentAuthority(ctx, tx, a)
	if err != nil {
		return err
	}
	if !allows(current) || a.External != nil && !current.CanAccessTenant(tenant) {
		return apierr.New(403, "credential is no longer authorized")
	}
	return nil
}

func (f *hostedLiveFlusher) withHostedMutationProject(ctx context.Context, project string, call func(context.Context, *lakeapi.ProjectWriteSession, lakeapi.HostedProjectScope, string) error) error {
	key := lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: project}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	return f.runActiveHostedProject(ctx, session, project, func(ctx context.Context) error {
		var tenant string
		if err := f.publisher.control.QueryRowContext(ctx, `SELECT tenant_id FROM projects WHERE id=$1`, project).Scan(&tenant); err != nil {
			return err
		}
		scope, err := f.native.LookupHostedProjectExisting(ctx, key)
		if err != nil {
			return err
		}
		return call(ctx, session, scope, tenant)
	})
}

func hostedMutationError(err error) error {
	if err == nil {
		return nil
	}
	var status interface{ GetStatus() int }
	switch {
	case errors.As(err, &status):
		return err
	case errors.Is(err, sql.ErrNoRows), errors.Is(err, lakeapi.ErrEventNotFound):
		return apierr.New(404, "event or mutation not found")
	case errors.Is(err, errHostedMutationConflict):
		return apierr.New(409, "idempotency_key was used for a different request")
	case errors.Is(err, errHostedMutationBusy):
		return apierr.New(409, "a project mutation is already pending")
	default:
		return apierr.New(503, "hosted mutation storage unavailable")
	}
}

func (f *hostedLiveFlusher) acceptHostedMutation(ctx context.Context, project, uuid, operation, keyHash, requestHash string, expected uint64, snapshot *EventSnapshot, authority hostedMutationAuthority) (MutationResponse, error) {
	var response MutationResponse
	if authority == nil {
		return response, apierr.New(503, "hosted mutation authority unavailable")
	}
	err := f.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
		fence := session.Fence(scope.StreamID)
		authorize := func(ctx context.Context, tx *sql.Tx) error {
			if err := lockActiveHostedMutationProject(ctx, tx, project, tenant); err != nil {
				return err
			}
			return authority(ctx, tx, project, tenant, operation)
		}
		if err := session.RunControlTx(ctx, authorize); err != nil {
			return err
		}
		i, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "key_hash", keyHash)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil && (i.requestHash != requestHash || i.matchedCount.Valid) {
			return errHostedMutationConflict
		}
		if err == nil && i.state != "staging" {
			response = i.response(uuid)
			return nil
		}
		if err := f.native.AdvanceWriteFence(ctx, fence); err != nil {
			return err
		}
		if err := f.reconcile(ctx, session, scope, fence, tenant); err != nil {
			return err
		}
		if i.id == "" {
			// This successor session has fenced stale native writers. Staging
			// never authorized a native write and keeps its old S3 charge.
			if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				cutoff := time.Now().UTC().Add(-hostedMutationStagingLifetime).Format(time.RFC3339Nano)
				return retireHostedMutationStagingTx(ctx, tx, scope, tenant, cutoff)
			}); err != nil {
				return err
			}
			pending, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "state", "ready")
			if err == nil && pending.id != "" {
				return errHostedMutationBusy
			}
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		current, err := f.native.LoadHostedEvent(ctx, scope, uuid)
		if err != nil {
			return err
		}
		projection, err := prepareMutation(current, false, operation, expected, snapshot)
		if err != nil {
			return err
		}
		newIntent := i.id == ""
		if newIntent {
			counter, err := hostedMutationProjectRevision(ctx, f.publisher.control, scope, tenant)
			if err != nil {
				return err
			}
			verified := max(counter, int64(current.Revision))
			if verified == math.MaxInt64 {
				return errHostedMutationCapacity
			}
			i = hostedMutationIntent{scope: scope, tenant: tenant, id: eventapi.NewID(), state: "staging", keyHash: keyHash, requestHash: requestHash, actorHash: hash(principal(ctx).ID), operation: operation, expected: int64(expected), revision: verified + 1, createdAt: time.Now().UTC().Format(time.RFC3339Nano)}
		}
		projection.Revision = uint64(i.revision)
		input := lakeapi.HostedMutationInput{Scope: scope, TenantID: tenant, MutationID: i.id, ExpectedRevision: expected, Projection: projection}
		i, err = f.stageHostedMutation(ctx, session, i, input)
		if err != nil {
			return err
		}
		// The successor cannot invalidate this check and still let this session
		// commit Ready. RunControlTx commits on the physical lock owner.
		current, err = f.native.LoadHostedEvent(ctx, scope, uuid)
		if err != nil {
			return err
		}
		if current.ID != projection.ID || current.Revision != expected || current.IsDeleted || current.IdentitySnapshot != "" {
			return errHostedMutationConflict
		}
		i, err = commitHostedMutationIntent(ctx, session, i, authorize)
		if err != nil {
			return err
		}
		response = i.response(uuid)
		return nil
	})
	return response, hostedMutationError(err)
}

func (f *hostedLiveFlusher) stageHostedMutation(ctx context.Context, session *lakeapi.ProjectWriteSession, i hostedMutationIntent, input lakeapi.HostedMutationInput) (hostedMutationIntent, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return i, err
	}
	objects := f.publisher.objects
	described, prefix, err := objects.DescribeImmutableMutationObject(i.scope, i.id, raw)
	if err != nil {
		return i, err
	}
	if i.body.Key == "" {
		i.body, i.prefix = described, prefix
		if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return reserveHostedMutationTx(ctx, tx, i)
		}); err != nil {
			return i, err
		}
	} else if i.body != described || i.prefix != prefix {
		return i, errHostedMutationConflict
	}
	i.body, err = objects.PutImmutableMutationObject(ctx, i.scope, i.id, raw)
	if err != nil {
		return i, err
	}
	_, err = f.publisher.readHostedMutation(ctx, i)
	return i, err
}

func commitHostedMutationIntent(ctx context.Context, session *lakeapi.ProjectWriteSession, i hostedMutationIntent, authorize func(context.Context, *sql.Tx) error) (hostedMutationIntent, error) {
	i.state = "ready"
	if i.matchedCount.Valid && i.matchedCount.Int64 == 0 {
		i.state, i.appliedAt = "applied", time.Now().UTC().Format(time.RFC3339Nano)
	}
	var rejected error
	if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := authorize(ctx, tx); err != nil {
			var status interface{ GetStatus() int }
			if !errors.Is(err, sql.ErrNoRows) && (!errors.As(err, &status) || status.GetStatus() >= 500) {
				return err
			}
			rejected = err
			return abortHostedMutationStagingTx(ctx, tx, i)
		}
		return acceptHostedMutationIntentTx(ctx, tx, i)
	}); err != nil {
		return i, err
	}
	return i, rejected
}

func (p *hostedEffectPublisher) readHostedMutation(ctx context.Context, i hostedMutationIntent) (lakeapi.HostedMutationInput, error) {
	var input lakeapi.HostedMutationInput
	raw, err := p.objects.ReadImmutableMutationObject(ctx, i.scope, i.body)
	if err != nil {
		return input, err
	}
	if err := decodeHostedJSON(raw, &input); err != nil {
		return input, err
	}
	fingerprint, err := lakeapi.HostedMutationFingerprint(input)
	if err != nil {
		return input, err
	}
	described, prefix, err := p.objects.DescribeImmutableMutationObject(i.scope, i.id, raw)
	if err != nil || described.Key != i.body.Key || prefix != i.prefix || fingerprint != i.body.SHA256 ||
		input.Scope != i.scope || input.TenantID != i.tenant || input.MutationID != i.id || input.ExpectedRevision != uint64(i.expected) ||
		i.matchedCount.Valid != (input.Deletion != nil) {
		return input, errHostedMutationConflict
	}
	if input.Deletion != nil {
		if i.operation != "delete" || input.Deletion.Revision != uint64(i.revision) || len(input.Deletion.Targets) != int(i.matchedCount.Int64) || !input.Deletion.Valid(input.ExpectedRevision) {
			return input, errHostedMutationConflict
		}
	} else if input.Projection.ProjectID != i.scope.Key.ProjectID || input.Projection.Revision != uint64(i.revision) ||
		input.Projection.IsDeleted != (i.operation == "delete") || !lakeapi.ValidStoredEventUUID(input.Projection.UUID) {
		return input, errHostedMutationConflict
	}
	return input, nil
}

func (f *hostedLiveFlusher) currentHostedEvent(ctx context.Context, project, uuid string) (lakeapi.EventProjection, bool, error) {
	var projection lakeapi.EventProjection
	var pending bool
	err := f.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
		if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return lockActiveHostedMutationProject(ctx, tx, project, tenant)
		}); err != nil {
			return err
		}
		i, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "state", "ready")
		if err == nil {
			input, err := f.publisher.readHostedMutation(ctx, i)
			if err != nil {
				return apierr.New(503, "pending mutation body unavailable")
			}
			if input.Deletion != nil {
				for _, target := range input.Deletion.Targets {
					if target.UUID == uuid {
						projection = lakeapi.EventProjection{Event: eventapi.Event{ProjectID: project, UUID: uuid}, Revision: input.Deletion.Revision, IsDeleted: true}
						pending = true
						return nil
					}
				}
			} else if input.Projection.UUID == uuid {
				projection, pending = input.Projection, true
				return nil
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		projection, err = f.native.LoadHostedEvent(ctx, scope, uuid)
		return err
	})
	return projection, pending, hostedMutationError(err)
}

func (f *hostedLiveFlusher) hostedMutationStatus(ctx context.Context, project, id string) (MutationResponse, error) {
	var response MutationResponse
	err := f.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
		if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return lockActiveHostedMutationProject(ctx, tx, project, tenant)
		}); err != nil {
			return err
		}
		i, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "mutation_id", id)
		if err != nil {
			return err
		}
		if i.matchedCount.Valid || i.state == "staging" || i.state == "aborted" {
			return sql.ErrNoRows
		}
		input, err := f.publisher.readHostedMutation(ctx, i)
		if err != nil {
			return apierr.New(503, "mutation body unavailable")
		}
		response = i.response(input.Projection.UUID)
		return nil
	})
	return response, hostedMutationError(err)
}

// settleReadyHostedMutation runs after the common effect predecessor has been
// reconciled. It needs neither the producer credential nor worker-local state.
func (f *hostedLiveFlusher) settleReadyHostedMutation(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, fence lakeapi.ProjectWriteFence, tenant string, deleting bool) error {
	i, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "state", "ready")
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	input, err := f.publisher.readHostedMutation(ctx, i)
	if err != nil {
		return err
	}
	if i.operation == "delete" {
		if !i.matchedCount.Valid && f.publisher.cancelMutationWebhook == nil || i.matchedCount.Int64 > 0 && f.publisher.cancelMutationWebhooks == nil {
			return errHostedMutationBusy
		}
	}
	if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if deleting {
			return f.publisher.effects.PrepareDeletingMutationStreamTx(ctx, tx, pipelineScope(scope), tenant)
		}
		return f.publisher.effects.PrepareNoOutputStreamTx(ctx, tx, pipelineScope(scope), tenant)
	}); err != nil {
		return err
	}
	e, err := f.native.CommitHostedMutation(ctx, fence, input)
	if err != nil {
		return err
	}
	if err := f.publisher.publishEvidence(ctx, output.PipelineBinding{}, e, session, deleting, lakeapi.ImmutableObjectRef{}); err != nil {
		return err
	}
	return f.acknowledgeHostedEffects(ctx, fence, scope, e.Sequence)
}

// withHostedMutationCut orders retained snapshot selection after Ready intents.
// The callback must pin the chosen snapshot before releasing the project session.
func (f *hostedLiveFlusher) withHostedMutationCut(ctx context.Context, project string, pin func(context.Context, *lakeapi.ProjectWriteSession, lakeapi.HostedProjectScope, string, int64) error) error {
	return f.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
		fence := session.Fence(scope.StreamID)
		if err := f.native.AdvanceWriteFence(ctx, fence); err != nil {
			return err
		}
		if err := f.reconcileHostedWrites(ctx, session, scope, fence, tenant); err != nil {
			return err
		}
		var accepted, settled int64
		if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			if err := lockActiveHostedMutationProject(ctx, tx, project, tenant); err != nil {
				return err
			}
			err := tx.QueryRowContext(ctx, `SELECT accepted_through,settled_through FROM hosted_mutation_projects
WHERE source_id=$1 AND project_id=$2 AND catalog_id=$3 AND stream_id=$4 AND tenant_id=$5`, scope.Key.SourceID, project, scope.CatalogID, scope.StreamID, tenant).Scan(&accepted, &settled)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}); err != nil {
			return err
		}
		if accepted != settled {
			return errHostedMutationBusy
		}
		return pin(ctx, session, scope, tenant, accepted)
	})
}
