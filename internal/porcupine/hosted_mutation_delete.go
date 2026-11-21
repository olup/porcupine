package porcupine

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (f *hostedLiveFlusher) deleteEventsHosted(ctx context.Context, input *EventsDeleteInput, keyHash, requestHash string, selectEvents func(context.Context) ([]eventDeletionTarget, error), authority hostedMutationAuthority) (EventDeletionResponse, error) {
	var response EventDeletionResponse
	if authority == nil {
		return response, apierr.New(503, "hosted mutation authority unavailable")
	}
	project := input.ID
	if input.Body.DryRun {
		if err := f.publisher.transaction(ctx, func(tx *sql.Tx) error {
			tenant, err := lockActiveHostedProject(ctx, tx, project)
			if err != nil {
				return err
			}
			return authority(ctx, tx, project, tenant, "delete-events")
		}); err != nil {
			return response, hostedEventDeletionError(err)
		}
		selected, err := selectEvents(ctx)
		if err != nil {
			return response, err
		}
		return checkEventDeletionSelection(input.Body, project, selected)
	}
	err := f.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
		authorize := func(ctx context.Context, tx *sql.Tx) error {
			if err := lockActiveHostedMutationProject(ctx, tx, project, tenant); err != nil {
				return err
			}
			return authority(ctx, tx, project, tenant, "delete-events")
		}
		if err := session.RunControlTx(ctx, authorize); err != nil {
			return err
		}
		i, err := hostedMutationBy(ctx, f.publisher.control, scope, tenant, "key_hash", keyHash)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if i.requestHash != requestHash || !i.matchedCount.Valid {
				return errHostedMutationConflict
			}
			if i.state != "staging" {
				response = i.eventDeletionResponse()
				return nil
			}
		}
		fence := session.Fence(scope.StreamID)
		if err := f.native.AdvanceWriteFence(ctx, fence); err != nil {
			return err
		}
		if err := f.reconcileHostedWrites(ctx, session, scope, fence, tenant); err != nil {
			return err
		}
		if i.id == "" {
			if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				cutoff := time.Now().UTC().Add(-hostedMutationStagingLifetime).Format(time.RFC3339Nano)
				return retireHostedMutationStagingTx(ctx, tx, scope, tenant, cutoff)
			}); err != nil {
				return err
			}
		}
		selected, err := selectEvents(ctx)
		if err != nil {
			return err
		}
		if _, err := checkEventDeletionSelection(input.Body, project, selected); err != nil {
			return err
		}
		targets := make([]lakeapi.HostedEventDeletionTarget, len(selected))
		var expected uint64
		for k, target := range selected {
			targets[k] = lakeapi.HostedEventDeletionTarget{UUID: target.UUID, ExpectedRevision: target.Revision}
			expected = max(expected, target.Revision)
		}
		slices.SortFunc(targets, func(a, b lakeapi.HostedEventDeletionTarget) int { return cmp.Compare(a.UUID, b.UUID) })
		if expected >= math.MaxInt64 {
			return errHostedMutationCapacity
		}
		mutationBody := func(i hostedMutationIntent) lakeapi.HostedMutationInput {
			return lakeapi.HostedMutationInput{Scope: scope, TenantID: tenant, MutationID: i.id, ExpectedRevision: expected,
				Deletion: &lakeapi.HostedEventDeletion{Revision: uint64(i.revision), Targets: targets}}
		}
		if i.id != "" {
			fingerprint, err := lakeapi.HostedMutationFingerprint(mutationBody(i))
			if err != nil {
				return err
			}
			if fingerprint != i.body.SHA256 {
				// Staging authorized no native work. Keep its object charge and
				// allow this confirmed selection to reserve a new immutable body.
				if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
					return abortHostedMutationStagingTx(ctx, tx, i)
				}); err != nil {
					return err
				}
				i = hostedMutationIntent{}
			}
		}
		if i.id == "" {
			counter, err := hostedMutationProjectRevision(ctx, f.publisher.control, scope, tenant)
			if err != nil {
				return err
			}
			latest := max(counter, int64(expected))
			if latest == math.MaxInt64 {
				return errHostedMutationCapacity
			}
			i = hostedMutationIntent{
				scope: scope, tenant: tenant, id: eventapi.NewID(), state: "staging", keyHash: keyHash,
				requestHash: requestHash, actorHash: hash(principal(ctx).ID), operation: "delete",
				expected: int64(expected), revision: latest + 1, matchedCount: sql.NullInt64{Int64: int64(len(selected)), Valid: true},
				createdAt: time.Now().UTC().Format(time.RFC3339Nano),
			}
		}
		body := mutationBody(i)
		if !body.Deletion.Valid(expected) {
			return errHostedMutationConflict
		}
		i, err = f.stageHostedMutation(ctx, session, i, body)
		if err != nil {
			return err
		}
		i, err = commitHostedMutationIntent(ctx, session, i, authorize)
		if err != nil {
			return err
		}
		response = i.eventDeletionResponse()
		return nil
	})
	return response, hostedEventDeletionError(err)
}

func (f *hostedLiveFlusher) hostedEventDeletionStatus(ctx context.Context, project, id string) (EventDeletionResponse, error) {
	var response EventDeletionResponse
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
		if !i.matchedCount.Valid || i.state == "staging" || i.state == "aborted" {
			return sql.ErrNoRows
		}
		response = i.eventDeletionResponse()
		return nil
	})
	return response, hostedEventDeletionError(err)
}

func hostedEventDeletionError(err error) error {
	if errors.Is(err, errHostedProjectFenced) {
		return apierr.New(404, "project not found")
	}
	return hostedMutationError(err)
}

func (i hostedMutationIntent) eventDeletionResponse() EventDeletionResponse {
	response := EventDeletionResponse{
		MutationID: i.id, ProjectID: i.scope.Key.ProjectID, State: i.state, MatchedCount: int(i.matchedCount.Int64),
		CreatedAt: i.createdAt, AppliedAt: i.appliedAt, ErrorCode: i.errorCode,
	}
	switch i.state {
	case "ready":
		response.State = "pending"
	case "applied":
		response.AppliedCount = response.MatchedCount
	case "failed":
		response.FailedCount = response.MatchedCount
	}
	return response
}
