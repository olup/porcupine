package porcupine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (s *Service) deleteEventsStandalone(ctx context.Context, input *EventsDeleteInput, keyHash, requestHash string, selectEvents func(context.Context) ([]eventDeletionTarget, error)) (EventDeletionResponse, error) {
	if input.Body.DryRun {
		selected, err := selectEvents(ctx)
		if err != nil {
			return EventDeletionResponse{}, err
		}
		return checkEventDeletionSelection(input.Body, input.ID, selected)
	}
	if prior, err := replayStandaloneEventDeletion(ctx, s.db, input.ID, keyHash, requestHash); prior.MutationID != "" || err != nil {
		return prior, err
	}
	selected, selectionErr := selectEvents(ctx)
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()
	if prior, err := replayStandaloneEventDeletion(ctx, s.db, input.ID, keyHash, requestHash); prior.MutationID != "" || err != nil {
		return prior, err
	}
	if selectionErr != nil {
		return EventDeletionResponse{}, selectionErr
	}
	response, err := checkEventDeletionSelection(input.Body, input.ID, selected)
	if err != nil {
		return EventDeletionResponse{}, err
	}
	actor := principal(ctx)
	if err := s.requireProjectActive(ctx, input.ID); err != nil {
		return EventDeletionResponse{}, err
	}
	if err := s.ensureProjectLocal(ctx, input.ID, actor); err != nil {
		return EventDeletionResponse{}, err
	}
	tx, err := s.beginMutationWrite(ctx)
	if err != nil {
		return EventDeletionResponse{}, err
	}
	defer tx.Rollback()
	if prior, err := replayStandaloneEventDeletion(ctx, tx, input.ID, keyHash, requestHash); prior.MutationID != "" || err != nil {
		return prior, err
	}
	var active bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM projects WHERE id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=$1))`, input.ID).Scan(&active); err != nil {
		return EventDeletionResponse{}, apierr.New(503, "project metadata unavailable")
	}
	if !active {
		return EventDeletionResponse{}, apierr.New(404, "project not found")
	}
	var newest uint64
	for _, target := range selected {
		if err := checkJournalMutationHead(ctx, tx, input.ID, target.UUID, target.Revision); err != nil {
			return EventDeletionResponse{}, err
		}
		newest = max(newest, target.Revision)
	}
	var bytes int64
	if len(selected) != 0 {
		bytes, err = s.checkMutationQueue(ctx, tx, len(selected))
		if err != nil {
			return EventDeletionResponse{}, err
		}
		if err := advanceMutationSequence(ctx, tx, newest); err != nil {
			return EventDeletionResponse{}, err
		}
	}
	response.MutationID, response.CreatedAt = eventapi.NewID(), now()
	response.State = "pending"
	if _, err := tx.ExecContext(ctx, `INSERT INTO event_deletions(mutation_id,project_id,actor_id,idempotency_key_hash,request_hash,matched_count,created_at) VALUES($1,$2,$3,$4,$5,$6,$7)`, response.MutationID, input.ID, actor.ID, keyHash, requestHash, response.MatchedCount, response.CreatedAt); err != nil {
		return EventDeletionResponse{}, apierr.New(503, "mutation storage unavailable")
	}
	for _, target := range selected {
		projection := eventTombstone(lakeapi.EventProjection{Event: eventapi.Event{
			ID: target.ID, ProjectID: input.ID, UUID: target.UUID, ReceivedAt: target.ReceivedAt, Imported: target.Imported,
		}})
		member := eventapi.NewID()
		if _, err := s.appendJournalMutation(ctx, tx, journalMutation{
			projection: projection, id: member, actorID: actor.ID, operation: "delete", deletionID: &response.MutationID,
			keyHash: "query-delete:" + member, requestHash: requestHash, expected: target.Revision, createdAt: response.CreatedAt,
		}, &bytes); err != nil {
			return EventDeletionResponse{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return EventDeletionResponse{}, apierr.New(503, "mutation storage unavailable")
	}
	if len(selected) == 0 {
		response.State, response.AppliedAt = "applied", response.CreatedAt
	} else {
		s.wakeDelivery()
	}
	return response, nil
}

func replayStandaloneEventDeletion(ctx context.Context, query oauthQueryer, project, key, requestHash string) (EventDeletionResponse, error) {
	previous, previousHash, err := findStandaloneEventDeletion(ctx, query, project, "idempotency_key_hash", key)
	if err == nil {
		if previousHash != requestHash {
			return EventDeletionResponse{}, apierr.New(409, "idempotency_key was used for a different request")
		}
		return previous, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return EventDeletionResponse{}, apierr.New(503, "mutation storage unavailable")
	}
	var used bool
	if err := query.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM mutation_audit WHERE project_id=$1 AND idempotency_key_hash=$2)`, project, key).Scan(&used); err != nil {
		return EventDeletionResponse{}, apierr.New(503, "mutation storage unavailable")
	}
	if used {
		return EventDeletionResponse{}, apierr.New(409, "idempotency_key was used for a different request")
	}
	return EventDeletionResponse{}, nil
}

func (s *Service) standaloneEventDeletionStatus(ctx context.Context, project, id string) (EventDeletionResponse, error) {
	response, _, err := findStandaloneEventDeletion(ctx, s.db, project, "mutation_id", id)
	if errors.Is(err, sql.ErrNoRows) {
		return EventDeletionResponse{}, apierr.New(404, "event deletion not found")
	}
	if err != nil {
		return EventDeletionResponse{}, apierr.New(503, "mutation storage unavailable")
	}
	return response, nil
}

func findStandaloneEventDeletion(ctx context.Context, query oauthQueryer, project, field, value string) (EventDeletionResponse, string, error) {
	var response EventDeletionResponse
	var requestHash, acknowledgedAt string
	// The field is selected by the status and replay callers.
	err := query.QueryRowContext(ctx, `SELECT d.mutation_id,d.project_id,d.request_hash,d.matched_count,d.created_at,
(SELECT count(*) FROM mutation_audit m WHERE m.deletion_id=d.mutation_id AND m.project_id=d.project_id AND m.state='applied'),
coalesce((SELECT m.applied_at FROM mutation_audit m WHERE m.deletion_id=d.mutation_id AND m.project_id=d.project_id ORDER BY m.seq DESC LIMIT 1),'')
FROM event_deletions d WHERE d.project_id=$1 AND d.`+field+`=$2`, project, value).Scan(
		&response.MutationID, &response.ProjectID, &requestHash, &response.MatchedCount,
		&response.CreatedAt, &response.AppliedCount, &acknowledgedAt)
	if err != nil {
		return EventDeletionResponse{}, "", err
	}
	response.State = "pending"
	if response.MatchedCount == 0 {
		response.State, response.AppliedAt = "applied", response.CreatedAt
	} else if response.AppliedCount == response.MatchedCount {
		response.State, response.AppliedAt = "applied", acknowledgedAt
	}
	return response, requestHash, nil
}
