package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedMutationRecovery(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	storage := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, storage)
	for _, n := range []*sharedJourneyNode{a, b} {
		n.service.usage.cancel()
		<-n.service.usage.done
		store := n.service.lake.(*lakeapi.DuckLakeStore)
		coordinator, err := lakeapi.NewPostgresProjectCoordinator(n.service.control, lakeapi.DefaultProjectCoordinatorConfig())
		if err != nil {
			t.Fatal(err)
		}
		f, err := newHostedLiveFlusher(n.service.control, store, coordinator, n.service.usage.store)
		if err != nil {
			t.Fatal(err)
		}
		n.service.hostedMutations = f
	}
	fa, fb := a.service.hostedMutations, b.service.hostedMutations
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "mutations A")
	otherTenant := sharedJourneyHTTP(t, b.host.URL, "POST", "/api/v1/tenants", adminToken, []byte(`{"name":"Mutation tenant B"}`), nil, 201)["tenant"].(map[string]any)["id"].(string)
	other, _ := sharedJourneyProject(t, b.host.URL, otherTenant, "mutations B")
	actor, token := sharedJourneyKey(t, a.host.URL, project, []string{"events:read", "events:edit", "events:delete"})
	actorContext := context.WithValue(t.Context(), actorContextKey{}, auth.Principal{ID: actor, Projects: []string{project}, Permissions: []string{"events:read", "events:edit", "events:delete"}})
	ctx := t.Context()
	scope, err := fa.native.InitializeHostedProject(ctx, lakeapi.ProjectWriteKey{SourceID: fa.native.Config.SharedSourceID, ProjectID: project})
	if err != nil {
		t.Fatal(err)
	}
	otherScope, err := fb.native.InitializeHostedProject(ctx, lakeapi.ProjectWriteKey{SourceID: fb.native.Config.SharedSourceID, ProjectID: other})
	if err != nil {
		t.Fatal(err)
	}
	limits := output.PipelineDemand{References: 10, QueuedBytes: 1 << 20, MetadataRows: 100, ArtifactBytes: 1 << 20, ProofSlots: 2}
	if err := fa.publisher.transaction(ctx, func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"instance", ""}, {"tenant", defaultTenantID}, {"tenant", otherTenant}, {"project", project}, {"project", other}} {
			if err := fa.publisher.effects.ConfigureCapacityTx(ctx, tx, v[0], v[1], limits); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	uuid, laterUUID := eventapi.NewID(), eventapi.NewID()
	live := func(project, uuid string) bufferedFlushBatch {
		e := eventapi.Event{ProjectID: project, UUID: uuid, Event: "original", DistinctID: "browser", Properties: json.RawMessage(`{"private":true}`), Context: json.RawMessage(`{}`), Timestamp: "2026-11-17T01:00:00Z", ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		return bufferedFlushBatch{id: eventapi.NewID(), projectID: project, events: []bufferedLiveEvent{{input: validatedLiveEvent{event: e}, admittedAt: time.Now().UTC()}}}
	}
	for _, batch := range []bufferedFlushBatch{live(project, uuid), live(project, laterUUID), live(other, uuid)} {
		if err := fa.FlushLive(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	_, err = a.service.editEvent(actorContext, mutationEdit(other, uuid, "forbidden", "0"))
	mutationErrorStatus(t, err, 403)
	edit := mutationEdit(project, uuid, "first", "0")
	first, err := a.service.editEvent(actorContext, edit)
	if err != nil || first.Body.State != "pending" || first.Body.Revision != "1" {
		t.Fatal(first, err)
	}
	repeat, err := b.service.editEvent(actorContext, edit)
	if err != nil || repeat.Body != first.Body {
		t.Fatal(repeat, err)
	}
	requestBody, err := json.Marshal(edit.Body)
	if err != nil {
		t.Fatal(err)
	}
	httpRetry := sharedJourneyHTTP(t, a.host.URL, "PATCH", "/api/v1/projects/"+project+"/events/"+uuid, token, requestBody, nil, 202)
	if httpRetry["mutation_id"] != first.Body.MutationID || httpRetry["revision"] != "1" || httpRetry["state"] != "pending" {
		t.Fatal(httpRetry)
	}
	sharedJourneyHTTP(t, b.host.URL, "PATCH", "/api/v1/projects/"+other+"/events/"+uuid, token, requestBody, nil, 403)
	changed := mutationEdit(project, uuid, "first", "0")
	changed.Body.Snapshot.Properties["amount"] = 43
	_, err = b.service.editEvent(actorContext, changed)
	mutationErrorStatus(t, err, 409)
	_, err = a.service.editEvent(actorContext, mutationEdit(project, laterUUID, "blocked", "0"))
	mutationErrorStatus(t, err, 409)
	current, err := b.service.getEvent(actorContext, &EventGetInput{ID: project, UUID: uuid})
	if err != nil || !current.Body.Pending || current.Body.Event.Event != "corrected" {
		t.Fatal(current, err)
	}
	before, err := fb.native.LoadHostedEvent(ctx, scope, uuid)
	if err != nil || before.Event.Event != "original" {
		t.Fatal(before, err)
	}
	i, err := hostedMutationBy(ctx, fa.publisher.control, scope, defaultTenantID, "mutation_id", first.Body.MutationID)
	if err != nil {
		t.Fatal(err)
	}
	body, err := fa.publisher.readHostedMutation(ctx, i)
	if err != nil || body.Projection.OriginalSnapshot == "" || body.Projection.ID != before.ID {
		t.Fatal(body, err)
	}
	if _, err := fa.publisher.objects.ReadImmutableEffectObject(ctx, scope, i.body); err == nil {
		t.Fatal("effect storage accepted a mutation object")
	}
	if _, err := fa.publisher.objects.ReadImmutableMutationObject(ctx, otherScope, i.body); err == nil {
		t.Fatal("mutation storage accepted the other project's object")
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(storage.storage.Bucket), Key: aws.String(i.body.Key)}); err != nil {
		t.Fatal(err)
	}
	_, err = b.service.getEvent(actorContext, &EventGetInput{ID: project, UUID: laterUUID})
	mutationErrorStatus(t, err, 503)
	if restored, err := fa.publisher.objects.PutImmutableMutationObject(ctx, scope, i.id, raw); err != nil || restored != i.body {
		t.Fatal("restore immutable staging body", restored, err)
	}
	var rawProof lakeapi.NativeCommitEvidence
	var commitFence lakeapi.ProjectWriteFence
	err = fa.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, _ string) error {
		commitFence = session.Fence(scope.StreamID)
		if err := fa.native.AdvanceWriteFence(ctx, commitFence); err != nil {
			return err
		}
		var err error
		rawProof, err = fa.native.CommitHostedMutation(ctx, commitFence, body)
		return err
	})
	if err != nil || rawProof.Sequence != 3 || rawProof.AcceptedLive != 0 || rawProof.Totals[0].Live != 2 {
		t.Fatal(rawProof, err)
	}
	// A cold worker publishes the committed original after response loss.
	if err := fb.RecoverProject(ctx, scope.Key); err != nil {
		t.Fatal(err)
	}
	after, err := b.service.getEventMutation(actorContext, &EventMutationInput{ID: project, MutationID: first.Body.MutationID})
	if err != nil || after.Body.State != "applied" {
		t.Fatal(after, err)
	}
	_, err = b.service.getEventMutation(actorContext, &EventMutationInput{ID: other, MutationID: first.Body.MutationID})
	mutationErrorStatus(t, err, 404)
	assertDeleteGate := func(project string, want bool) {
		t.Helper()
		err := fb.withHostedMutationProject(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) error {
			return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				blocked, err := hostedReadyDelete(ctx, tx, scope, tenant)
				if err == nil && blocked != want {
					t.Fatalf("sender delete gate for %s = %v, want %v", project, blocked, want)
				}
				return err
			})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertDeleteGate(project, false)
	pendingDelete, err := a.service.deleteEvent(actorContext, &EventDeleteInput{ID: project, UUID: uuid, Body: EventDeleteBody{ExpectedRevision: "1", IdempotencyKey: "delete"}})
	if err != nil || pendingDelete.Body.State != "pending" {
		t.Fatal(pendingDelete, err)
	}
	assertDeleteGate(project, true)
	assertDeleteGate(other, false)
	_, err = b.service.getEvent(actorContext, &EventGetInput{ID: project, UUID: uuid})
	mutationErrorStatus(t, err, 410)
	if err := fb.RecoverProject(ctx, scope.Key); !errors.Is(err, errHostedMutationBusy) {
		t.Fatal("delete bypassed sender cancellation gate", err)
	}
	if err := fb.withHostedMutationCut(ctx, project, func(context.Context, *lakeapi.ProjectWriteSession, lakeapi.HostedProjectScope, string, int64) error {
		t.Fatal("snapshot selection bypassed Ready delete")
		return nil
	}); !errors.Is(err, errHostedMutationBusy) {
		t.Fatal("Ready delete did not block snapshot selection", err)
	}
	otherEvent, err := fb.native.LoadHostedEvent(ctx, otherScope, uuid)
	if err != nil || otherEvent.Event.Event != "original" || otherEvent.Revision != 0 {
		t.Fatal(otherEvent, err)
	}

	// A revoked producer cannot receive a new acceptance, but its Ready delete
	// still drains on the worker after the sender cancellation transaction is wired.
	if _, err := fa.publisher.control.ExecContext(ctx, "UPDATE access_keys SET revoked_at=$1 WHERE id=$2", now(), actor); err != nil {
		t.Fatal(err)
	}
	var deletionParticipantCalled bool
	fb.publisher.cancelMutationWebhook = func(_ context.Context, _ *sql.Tx, got output.PipelineScope, tenant, target string) error {
		if got != pipelineScope(scope) || tenant != defaultTenantID || target != uuid {
			t.Fatal(got, tenant, target)
		}
		deletionParticipantCalled = true
		return nil
	}
	if err := fb.withHostedMutationCut(ctx, project, func(ctx context.Context, _ *lakeapi.ProjectWriteSession, s lakeapi.HostedProjectScope, tenant string, cut int64) error {
		if s != scope || tenant != defaultTenantID || cut != 2 || !deletionParticipantCalled {
			t.Fatal(s, tenant, cut, deletionParticipantCalled)
		}
		visible, err := fb.native.LoadHostedEvent(ctx, scope, uuid)
		if err == nil && !visible.IsDeleted {
			t.Fatal("snapshot cut preceded native delete")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	assertDeleteGate(project, false)
	tombstone, err := fb.native.LoadHostedEvent(ctx, scope, uuid)
	if err != nil || !tombstone.IsDeleted || tombstone.Revision != 2 || tombstone.ID != before.ID {
		t.Fatal(tombstone, err)
	}
	replay, err := fb.native.CommitHostedMutation(ctx, commitFence, body)
	if err != nil || !reflect.DeepEqual(replay, rawProof) {
		t.Fatal("original edit outcome changed after delete", replay, err)
	}
	body.Projection.DistinctID = "altered"
	if _, err := fb.native.CommitHostedMutation(ctx, commitFence, body); !errors.Is(err, lakeapi.ErrHostedMutationConflict) {
		t.Fatal("changed native input bypassed the original fingerprint", err)
	}
	body.MutationID = eventapi.NewID()
	if _, err := fb.native.CommitHostedMutation(ctx, commitFence, body); !errors.Is(err, lakeapi.ErrProjectWriteFenceStale) {
		t.Fatal("stale fence authorized a new mutation", err)
	}
	if err := fa.FlushLive(ctx, live(project, uuid)); err != nil {
		t.Fatal(err)
	}
	page, err := fb.native.PendingNativeEffects(ctx, scope, 2, 5)
	if err != nil || len(page.Evidence) != 2 || page.NativeHighWater != 4 || page.AcknowledgedThrough != 4 || page.Evidence[1].Totals[0].Live != 2 {
		t.Fatal(page, err)
	}
	revokeActor, _ := sharedJourneyKey(t, a.host.URL, project, []string{"events:edit"})
	revokeContext := context.WithValue(ctx, actorContextKey{}, auth.Principal{ID: revokeActor, Projects: []string{project}, Permissions: []string{"events:edit"}})
	var checks int
	request := mutationEdit(project, laterUUID, "revoked-during-put", "0")
	normalized, err := validateEventSnapshotActor(request.Body.Snapshot, true)
	if err != nil {
		t.Fatal(err)
	}
	keyHash := hash(request.Body.IdempotencyKey)
	_, err = fa.acceptHostedMutation(revokeContext, project, laterUUID, "edit", keyHash, hash("revoked request"), 0, &normalized, func(ctx context.Context, tx *sql.Tx, project, tenant, operation string) error {
		checks++
		if checks == 2 {
			staging, err := hostedMutationBy(ctx, fa.publisher.control, scope, defaultTenantID, "key_hash", keyHash)
			if err != nil || staging.state != "staging" {
				t.Fatal(staging, err)
			}
			if _, err := fa.publisher.readHostedMutation(ctx, staging); err != nil {
				t.Fatal("Ready authorization check preceded immutable PUT", err)
			}
			if _, err := fa.publisher.control.ExecContext(ctx, "UPDATE access_keys SET revoked_at=$1 WHERE id=$2", now(), revokeActor); err != nil {
				t.Fatal(err)
			}
		}
		return a.service.authorizeHostedMutation(ctx, tx, project, tenant, operation)
	})
	mutationErrorStatus(t, err, 403)
	var held, order int
	if err := fa.publisher.control.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(acceptance_order),0)
FROM hosted_mutation_intents WHERE project_id=$1 AND key_hash=$2 AND state='aborted' AND body_bytes>0`, project, keyHash).Scan(&held, &order); err != nil || held != 1 || order != 0 {
		t.Fatal("revoked upload lost ownership or became accepted", held, order, err)
	}

	external := newExternalAuthFixture(t)
	audience := external.s.resources.apiResource
	signed := external.token(t, "hosted-producer", defaultTenantID, "events:edit", audience, project)
	grant, err := external.s.resources.external.Verify(ctx, signed, audience, external.s.clock())
	if err != nil {
		t.Fatal(err)
	}
	outside := context.WithValue(ctx, actorContextKey{}, grant.Principal())
	checks = 0
	keyHash = hash("expired-during-put")
	_, err = fa.acceptHostedMutation(outside, project, laterUUID, "edit", keyHash, hash("external request"), 0, &normalized, func(ctx context.Context, tx *sql.Tx, project, tenant, operation string) error {
		checks++
		if checks == 2 {
			external.now.Store(external.clock.Add(6 * time.Minute).UnixMilli())
		}
		return external.s.authorizeHostedMutation(ctx, tx, project, tenant, operation)
	})
	mutationErrorStatus(t, err, 401)
	if _, err := hostedMutationBy(ctx, fa.publisher.control, scope, defaultTenantID, "key_hash", keyHash); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("expired external intent became accepted", err)
	}

	_, err = b.service.editEvent(actorContext, edit)
	mutationErrorStatus(t, err, 403)
}
