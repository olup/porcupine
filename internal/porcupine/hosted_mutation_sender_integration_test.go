package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/acceptance"
	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestDuckLakeS3SharedPostgresHostedMutationSenderOrdering(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	a, _ := sharedNativeJourneyPair(t, fixture, func(c *Config) {
		c.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999"}
	})
	admin := accountTestToken(t, a.service)
	project, _ := sharedJourneyProjectAs(t, a.host.URL, admin, defaultTenantID, "hosted send")
	native := a.service.lake
	key := lakeapi.ProjectWriteKey{SourceID: native.Config.SharedSourceID, ProjectID: project}
	scope, err := native.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(a.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	effects, err := output.NewPGPipelineEffects(hostedPipelineNativeAuthority{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := newHostedLiveFlusher(a.service.control, native, coordinator, a.service.usage.store, effects, hostedPublisherHooks{
		cancelMutationWebhook: effects.CancelHostedWebhookEventTx,
	})
	if err != nil {
		t.Fatal(err)
	}
	creator, _ := sharedJourneyServiceKey(t, a.host.URL, admin, defaultTenantID, "webhook creator", project, []string{"pipelines:write", "events:read"})
	raw, _ := json.Marshal(delivery.WebhookRule{DeliveryMode: "event"})
	ref, err := native.Remote.PutImmutableEffectObject(ctx, scope, eventapi.NewID(), raw)
	if err != nil {
		t.Fatal(err)
	}
	s, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	err = s.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if err := f.publisher.effects.ConfigureCapacityTx(ctx, tx, v[0], v[1], output.PipelineDemand{References: 100, QueuedBytes: 1 << 20, MetadataRows: 1000, ArtifactBytes: 128 << 20, ProofSlots: 4}); err != nil {
				return err
			}
		}
		t := output.PipelineTarget{ID: "pipeline", Incarnation: "incarnation", RuleRevision: "rule", DestinationRevision: "destination", CreatorID: creator, Rule: pipelineRef(ref)}
		return f.publisher.effects.CompareAndSwapDefinitionTx(ctx, tx, nil, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "active", Target: t})
	})
	if err := errors.Join(err, s.Release()); err != nil {
		t.Fatal(err)
	}
	a.service.hostedMutations = f
	_, token := sharedJourneyServiceKey(t, a.host.URL, admin, defaultTenantID, "event deleter", project, []string{"events:delete"})
	actor, err := a.service.authenticateResource(ctx, token, a.service.resources.apiResource)
	if err != nil {
		t.Fatal(err)
	}
	actorContext := context.WithValue(ctx, actorContextKey{}, actor)
	acceptDelete := func(uuid string) {
		t.Helper()
		accepted, err := a.service.deleteEvent(actorContext, &EventDeleteInput{ID: project, UUID: uuid, Body: EventDeleteBody{IdempotencyKey: "delete-" + uuid, ExpectedRevision: "0"}})
		if err != nil || accepted.Body.State != "pending" {
			t.Fatal(accepted, err)
		}
	}
	flush := func(uuid string) {
		t.Helper()
		event := eventapi.Event{ProjectID: project, UUID: uuid, Event: "page", DistinctID: "person", Properties: json.RawMessage(`{"x":1}`), Context: json.RawMessage(`{}`), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		if err := f.FlushLive(ctx, acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project, Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: event}, AdmittedAt: time.Now().UTC()}}}); err != nil {
			t.Fatal(err)
		}
	}
	deleteFirst, freezeFirst := eventapi.NewID(), eventapi.NewID()
	db := a.service.control
	if _, err := db.Exec(`CREATE TABLE test_delivery_activity(project TEXT,attempt TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := &http.Client{Transport: hostedWebhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if r.Header.Get("Idempotency-Key") == "" || !strings.Contains(string(raw), freezeFirst) || strings.Contains(string(raw), deleteFirst) {
			t.Fatal("incorrect immutable body crossed the send boundary")
		}
		// The frozen transition committed before the transport began. A delete
		// can proceed on the same project while this transport still runs.
		acceptDelete(freezeFirst)
		if err := f.RecoverProject(ctx, key); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	destination := func(context.Context, *sql.Tx, string, string, string, string, string) (output.WebhookPipelineSecret, error) {
		return output.WebhookPipelineSecret{Endpoint: "https://receiver.example/webhook"}, nil
	}
	beginAttemptTx := func(ctx context.Context, tx *sql.Tx, project, id string) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO test_delivery_activity VALUES($1,$2)`, project, id)
		return err
	}
	endAttempt := func(ctx context.Context, project, id string) error {
		_, err := db.ExecContext(ctx, `DELETE FROM test_delivery_activity WHERE project=$1 AND attempt=$2`, project, id)
		return err
	}
	limits := output.HostedWebhookLimits{MaxAttempts: 3, Timeout: 3 * time.Second}
	w := newHostedPipelineDelivery(f, destination, limits, client, hostedReadyDelete, beginAttemptTx, endAttempt)
	flush(deleteFirst)
	acceptDelete(deleteFirst)
	if _, err := w.Step(ctx); err != nil || calls != 0 {
		t.Fatal("Ready delete allowed HTTP", calls, err)
	}
	flush(freezeFirst)
	if _, err := w.Step(ctx); err != nil || calls != 1 {
		t.Fatal("frozen send did not complete once", calls, err)
	}
	if _, err := w.Step(ctx); err != nil || calls != 1 {
		t.Fatal("deleted frozen body attempted a retry", calls, err)
	}
	var retired, tickets int
	err = db.QueryRowContext(ctx, `SELECT count(*),(SELECT count(*) FROM test_delivery_activity)
FROM pipeline_effect_jobs WHERE project_id=$1 AND kind='delivery' AND state='retired' AND error_code='event_deleted'`, project).Scan(&retired, &tickets)
	if err != nil || retired != 2 || tickets != 0 {
		t.Fatal(retired, tickets, err)
	}
}
