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

type hostedWebhookRoundTrip func(*http.Request) (*http.Response, error)

func (f hostedWebhookRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestDuckLakeS3SharedPostgresHostedWebhookSendGate(t *testing.T) {
	t.Parallel()
	skipInShort(t)
	ctx := t.Context()
	fixture := nativeS3Fixture(t)
	a, b := sharedNativeJourneyPair(t, fixture, func(c *Config) {
		c.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999", AllowLegacyKeys: true}
	})
	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "hosted send")
	native := a.service.lake.(*lakeapi.DuckLakeStore)
	key := lakeapi.ProjectWriteKey{SourceID: native.Config.SharedSourceID, ProjectID: project}
	scope, err := native.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(a.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	sibling, err := lakeapi.NewPostgresProjectCoordinator(b.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	f, err := newHostedLiveFlusherFixture(a.service.control, native, coordinator, a.service.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	accountBody, _ := json.Marshal(TenantServiceAccountSpec{Name: "webhook creator", Projects: []string{project}, Permissions: []string{"pipelines:write", "events:read"}})
	account := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants/default/service-accounts", adminToken, accountBody, nil, 201)
	keyBody, _ := json.Marshal(ServiceKeySpec{Name: "webhook key", Audience: "api", Projects: []string{project}, Permissions: []string{"pipelines:write", "events:read"}})
	token := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keyBody, nil, 201)
	creator := token["id"].(string)
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
	event := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: "page", DistinctID: "person", Properties: json.RawMessage(`{"x":1}`), Context: json.RawMessage(`{}`), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	if err := f.FlushLive(ctx, acceptance.FlushBatch{ID: eventapi.NewID(), ProjectID: project, Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: event}, AdmittedAt: time.Now().UTC()}}}); err != nil {
		t.Fatal(err)
	}
	db := a.service.control
	if _, err := db.Exec(`CREATE TABLE test_delivery_intents(ready BOOLEAN NOT NULL); INSERT INTO test_delivery_intents VALUES(TRUE);
 CREATE TABLE test_delivery_activity(project TEXT,attempt TEXT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	var gotID, gotBody string
	calls := 0
	client := &http.Client{Transport: hostedWebhookRoundTrip(func(r *http.Request) (*http.Response, error) {
		gateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		other, err := sibling.Acquire(gateCtx, key)
		if err != nil {
			t.Error("project session held during HTTP")
			return nil, err
		}
		if err := other.Release(); err != nil {
			t.Error(err)
			return nil, err
		}
		var activity int
		if err := db.QueryRowContext(gateCtx, `SELECT count(*) FROM test_delivery_activity WHERE project=$1`, project).Scan(&activity); err != nil || activity != 1 {
			t.Error("missing committed activity during send", activity, err)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		id := r.Header.Get("Idempotency-Key")
		calls++
		status := http.StatusNoContent
		if calls == 1 {
			gotID, gotBody = id, string(body)
			status = http.StatusServiceUnavailable
		} else if gotID != id || gotBody != string(body) {
			t.Error("retry changed stable webhook identity or body")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	})}
	w := newHostedPipelineDelivery(f, func(context.Context, *sql.Tx, string, string, string, string, string) (output.WebhookPipelineSecret, error) {
		return output.WebhookPipelineSecret{Endpoint: "https://receiver.example/webhook"}, nil
	}, output.HostedWebhookLimits{MaxAttempts: 3, Timeout: 3 * time.Second}, client)
	w.deleteGate = func(ctx context.Context, tx *sql.Tx, _ lakeapi.HostedProjectScope, _ string) (bool, error) {
		var ready bool
		err := tx.QueryRowContext(ctx, `SELECT ready FROM test_delivery_intents`).Scan(&ready)
		return ready, err
	}
	w.beginAttemptTx = func(ctx context.Context, tx *sql.Tx, project, id string) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO test_delivery_activity VALUES($1,$2)`, project, id)
		return err
	}
	w.endAttempt = func(ctx context.Context, project, id string) error {
		_, err := db.ExecContext(ctx, `DELETE FROM test_delivery_activity WHERE project=$1 AND attempt=$2`, project, id)
		return err
	}
	if progress, err := w.Step(ctx); err != nil || progress || calls != 0 {
		t.Fatal(progress, err, calls)
	}
	if _, err := db.Exec(`UPDATE test_delivery_intents SET ready=FALSE; UPDATE pipeline_effect_jobs SET next_attempt_at=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if progress, err := w.Step(ctx); err != nil || !progress || calls != 1 {
		t.Fatal(progress, err, calls)
	}
	if gotID == "" || !strings.Contains(gotBody, event.UUID) {
		t.Fatal("missing canonical identifiers")
	}
	if _, err := db.Exec(`UPDATE pipeline_effect_jobs SET next_attempt_at=clock_timestamp()-interval '1 second';
 UPDATE pipeline_effect_cooldowns SET not_before=clock_timestamp()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	if progress, err := w.Step(ctx); err != nil || !progress || calls != 2 {
		t.Fatal(progress, err, calls)
	}
	var success, activity int
	err = db.QueryRow(`SELECT sum(webhook_success),(SELECT count(*) FROM test_delivery_activity) FROM usage_rollups WHERE project_id=$1`, project).Scan(&success, &activity)
	if err != nil || success != 1 || activity != 0 {
		t.Fatal(success, activity, err)
	}
}
