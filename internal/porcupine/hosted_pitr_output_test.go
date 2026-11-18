package porcupine

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func pitrCreateHostedOutput(t *testing.T, worker pitrWorker, project string) lakeapi.HostedProjectScope {
	t.Helper()
	ctx := t.Context()
	s := worker.service
	store := s.lake.(*lakeapi.DuckLakeStore)
	key := lakeapi.ProjectWriteKey{SourceID: store.Config.SharedSourceID, ProjectID: project}
	scope, err := store.InitializeHostedProject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(s.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	flusher, err := newHostedLiveFlusher(s.control, store, coordinator, s.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	permissions := []string{"pipelines:write", "events:read"}
	accountBody, _ := json.Marshal(TenantServiceAccountSpec{Name: "PITR output creator", Projects: []string{project}, Permissions: permissions})
	account := sharedJourneyHTTP(t, worker.URL, http.MethodPost, "/api/v1/tenants/default/service-accounts", adminToken, accountBody, nil, 201)
	keyBody, _ := json.Marshal(ServiceKeySpec{Name: "PITR output key", Audience: "api", Projects: []string{project}, Permissions: permissions})
	creator := sharedJourneyHTTP(t, worker.URL, http.MethodPost, "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keyBody, nil, 201)
	rule, _ := json.Marshal(delivery.WebhookRule{DeliveryMode: "event"})
	ref, err := store.Remote.PutImmutableEffectObject(ctx, scope, eventapi.NewID(), rule)
	if err != nil {
		t.Fatal(err)
	}
	session, err := coordinator.Acquire(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	err = flusher.publisher.transaction(ctx, func(tx *sql.Tx) error {
		limits := output.PipelineDemand{References: 100, QueuedBytes: 1 << 20, MetadataRows: 1000, ArtifactBytes: 128 << 20, ProofSlots: 4}
		for _, pair := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if err := flusher.publisher.effects.ConfigureCapacityTx(ctx, tx, pair[0], pair[1], limits); err != nil {
				return err
			}
		}
		return flusher.publisher.effects.CompareAndSwapDefinitionTx(ctx, tx, nil, output.PipelineDefinition{
			TenantID: defaultTenantID, ProjectID: project, State: "active",
			Target: output.PipelineTarget{ID: "pitr-webhook", Incarnation: "pitr-incarnation", RuleRevision: "pitr-rule", DestinationRevision: "pitr-destination", CreatorID: creator["id"].(string), Rule: pipelineRef(ref)},
		})
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	event := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: "hosted", DistinctID: "c",
		Properties: json.RawMessage("{}"), Context: json.RawMessage("{}"),
		Timestamp: now.Format(time.RFC3339Nano), ReceivedAt: now.Format(time.RFC3339Nano)}
	input, err := validateLiveEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := flusher.FlushLive(ctx, bufferedFlushBatch{id: "pitr-flush", projectID: project,
		events: []bufferedLiveEvent{{input: input, admittedAt: now}}}); err != nil {
		t.Fatal(err)
	}
	return scope
}

func pitrReadHostedOutput(t *testing.T, worker pitrWorker, scope lakeapi.HostedProjectScope) string {
	t.Helper()
	var id string
	var ref lakeapi.ImmutableObjectRef
	err := worker.service.control.QueryRowContext(t.Context(), "SELECT id,body_key,body_version,body_sha,body_bytes FROM pipeline_effect_jobs WHERE project_id=$1 AND state='pending' AND kind='delivery'", scope.Key.ProjectID).
		Scan(&id, &ref.Key, &ref.VersionID, &ref.SHA256, &ref.Bytes)
	if err != nil {
		t.Fatal("read pending hosted delivery", err)
	}
	store := worker.service.lake.(*lakeapi.DuckLakeStore)
	raw, err := store.Remote.ReadImmutableEffectObject(t.Context(), scope, ref)
	if err != nil {
		t.Fatal(err)
	}
	var payload delivery.WebhookPayload
	if err := json.Unmarshal(raw, &payload); err != nil || payload.DeliveryID != id || payload.ProjectID != scope.Key.ProjectID || payload.Event == nil || payload.Event.Event != "hosted" {
		t.Fatalf("hosted output disagrees with its durable delivery identity: %v", err)
	}
	return id
}
