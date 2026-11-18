package porcupine

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/olup/porcupine/internal/acceptance"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

func TestHostedFlusherRequiresRealProviders(t *testing.T) {
	t.Parallel()
	if _, err := newHostedLiveFlusher(nil, nil, nil, nil); err == nil {
		t.Fatal("missing providers accepted")
	}
	if err := validateAcceptanceMode(AcceptanceModeBuffered); err == nil {
		t.Fatal("production buffered mode enabled")
	}
}
func TestHostedNativeUsageUsesOriginalBucket(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 1, 0, 30, 0, 0, time.FixedZone("east", 3600))
	e := lakeapi.NativeCommitEvidence{Scope: lakeapi.HostedProjectScope{Key: lakeapi.ProjectWriteKey{ProjectID: "p"}, StreamID: "stream"}, TenantID: "t", Sequence: 7, AccountedAt: at, AcceptedLive: 1, Totals: []lakeapi.NativeSourceTotals{{Day: "2026-10-31T00:00:00Z", Revision: 7, Live: 15, Import: 3}}}
	r, err := nativeUsageReports(e)
	if err != nil || len(r) != 1 || r[0].SourceID != "stream" || r[0].Counters.Live != 15 || r[0].Counters.Import != 3 || r[0].LastObserved != "" {
		t.Fatal(r, err)
	}
	e.Totals[0].Revision++
	if _, err = nativeUsageReports(e); err == nil {
		t.Fatal("changed revision accepted")
	}
}
func TestDuckLakeS3SharedPostgresHostedFlusher(t *testing.T) {
	t.Parallel()
	runHostedFlusherFixture(t, false)
}
func TestDuckLakeS3SharedPostgresHostedFlusherVersioned(t *testing.T) {
	t.Parallel()
	runHostedFlusherFixture(t, true)
}
func runHostedFlusherFixture(t *testing.T, versioned bool) {
	skipInShort(t)
	fixture := nativeS3Fixture(t)
	if versioned {
		fixture.storage.Bucket += "-v-" + eventapi.NewID()[:8]
		if _, err := fixture.client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(fixture.storage.Bucket)}); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{Bucket: aws.String(fixture.storage.Bucket), VersioningConfiguration: &s3types.VersioningConfiguration{Status: s3types.BucketVersioningStatusEnabled}}); err != nil {
			t.Fatal(err)
		}
	}
	upstream, _ := url.Parse(fixture.storage.Endpoint)
	forward := httputil.NewSingleHostReverseProxy(upstream)
	forward.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	var rejectEffects, blockEffects, trackEffectKeys atomic.Bool
	var effectPuts, effectGets atomic.Int64
	effectKeys := make(chan string, 16)
	effectEntered, resumeEffects := make(chan struct{}), make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/effects/") {
			switch r.Method {
			case http.MethodPut:
				effectPuts.Add(1)
				if trackEffectKeys.Load() {
					select {
					case effectKeys <- r.URL.Path:
					default:
					}
				}
			case http.MethodGet:
				effectGets.Add(1)
			}
		}
		if rejectEffects.Load() && r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/effects/") {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/effects/") && blockEffects.CompareAndSwap(true, false) {
			close(effectEntered)
			select {
			case <-resumeEffects:
			case <-r.Context().Done():
				return
			}
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(proxy.Close)
	fixture.storage.Endpoint = proxy.URL
	fixture.storage.CACertFile = ""

	a, b := sharedNativeJourneyPair(t, fixture, func(c *Config) { c.OAuth = &OAuthConfig{Issuer: "http://127.0.0.1:42999", AllowLegacyKeys: true} })
	for _, n := range []*sharedJourneyNode{a, b} {
		n.service.usage.cancel()
		<-n.service.usage.done
	}

	project, ingest := sharedJourneyProject(t, a.host.URL, defaultTenantID, "hosted flush")
	sa := a.service.lake.(*lakeapi.DuckLakeStore)
	sb := b.service.lake.(*lakeapi.DuckLakeStore)
	key := lakeapi.ProjectWriteKey{SourceID: sa.Config.SharedSourceID, ProjectID: project}
	scope, err := sa.InitializeHostedProject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := lakeapi.NewPostgresProjectCoordinator(a.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	cb, err := lakeapi.NewPostgresProjectCoordinator(b.service.control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		t.Fatal(err)
	}
	fa, err := newHostedLiveFlusher(a.service.control, sa, ca, a.service.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	fb, err := newHostedLiveFlusher(b.service.control, sb, cb, b.service.usage.store)
	if err != nil {
		t.Fatal(err)
	}
	limits := output.PipelineDemand{References: 100, QueuedBytes: 1 << 20, MetadataRows: 1000, ArtifactBytes: 128 << 20, ProofSlots: 4}
	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	batch := func(id string) acceptance.FlushBatch {
		e := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: "page", DistinctID: "person", Properties: json.RawMessage(`{"a":1}`), Context: json.RawMessage(`{}`), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		return acceptance.FlushBatch{ID: id, ProjectID: project, Events: []acceptance.BufferedEvent{{Input: acceptance.Input{Event: e}, AdmittedAt: time.Now().UTC()}}}
	}
	empty := batch("empty")
	identityEvent := empty.Events[0].Input.Event
	identityEvent.UUID = eventapi.NewID()
	identityEvent.Event = "$identify"
	identityEvent.Properties = json.RawMessage(`{"$set":{"plan":"team"}}`)
	identityInput, er := acceptance.ValidateInput(identityEvent)
	if er != nil {
		t.Fatal(er)
	}
	empty.Events = append(empty.Events, acceptance.BufferedEvent{Input: identityInput, AdmittedAt: empty.Events[0].AdmittedAt})
	snapshot := func() int64 {
		t.Helper()
		p, release, err := sa.LeaseProject(t.Context(), project)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		var id int64
		if err := p.DB.QueryRowContext(t.Context(), `SELECT max(snapshot_id) FROM ducklake_snapshots('lake')`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	before := snapshot()
	if err = fa.FlushLive(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	if got := snapshot() - before; got != 1 {
		t.Fatalf("clean live flush advanced %d native snapshots, want 1", got)
	}
	if err = fb.FlushLive(t.Context(), empty); err != nil {
		t.Fatal("cold stable replay", err)
	}
	var n int
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_cleanup WHERE project_id=$1`, project).Scan(&n); err != nil || n != 0 {
		t.Fatal("empty output left cleanup", n, err)
	}

	accountBody, _ := json.Marshal(TenantServiceAccountSpec{Name: "pipeline creator", Projects: []string{project}, Permissions: []string{"pipelines:write", "events:read"}})
	account := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants/default/service-accounts", adminToken, accountBody, nil, 201)
	keyBody, _ := json.Marshal(ServiceKeySpec{Name: "pipeline native credential", Audience: "api", Projects: []string{project}, Permissions: []string{"pipelines:write", "events:read"}})
	nativeKey := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants/default/service-accounts/"+account["client_id"].(string)+"/api-keys", adminToken, keyBody, nil, 201)
	creator := nativeKey["id"].(string)

	// The same authority adapter must reject parent/key revocation, grant
	// reduction and a different tenant after acquiring real durable row locks.
	sessionCheck, er := ca.Acquire(t.Context(), key)
	if er != nil {
		t.Fatal(er)
	}
	for _, check := range []struct {
		sql, id, tenant string
		want            bool
	}{
		{tenant: defaultTenantID, want: true},
		{tenant: "other", want: false},
		{sql: `UPDATE access_keys SET revoked_at='revoked' WHERE id=$1`, id: creator, tenant: defaultTenantID},
		{sql: `UPDATE access_keys SET revoked_at='revoked' WHERE id=$1`, id: account["client_id"].(string), tenant: defaultTenantID},
		{sql: `UPDATE access_keys SET permissions='["events:read"]' WHERE id=$1`, id: account["client_id"].(string), tenant: defaultTenantID},
	} {
		tx, er := a.service.control.Begin()
		if er != nil {
			t.Fatal(er)
		}
		if check.sql != "" {
			if _, er = tx.Exec(check.sql, check.id); er != nil {
				t.Fatal(er)
			}
		}
		ok, er := (hostedPipelineNativeAuthority{}).ResolveCreatorTx(t.Context(), tx, check.tenant, project, creator)
		tx.Rollback()
		if er != nil || ok != check.want {
			t.Fatal("native execution authority", ok, er, check)
		}
	}
	sessionCheck.Release()

	raw, _ := json.Marshal(delivery.WebhookRule{DeliveryMode: "event"})
	ref, err := sa.Remote.PutImmutableEffectObject(t.Context(), scope, eventapi.NewID(), raw)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		return fa.publisher.effects.CompareAndSwapDefinitionTx(t.Context(), tx, nil, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "active", Target: output.PipelineTarget{ID: "hook", Incarnation: "inc", RuleRevision: "rule", DestinationRevision: "dest", CreatorID: creator, Rule: pipelineRef(ref)}})
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}
	// A real 100-target plan can exceed its preparation memory bound before the
	// numeric reservation. Refuse it before any PUT, with no native growth or
	// unresolved reservation, rather than encoding all fanout then rejecting.
	generous := output.PipelineDemand{References: 1000, QueuedBytes: 256 << 20, MetadataRows: 10000, ArtifactBytes: 256 << 20, ProofSlots: 10}
	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], generous); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	fanout := make([]output.PipelineTarget, 99)
	session, err = ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for i := range fanout {
			fanout[i] = output.PipelineTarget{ID: fmt.Sprintf("fanout-%02d", i), Incarnation: fmt.Sprintf("fanout-inc-%02d", i), RuleRevision: "rule", DestinationRevision: "dest", CreatorID: creator, Rule: pipelineRef(ref)}
			if e := fa.publisher.effects.CompareAndSwapDefinitionTx(t.Context(), tx, nil, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "active", Target: fanout[i]}); e != nil {
				return e
			}
		}
		return nil
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}
	oversized := batch("prepared-byte-cap")
	oversized.Events[0].Input.Event.Properties = json.RawMessage(`{"value":"` + strings.Repeat("x", 900000) + `"}`)
	putsBefore := effectPuts.Load()
	if err = fa.FlushLive(t.Context(), oversized); !errors.Is(err, output.ErrPipelineCapacity) || !errors.Is(err, acceptance.ErrBatchTooLarge) {
		t.Fatal("oversized preparation did not report a permanent pre-commit capacity error", err)
	}
	if effectPuts.Load() != putsBefore {
		t.Fatal("preparation bound uploaded an object before refusal")
	}
	if _, err = fa.publisher.reservation(t.Context(), scope); !errors.Is(err, sql.ErrNoRows) {
		t.Fatal("preparation overflow retained a reservation", err)
	}
	session, err = ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, target := range fanout {
			if e := fa.publisher.effects.CompareAndSwapDefinitionTx(t.Context(), tx, &target, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "retired", Target: target}); e != nil {
				return e
			}
		}
		return nil
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}

	if err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var loseCommit atomic.Bool
	var reservationReads atomic.Int64
	wrapped, er := installLostHostedCommit(a.controlDSN, fa.publisher, &loseCommit, &reservationReads)
	if er != nil {
		t.Fatal(er)
	}
	defer wrapped.Close()
	loseCommit.Store(true)

	body := batch("body")
	retryIdentity := body.Events[0].Input.Event
	retryIdentity.UUID = eventapi.NewID()
	retryIdentity.DistinctID = "native-conflict-person"
	retryIdentity.Event = "$set"
	retryIdentity.Properties = json.RawMessage(`{"$set":{"plan":"retry"}}`)
	retryIdentityInput, err := acceptance.ValidateInput(retryIdentity)
	if err != nil {
		t.Fatal(err)
	}
	body.Events = append(body.Events, acceptance.BufferedEvent{Input: retryIdentityInput, AdmittedAt: body.Events[0].AdmittedAt})
	rejectEffects.Store(true)
	if err = fa.FlushLive(t.Context(), body); err == nil {
		t.Fatal("failed precommit upload acknowledged input")
	}
	abandoned, er := fa.publisher.reservation(t.Context(), scope)
	if er != nil {
		t.Fatal(er)
	}
	rejectEffects.Store(false)

	nativeProject, releaseNative, er := sa.LeaseProject(t.Context(), project)
	if er != nil {
		t.Fatal(er)
	}
	realNativeDB := nativeProject.DB
	var loseNative, failACK, stageConflict atomic.Bool
	var getsBeforePublication int64
	bridged, er := installHostedNativeBoundary(realNativeDB, &loseNative, &failACK, func() error {
		if stageConflict.CompareAndSwap(true, false) {
			return &duckdb.Error{Type: duckdb.ErrorTypeTransaction, Msg: "injected native conflict after effect staging"}
		}
		if trackEffectKeys.Load() {
			getsBeforePublication = effectGets.Load()
		}
		return nil
	})
	if er != nil {
		t.Fatal(er)
	}
	nativeProject.DB = bridged
	releaseNative()
	restoreNative := sync.OnceFunc(func() {
		nativeProject.DB = realNativeDB
		bridged.Close()
	})
	defer restoreNative()
	var ruleReads int
	fa.publisher.loadRule = func(ctx context.Context, _, _ string, target output.PipelineTarget) ([]byte, error) {
		ruleReads++
		return sa.Remote.ReadImmutableEffectObject(ctx, scope, nativeRef(target.Rule))
	}
	stageConflict.Store(true)
	trackEffectKeys.Store(true)

	blockEffects.Store(true)
	completed := make(chan error, 1)
	go func() { completed <- fa.FlushLive(t.Context(), body) }()
	select {
	case <-effectEntered:
	case <-t.Context().Done():
		t.Fatal("admitted upload did not enter")
	}
	readsBeforePublication := reservationReads.Load()
	var reserved, published bool
	boundaryErr := a.service.control.QueryRowContext(t.Context(), `
		SELECT EXISTS(SELECT 1 FROM pipeline_effect_reservations WHERE project_id=$1 AND batch_id=$2),
		       EXISTS(SELECT 1 FROM pipeline_effect_jobs WHERE project_id=$1 AND batch_id=$2)
	`, project, body.ID).Scan(&reserved, &published)
	_, er = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=0 WHERE id=$1`, defaultTenantID)
	close(resumeEffects)
	if boundaryErr != nil || !reserved || published {
		t.Fatal("first S3 PUT must see committed reservation and no published output", reserved, published, boundaryErr)
	}
	if er != nil {
		t.Fatal(er)
	}
	if err = <-completed; err != nil {
		t.Fatal("native retry lost its staged plan or admission after budget reduction", err)
	}
	if reads := reservationReads.Load() - readsBeforePublication; reads != 0 {
		t.Fatalf("fresh publication reread the reservation %d times", reads)
	}
	fa.publisher.loadRule = nil
	trackEffectKeys.Store(false)
	if stageConflict.Load() || ruleReads != 1 {
		t.Fatalf("expected one native conflict and one rule read, conflict pending=%v reads=%d", stageConflict.Load(), ruleReads)
	}
	assets := make([]string, 0, len(effectKeys))
	for len(effectKeys) > 0 {
		assets = append(assets, <-effectKeys)
	}
	if len(assets) != 6 || !slices.Equal(assets[:3], assets[3:]) {
		t.Fatalf("native retry changed immutable output keys: %v", assets)
	}
	var traits string
	if err := realNativeDB.QueryRowContext(t.Context(), `SELECT traits FROM lake.identity_profiles WHERE identified_id='native-conflict-person'`).Scan(&traits); err != nil || traits != `{"plan":"retry"}` {
		t.Fatalf("identity projection after retry = %q, %v", traits, err)
	}
	if got := effectGets.Load() - getsBeforePublication; got != 2 {
		t.Fatalf("fresh publication made %d effect GETs, want 2 (manifest and plan)", got)
	}
	restoreNative()
	if _, er = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=NULL WHERE id=$1`, defaultTenantID); er != nil {
		t.Fatal(er)
	}

	var oldPrefix, newPrefix string
	if er = a.service.control.QueryRow(`SELECT prefix FROM pipeline_effect_cleanup WHERE project_id=$1 AND owner_id=$2`, project, abandoned.AcquisitionID).Scan(&oldPrefix); er != nil {
		t.Fatal("uncertain attempt ownership disappeared", er)
	}
	if er = a.service.control.QueryRow(`SELECT prefix FROM pipeline_effect_cleanup WHERE project_id=$1 AND owner_id=$2`, project, body.ID).Scan(&newPrefix); er != nil {
		t.Fatal(er)
	}
	if oldPrefix != abandoned.ArtifactPrefix || oldPrefix == newPrefix {
		t.Fatal("retry reused abandoned cleanup namespace")
	}

	if loseCommit.Load() {
		t.Fatal("publication commit-loss boundary was not exercised")
	}
	if err = fb.FlushLive(t.Context(), body); err != nil {
		t.Fatal(err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 4 {
		t.Fatal("wrong atomic usage", n, err)
	}
	assertAccepted := func(want int64) {
		t.Helper()
		var accepted int64
		err := b.service.control.QueryRow(`SELECT coalesce(sum(accepted_events),0) FROM tenant_usage WHERE tenant_id=$1`, defaultTenantID).Scan(&accepted)
		if err != nil || accepted != want {
			t.Fatalf("monthly acceptance ledger = %d, want %d: %v", accepted, want, err)
		}
	}
	assertAccepted(4)
	// A restored cursor must match the original retained native proof even when
	// the cursor already equals high-water and there are no pending effects.
	_, originalHash, er := fa.publisher.checkpoint(t.Context(), scope, defaultTenantID)
	if er != nil {
		t.Fatal(er)
	}
	if _, er = a.service.control.Exec(`UPDATE pipeline_effect_streams SET proof_sha=$1 WHERE project_id=$2`, strings.Repeat("f", 64), project); er != nil {
		t.Fatal(er)
	}
	if er = fb.RecoverProject(t.Context(), key); !errors.Is(er, lakeapi.ErrNativeEvidenceGap) {
		t.Fatal("mismatched covered proof was acknowledged", er)
	}
	if _, er = a.service.control.Exec(`UPDATE pipeline_effect_streams SET proof_sha=$1 WHERE project_id=$2`, originalHash, project); er != nil {
		t.Fatal(er)
	}

	// Add the real immutable batch rule alongside the direct destination.
	raw, _ = json.Marshal(delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60})
	batchRef, e := sa.Remote.PutImmutableEffectObject(t.Context(), scope, eventapi.NewID(), raw)
	if e != nil {
		t.Fatal(e)
	}
	session, e = ca.Acquire(t.Context(), key)
	if e != nil {
		t.Fatal(e)
	}
	e = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		return fa.publisher.effects.CompareAndSwapDefinitionTx(t.Context(), tx, nil, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "active", Target: output.PipelineTarget{ID: "batch-hook", Incarnation: "batch-inc", RuleRevision: "batch-rule", DestinationRevision: "batch-dest", CreatorID: creator, Rule: pipelineRef(batchRef)}})
	})
	session.Release()
	if e != nil {
		t.Fatal(e)
	}
	// Fail the actual usage participant after B has written jobs and cursor.
	// All PG effects must roll back together while the native proof survives.
	_, err = a.service.control.Exec(`CREATE FUNCTION reject_hosted_usage() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected usage failure'; END $$; CREATE TRIGGER reject_hosted_usage BEFORE INSERT OR UPDATE ON usage_rollups FOR EACH ROW EXECUTE FUNCTION reject_hosted_usage()`)
	if err != nil {
		t.Fatal(err)
	}
	crash := batch("native-before-pg")
	if err = fa.FlushLive(t.Context(), crash); err == nil {
		t.Fatal("usage failure acknowledged volatile input")
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&n); err != nil || n != 1 {
		t.Fatal("outputs escaped rollback", n, err)
	}
	seq, _, err := fa.publisher.checkpoint(t.Context(), scope, defaultTenantID)
	if err != nil || seq != 2 {
		t.Fatal("cursor escaped rollback", seq, err)
	}
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 4 {
		t.Fatal("usage escaped rollback", n, err)
	}
	assertAccepted(4)
	binding, er := fa.publisher.reservation(t.Context(), scope)
	if er != nil {
		t.Fatal(er)
	}
	var original lakeapi.NativeCommitEvidence
	recovery, er := cb.Acquire(t.Context(), key)
	if er != nil {
		t.Fatal(er)
	}
	er = recovery.RunNative(t.Context(), func(ctx context.Context) error {
		fence := recovery.Fence(scope.StreamID)
		if e := sb.AdvanceRecoveryFence(ctx, fence); e != nil {
			return e
		}
		page, e := sb.PendingNativeEffects(ctx, scope, 2, 1)
		if e != nil {
			return e
		}
		if len(page.Evidence) != 1 {
			return output.ErrPipelineConflict
		}
		original = page.Evidence[0]
		return nil
	})
	recovery.Release()
	if er != nil {
		t.Fatal(er)
	}
	if versioned && original.Effects.Manifest.VersionID == "" {
		t.Fatal("versioned manifest pin discarded")
	}
	missing := original
	missing.Effects.Manifest.Key += "missing"
	if _, er = fb.publisher.verify(t.Context(), binding, missing, lakeapi.ImmutableObjectRef{}); er == nil {
		t.Fatal("missing manifest accepted")
	}
	get, er := fixture.client.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(original.Effects.Manifest.Key)})
	if er != nil {
		t.Fatal(er)
	}
	originalRaw, er := io.ReadAll(get.Body)
	get.Body.Close()
	if er != nil {
		t.Fatal(er)
	}
	if _, er = fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(original.Effects.Manifest.Key), Body: bytes.NewReader([]byte("tampered"))}); er != nil {
		t.Fatal(er)
	}
	if versioned {
		var m hostedPublicationManifest
		if er = decodeHostedJSON(originalRaw, &m); er != nil {
			t.Fatal(er)
		}
		if m.PlanRef.VersionID == "" {
			t.Fatal("plan version pin discarded")
		}
		for _, o := range m.Outputs {
			if o.Body.VersionID == "" {
				t.Fatal("body version pin discarded")
			}
		}
		if _, er = fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(m.PlanRef.Key), Body: bytes.NewReader([]byte("modified plan latest"))}); er != nil {
			t.Fatal(er)
		}
		if _, er = fb.publisher.verify(t.Context(), binding, original, lakeapi.ImmutableObjectRef{}); er != nil {
			t.Fatal("actual version pin did not survive overwrite", er)
		}
	} else {
		if _, er = fb.publisher.verify(t.Context(), binding, original, lakeapi.ImmutableObjectRef{}); er == nil {
			t.Fatal("tampered immutable manifest accepted")
		}
		if _, er = fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(original.Effects.Manifest.Key), Body: bytes.NewReader(originalRaw)}); er != nil {
			t.Fatal(er)
		}
	}

	if _, err = a.service.control.Exec(`DROP TRIGGER reject_hosted_usage ON usage_rollups; DROP FUNCTION reject_hosted_usage()`); err != nil {
		t.Fatal(err)
	}
	getsBeforeRecovery := effectGets.Load()
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold publication", err)
	}
	if got := effectGets.Load() - getsBeforeRecovery; got != 4 {
		t.Fatalf("cold publication made %d effect GETs, want 4 (manifest, plan, delivery, fragment)", got)
	}
	if err = fa.FlushLive(t.Context(), crash); err != nil {
		t.Fatal("original native proof replay", err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&n); err != nil || n != 3 {
		t.Fatal("fragment/direct publication", n, err)
	}
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 5 {
		t.Fatal("recovery double charged", n, err)
	}
	assertAccepted(5)

	// Capacity failure rolls native sequence/data/usage back before any body PUT.
	_, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=greatest(used_refs,1) WHERE scope_kind='project' AND scope_id=$1`, project)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_definitions WHERE project_id=$1 AND state='active'`, project).Scan(&n); err != nil || n != 2 {
		t.Fatal("retired fanout definitions were revived", n, err)
	}
	blocked := batch("blocked")
	if err = fa.FlushLive(t.Context(), blocked); !errors.Is(err, output.ErrPipelineCapacity) || errors.Is(err, acceptance.ErrBatchTooLarge) {
		t.Fatal("full queue did not report a retryable capacity error", err)
	}
	if _, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=1 WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	if err = fa.FlushLive(t.Context(), blocked); !errors.Is(err, output.ErrPipelineDemandTooLarge) || !errors.Is(err, acceptance.ErrBatchTooLarge) {
		t.Fatal("oversized reservation did not report a permanent pre-commit capacity error", err)
	}
	if _, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=greatest(used_refs,1) WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	setPipelineState := func(state string) {
		t.Helper()
		session, err := ca.Acquire(t.Context(), key)
		if err != nil {
			t.Fatal(err)
		}
		defer session.Release()
		err = session.RunControlTx(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
			for _, target := range []output.PipelineTarget{
				{ID: "hook", Incarnation: "inc", RuleRevision: "rule", DestinationRevision: "dest", CreatorID: creator, Rule: pipelineRef(ref)},
				{ID: "batch-hook", Incarnation: "batch-inc", RuleRevision: "batch-rule", DestinationRevision: "batch-dest", CreatorID: creator, Rule: pipelineRef(batchRef)},
			} {
				if err := fa.publisher.effects.CompareAndSwapDefinitionTx(ctx, tx, &target, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: state, Target: target}); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	setPipelineState("paused")
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_definitions WHERE project_id=$1 AND state='paused'`, project).Scan(&n); err != nil || n != 2 {
		t.Fatal("pause changed an unintended definition", n, err)
	}
	if err = fa.FlushLive(t.Context(), batch("skip-paused-pipelines")); err != nil {
		t.Fatal("paused targets still reserved output capacity", err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&n); err != nil || n != 3 {
		t.Fatal("paused targets received new jobs", n, err)
	}

	assertFirstImportIgnoresWebhookOvershoot(t, fa, a, limits)

	if err = fb.FlushLive(t.Context(), body); err != nil {
		t.Fatal("duplicate at full quota", err)
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal(err)
	}
	tenantBody, _ := json.Marshal(map[string]string{"name": "Other progressing tenant"})
	otherTenant := sharedJourneyHTTP(t, a.host.URL, "POST", "/api/v1/tenants", adminToken, tenantBody, nil, 201)["tenant"].(map[string]any)["id"].(string)
	otherProject, _ := sharedJourneyProject(t, a.host.URL, otherTenant, "other bounded tenant")
	otherKey := lakeapi.ProjectWriteKey{SourceID: key.SourceID, ProjectID: otherProject}
	if _, err = sa.InitializeHostedProject(t.Context(), otherKey); err != nil {
		t.Fatal(err)
	}
	err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, v := range [][2]string{{"tenant", otherTenant}, {"project", otherProject}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	other := batch("other-tenant")
	other.ProjectID = otherProject
	other.Events[0].Input.Event.ProjectID = otherProject
	if err = fb.FlushLive(t.Context(), other); err != nil {
		t.Fatal("full A blocked unrelated tenant B", err)
	}

	if _, err = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=0 WHERE id=$1`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if err = fa.FlushLive(t.Context(), body); err != nil {
		t.Fatal("same-batch replay needed new budget", err)
	}
	duplicate := body
	duplicate.ID = "duplicate-uuid-new-batch"
	if err = fa.FlushLive(t.Context(), duplicate); err != nil {
		t.Fatal("duplicate-only batch needed new budget/capacity", err)
	}
	identityDuplicate := empty
	identityDuplicate.ID = "known-identity-new-batch"
	identityDuplicate.Events = empty.Events[1:]
	if err = fa.FlushLive(t.Context(), identityDuplicate); err != nil {
		t.Fatal("known identity needed new budget", err)
	}
	for _, payload := range []string{
		`{"type":"track","message_id":"new-track","actor_id":"person","name":"page"}`,
		`{"type":"identify","message_id":"new-identity","user_id":"person","anonymous_id":"anon","set":{"plan":"team"}}`,
	} {
		result := sharedJourneyNative(t, a.host.URL, ingest, payload, http.StatusTooManyRequests)
		if result["code"] != "ingestion_budget_exceeded" || result["receipts"] != nil {
			t.Fatal("exhausted monthly policy accepted native input", result)
		}
	}
	mixed := fmt.Sprintf(`{"api_key":%q,"batch":[{"uuid":%q,"event":"page","distinct_id":"person"},{"uuid":%q,"event":"$identify","distinct_id":"person","$set":{"plan":"team"}}]}`,
		ingest, body.Events[0].Input.Event.UUID, eventapi.NewID())
	result := sharedJourneyHTTP(t, a.host.URL, "POST", "/batch/", "", []byte(mixed), nil, http.StatusOK)
	if result["code"] != "ingestion_budget_exceeded" || result["accepted"] != float64(0) {
		t.Fatal("exhausted monthly policy accepted mixed input", result)
	}
	if queued := a.service.hosted.buffer.Snapshot().Events; queued != 0 {
		t.Fatalf("denied requests left %d buffered events", queued)
	}
	newIdentity := identityInput
	newIdentity.Event.UUID = eventapi.NewID()
	admittedIdentity := acceptance.FlushBatch{ID: "admitted-before-budget-exhausted", ProjectID: project, Events: []acceptance.BufferedEvent{{Input: newIdentity, AdmittedAt: time.Now().UTC()}}}
	if err = a.service.hosted.flusher.FlushLive(t.Context(), admittedIdentity); err != nil {
		t.Fatal("admitted identity could not flush after budget exhaustion", err)
	}

	// Lose the response from a successful REAL native transaction, then recover
	// without the original payload through the other engine.
	if _, err = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=NULL WHERE id=$1`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=100 WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	setPipelineState("active")
	finalProject, finalRelease, er := sa.LeaseProject(t.Context(), project)
	if er != nil {
		t.Fatal(er)
	}
	finalDB := finalProject.DB
	finalBridge, er := installHostedNativeBoundary(finalDB, &loseNative, &failACK, nil)
	if er != nil {
		finalRelease()
		t.Fatal(er)
	}
	finalProject.DB = finalBridge
	finalRelease()
	defer func() { finalProject.DB = finalDB; finalBridge.Close() }()
	nativeLost := batch("lost-native-response")
	loseNative.Store(true)
	if err = fa.FlushLive(t.Context(), nativeLost); err == nil {
		t.Fatal("unknown native outcome released input")
	}
	if loseNative.Load() {
		t.Fatal("native commit-loss boundary not exercised")
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold lost-native recovery", err)
	}
	if err = fa.FlushLive(t.Context(), nativeLost); err != nil {
		t.Fatal("native lost-response replay", err)
	}
	ackLost := batch("failed-native-ack")
	failACK.Store(true)
	if err = fa.FlushLive(t.Context(), ackLost); err != nil || !failACK.Load() {
		t.Fatalf("live flush attempted standalone native ACK: %v", err)
	}
	if err = fa.RecoverProject(t.Context(), key); err != nil || failACK.Load() {
		t.Fatalf("best-effort ACK failure boundary not exercised: %v", err)
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold ACK retry", err)
	}

	// Lose a native response immediately before deleting the tenant project.
	// Recovery must reconcile the proof rather than dropping its usage or data.
	unpublished := batch("unpublished-before-deletion")
	loseNative.Store(true)
	if err = fa.FlushLive(t.Context(), unpublished); err == nil || loseNative.Load() {
		t.Fatalf("did not exercise committed native response loss: %v", err)
	}
	if err = a.service.beginControlProjectDeletion(t.Context(), project); err != nil {
		t.Fatal(err)
	}
	for _, flusher := range []*hostedLiveFlusher{fb, fa} {
		if err = flusher.sealDeletedProject(t.Context(), key); err != nil {
			t.Fatal("cold deletion seal", err)
		}
	}
	if err = fb.FlushLive(t.Context(), batch("fenced")); !errors.Is(err, errHostedProjectFenced) {
		t.Fatalf("sealed project accepted live input: %v", err)
	}
	page, err := sb.PendingNativeEffects(t.Context(), scope, 0, 100)
	if err != nil || len(page.Evidence) == 0 {
		t.Fatalf("sealed proof history unavailable: %+v, %v", page, err)
	}
	last := page.Evidence[len(page.Evidence)-1]
	var final int64
	var proof string
	err = a.service.control.QueryRow(`SELECT final_sequence,proof_sha FROM hosted_project_seals WHERE project_id=$1`, project).Scan(&final, &proof)
	if err != nil || last.OperationID != unpublished.ID || final != last.Sequence || proof != last.ProofFingerprint || page.AcknowledgedThrough != final {
		t.Fatalf("seal did not preserve and confirm the final native proof: %d, %q, %+v, %v", final, proof, last, err)
	}
	other.ID = "other-tenant-after-a-seal"
	other.Events[0].Input.Event.UUID = eventapi.NewID()
	if err = fb.FlushLive(t.Context(), other); err != nil {
		t.Fatal("sealed A blocked tenant B", err)
	}
}
