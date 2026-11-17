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
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
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
	var rejectEffects, blockEffects atomic.Bool
	var effectPuts atomic.Int64
	effectEntered, resumeEffects := make(chan struct{}), make(chan struct{})
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/effects/") {
			effectPuts.Add(1)
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

	project, _ := sharedJourneyProject(t, a.host.URL, defaultTenantID, "hosted flush")
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
		for _, v := range [][2]string{{"instance", ""}, {"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	batch := func(id string) bufferedFlushBatch {
		e := eventapi.Event{ProjectID: project, UUID: eventapi.NewID(), Event: "page", DistinctID: "person", Properties: json.RawMessage(`{"a":1}`), Context: json.RawMessage(`{}`), Timestamp: time.Now().UTC().Format(time.RFC3339Nano), ReceivedAt: time.Now().UTC().Format(time.RFC3339Nano)}
		return bufferedFlushBatch{id: id, projectID: project, events: []bufferedLiveEvent{{input: validatedLiveEvent{event: e}, admittedAt: time.Now().UTC()}}}
	}
	empty := batch("empty")
	identityEvent := empty.events[0].input.event
	identityEvent.UUID = eventapi.NewID()
	identityEvent.Event = "$identify"
	identityEvent.Properties = json.RawMessage(`{"$set":{"plan":"team"}}`)
	identityInput, er := validateLiveEvent(identityEvent)
	if er != nil {
		t.Fatal(er)
	}
	empty.events = append(empty.events, bufferedLiveEvent{input: identityInput, admittedAt: empty.events[0].admittedAt})
	if err = fa.FlushLive(t.Context(), empty); err != nil {
		t.Fatal(err)
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
		for _, v := range [][2]string{{"instance", ""}, {"tenant", defaultTenantID}, {"project", project}} {
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
	oversized.events[0].input.event.Properties = json.RawMessage(`{"value":"` + strings.Repeat("x", 900000) + `"}`)
	putsBefore := effectPuts.Load()
	if err = fa.FlushLive(t.Context(), oversized); !errors.Is(err, output.ErrPipelineCapacity) {
		t.Fatal("oversized preparation accepted", err)
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
		for _, v := range [][2]string{{"instance", ""}, {"tenant", defaultTenantID}, {"project", project}} {
			if e := fa.publisher.effects.ConfigureCapacityTx(t.Context(), tx, v[0], v[1], limits); e != nil {
				return e
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var loseCommit atomic.Bool
	wrapped, er := installLostHostedCommit(a.service.shared.config.DatabaseURL, fa.publisher, &loseCommit)
	if er != nil {
		t.Fatal(er)
	}
	defer wrapped.Close()
	loseCommit.Store(true)

	body := batch("body")
	rejectEffects.Store(true)
	if err = fa.FlushLive(t.Context(), body); err == nil {
		t.Fatal("failed precommit upload acknowledged input")
	}
	abandoned, er := fa.publisher.reservation(t.Context(), scope)
	if er != nil {
		t.Fatal(er)
	}
	rejectEffects.Store(false)

	blockEffects.Store(true)
	completed := make(chan error, 1)
	go func() { completed <- fa.FlushLive(t.Context(), body) }()
	select {
	case <-effectEntered:
	case <-t.Context().Done():
		t.Fatal("admitted upload did not enter")
	}
	_, er = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=0 WHERE id=$1`, defaultTenantID)
	close(resumeEffects)
	if er != nil {
		t.Fatal(er)
	}
	if err = <-completed; err != nil {
		t.Fatal("accepted native work failed after budget reduction", err)
	}
	if _, er = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=NULL WHERE id=$1`, defaultTenantID); er != nil {
		t.Fatal(er)
	}

	var oldPrefix, newPrefix string
	if er = a.service.control.QueryRow(`SELECT prefix FROM pipeline_effect_cleanup WHERE project_id=$1 AND owner_id=$2`, project, abandoned.AcquisitionID).Scan(&oldPrefix); er != nil {
		t.Fatal("uncertain attempt ownership disappeared", er)
	}
	if er = a.service.control.QueryRow(`SELECT prefix FROM pipeline_effect_cleanup WHERE project_id=$1 AND owner_id=$2`, project, body.id).Scan(&newPrefix); er != nil {
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
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 3 {
		t.Fatal("wrong atomic usage", n, err)
	}
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
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 3 {
		t.Fatal("usage escaped rollback", n, err)
	}
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
	if _, er = fb.publisher.verify(t.Context(), binding, missing); er == nil {
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
		if _, er = fb.publisher.verify(t.Context(), binding, original); er != nil {
			t.Fatal("actual version pin did not survive overwrite", er)
		}
	} else {
		if _, er = fb.publisher.verify(t.Context(), binding, original); er == nil {
			t.Fatal("tampered immutable manifest accepted")
		}
		if _, er = fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(original.Effects.Manifest.Key), Body: bytes.NewReader(originalRaw)}); er != nil {
			t.Fatal(er)
		}
	}

	if _, err = a.service.control.Exec(`DROP TRIGGER reject_hosted_usage ON usage_rollups; DROP FUNCTION reject_hosted_usage()`); err != nil {
		t.Fatal(err)
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold publication", err)
	}
	if err = fa.FlushLive(t.Context(), crash); err != nil {
		t.Fatal("original native proof replay", err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_jobs WHERE project_id=$1`, project).Scan(&n); err != nil || n != 3 {
		t.Fatal("fragment/direct publication", n, err)
	}
	if err = a.service.control.QueryRow(`SELECT sum(live_events) FROM usage_rollups WHERE project_id=$1 AND source_id=$2`, project, scope.StreamID).Scan(&n); err != nil || n != 4 {
		t.Fatal("recovery double charged", n, err)
	}

	// Capacity failure rolls native sequence/data/usage back before any body PUT.
	_, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=3 WHERE scope_kind='project' AND scope_id=$1`, project)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_definitions WHERE project_id=$1 AND state='active'`, project).Scan(&n); err != nil || n != 2 {
		t.Fatal("retired fanout definitions were revived", n, err)
	}
	session, err = ca.Acquire(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	err = fa.publisher.transaction(t.Context(), func(tx *sql.Tx) error {
		for _, target := range []output.PipelineTarget{
			{ID: "hook", Incarnation: "inc", RuleRevision: "rule", DestinationRevision: "dest", CreatorID: creator, Rule: pipelineRef(ref)},
			{ID: "batch-hook", Incarnation: "batch-inc", RuleRevision: "batch-rule", DestinationRevision: "batch-dest", CreatorID: creator, Rule: pipelineRef(batchRef)},
		} {
			if e := fa.publisher.effects.CompareAndSwapDefinitionTx(t.Context(), tx, &target, output.PipelineDefinition{TenantID: defaultTenantID, ProjectID: project, State: "paused", Target: target}); e != nil {
				return e
			}
		}
		return nil
	})
	session.Release()
	if err != nil {
		t.Fatal(err)
	}
	if err = a.service.control.QueryRow(`SELECT count(*) FROM pipeline_effect_definitions WHERE project_id=$1 AND state='paused'`, project).Scan(&n); err != nil || n != 2 {
		t.Fatal("pause changed an unintended definition", n, err)
	}

	blocked := batch("blocked")
	if err = fa.FlushLive(t.Context(), blocked); !errors.Is(err, output.ErrPipelineCapacity) {
		t.Fatal("full queue admitted", err)
	}
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
	other.projectID = otherProject
	other.events[0].input.event.ProjectID = otherProject
	if err = fb.FlushLive(t.Context(), other); err != nil {
		t.Fatal("full A blocked unrelated tenant B", err)
	}

	if _, err = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=0 WHERE id=$1`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	fa.policy.refreshed = time.Time{}
	if err = fa.FlushLive(t.Context(), body); err != nil {
		t.Fatal("same-batch replay needed new budget", err)
	}
	duplicate := body
	duplicate.id = "duplicate-uuid-new-batch"
	if err = fa.FlushLive(t.Context(), duplicate); err != nil {
		t.Fatal("duplicate-only batch needed new budget/capacity", err)
	}
	if err = fa.FlushLive(t.Context(), batch("no-new-budget")); err == nil {
		t.Fatal("exhausted monthly policy admitted new data")
	}

	identityDuplicate := empty
	identityDuplicate.id = "known-identity-new-batch"
	identityDuplicate.events = empty.events[1:]
	if err = fa.FlushLive(t.Context(), identityDuplicate); err != nil {
		t.Fatal("known identity needed new budget", err)
	}
	newIdentity := identityInput
	newIdentity.event.UUID = eventapi.NewID()
	identityOnly := bufferedFlushBatch{id: "new-identity-no-budget", projectID: project, events: []bufferedLiveEvent{{input: newIdentity, admittedAt: time.Now().UTC()}}}
	var budget *tenantIngestionError
	if err = fa.FlushLive(t.Context(), identityOnly); !errors.As(err, &budget) || budget.code != "ingestion_budget_exceeded" {
		t.Fatal("new identity bypassed exhausted budget", err)
	}
	mixed := body
	mixed.id = "mixed-known-new-identity"
	mixed.events = append(append([]bufferedLiveEvent{}, body.events...), identityOnly.events...)
	budget = nil
	if err = fa.FlushLive(t.Context(), mixed); !errors.As(err, &budget) || budget.code != "ingestion_budget_exceeded" {
		t.Fatal("mixed duplicate/new identity bypassed exhausted budget", err)
	}

	// Lose the response from a successful REAL native transaction, then recover
	// without the original payload through the other engine.
	if _, err = a.service.control.Exec(`UPDATE tenants SET ingestion_monthly_events=NULL WHERE id=$1`, defaultTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err = a.service.control.Exec(`UPDATE pipeline_capacity SET limit_refs=100 WHERE scope_kind='project' AND scope_id=$1`, project); err != nil {
		t.Fatal(err)
	}
	fa.policy.refreshed = time.Time{}
	nativeProject, release, er := sa.LeaseProject(t.Context(), project)
	if er != nil {
		t.Fatal(er)
	}
	realNativeDB := nativeProject.DB
	var loseNative, failACK atomic.Bool
	bridged, er := installHostedNativeBoundary(realNativeDB, &loseNative, &failACK)
	if er != nil {
		t.Fatal(er)
	}
	nativeProject.DB = bridged
	release()
	defer func() { nativeProject.DB = realNativeDB; bridged.Close() }()
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
	if err = fa.FlushLive(t.Context(), ackLost); err != nil {
		t.Fatal("confirmed PG publication retained volatile input on ACK failure", err)
	}
	if failACK.Load() {
		t.Fatal("native ACK failure boundary not exercised")
	}
	if err = fb.RecoverProject(t.Context(), key); err != nil {
		t.Fatal("cold ACK retry", err)
	}

}
