package porcupine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type assemblyS3Harness struct {
	objects  *lakeapi.DuckLakeObjectStore
	fence    lakeapi.ProjectWriteFence
	scope    lakeapi.HostedProjectScope
	target   frozenWebhookTarget
	sequence int64
}

func newAssemblyS3Harness(t *testing.T, config lakeapi.DuckLakeS3Config) *assemblyS3Harness {
	t.Helper()
	store, err := lakeapi.NewDuckLake(lakeapi.DuckLakeConfig{Directory: filepath.Join(t.TempDir(), "lake"), S3: &config, DisableCleanup: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	source, err := store.SourceID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	key := lakeapi.ProjectWriteKey{SourceID: source, ProjectID: "project"}
	fence := lakeapi.ProjectWriteFence{Key: key, StreamID: eventapi.NewID(), Epoch: 1, AcquisitionID: eventapi.NewID()}
	return &assemblyS3Harness{objects: store.Remote, fence: fence, scope: lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(source, key.ProjectID), StreamID: fence.StreamID}, target: frozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "pipeline-v1", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60}}}
}

func assemblyEvents(count int) []eventapi.Event {
	events := make([]eventapi.Event, count)
	for i := range events {
		events[i] = eventapi.Event{ID: int64(i + 1), ProjectID: "project", UUID: eventapi.NewID(), Event: "page_viewed", DistinctID: "person", Properties: json.RawMessage(`{"page":"/"}`), Timestamp: "2026-11-17T12:00:00Z", ReceivedAt: "2026-11-17T12:00:01Z"}
	}
	return events
}

func (h *assemblyS3Harness) stage(t *testing.T, events []eventapi.Event) []hostedAssemblyFragment {
	t.Helper()
	h.sequence++
	stager, err := newHostedImmutableEffects(h.objects, h.fence, frozenLiveOutputPlan{Scope: h.scope, TenantID: "tenant", Targets: []frozenWebhookTarget{h.target}, MaxEventReferences: int64(len(events))})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := stager.prepare(t.Context(), lakeapi.SelectedLiveEffects{Scope: h.scope, TenantID: "tenant", BatchID: eventapi.NewID(), Sequence: h.sequence, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]hostedAssemblyFragment, len(prepared))
	for i, o := range prepared {
		if o.Fragment == nil {
			t.Fatal("stager created a delivery instead of a fragment", o)
		}
		fragment := immutableWebhookFragment{FragmentID: o.ID, Target: h.target, Body: nativeRef(o.Body), WindowStartMS: o.Fragment.WindowStartMS, WindowEndMS: o.Fragment.WindowEndMS}
		members := make([]int, len(o.Members))
		for j, m := range o.Members {
			fragment.EventUUIDs = append(fragment.EventUUIDs, m.UUID)
			fragment.EventBytes = append(fragment.EventBytes, m.Bytes)
			members[j] = j
		}
		inputs[i] = hostedAssemblyFragment{hostedFragmentPosition{h.scope, h.sequence, fragment.FragmentID, 0}, fragment, members}
	}
	return inputs
}

func (h *assemblyS3Harness) claim(t *testing.T, fragments []hostedAssemblyFragment) frozenWebhookAssemblyClaim {
	t.Helper()
	claim := frozenWebhookAssemblyClaim{Scope: h.scope, TenantID: "tenant", Target: h.target, WindowStartMS: fragments[0].Fragment.WindowStartMS, WindowEndMS: fragments[0].Fragment.WindowEndMS, Generation: eventapi.NewID(), AttemptID: eventapi.NewID(), Fragments: fragments, MaxFragments: hostedAssemblyMaxFragments, MaxReadBytes: hostedAssemblyMaxReadBytes, MaxBodyBytes: lakeapi.MaxImmutableEffectObjectBytes}
	freezeAssemblyTestClaim(t, &claim)
	return claim
}

func (h *assemblyS3Harness) readResult(t *testing.T, result hostedWebhookAssemblyResult) delivery.WebhookPayload {
	t.Helper()
	raw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, result.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload delivery.WebhookPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	manifestRaw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, result.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	var manifest immutableWebhookAssemblyManifest
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.DeliveryID != result.DeliveryID || payload.DeliveryID != result.DeliveryID || manifest.Body != result.Body || !reflect.DeepEqual(manifest.Next, result.Next) || len(manifest.Members) != result.EventCount || len(payload.Events) != result.EventCount || payload.Event != nil {
		t.Fatal("body/manifest/result diverged", manifest, result)
	}
	return payload
}

func TestDuckLakeS3HostedWebhookAssembly(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	h := newAssemblyS3Harness(t, fixture.storage)
	t.Run("coalesces commits and retries immutable output", func(t *testing.T) {
		events := assemblyEvents(5)
		fragments := append(h.stage(t, events[:2]), h.stage(t, events[2:])...)
		claim := h.claim(t, fragments)
		result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil || result.EventCount != 5 {
			t.Fatal(result, err)
		}
		if result.Body.VersionID != "" {
			t.Fatal("expected unversioned content-addressed fixture object")
		}
		payload := h.readResult(t, result)
		if !reflect.DeepEqual(payload.Events, events) {
			t.Fatal("multiple native commits did not coalesce in original order")
		}
		for i, p := range result.Next {
			if p.NextMember != len(fragments[i].Fragment.EventUUIDs) {
				t.Fatal("cursor did not consume full fragments", result.Next)
			}
		}
		replay, err := assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil || !reflect.DeepEqual(replay, result) {
			t.Fatal("retry changed frozen output", replay, result, err)
		}
		takeover := cloneAssemblyTestClaim(t, claim)
		takeover.AttemptID = eventapi.NewID()
		replay, err = assembleHostedWebhook(t.Context(), h.objects, takeover)
		if err != nil || replay.DeliveryID != result.DeliveryID || replay.Body.SHA256 != result.Body.SHA256 {
			t.Fatal("worker attempt entered logical delivery identity", replay, err)
		}
	})
	t.Run("hundred event limit and resume inside fragment", func(t *testing.T) {
		events := assemblyEvents(130)
		fragments := append(h.stage(t, events[:70]), h.stage(t, events[70:])...)
		claim := h.claim(t, fragments)
		result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil || result.EventCount != 100 || result.Next[0].NextMember != 70 || result.Next[1].NextMember != 30 {
			t.Fatal(result, err)
		}
		if !reflect.DeepEqual(h.readResult(t, result).Events, events[:100]) {
			t.Fatal("first chunk membership diverged")
		}
		resume := h.claim(t, fragments[1:])
		resume.Fragments[0].Position = result.Next[1]
		resume.Fragments[0].Members = resume.Fragments[0].Members[30:]
		freezeAssemblyTestClaim(t, &resume)
		rest, err := assembleHostedWebhook(t.Context(), h.objects, resume)
		if err != nil || rest.EventCount != 30 || rest.Next[0].NextMember != 60 {
			t.Fatal(rest, err)
		}
		if !reflect.DeepEqual(h.readResult(t, rest).Events, events[100:]) {
			t.Fatal("resumed chunk membership diverged")
		}
	})
	t.Run("byte limit and oversized singleton", func(t *testing.T) {
		events := assemblyEvents(3)
		for i := range events {
			events[i].Properties = json.RawMessage(fmt.Sprintf(`{"value":%q}`, strings.Repeat("x", 520000)))
		}
		claim := h.claim(t, h.stage(t, events))
		result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil || result.EventCount != 2 || result.Body.Bytes > delivery.WebhookChunkBytes {
			t.Fatal(result, err)
		}
		h.readResult(t, result)
		events = assemblyEvents(2)
		events[0].Properties = json.RawMessage(fmt.Sprintf(`{"value":%q}`, strings.Repeat("x", delivery.WebhookChunkBytes)))
		claim = h.claim(t, h.stage(t, events))
		result, err = assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil || result.EventCount != 1 || result.Body.Bytes <= delivery.WebhookChunkBytes {
			t.Fatal(result, err)
		}
		h.readResult(t, result)
		claim.MaxBodyBytes = delivery.WebhookChunkBytes
		if failed, err := assembleHostedWebhook(t.Context(), h.objects, claim); err == nil || !reflect.DeepEqual(failed, hostedWebhookAssemblyResult{}) {
			t.Fatal("oversized hard bound returned success", failed, err)
		}
	})
	t.Run("recomposition before and after possible attempt and late work", func(t *testing.T) {
		events := assemblyEvents(3)
		claim := h.claim(t, h.stage(t, events))
		original, err := assembleHostedWebhook(t.Context(), h.objects, claim)
		if err != nil {
			t.Fatal(err)
		}
		for _, stage := range []string{"before attempt", "after possible attempt"} {
			t.Log(stage)
			survivor := cloneAssemblyTestClaim(t, claim)
			survivor.Fragments[0].Members = []int{0, 2}
			freezeAssemblyTestClaim(t, &survivor)
			result, err := assembleHostedWebhook(t.Context(), h.objects, survivor)
			if err != nil || result.DeliveryID == original.DeliveryID {
				t.Fatal("recomposed body reused delivery ID", result, err)
			}
			payload := h.readResult(t, result)
			if len(payload.Events) != 2 || payload.Events[1].UUID != events[2].UUID {
				t.Fatal(payload)
			}
		}
		unchanged := h.readResult(t, original)
		if !reflect.DeepEqual(unchanged.Events, events) {
			t.Fatal("recomposition amended immutable original")
		}
		late := h.claim(t, h.stage(t, assemblyEvents(1)))
		late.Generation = claim.Generation
		result, err := assembleHostedWebhook(t.Context(), h.objects, late)
		if err != nil || result.DeliveryID == original.DeliveryID {
			t.Fatal("late-window work reused frozen ID", result, err)
		}
	})
	t.Run("mismatches tampering truncation and private fields", func(t *testing.T) {
		for _, kind := range []string{"member", "received window", "private control", "raw peer", "tampered bytes", "truncated", "extra bytes"} {
			t.Run(kind, func(t *testing.T) {
				claim := h.claim(t, h.stage(t, assemblyEvents(1)))
				ref := claim.Fragments[0].Fragment.Body
				raw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, ref)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "member":
					claim.Fragments[0].Fragment.EventUUIDs[0] = eventapi.NewID()
				case "received window", "private control", "raw peer":
					var events []map[string]any
					if err := json.Unmarshal(raw, &events); err != nil {
						t.Fatal(err)
					}
					if kind == "received window" {
						events[0]["received_at"] = "2026-11-17T12:01:01Z"
					}
					if kind == "private control" {
						events[0]["event"] = "$identify"
					}
					if kind == "raw peer" {
						events[0]["peer_ip"] = "private-peer"
					}
					eventRaw, err := delivery.EncodeJSON(events[0])
					if err != nil {
						t.Fatal(err)
					}
					raw = append(append([]byte{'['}, eventRaw...), ']')
					ref, err = h.objects.PutImmutableEffectObject(t.Context(), h.scope, eventapi.NewID(), raw)
					if err != nil {
						t.Fatal(err)
					}
					claim.Fragments[0].Fragment.Body = ref
					claim.Fragments[0].Fragment.EventBytes[0] = int64(len(eventRaw))
				case "tampered bytes":
					raw = bytes.Replace(raw, []byte("page_viewed"), []byte("page_edited"), 1)
				case "truncated":
					raw = raw[:len(raw)-1]
				case "extra bytes":
					raw = append(raw, 'x')
				}
				if kind == "tampered bytes" || kind == "truncated" || kind == "extra bytes" {
					if _, err := fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(fixture.storage.Bucket), Key: aws.String(ref.Key), Body: bytes.NewReader(raw)}); err != nil {
						t.Fatal(err)
					}
				}
				freezeAssemblyTestClaim(t, &claim)
				result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
				if err == nil || !reflect.DeepEqual(result, hostedWebhookAssemblyResult{}) {
					t.Fatal("invalid fragment returned publication result", result, err)
				}
			})
		}
	})
}

func TestDuckLakeS3HostedWebhookAssemblyVersionPin(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	bucket := "porcupine-effects-" + eventapi.NewID()
	if _, err := fixture.client.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		versions, err := fixture.client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{Bucket: aws.String(bucket)})
		if err != nil {
			t.Error(err)
			return
		}
		var keys []types.ObjectIdentifier
		for _, v := range versions.Versions {
			keys = append(keys, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		for _, v := range versions.DeleteMarkers {
			keys = append(keys, types.ObjectIdentifier{Key: v.Key, VersionId: v.VersionId})
		}
		if len(keys) > 0 {
			if _, err := fixture.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: aws.String(bucket), Delete: &types.Delete{Objects: keys}}); err != nil {
				t.Error(err)
				return
			}
		}
		if _, err := fixture.client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Error(err)
		}
	})
	if _, err := fixture.client.PutBucketVersioning(t.Context(), &s3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatal(err)
	}
	config := fixture.storage
	config.Bucket = bucket
	h := newAssemblyS3Harness(t, config)
	claim := h.claim(t, h.stage(t, assemblyEvents(2)))
	ref := claim.Fragments[0].Fragment.Body
	if ref.VersionID == "" {
		t.Fatal("versioned fragment lost pin")
	}
	if _, err := fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(ref.Key), Body: strings.NewReader(`{"changed":"latest"}`)}); err != nil {
		t.Fatal(err)
	}
	result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err != nil || result.EventCount != 2 {
		t.Fatal("assembly followed changed latest version", result, err)
	}
	h.readResult(t, result)
	claim.Fragments[0].Fragment.Body.VersionID = ""
	freezeAssemblyTestClaim(t, &claim)
	if _, err := assembleHostedWebhook(t.Context(), h.objects, claim); err == nil {
		t.Fatal("unversioned changed content address was accepted")
	}
}

func TestDuckLakeS3HostedWebhookAssemblyAmbiguousPut(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	upstream, err := url.Parse(fixture.storage.Endpoint)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(upstream)
	proxy.Transport = fixture.client.Options().HTTPClient.(*http.Client).Transport
	// Preserve the signed Host while routing to the loopback S3 endpoint.
	var loseNext atomic.Bool
	var lost atomic.Int64
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.Request.Method == http.MethodPut && response.StatusCode == http.StatusOK && loseNext.CompareAndSwap(true, false) {
			lost.Add(1)
			response.Body.Close()
			return errors.New("fixture lost successful PUT response")
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) { w.WriteHeader(http.StatusBadGateway) }
	var rejectManifest atomic.Bool
	var bodyPath atomic.Value
	bodyPath.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rejectManifest.Load() && r.Method == http.MethodPut {
			bodyPath.CompareAndSwap("", r.URL.Path)
			if bodyPath.Load().(string) != r.URL.Path {
				w.WriteHeader(http.StatusForbidden)
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	config := fixture.storage
	config.Endpoint = server.URL
	h := newAssemblyS3Harness(t, config)
	claim := h.claim(t, h.stage(t, assemblyEvents(3)))
	loseNext.Store(true)
	result, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err != nil || lost.Load() != 1 {
		t.Fatal("lost successful PUT was not recovered", result, lost.Load(), err)
	}
	h.readResult(t, result)
	retry, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err != nil || !reflect.DeepEqual(retry, result) {
		t.Fatal("ambiguous PUT retry changed output", retry, result, err)
	}
	// A completed body PUT cannot be reported as a complete assembly when the
	// manifest fails. Retrying recovers that orphan and produces the manifest.
	claim = h.claim(t, h.stage(t, assemblyEvents(2)))
	rejectManifest.Store(true)
	failed, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err == nil || !reflect.DeepEqual(failed, hostedWebhookAssemblyResult{}) || bodyPath.Load().(string) == "" {
		t.Fatal("manifest failure returned partial success", failed, err)
	}
	rejectManifest.Store(false)
	recovered, err := assembleHostedWebhook(t.Context(), h.objects, claim)
	if err != nil || !strings.HasSuffix(bodyPath.Load().(string), recovered.Body.Key) {
		t.Fatal("retry did not recover the orphaned body", recovered, err)
	}
	h.readResult(t, recovered)
}
