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
	"slices"
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
	"github.com/olup/porcupine/internal/output"
)

type assemblyS3Harness struct {
	objects  *lakeapi.DuckLakeObjectStore
	fence    lakeapi.ProjectWriteFence
	scope    lakeapi.HostedProjectScope
	target   output.FrozenWebhookTarget
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
	return &assemblyS3Harness{objects: store.Remote, fence: fence, scope: lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(source, key.ProjectID), StreamID: fence.StreamID}, target: output.FrozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "pipeline-v1", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60}}}
}

func assemblyEvents(count int) []eventapi.Event {
	events := make([]eventapi.Event, count)
	for i := range events {
		events[i] = eventapi.Event{ID: int64(i + 1), ProjectID: "project", UUID: eventapi.NewID(), Event: "page_viewed", DistinctID: "person", Properties: json.RawMessage(`{"page":"/"}`), Timestamp: "2026-11-17T12:00:00Z", ReceivedAt: "2026-11-17T12:00:01Z"}
	}
	return events
}

func (h *assemblyS3Harness) stage(t *testing.T, events []eventapi.Event) []output.PipelineAssemblyInput {
	t.Helper()
	h.sequence++
	stager, err := output.NewHostedWebhookPreparation(h.objects, h.fence, output.HostedWebhookPlan{Scope: h.scope, TenantID: "tenant", Targets: []output.FrozenWebhookTarget{h.target}, MaxEventReferences: int64(len(events))})
	if err != nil {
		t.Fatal(err)
	}
	prepared, objects, err := stager.Prepare(lakeapi.SelectedLiveEffects{Scope: h.scope, TenantID: "tenant", BatchID: eventapi.NewID(), Sequence: h.sequence, Events: events})
	if err != nil {
		t.Fatal(err)
	}
	inputs := make([]output.PipelineAssemblyInput, len(prepared))
	for i, o := range prepared {
		if o.Fragment == nil {
			t.Fatal("stager created a delivery instead of a fragment", o)
		}
		ref := uploadPreparedWebhookBody(t, h.objects, h.scope, h.fence, objects[i])
		fragment := output.PipelineFragment{
			Claim:  output.PipelineClaim{ID: o.ID, Scope: pipelineScope(h.scope), TenantID: "tenant"},
			Target: output.PipelineTarget{ID: o.PipelineID, Incarnation: o.Incarnation, RuleRevision: o.RuleRevision, DestinationRevision: o.DestinationRevision},
			Spec:   *o.Fragment, Body: pipelineRef(ref), Sequence: h.sequence, Count: len(o.Members), Members: o.Members,
		}
		members := make([]int, len(o.Members))
		for j := range members {
			members[j] = j
		}
		inputs[i] = output.PipelineAssemblyInput{Fragment: fragment, Eligible: members}
	}
	return inputs
}

func (h *assemblyS3Harness) claim(fragments []output.PipelineAssemblyInput) output.PipelineAssembly {
	return output.PipelineAssembly{Scope: pipelineScope(h.scope), TenantID: "tenant", Generation: eventapi.NewID(), Attempt: eventapi.NewID(), Inputs: slices.Clone(fragments)}
}

type assemblyS3Artifacts struct {
	payload delivery.WebhookPayload
	next    []int
}

func (h *assemblyS3Harness) readResult(t *testing.T, result output.PipelineAssemblyResult) assemblyS3Artifacts {
	t.Helper()
	raw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, nativeRef(result.Body))
	if err != nil {
		t.Fatal(err)
	}
	var artifacts assemblyS3Artifacts
	if err := json.Unmarshal(raw, &artifacts.payload); err != nil {
		t.Fatal(err)
	}
	manifestRaw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, nativeRef(result.Manifest))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		DeliveryID string
		Body       lakeapi.ImmutableObjectRef
		Members    []json.RawMessage
		Next       []struct{ NextMember int }
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.DeliveryID != result.ID || artifacts.payload.DeliveryID != result.ID || manifest.Body != nativeRef(result.Body) || len(manifest.Members) != result.Count || len(artifacts.payload.Events) != result.Count || artifacts.payload.Event != nil {
		t.Fatal("body/manifest/result diverged", manifest, result)
	}
	for _, position := range manifest.Next {
		artifacts.next = append(artifacts.next, position.NextMember)
	}
	return artifacts
}

func TestDuckLakeS3HostedWebhookAssembly(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	h := newAssemblyS3Harness(t, fixture.storage)
	t.Run("coalesces commits and retries immutable output", func(t *testing.T) {
		events := assemblyEvents(5)
		fragments := append(h.stage(t, events[:2]), h.stage(t, events[2:])...)
		claim := h.claim(fragments)
		result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil || result.Count != 5 {
			t.Fatal(result, err)
		}
		if result.Body.VersionID != "" {
			t.Fatal("expected unversioned content-addressed fixture object")
		}
		artifacts := h.readResult(t, result)
		if !reflect.DeepEqual(artifacts.payload.Events, events) {
			t.Fatal("multiple native commits did not coalesce in original order")
		}
		if !slices.Equal(artifacts.next, []int{2, 3}) {
			t.Fatal("cursor did not consume full fragments", artifacts.next)
		}
		replay, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil || !reflect.DeepEqual(replay, result) {
			t.Fatal("retry changed frozen output", replay, result, err)
		}
		takeover := claim
		takeover.Attempt = eventapi.NewID()
		replay, err = output.StageWebhookAssembly(t.Context(), h.objects, h.scope, takeover)
		if err != nil || replay.ID != result.ID || replay.Body.SHA256 != result.Body.SHA256 {
			t.Fatal("worker attempt entered logical delivery identity", replay, err)
		}
	})
	t.Run("hundred event limit and resume inside fragment", func(t *testing.T) {
		events := assemblyEvents(130)
		fragments := append(h.stage(t, events[:70]), h.stage(t, events[70:])...)
		claim := h.claim(fragments)
		result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil || result.Count != 100 {
			t.Fatal(result, err)
		}
		artifacts := h.readResult(t, result)
		if !slices.Equal(artifacts.next, []int{70, 30}) {
			t.Fatal("cursor did not stop inside the second fragment", artifacts.next)
		}
		if !reflect.DeepEqual(artifacts.payload.Events, events[:100]) {
			t.Fatal("first chunk membership diverged")
		}
		resume := h.claim(fragments[1:])
		resume.Inputs[0].Fragment.Next = artifacts.next[1]
		resume.Inputs[0].Eligible = resume.Inputs[0].Eligible[artifacts.next[1]:]
		rest, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, resume)
		if err != nil || rest.Count != 30 {
			t.Fatal(rest, err)
		}
		artifacts = h.readResult(t, rest)
		if !slices.Equal(artifacts.next, []int{60}) {
			t.Fatal("cursor did not consume the rest of the fragment", artifacts.next)
		}
		if !reflect.DeepEqual(artifacts.payload.Events, events[100:]) {
			t.Fatal("resumed chunk membership diverged")
		}
	})
	t.Run("byte limit and oversized singleton", func(t *testing.T) {
		events := assemblyEvents(3)
		for i := range events {
			events[i].Properties = json.RawMessage(fmt.Sprintf(`{"value":%q}`, strings.Repeat("x", 520000)))
		}
		claim := h.claim(h.stage(t, events))
		result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil || result.Count != 2 || result.Body.Bytes > delivery.WebhookChunkBytes {
			t.Fatal(result, err)
		}
		h.readResult(t, result)
		events = assemblyEvents(2)
		events[0].Properties = json.RawMessage(fmt.Sprintf(`{"value":%q}`, strings.Repeat("x", delivery.WebhookChunkBytes)))
		claim = h.claim(h.stage(t, events))
		result, err = output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil || result.Count != 1 || result.Body.Bytes <= delivery.WebhookChunkBytes {
			t.Fatal(result, err)
		}
		h.readResult(t, result)
	})
	t.Run("recomposition before and after possible attempt and late work", func(t *testing.T) {
		events := assemblyEvents(3)
		claim := h.claim(h.stage(t, events))
		original, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
		if err != nil {
			t.Fatal(err)
		}
		for _, stage := range []string{"before attempt", "after possible attempt"} {
			t.Log(stage)
			survivor := claim
			survivor.Inputs = slices.Clone(claim.Inputs)
			survivor.Inputs[0].Eligible = []int{0, 2}
			result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, survivor)
			if err != nil || result.ID == original.ID {
				t.Fatal("recomposed body reused delivery ID", result, err)
			}
			payload := h.readResult(t, result).payload
			if len(payload.Events) != 2 || payload.Events[1].UUID != events[2].UUID {
				t.Fatal(payload)
			}
		}
		unchanged := h.readResult(t, original).payload
		if !reflect.DeepEqual(unchanged.Events, events) {
			t.Fatal("recomposition amended immutable original")
		}
		late := h.claim(h.stage(t, assemblyEvents(1)))
		late.Generation = claim.Generation
		result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, late)
		if err != nil || result.ID == original.ID {
			t.Fatal("late-window work reused frozen ID", result, err)
		}
	})
	t.Run("mismatches tampering truncation and private fields", func(t *testing.T) {
		for _, kind := range []string{"member", "received window", "private control", "raw peer", "tampered bytes", "truncated", "extra bytes"} {
			t.Run(kind, func(t *testing.T) {
				claim := h.claim(h.stage(t, assemblyEvents(1)))
				ref := nativeRef(claim.Inputs[0].Fragment.Body)
				raw, err := h.objects.ReadImmutableEffectObject(t.Context(), h.scope, ref)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "member":
					claim.Inputs[0].Fragment.Members[0].UUID = eventapi.NewID()
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
					claim.Inputs[0].Fragment.Body = pipelineRef(ref)
					claim.Inputs[0].Fragment.Members[0].Bytes = int64(len(eventRaw))
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
				result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
				if err == nil || result != (output.PipelineAssemblyResult{}) {
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
	claim := h.claim(h.stage(t, assemblyEvents(2)))
	ref := nativeRef(claim.Inputs[0].Fragment.Body)
	if ref.VersionID == "" {
		t.Fatal("versioned fragment lost pin")
	}
	if _, err := fixture.client.PutObject(t.Context(), &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(ref.Key), Body: strings.NewReader(`{"changed":"latest"}`)}); err != nil {
		t.Fatal(err)
	}
	result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err != nil || result.Count != 2 {
		t.Fatal("assembly followed changed latest version", result, err)
	}
	if result.Body.VersionID == "" || result.Manifest.VersionID == "" {
		t.Fatal("versioned assembly artifacts lost their pins", result)
	}
	h.readResult(t, result)
	claim.Inputs[0].Fragment.Body.VersionID = ""
	if _, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim); err == nil {
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
	claim := h.claim(h.stage(t, assemblyEvents(3)))
	loseNext.Store(true)
	result, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err != nil || lost.Load() != 1 {
		t.Fatal("lost successful PUT was not recovered", result, lost.Load(), err)
	}
	h.readResult(t, result)
	retry, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err != nil || !reflect.DeepEqual(retry, result) {
		t.Fatal("ambiguous PUT retry changed output", retry, result, err)
	}
	// A completed body PUT cannot be reported as a complete assembly when the
	// manifest fails. Retrying recovers that orphan and produces the manifest.
	claim = h.claim(h.stage(t, assemblyEvents(2)))
	rejectManifest.Store(true)
	failed, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err == nil || failed != (output.PipelineAssemblyResult{}) || bodyPath.Load().(string) == "" {
		t.Fatal("manifest failure returned partial success", failed, err)
	}
	rejectManifest.Store(false)
	recovered, err := output.StageWebhookAssembly(t.Context(), h.objects, h.scope, claim)
	if err != nil || !strings.HasSuffix(bodyPath.Load().(string), recovered.Body.Key) {
		t.Fatal("retry did not recover the orphaned body", recovered, err)
	}
	h.readResult(t, recovered)
}
