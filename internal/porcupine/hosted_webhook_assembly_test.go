package porcupine

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/olup/porcupine/internal/delivery"
	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestHostedWebhookAssemblyMemberDecodeBound(t *testing.T) {
	t.Parallel()
	input := strings.NewReader(`[{},` + strings.Repeat(`0,`, 1<<20) + `0]`)
	if _, err := decodeHostedAssemblyMembers(input, 1); err == nil || err.Error() != "webhook fragment event count mismatch" {
		t.Fatalf("surplus members: %v", err)
	}
	if read := input.Size() - int64(input.Len()); read > 1024 {
		t.Fatalf("read %d bytes before rejecting surplus members", read)
	}
	for _, raw := range []string{`[]`, `[{},{}]`, `{}`, `[{}] {}`, `[{}] [`, `[{}]`, `[{} `} {
		wantValid := raw == `[{}]`
		members, err := decodeHostedAssemblyMembers(strings.NewReader(raw), 1)
		if (err == nil) != wantValid || wantValid && len(members) != 1 {
			t.Fatalf("decode %q: members=%d error=%v", raw, len(members), err)
		}
	}
	for _, count := range []int{0, -1, delivery.WebhookChunkEvents + 1} {
		if _, err := decodeHostedAssemblyMembers(strings.NewReader(`[{}]`), count); err == nil {
			t.Fatalf("accepted count %d", count)
		}
	}
}

func assemblyTestClaim(t *testing.T) frozenWebhookAssemblyClaim {
	t.Helper()
	key := lakeapi.ProjectWriteKey{SourceID: eventapi.NewID(), ProjectID: "project"}
	scope := lakeapi.HostedProjectScope{Key: key, CatalogID: lakeapi.SharedNativeCatalogID(key.SourceID, key.ProjectID), StreamID: eventapi.NewID()}
	target := frozenWebhookTarget{PipelineID: "pipeline", PipelineIncarnation: "pipeline-v1", RuleRevision: "rule-v1", DestinationRevision: "destination-v1", Rule: delivery.WebhookRule{DeliveryMode: "batch", IntervalSeconds: 60}}
	fragment := immutableWebhookFragment{FragmentID: "fragment", Target: target, EventUUIDs: []string{"a", "b"}, EventBytes: []int64{2, 2}, Body: lakeapi.ImmutableObjectRef{Key: "key", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Bytes: 7}, WindowStartMS: 0, WindowEndMS: 60000}
	claim := frozenWebhookAssemblyClaim{Scope: scope, TenantID: "tenant", Target: target, WindowStartMS: 0, WindowEndMS: 60000, Generation: "generation-v1", AttemptID: eventapi.NewID(), Fragments: []hostedAssemblyFragment{{Position: hostedFragmentPosition{scope, 1, fragment.FragmentID, 0}, Fragment: fragment, Members: []int{0, 1}}}, MaxFragments: hostedAssemblyMaxFragments, MaxReadBytes: hostedAssemblyMaxReadBytes, MaxBodyBytes: lakeapi.MaxImmutableEffectObjectBytes}
	freezeAssemblyTestClaim(t, &claim)
	return claim
}

func freezeAssemblyTestClaim(t *testing.T, claim *frozenWebhookAssemblyClaim) {
	t.Helper()
	fingerprint, err := hostedAssemblyFingerprint(claim.Fragments)
	if err != nil {
		t.Fatal(err)
	}
	claim.MembershipFingerprint = fingerprint
}

func cloneAssemblyTestClaim(t *testing.T, claim frozenWebhookAssemblyClaim) frozenWebhookAssemblyClaim {
	t.Helper()
	raw, err := json.Marshal(claim)
	if err != nil {
		t.Fatal(err)
	}
	var copy frozenWebhookAssemblyClaim
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func TestHostedWebhookAssemblyClaimBounds(t *testing.T) {
	t.Parallel()
	claim := assemblyTestClaim(t)
	if err := validateHostedAssemblyClaim(claim); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*frozenWebhookAssemblyClaim)
	}{
		{"destination", func(c *frozenWebhookAssemblyClaim) { c.Target.DestinationRevision = "destination-v2" }},
		{"pipeline incarnation", func(c *frozenWebhookAssemblyClaim) { c.Target.PipelineIncarnation = "pipeline-v2" }},
		{"stream", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.Scope.StreamID = eventapi.NewID() }},
		{"source", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.Scope.Key.SourceID = eventapi.NewID() }},
		{"catalog", func(c *frozenWebhookAssemblyClaim) { c.Scope.CatalogID = "other" }},
		{"window", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Fragment.WindowEndMS++ }},
		{"cursor negative", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.NextMember = -1 }},
		{"cursor past fragment", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.NextMember = 2 }},
		{"cursor skipped backwards", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.NextMember = 1 }},
		{"member duplicate", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Members = []int{0, 0} }},
		{"member unordered", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Members = []int{1, 0} }},
		{"member past fragment", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Members = []int{2} }},
		{"member bytes", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Fragment.EventBytes[0]++ }},
		{"zero sequence", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.CommitSequence = 0 }},
		{"fragment identity", func(c *frozenWebhookAssemblyClaim) { c.Fragments[0].Position.FragmentID = "other" }},
		{"fragment budget", func(c *frozenWebhookAssemblyClaim) { c.MaxFragments = hostedAssemblyMaxFragments + 1 }},
		{"read budget", func(c *frozenWebhookAssemblyClaim) { c.MaxReadBytes = 6 }},
		{"body budget", func(c *frozenWebhookAssemblyClaim) { c.MaxBodyBytes = lakeapi.MaxImmutableEffectObjectBytes + 1 }},
		{"unbounded window", func(c *frozenWebhookAssemblyClaim) { c.WindowEndMS = 1 << 62 }},
		{"missing generation", func(c *frozenWebhookAssemblyClaim) { c.Generation = "" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			copy := cloneAssemblyTestClaim(t, claim)
			test.mutate(&copy)
			freezeAssemblyTestClaim(t, &copy)
			if err := validateHostedAssemblyClaim(copy); err == nil {
				t.Fatal("invalid frozen claim accepted")
			}
		})
	}
	changed := cloneAssemblyTestClaim(t, claim)
	changed.Fragments[0].Members = []int{1}
	if err := validateHostedAssemblyClaim(changed); err == nil {
		t.Fatal("changed membership accepted under old fingerprint")
	}
}
