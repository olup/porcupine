//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDuckLakeEventProfileCumulativeSQLTransferBudget(t *testing.T) {
	s := duckLakeJourneyService(t)
	a, _ := project(t, s)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return now }
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	properties, err := json.Marshal(map[string]any{"app": "same-app", "padding": strings.Repeat("x", 15<<10)})
	if err != nil || len(properties) >= eventProfileFieldBytes {
		t.Fatalf("fixture must fit the individual field budget: bytes=%d error=%v", len(properties), err)
	}
	for start := 0; start < 1000; start += 10 {
		batch := make([]eventProjection, 0, 10)
		for i := start; i < start+10; i++ {
			projection := lakeStorageProjection(t, a, fmt.Sprintf("profile-budget-%04d", i), int64(i+1))
			projection.Event.Event = "transfer-budget"
			projection.Properties = properties
			projection.OriginalSnapshot, err = originalSnapshot(projection.Event)
			if err != nil {
				t.Fatal(err)
			}
			batch = append(batch, projection)
		}
		if err := s.lake.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	input := EventProfileInput{ID: a, Event: "transfer-budget", SampleLimit: 1000, ExampleLimit: 3, PropertyLimit: 20, PropertyKind: "custom", IncludeValues: true}
	options, err := normalizeEventProfileInput(input)
	if err != nil {
		t.Fatal(err)
	}
	read, err := s.lake.Profile(ctx, a, options, now)
	if err != nil {
		t.Fatal(err)
	}
	retained, excluded := 0, 0
	for _, row := range read.rows {
		retained += len(row.Properties) + len(row.Context)
		if row.Properties == "" {
			excluded++
		}
		if row.PropertyBytes != len(properties) {
			t.Fatalf("original size was lost for a bounded row: got=%d want=%d", row.PropertyBytes, len(properties))
		}
	}
	if len(read.rows) != 1000 || retained <= 0 || retained > eventProfileSampleBytes || excluded == 0 {
		t.Fatalf("SQL did not bound cumulative transfer before Go decoding: rows=%d retained=%d excluded=%d", len(read.rows), retained, excluded)
	}
	result, err := s.eventProfile(ctx, &input)
	if err != nil {
		t.Fatal(err)
	}
	profile := result.Body
	if profile.Activity.Total != "1000" || profile.Sampling.SampledEvents != 1000 || profile.Sampling.ProfiledEvents <= 0 || profile.Sampling.ProfiledEvents >= 1000 || profile.Sampling.OversizedEvents != 0 || profile.Sampling.ByteBudgetExcludedEvents != excluded || profile.Sampling.ProfiledEvents+excluded != 1000 || profile.Sampling.SampleTruncated {
		t.Fatalf("cumulative exclusions were not reported separately: %+v", profile.Sampling)
	}
	if len(profile.Examples) != 3 {
		t.Fatalf("example limit changed with the sample: %d", len(profile.Examples))
	}
	for _, key := range []string{"app", "padding"} {
		property := profileProperty(t, profile, key)
		if property.Present != profile.Sampling.ProfiledEvents || property.Missing != 0 || property.Null != 0 || property.DistinctValues != 1 {
			t.Fatalf("byte-budget exclusions became false missing values for %s: %+v", key, property)
		}
	}
	raw, err := json.Marshal(profile)
	if err != nil || len(raw) > analyticsMaxBytes {
		t.Fatalf("profile response exceeded its public JSON budget: bytes=%d error=%v", len(raw), err)
	}
	t.Logf("sampled=1000 profiled=%d byte_budget_excluded=%d retained_payload_bytes=%d response_bytes=%d", profile.Sampling.ProfiledEvents, excluded, retained, len(raw))
}
