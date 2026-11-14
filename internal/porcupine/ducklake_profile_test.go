//go:build ducklake

package porcupine

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"
)

func readProfileHTTP(t *testing.T, s *Service, project, event, token, options string) EventProfile {
	t.Helper()
	result := request(t, s, http.MethodGet, profileRequestPath(project, event)+options, token, "", http.StatusOK)
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var profile EventProfile
	if err := json.Unmarshal(raw, &profile); err != nil {
		t.Fatal(err)
	}
	return profile
}

func profileProperty(t *testing.T, profile EventProfile, key string) EventProfileProperty {
	t.Helper()
	for _, property := range profile.Properties {
		if property.Key == key {
			return property
		}
	}
	t.Fatalf("property %q absent: %+v", key, profile)
	return EventProfileProperty{}
}

func profileJourneyFixture(t *testing.T) (*Service, string, string, string, string, string, time.Time) {
	t.Helper()
	s := duckLakeJourneyService(t)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return now }
	a, ingest := project(t, s)
	b, otherIngest := project(t, s)
	readerID, reader := key(t, s, []string{"events:read", "management:read"}, []string{a})
	name := "Message ' / ? é"
	rows := []struct {
		uuid  string
		at    time.Time
		props map[string]any
	}{
		{"profile-week-out", now.Add(-7*24*time.Hour - time.Millisecond), map[string]any{"app": "old"}},
		{"profile-week-start", now.Add(-7 * 24 * time.Hour), map[string]any{"app": "week"}},
		{"profile-day-start", now.Add(-24 * time.Hour), map[string]any{"app": "day"}},
		{"profile-recent", now.Add(-time.Hour), map[string]any{"app": "live", "kind": 42, "nullish": nil}},
		{"profile-window-end", now, map[string]any{"app": "live", "kind": "text", "nullish": "set"}},
		{"profile-future", now.Add(time.Hour), map[string]any{"app": "future"}},
		{"profile-old-import", now.Add(-365 * 24 * time.Hour), map[string]any{"old_only": "retired"}},
	}
	for i, row := range rows {
		receipt := now.Add(-2*time.Hour + time.Duration(i)*time.Minute)
		s.clock = func() time.Time { return receipt }
		row.props["$sdk"] = "reserved-profile-value"
		identityCapture(t, s, ingest, []map[string]any{{"uuid": row.uuid, "event": name, "distinct_id": "browser", "timestamp": row.at.Format(time.RFC3339Nano), "properties": row.props}}, 200)
	}
	s.clock = func() time.Time { return now }
	identityCapture(t, s, ingest, []map[string]any{{"uuid": "profile-other-event", "event": "other-event", "distinct_id": "browser", "properties": map[string]any{"unrelated": "not-the-requested-event"}}}, 200)
	identityCapture(t, s, otherIngest, []map[string]any{{"uuid": "foreign-profile", "event": name, "distinct_id": "foreign-person", "properties": map[string]any{"foreign_only": "do-not-leak-profile"}}}, 200)
	duckLakeJourneyDrain(t, s)
	return s, a, b, readerID, reader, name, now
}

func TestDuckLakeEventProfileAgentHTTPMCPAndRevocation(t *testing.T) {
	s, a, b, readerID, reader, event, _ := profileJourneyFixture(t)
	path := profileRequestPath(a, event)
	viaHTTP := request(t, s, http.MethodGet, path, reader, "", http.StatusOK)
	raw, _ := json.Marshal(viaHTTP)
	for _, unwanted := range []string{"foreign_only", "do-not-leak-profile", "foreign-person", "not-the-requested-event"} {
		if strings.Contains(string(raw), unwanted) {
			t.Fatalf("profile leaked unrelated data %q: %s", unwanted, raw)
		}
	}
	request(t, s, http.MethodGet, profileRequestPath(b, event), reader, "", http.StatusForbidden)
	host := httptest.NewServer(s.Handler())
	defer host.Close()
	ctx, session := mcpTestSession(t, host.URL, reader, "2025-11-25")
	tools, err := session.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 2 {
		t.Fatalf("expected two progressive discovery tools: %v %v", tools, err)
	}
	docs := mcpTestTool(t, ctx, session, "porcupine_docs", mcpDocsInput{OperationID: "get-event-profile"}, false)
	if docs["contract"] == nil {
		t.Fatal("event profile contract was not discoverable")
	}
	viaMCP := mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-profile", Path: map[string]string{"id": a}, Query: map[string]string{"event": event}}, 200)
	if !reflect.DeepEqual(viaHTTP, viaMCP) {
		t.Fatalf("HTTP and MCP profile differ:\nHTTP=%v\nMCP=%v", viaHTTP, viaMCP)
	}
	mcpTestCall(t, ctx, session, mcpCallInput{OperationID: "get-event-profile", Path: map[string]string{"id": b}, Query: map[string]string{"event": event}}, 403)
	request(t, s, http.MethodDelete, "/api/v1/access-keys/"+readerID, adminToken, "", http.StatusNoContent)
	request(t, s, http.MethodGet, path, reader, "", http.StatusUnauthorized)
	if _, err := session.ListTools(ctx, nil); err == nil || !strings.Contains(err.Error(), "Unauthorized") {
		t.Fatalf("revoked reader retained MCP access: %v", err)
	}
}

func TestDuckLakeEventProfileSharesQueryBudget(t *testing.T) {
	s, a, _, _, reader, event, _ := profileJourneyFixture(t)
	s.querySlots <- struct{}{}
	s.querySlots <- struct{}{}
	defer func() { <-s.querySlots; <-s.querySlots }()
	request(t, s, http.MethodGet, profileRequestPath(a, event), reader, "", http.StatusTooManyRequests)
}

func TestDuckLakeEventProfileOccurrenceWindowsAndSample(t *testing.T) {
	s, a, _, _, reader, event, now := profileJourneyFixture(t)
	profile := readProfileHTTP(t, s, a, event, reader, "")
	activity := profile.Activity
	asOf, err := time.Parse(time.RFC3339Nano, profile.AsOf)
	if err != nil || !asOf.Equal(now) || profile.Event != event || activity.Total != "7" || activity.Last24Hours != "2" || activity.Last7Days != "3" || activity.FutureEvents != "2" {
		t.Fatalf("incorrect occurrence windows: %+v", profile)
	}
	for _, tc := range []struct {
		label string
		got   *string
		want  time.Time
	}{
		{"first occurrence", activity.FirstOccurredAt, now.Add(-365 * 24 * time.Hour)},
		{"last occurrence", activity.LastOccurredAt, now.Add(time.Hour)},
		{"first receipt", activity.FirstReceivedAt, now.Add(-2 * time.Hour)},
		{"last receipt", activity.LastReceivedAt, now.Add(-2*time.Hour + 6*time.Minute)},
	} {
		if tc.got == nil {
			t.Fatalf("missing %s", tc.label)
		}
		actual, err := time.Parse(time.RFC3339Nano, *tc.got)
		if err != nil || !actual.Equal(tc.want) {
			t.Fatalf("wrong %s: %v want=%v error=%v", tc.label, *tc.got, tc.want, err)
		}
	}
	if profile.Sampling.RequestedLimit != 1000 || profile.Sampling.SampledEvents != 7 || profile.Sampling.ProfiledEvents != 7 || profile.Sampling.HistoricalPropertiesComplete {
		t.Fatalf("incorrect sample metadata: %+v", profile.Sampling)
	}
	if len(profile.Examples) != 3 || profile.Examples[0].UUID != "profile-future" || profile.Examples[1].UUID != "profile-window-end" || profile.Examples[2].UUID != "profile-recent" {
		t.Fatalf("examples followed receipt order instead of occurrence order: %+v", profile.Examples)
	}

	small := readProfileHTTP(t, s, a, event, reader, "&sample_limit=3&example_limit=0&property_kind=custom")
	if len(small.Examples) != 0 || small.Sampling.RequestedLimit != 3 || small.Sampling.ProfiledEvents != 3 || small.Activity.Total != "7" {
		t.Fatalf("sample size changed activity or explicit example omission: %+v", small)
	}
	kind := profileProperty(t, small, "kind")
	if kind.Present != 2 || kind.Missing != 1 || kind.Null != 0 || kind.DistinctValues != 2 || !reflect.DeepEqual(kind.Types, []string{"Int64", "String"}) || len(kind.TopValues) != 0 {
		t.Fatalf("type drift or presence counts are wrong: %+v", kind)
	}
	nullish := profileProperty(t, small, "nullish")
	if nullish.Present != 2 || nullish.Missing != 1 || nullish.Null != 1 || nullish.DistinctValues != 1 || !reflect.DeepEqual(nullish.Types, []string{"Null", "String"}) {
		t.Fatalf("missing and null were conflated: %+v", nullish)
	}
	for _, property := range small.Properties {
		if strings.HasPrefix(property.Key, "$") || len(property.TopValues) != 0 {
			t.Fatalf("custom-only profile returned reserved keys or unsolicited values: %+v", property)
		}
	}
	values := readProfileHTTP(t, s, a, event, reader, "&sample_limit=3&example_limit=0&include_values=true&property_prefix=app")
	app := profileProperty(t, values, "app")
	if len(values.Properties) != 1 || app.Present != 3 || app.Missing != 0 || app.DistinctValues != 2 || len(app.TopValues) != 2 || string(app.TopValues[0].Value) != `"live"` || app.TopValues[0].Count != 2 {
		t.Fatalf("incorrect frequent values: %+v", values)
	}
	capped := readProfileHTTP(t, s, a, event, reader, "&property_kind=custom&property_limit=1&example_limit=0")
	if len(capped.Properties) != 1 || !capped.Sampling.PropertiesTruncated {
		t.Fatalf("property cap did not report truncation: %+v", capped)
	}
	for _, name := range []string{"missing-event", event + "' OR 1=1 --"} {
		empty := readProfileHTTP(t, s, a, name, reader, "")
		if empty.Activity.Total != "0" || empty.Activity.Last24Hours != "0" || empty.Activity.Last7Days != "0" || empty.Activity.FirstOccurredAt != nil || empty.Activity.LastOccurredAt != nil || empty.Activity.FirstReceivedAt != nil || empty.Activity.LastReceivedAt != nil || len(empty.Examples) != 0 || len(empty.Properties) != 0 {
			t.Fatalf("empty or literal event filter fabricated data: %+v", empty)
		}
	}
}

func TestDuckLakeEventProfileCurrentEditsDeletionAndTieOrder(t *testing.T) {
	s := duckLakeJourneyService(t)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return now }
	a, ingest := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	at := now.Add(-time.Minute).Format(time.RFC3339Nano)
	identityCapture(t, s, ingest, []map[string]any{
		{"uuid": "profile-tie-a", "event": "mutable-profile", "distinct_id": "browser", "timestamp": at, "properties": map[string]any{"app": "before"}},
		{"uuid": "profile-tie-b", "event": "mutable-profile", "distinct_id": "browser", "timestamp": at, "properties": map[string]any{"app": "other"}},
	}, 200)
	duckLakeJourneyDrain(t, s)
	initial := readProfileHTTP(t, s, a, "mutable-profile", reader, "&sample_limit=1&example_limit=1")
	if initial.Activity.Total != "2" || len(initial.Examples) != 1 || initial.Examples[0].UUID != "profile-tie-b" {
		t.Fatalf("same-time sample did not use durable ID descending: %+v", initial)
	}
	path := "/api/v1/projects/" + a + "/events/profile-tie-a"
	edit := fmt.Sprintf(`{"expected_revision":"0","idempotency_key":"profile-edit","snapshot":{"event":"corrected-profile","distinct_id":"browser","properties":{"app":"after"},"timestamp":%q}}`, at)
	updated := request(t, s, http.MethodPatch, path, adminToken, edit, http.StatusAccepted)
	duckLakeJourneyDrain(t, s)
	previous := readProfileHTTP(t, s, a, "mutable-profile", reader, "")
	corrected := readProfileHTTP(t, s, a, "corrected-profile", reader, "&include_values=true")
	if previous.Activity.Total != "1" || corrected.Activity.Total != "1" || len(corrected.Examples) != 1 || !strings.Contains(string(corrected.Examples[0].Properties), `"after"`) {
		t.Fatalf("profile ignored projected edit: before=%+v corrected=%+v", previous, corrected)
	}
	deleted, _ := json.Marshal(map[string]any{"expected_revision": updated["revision"], "idempotency_key": "profile-delete"})
	request(t, s, http.MethodDelete, path, adminToken, string(deleted), http.StatusAccepted)
	duckLakeJourneyDrain(t, s)
	removed := readProfileHTTP(t, s, a, "corrected-profile", reader, "&include_values=true")
	if removed.Activity.Total != "0" || len(removed.Examples) != 0 || len(removed.Properties) != 0 {
		t.Fatalf("deleted event remains in discovery profile: %+v", removed)
	}
}

func TestDuckLakeEventProfileLargePayloadsDoNotBecomeMissingProperties(t *testing.T) {
	s := duckLakeJourneyService(t)
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	s.clock = func() time.Time { return now }
	a, ingest := project(t, s)
	_, reader := key(t, s, []string{"events:read"}, []string{a})
	large := strings.Repeat("é", eventProfileFieldBytes)
	longValue := strings.Repeat("é", 120)
	identityCapture(t, s, ingest, []map[string]any{
		{"uuid": "profile-small", "event": "bounded-profile", "distinct_id": "browser", "timestamp": now.Add(-2 * time.Minute).Format(time.RFC3339Nano), "properties": map[string]any{"app": "safe", "text": longValue}},
		{"uuid": "profile-large", "event": "bounded-profile", "distinct_id": "browser", "timestamp": now.Add(-time.Minute).Format(time.RFC3339Nano), "properties": map[string]any{"oversized_only": large, "$current_url": large}},
	}, 200)
	duckLakeJourneyDrain(t, s)
	profile := readProfileHTTP(t, s, a, "bounded-profile", reader, "&example_limit=2&include_values=true&property_kind=custom")
	if profile.Activity.Total != "2" || profile.Sampling.SampledEvents != 2 || profile.Sampling.ProfiledEvents != 1 || profile.Sampling.OversizedEvents != 1 {
		t.Fatalf("oversized payload was not accounted for separately: %+v", profile)
	}
	app := profileProperty(t, profile, "app")
	if app.Present != 1 || app.Missing != 0 || app.Null != 0 {
		t.Fatalf("omitted oversized payload counted as an absent property: %+v", app)
	}
	value := profileProperty(t, profile, "text")
	if value.DistinctValues != 1 || !value.ValuesOmitted || len(value.TopValues) != 0 {
		t.Fatalf("long UTF-8 value preview was incorrectly included or lost cardinality: %+v", value)
	}
	if len(profile.Examples) != 2 || profile.Examples[0].UUID != "profile-large" || !slices.Contains(profile.Examples[0].TruncatedFields, "properties") || !slices.Contains(profile.Examples[0].TruncatedFields, "context") || string(profile.Examples[0].Properties) != "null" || string(profile.Examples[0].Context) != "null" {
		t.Fatalf("oversized example was not explicitly omitted: %+v", profile.Examples)
	}
	raw, err := json.Marshal(profile)
	if err != nil || len(raw) > analyticsMaxBytes || !utf8.Valid(raw) {
		t.Fatalf("invalid or oversized profile response: bytes=%d error=%v", len(raw), err)
	}
}

func TestDuckLakeEventProfileUsesOneSnapshotDuringConcurrentProjection(t *testing.T) {
	s := duckLakeJourneyService(t)
	a, _ := project(t, s)
	base := lakeStorageProjection(t, a, "profile-snapshot-base", 900)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := s.lake.Apply(ctx, []eventProjection{base}); err != nil {
		t.Fatal(err)
	}
	updates := make([]eventProjection, 24)
	for i := range updates {
		updates[i] = lakeStorageProjection(t, a, fmt.Sprintf("profile-snapshot-%02d", i), int64(1000+i))
	}
	var writes atomic.Int64
	done := make(chan struct{})
	failure := make(chan error, 1)
	go func() {
		defer close(done)
		for _, update := range updates {
			if err := s.lake.Apply(ctx, []eventProjection{update}); err != nil {
				failure <- err
				return
			}
			writes.Add(1)
			update.IsDeleted, update.Revision = true, 1
			if err := s.lake.Apply(ctx, []eventProjection{update}); err != nil {
				failure <- err
				return
			}
			writes.Add(1)
		}
	}()
	defer func() {
		cancel()
		<-done
	}()
	overlapped := false
	for range 60 {
		before := writes.Load()
		result, err := s.eventProfile(ctx, &EventProfileInput{ID: a, Event: "page_viewed", SampleLimit: 1000, ExampleLimit: 5, PropertyLimit: 20, PropertyPrefix: "page", PropertyKind: "custom"})
		if err != nil {
			t.Fatalf("concurrent event profile failed after %d writes: %v", writes.Load(), err)
		}
		overlapped = overlapped || writes.Load() != before
		profile := result.Body
		total, err := strconv.Atoi(profile.Activity.Total)
		if err != nil || total < 1 || total > 2 || profile.Sampling.SampledEvents != total || profile.Sampling.ProfiledEvents != total || len(profile.Examples) != total {
			t.Fatalf("activity and sample describe different snapshots: total=%s sample=%+v examples=%+v error=%v", profile.Activity.Total, profile.Sampling, profile.Examples, err)
		}
		property := profileProperty(t, profile, "page")
		if property.Present != total || property.Missing != 0 || property.DistinctValues != 1 {
			t.Fatalf("property profile disagrees with snapshot: total=%d property=%+v", total, property)
		}
	}
	<-done
	select {
	case err := <-failure:
		t.Fatalf("concurrent projection failed: %v", err)
	default:
	}
	if !overlapped {
		t.Fatal("test did not overlap a profile read with a committed projection")
	}
}
