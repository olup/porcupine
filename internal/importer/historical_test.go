package importer

import (
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/identity"
)

func TestHistoricalPersonOwnershipAndVersions(t *testing.T) {
	t.Parallel()
	previous := HistoricalPersonState{Found: true, SourceID: "archive", PersonID: "internal", PersonVersion: 10, MappingVersion: 20, ProfileVersion: 7}
	owned := HistoricalIdentityState{ProfileFound: true, ProfileVersion: 7, Person: previous}
	row := Row{Kind: "persons", Event: event.Event{SourceID: "archive"}, Person: Person{IdentifiedID: "user", InternalID: "internal", PersonVersion: 11, MappingVersion: 20, Properties: json.RawMessage(`{"n":9007199254740993}`)}}
	cases := []struct {
		name   string
		change func(*Row, *HistoricalIdentityState)
		accept bool
	}{
		{"same mapping newer person", func(*Row, *HistoricalIdentityState) {}, true},
		{"equal person", func(r *Row, _ *HistoricalIdentityState) { r.Person.PersonVersion = 10 }, false},
		{"older person", func(r *Row, _ *HistoricalIdentityState) { r.Person.PersonVersion = 9 }, false},
		{"older mapping newer person", func(r *Row, _ *HistoricalIdentityState) {
			r.Person.MappingVersion = 19
			r.Person.PersonVersion = math.MaxUint64
		}, false},
		{"newer mapping older person", func(r *Row, _ *HistoricalIdentityState) { r.Person.MappingVersion = 21; r.Person.PersonVersion = 1 }, true},
		{"different person same mapping", func(r *Row, _ *HistoricalIdentityState) {
			r.Person.InternalID = "other"
			r.Person.PersonVersion = math.MaxUint64
		}, false},
		{"different person newer mapping", func(r *Row, _ *HistoricalIdentityState) { r.Person.InternalID = "other"; r.Person.MappingVersion = 21 }, true},
		{"foreign source", func(_ *Row, s *HistoricalIdentityState) { s.Person.SourceID = "other" }, false},
		{"live un-identified version", func(_ *Row, s *HistoricalIdentityState) { s.ProfileVersion++ }, false},
		{"identified profile", func(_ *Row, s *HistoricalIdentityState) { s.Profile.Identified = true }, false},
		{"unowned profile", func(_ *Row, s *HistoricalIdentityState) { s.Person.Found = false }, false},
		{"current link", func(_ *Row, s *HistoricalIdentityState) { s.Link.Target = "account" }, false},
		{"missing profile same source", func(_ *Row, s *HistoricalIdentityState) { s.ProfileFound = false }, true},
		{"empty identity", func(r *Row, _ *HistoricalIdentityState) { r.Person.IdentifiedID = "" }, false},
		{"max unsigned person", func(r *Row, s *HistoricalIdentityState) {
			r.Person.PersonVersion = math.MaxUint64
			s.Person.PersonVersion = math.MaxUint64 - 1
		}, true},
		{"max unsigned mapping", func(r *Row, s *HistoricalIdentityState) {
			r.Person.MappingVersion = math.MaxUint64
			s.Person.MappingVersion = math.MaxUint64 - 1
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, state := row, owned
			tc.change(&r, &state)
			got, err := ReduceHistorical(r, HistoricalState{Target: state}, time.Unix(100, 0))
			if err != nil {
				t.Fatal(err)
			}
			if tc.accept {
				if got.Accepted != 1 || got.Unresolved != 1 || got.Ignored != 0 || got.Profile == nil || got.Profile.Identified || string(got.Profile.Traits) != string(row.Person.Properties) || got.Person.PersonVersion != r.Person.PersonVersion || got.Person.MappingVersion != r.Person.MappingVersion {
					t.Fatalf("accepted snapshot: %+v", got)
				}
			} else if got.Accepted != 0 || got.Ignored != 1 || got.Unresolved != 1 || got.Person != nil || got.Profile != nil {
				t.Fatalf("ignored snapshot: %+v", got)
			}
		})
	}
}

func TestHistoricalDeletionAndOccurrenceTime(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 17, 12, 0, 0, 123, time.UTC)
	row := Row{Kind: "persons", Person: Person{IdentifiedID: "user", InternalID: "internal", PersonVersion: 1, MappingVersion: 1, Deleted: true, Properties: json.RawMessage(`null`)}}
	got, err := ReduceHistorical(row, HistoricalState{}, at)
	if err != nil || got.Ignored != 1 || got.Accepted != 0 || got.Unresolved != 0 || got.Person == nil || !got.Person.Deleted || got.Person.ProfileVersion != 0 {
		t.Fatalf("missing-profile tombstone: %+v %v", got, err)
	}
	state := HistoricalState{Target: HistoricalIdentityState{ProfileFound: true, ProfileVersion: 5, Person: HistoricalPersonState{Found: true, SourceID: "posthog", ProfileVersion: 5}}}
	got, err = ReduceHistorical(row, state, at)
	if err != nil || got.Accepted != 1 || got.Unresolved != 0 || string(got.Profile.Traits) != `{}` || got.Event.Timestamp != at.Format(time.RFC3339Nano) {
		t.Fatalf("owned deletion: %+v %v", got, err)
	}
	row.Person.Deleted, row.Person.Properties = false, json.RawMessage(`{}`)
	row.Person.Timestamp = at.Add(-365 * 24 * time.Hour)
	got, err = ReduceHistorical(row, state, at)
	if err != nil || got.Event.Timestamp != row.Person.Timestamp.Format(time.RFC3339Nano) {
		t.Fatalf("original created_at: %+v %v", got, err)
	}
	ordinary := Row{Kind: "events", Event: event.Event{Event: "page", DistinctID: "cookieless_archived", Timestamp: "2020-01-01T00:00:00.123456Z", Properties: json.RawMessage(`{"x":1}`)}}
	got, err = ReduceHistorical(ordinary, HistoricalState{}, at)
	if err != nil || got.Event.Timestamp != ordinary.Event.Timestamp || got.Event.DistinctID != ordinary.Event.DistinctID || !got.Event.Imported || got.Control {
		t.Fatalf("ordinary occurrence: %+v %v", got, err)
	}
}

func TestHistoricalIdentifyEvidence(t *testing.T) {
	t.Parallel()
	owned := HistoricalIdentityState{ProfileFound: true, ProfileVersion: 9, Profile: identity.Profile{Traits: json.RawMessage(`{"target":true,"same":"target"}`)}, Person: HistoricalPersonState{Found: true, SourceID: "posthog", ProfileVersion: 9}}
	source := owned
	source.Profile.Traits = json.RawMessage(`{"anonymous":9007199254740993,"same":"source"}`)
	row := Row{Kind: "events", Event: event.Event{Event: "$identify", DistinctID: "user", Properties: json.RawMessage(`{"$anon_distinct_id":"browser","$set":{"stale":true},"$set_once":{"stale_once":true}}`), Context: json.RawMessage(`{"private":true}`)}}
	cases := []struct {
		name           string
		change         func(*Row, *HistoricalState)
		resolved, link bool
	}{
		{"explicit anonymous", func(*Row, *HistoricalState) {}, true, true},
		{"target only", func(r *Row, _ *HistoricalState) { r.Event.Properties = json.RawMessage(`{}`) }, true, false},
		{"same endpoint", func(r *Row, _ *HistoricalState) { r.Event.Properties = json.RawMessage(`{"$anon_distinct_id":"user"}`) }, true, false},
		{"missing endpoints", func(_ *Row, s *HistoricalState) { *s = HistoricalState{} }, true, true},
		{"invalid explicit anonymous", func(r *Row, _ *HistoricalState) { r.Event.Properties = json.RawMessage(`{"$anon_distinct_id":null}`) }, false, false},
		{"invalid target", func(r *Row, _ *HistoricalState) { r.Event.DistinctID = "cookieless_invalid" }, false, false},
		{"deleted target", func(_ *Row, s *HistoricalState) { s.Target.Person.Deleted = true; s.Target.ProfileFound = false }, false, false},
		{"deleted source", func(_ *Row, s *HistoricalState) { s.Anonymous.Person.Deleted = true }, false, false},
		{"live target", func(_ *Row, s *HistoricalState) { s.Target.ProfileVersion++ }, false, false},
		{"live source", func(_ *Row, s *HistoricalState) { s.Anonymous.ProfileVersion++ }, false, false},
		{"identified source", func(_ *Row, s *HistoricalState) { s.Anonymous.Profile.Identified = true }, false, false},
		{"foreign target", func(_ *Row, s *HistoricalState) { s.Target.Person.SourceID = "other" }, false, false},
		{"foreign source", func(_ *Row, s *HistoricalState) { s.Anonymous.Person.SourceID = "other" }, false, false},
		{"target linked", func(_ *Row, s *HistoricalState) { s.Target.Link.Target = "another" }, false, false},
		{"source already linked to target", func(_ *Row, s *HistoricalState) { s.Anonymous.Link.Target = "user" }, false, false},
		{"source conflict", func(_ *Row, s *HistoricalState) { s.Anonymous.Link.Target = "another" }, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r, state := row, HistoricalState{Target: owned, Anonymous: source}
			tc.change(&r, &state)
			got, err := ReduceHistorical(r, state, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Accepted != 1 || !got.Control || got.Event.DistinctID != "" || string(got.Event.Properties) != `{}` || string(got.Event.Context) != `{}` {
				t.Fatalf("unsafe canonical control: %+v", got)
			}
			if tc.resolved {
				if got.Unresolved != 0 || got.Profile == nil || !got.Profile.Identified || (got.Profile.Link != nil) != tc.link {
					t.Fatalf("resolved evidence: %+v", got)
				}
				var traits map[string]any
				if json.Unmarshal(got.Profile.Traits, &traits) != nil || traits["stale"] != nil || traits["stale_once"] != nil {
					t.Fatalf("incremental traits replayed: %s", got.Profile.Traits)
				}
				if tc.name == "explicit anonymous" && string(got.Profile.Traits) != `{"anonymous":9007199254740993,"same":"target","target":true}` {
					t.Fatalf("inheritance changed: %s", got.Profile.Traits)
				}
			} else if got.Unresolved != 1 || got.Profile != nil {
				t.Fatalf("unresolved evidence: %+v", got)
			}
		})
	}
}

func TestHistoricalControlsAndDetachedDecisions(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"$set", "$create_alias", "$groupidentify"} {
		row := Row{Kind: "events", Event: event.Event{Event: name, DistinctID: "user", Properties: json.RawMessage(`{"$set":{"x":1}}`), Context: json.RawMessage(`{"secret":1}`)}}
		got, err := ReduceHistorical(row, HistoricalState{}, time.Time{})
		if err != nil || got.Accepted != 1 || got.Unresolved != 1 || got.Profile != nil || got.Event.DistinctID != "" || string(got.Event.Properties) != `{}` || string(got.Event.Context) != `{}` {
			t.Fatalf("%s: %+v %v", name, got, err)
		}
	}
	row := Row{Kind: "events", Event: event.Event{Event: "ordinary", Properties: json.RawMessage(`{"x":1}`), Context: json.RawMessage(`{"y":2}`)}}
	before := row
	before.Event.Properties, before.Event.Context = append(json.RawMessage(nil), row.Event.Properties...), append(json.RawMessage(nil), row.Event.Context...)
	got, err := ReduceHistorical(row, HistoricalState{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	got.Event.Properties[2], got.Event.Context[2] = 'z', 'z'
	if !reflect.DeepEqual(row, before) {
		t.Fatal("decision aliases input row")
	}
	state := HistoricalState{Target: HistoricalIdentityState{Profile: identity.Profile{Traits: json.RawMessage(`{"x":1}`)}}}
	row.Event.Event, row.Event.DistinctID, row.Event.Properties = "$identify", "user", json.RawMessage(`{}`)
	got, err = ReduceHistorical(row, state, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	got.Profile.Traits[2] = 'z'
	if string(state.Target.Profile.Traits) != `{"x":1}` {
		t.Fatal("decision aliases profile state")
	}
	person := Row{Kind: "persons", Person: Person{IdentifiedID: "user", Properties: json.RawMessage(`{"x":{"nested":true}}`)}}
	got, err = ReduceHistorical(person, HistoricalState{}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	got.Profile.Traits[2] = 'z'
	if string(person.Person.Properties) != `{"x":{"nested":true}}` {
		t.Fatal("decision aliases person properties")
	}
	if _, err = ReduceHistorical(Row{Kind: "invalid"}, HistoricalState{}, time.Time{}); err != ErrRow {
		t.Fatalf("invalid kind: %v", err)
	}
}
