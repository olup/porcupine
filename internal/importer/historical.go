package importer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/identity"
	"github.com/olup/porcupine/internal/ingest"
)

// HistoricalPersonState records which source and snapshot own a profile.
// Person and mapping versions are unsigned PostHog versions, not event IDs.
type HistoricalPersonState struct {
	Found                         bool
	SourceID, PersonID            string
	PersonVersion, MappingVersion uint64
	ProfileVersion                uint64
	Deleted                       bool
}

type HistoricalIdentityState struct {
	ProfileFound   bool
	ProfileVersion uint64
	Profile        identity.Profile
	Link           identity.Link
	Person         HistoricalPersonState
}

// HistoricalState is detached transaction-local state for the IDs returned by
// HistoricalStateIDs. Each backend reloads it after preceding rows are applied.
type HistoricalState struct {
	Target, Anonymous HistoricalIdentityState
}

// HistoricalDecision contains no allocated event ID or snapshot version.
// Accepted profile/person changes use the adapter's accepted event ID as their
// version. An ignored person's tombstone uses profile version zero.
type HistoricalDecision struct {
	Accepted, Ignored, Unresolved int64
	Event                         event.Event
	Control                       bool
	Profile                       *identity.Decision
	Person                        *HistoricalPersonState
}

// HistoricalStateIDs selects only explicit, valid identity evidence. It does
// not infer an application-user target from a PostHog internal person ID.
func HistoricalStateIDs(row Row) (target, anonymous string) {
	if row.Kind == "persons" {
		if identity.ValidID(row.Person.IdentifiedID) {
			return row.Person.IdentifiedID, ""
		}
		return "", ""
	}
	if row.Event.Event != "$identify" || !identity.ValidID(row.Event.DistinctID) {
		return "", ""
	}
	var properties map[string]any
	_ = ingest.DecodeJSON(row.Event.Properties, &properties)
	if value, exists := properties["$anon_distinct_id"]; exists {
		id, ok := value.(string)
		if !ok || !identity.ValidID(id) {
			return "", ""
		}
		anonymous = id
	}
	return row.Event.DistinctID, anonymous
}

// ReduceHistorical evaluates a row only after UUID deduplication. It never
// reads storage or a clock, allocates IDs, or mutates the supplied row/state.
func ReduceHistorical(row Row, state HistoricalState, acceptedAt time.Time) (HistoricalDecision, error) {
	if row.Kind != "events" && row.Kind != "persons" {
		return HistoricalDecision{}, ErrRow
	}
	canonical := row.Event
	canonical.Properties = append(json.RawMessage(nil), canonical.Properties...)
	canonical.Context = append(json.RawMessage(nil), canonical.Context...)
	canonical.Imported = true
	source := canonical.SourceID
	if source == "" {
		source = "posthog"
	}
	target, anonymous := HistoricalStateIDs(row)
	decision := HistoricalDecision{Accepted: 1, Event: canonical}
	if row.Kind == "persons" {
		p, previous := row.Person, state.Target.Person
		if target == "" {
			return HistoricalDecision{Ignored: 1, Unresolved: 1}, nil
		}
		if previous.Found && (previous.SourceID != source || p.InternalID != previous.PersonID && p.MappingVersion <= previous.MappingVersion || p.MappingVersion < previous.MappingVersion || p.MappingVersion == previous.MappingVersion && p.PersonVersion <= previous.PersonVersion) {
			return HistoricalDecision{Ignored: 1, Unresolved: 1}, nil
		}
		if !historicalProfileOwned(state.Target, source) || state.Target.Link.Target != "" {
			return HistoricalDecision{Ignored: 1, Unresolved: 1}, nil
		}
		decision.Person = &HistoricalPersonState{Found: true, SourceID: source, PersonID: p.InternalID, PersonVersion: p.PersonVersion, MappingVersion: p.MappingVersion, Deleted: p.Deleted}
		if p.Deleted && !state.Target.ProfileFound {
			decision.Accepted, decision.Ignored = 0, 1
			return decision, nil
		}
		properties := append(json.RawMessage(nil), p.Properties...)
		if p.Deleted {
			properties = json.RawMessage(`{}`)
		}
		var traits map[string]any
		if ingest.DecodeJSON(properties, &traits) != nil {
			return HistoricalDecision{}, ErrRow
		}
		decision.Profile = &identity.Decision{Target: target, Traits: properties}
		decision.Event.Event, decision.Event.DistinctID = "$set", target
		decision.Event.Properties = append(json.RawMessage(nil), properties...)
		when := p.Timestamp
		if when.IsZero() {
			when = acceptedAt
		}
		decision.Event.Timestamp = when.UTC().Format(time.RFC3339Nano)
		if !p.Deleted {
			decision.Unresolved = 1
		}
	}
	decision.Control = decision.Event.Event == "$identify" || decision.Event.Event == "$set" || decision.Event.Event == "$create_alias" || decision.Event.Event == "$groupidentify"
	if !decision.Control {
		return decision, nil
	}
	decision.Event.DistinctID = ""
	decision.Event.Properties, decision.Event.Context = json.RawMessage(`{}`), json.RawMessage(`{}`)
	if row.Kind == "persons" {
		return decision, nil
	}
	decision.Unresolved = 1
	if row.Event.Event != "$identify" || target == "" || !historicalEvidenceOwned(state.Target, source) || state.Target.Link.Target != "" {
		return decision, nil
	}
	if anonymous != "" && anonymous != target && (!historicalEvidenceOwned(state.Anonymous, source) || state.Anonymous.Link.Target != "") {
		return decision, nil
	}
	// Archived incremental traits are deliberately absent. Evaluate only the
	// explicit identify evidence, retaining its existing trait inheritance.
	profile, err := identity.Evaluate(context.Background(), identity.Command{Operation: identity.Identify, Target: target, Anonymous: anonymous, Set: json.RawMessage(`{}`), SetOnce: json.RawMessage(`{}`)}, historicalIdentityReader{target: target, state: state})
	if err != nil {
		return HistoricalDecision{}, err
	}
	if !profile.Ignored {
		decision.Profile, decision.Unresolved = &profile, 0
	}
	return decision, nil
}

func historicalProfileOwned(state HistoricalIdentityState, source string) bool {
	if !state.ProfileFound {
		return !state.Person.Found || state.Person.SourceID == source
	}
	return !state.Profile.Identified && state.Person.Found && state.Person.ProfileVersion == state.ProfileVersion && state.Person.SourceID == source
}

func historicalEvidenceOwned(state HistoricalIdentityState, source string) bool {
	return historicalProfileOwned(state, source) && !state.Person.Deleted
}

type historicalIdentityReader struct {
	target string
	state  HistoricalState
}

func (r historicalIdentityReader) Profile(_ context.Context, id string) (identity.Profile, error) {
	if id == r.target {
		return r.state.Target.Profile, nil
	}
	return r.state.Anonymous.Profile, nil
}

func (r historicalIdentityReader) Link(_ context.Context, id string) (identity.Link, error) {
	if id == r.target {
		return r.state.Target.Link, nil
	}
	return r.state.Anonymous.Link, nil
}
