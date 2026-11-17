package identity

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

type state struct {
	profiles map[string]Profile
	links    map[string]Link
	reads    int
}

func (s *state) Profile(_ context.Context, id string) (Profile, error) {
	s.reads++
	return s.profiles[id], nil
}
func (s *state) Link(_ context.Context, id string) (Link, error) { s.reads++; return s.links[id], nil }
func command(t *testing.T, name, target, raw string) Command {
	t.Helper()
	c, err := Parse(name, target, json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func TestEvaluateIdentityMergeAndDetachedDecision(t *testing.T) {
	t.Parallel()
	s := &state{profiles: map[string]Profile{
		"browser": {Traits: json.RawMessage(`{"first":"anonymous","plan":"anon"}`)},
		"person":  {Traits: json.RawMessage(`{"plan":"old","origin":"known"}`), Identified: true},
	}, links: map[string]Link{}}
	c := command(t, "$identify", "person", `{"$anon_distinct_id":"browser","$set":{"plan":"new","large":9007199254740993},"$set_once":{"origin":"ignored","campaign":"docs"}}`)
	result, err := Evaluate(t.Context(), c, s)
	if err != nil || result.Ignored || !result.Identified || result.Target != "person" || result.Link == nil || !result.Link.Create || result.Link.Anonymous != "browser" {
		t.Fatal(result, err)
	}
	if string(result.Traits) != `{"campaign":"docs","first":"anonymous","large":9007199254740993,"origin":"known","plan":"new"}` {
		t.Fatal(string(result.Traits))
	}
	if string(s.profiles["person"].Traits) != `{"plan":"old","origin":"known"}` || len(s.links) != 0 {
		t.Fatal("evaluation mutated stored state")
	}
	c.Set[0] = '!'
	if !json.Valid(result.Traits) {
		t.Fatal("decision retained command storage")
	}
}
func TestEvaluateIdentityConflictAndExistingLink(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		profile    Profile
		link       Link
		targetLink Link
		ignored    bool
	}{
		{name: "identified source", profile: Profile{Identified: true}, ignored: true},
		{name: "conflicting link", link: Link{Target: "other", Version: 7}, ignored: true},
		{name: "already same link", link: Link{Target: "person", Version: 7}},
		{name: "target is anonymous link", targetLink: Link{Target: "other", Version: 9}, ignored: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			s := &state{profiles: map[string]Profile{"browser": test.profile}, links: map[string]Link{"browser": test.link, "person": test.targetLink}}
			result, err := Evaluate(t.Context(), command(t, "$identify", "person", `{"$anon_distinct_id":"browser"}`), s)
			if err != nil || result.Ignored != test.ignored {
				t.Fatal(result, err)
			}
			if !test.ignored && (result.Link == nil || result.Link.Create || result.Link.Version != 7) {
				t.Fatal("existing link version lost", result)
			}
		})
	}
}
func TestEvaluateSetFollowsLinkWithoutIdentifyingAnonymousProfile(t *testing.T) {
	t.Parallel()
	s := &state{profiles: map[string]Profile{"person": {Traits: json.RawMessage(`{"first":"kept"}`)}}, links: map[string]Link{"browser": {Target: "person", Version: 2}}}
	result, err := Evaluate(t.Context(), command(t, "$set", "browser", `{"$set":{"plan":"team"}}`), s)
	if err != nil || result.Ignored || result.Target != "person" || result.Identified || result.Link != nil || string(result.Traits) != `{"first":"kept","plan":"team"}` {
		t.Fatal(result, err)
	}
}
func TestIdentityInvalidControlsRemainNoops(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{`{"$set":null}`, `{"$set":[]}`, `{"$anon_distinct_id":42}`, `{"$anon_distinct_id":"cookieless_private"}`} {
		s := &state{}
		result, err := Evaluate(t.Context(), command(t, "$identify", "person", raw), s)
		if err != nil || !result.Ignored || s.reads != 0 {
			t.Fatal("invalid control read or changed state", raw, result, err)
		}
	}
	if _, err := Parse("$identify", "person", json.RawMessage(`{`)); err == nil {
		t.Fatal("malformed JSON was accepted")
	}
}
func TestIdentityMergedBudgetFailureIsNoop(t *testing.T) {
	t.Parallel()
	s := &state{profiles: map[string]Profile{"person": {Traits: json.RawMessage(`{"large":"` + strings.Repeat("x", 32750) + `"}`)}}, links: map[string]Link{}}
	result, err := Evaluate(t.Context(), command(t, "$set", "person", `{"$set":{"new":"exceeds-budget"}}`), s)
	if err != nil || !result.Ignored {
		t.Fatal("merged profile exceeded shared trait budget", result, err)
	}
}
