// Package identity evaluates canonical identity commands without owning storage
// transactions. SQLite and native adapters read and apply the returned decision
// inside the transaction that owns UUID acceptance.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/olup/porcupine/internal/ingest"
)

type Operation uint8

const (
	Ignored Operation = iota
	Identify
	Set
)

type Command struct {
	Operation         Operation
	Target, Anonymous string
	Set, SetOnce      json.RawMessage
}

func IsControl(name string) bool { return name == "$identify" || name == "$set" }

func ValidID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 1000 && id != ingest.CookielessSentinel && !strings.HasPrefix(id, "cookieless_")
}

// Parse preserves SDK invalid-control no-ops. Invalid JSON remains an error;
// invalid identity IDs and trait shapes produce an Ignored command.
func Parse(name, target string, raw json.RawMessage) (Command, error) {
	if !IsControl(name) {
		return Command{}, errors.New("unsupported identity operation")
	}
	if !ValidID(target) {
		return Command{}, nil
	}
	var properties map[string]any
	if err := ingest.DecodeJSON(raw, &properties); err != nil {
		return Command{}, err
	}
	for _, key := range []string{"$set", "$set_once"} {
		if value, exists := properties[key]; exists && value == nil {
			return Command{}, nil
		}
	}
	set, validSet := ingest.Traits(properties["$set"])
	once, validOnce := ingest.Traits(properties["$set_once"])
	if !validSet || !validOnce {
		return Command{}, nil
	}
	command := Command{Operation: Set, Target: target}
	if name == "$identify" {
		command.Operation = Identify
		if raw, exists := properties["$anon_distinct_id"]; exists {
			anonymous, ok := raw.(string)
			if !ok || !ValidID(anonymous) {
				return Command{}, nil
			}
			command.Anonymous = anonymous
		}
	}
	command.Set, _ = json.Marshal(set)
	command.SetOnce, _ = json.Marshal(once)
	return command, nil
}

type Profile struct {
	Traits     json.RawMessage
	Identified bool
}
type Link struct {
	Target  string
	Version uint64
}

// Reader is scoped to one project and the caller's existing transaction.
// Missing profiles have empty traits; missing links have an empty target.
type Reader interface {
	Profile(context.Context, string) (Profile, error)
	Link(context.Context, string) (Link, error)
}
type LinkChange struct {
	Anonymous, Target string
	Version           uint64
	Create            bool
}
type Decision struct {
	Ignored    bool
	Target     string
	Traits     json.RawMessage
	Identified bool
	Link       *LinkChange
}

// Evaluate only reads the caller's snapshot. Its decision owns detached JSON;
// neither profiles, links nor a transaction are changed here.
func Evaluate(ctx context.Context, command Command, reader Reader) (Decision, error) {
	if command.Operation == Ignored {
		return Decision{Ignored: true}, nil
	}
	if command.Operation != Identify && command.Operation != Set {
		return Decision{}, errors.New("invalid parsed identity operation")
	}
	target := command.Target
	targetLink, err := reader.Link(ctx, target)
	if err != nil {
		return Decision{}, err
	}
	if targetLink.Target != "" {
		if command.Operation == Identify {
			return Decision{Ignored: true}, nil
		}
		target = targetLink.Target
	}
	profile, err := reader.Profile(ctx, target)
	if err != nil {
		return Decision{}, err
	}
	traits, err := decodeTraits(profile.Traits)
	if err != nil {
		return Decision{}, err
	}
	result := Decision{Target: target, Identified: profile.Identified}
	if command.Operation == Identify {
		anonymous := command.Anonymous
		if anonymous != "" && anonymous != target {
			source, err := reader.Profile(ctx, anonymous)
			if err != nil {
				return Decision{}, err
			}
			sourceTraits, err := decodeTraits(source.Traits)
			if err != nil {
				return Decision{}, err
			}
			link, err := reader.Link(ctx, anonymous)
			if err != nil {
				return Decision{}, err
			}
			if source.Identified || link.Target != "" && link.Target != target {
				return Decision{Ignored: true}, nil
			}
			result.Link = &LinkChange{Anonymous: anonymous, Target: target, Version: link.Version, Create: link.Version == 0}
			for key, value := range sourceTraits {
				if _, exists := traits[key]; !exists {
					traits[key] = value
				}
			}
		}
		result.Identified = true
	}
	var set, once map[string]any
	if err := ingest.DecodeJSON(command.Set, &set); err != nil {
		return Decision{}, err
	}
	if err := ingest.DecodeJSON(command.SetOnce, &once); err != nil {
		return Decision{}, err
	}
	for key, value := range once {
		if _, exists := traits[key]; !exists {
			traits[key] = value
		}
	}
	for key, value := range set {
		traits[key] = value
	}
	if _, valid := ingest.Traits(traits); !valid {
		return Decision{Ignored: true}, nil
	}
	result.Traits, err = json.Marshal(traits)
	if err != nil {
		return Decision{}, err
	}
	return result, nil
}
func decodeTraits(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var traits map[string]any
	if err := ingest.DecodeJSON(raw, &traits); err != nil || traits == nil {
		return nil, errors.New("invalid stored identity profile")
	}
	return traits, nil
}
