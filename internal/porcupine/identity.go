package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

func isIdentityControl(event string) bool { return event == "$identify" || event == "$set" }

func validIdentityID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 1000 && id != ingestfmt.CookielessSentinel && !strings.HasPrefix(id, "cookieless_")
}

func identityProfile(ctx context.Context, tx *sql.Tx, project, id string) (map[string]any, bool, error) {
	var raw string
	var identified bool
	err := tx.QueryRowContext(ctx, `SELECT traits,is_identified FROM identity_profiles WHERE project_id=$1 AND identified_id=$2`, project, id).Scan(&raw, &identified)
	if errors.Is(err, sql.ErrNoRows) {
		return map[string]any{}, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var traits map[string]any
	if err := ingestfmt.DecodeJSON([]byte(raw), &traits); err != nil || traits == nil {
		return nil, false, errors.New("invalid stored identity profile")
	}
	return traits, identified, nil
}

func identityLink(ctx context.Context, tx *sql.Tx, project, id string) (string, int64, error) {
	var target string
	var version int64
	err := tx.QueryRowContext(ctx, `SELECT identified_id,version FROM identity_links WHERE project_id=$1 AND anonymous_id=$2`, project, id).Scan(&target, &version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", 0, nil
	}
	return target, version, err
}

// applyParsedIdentityControl commits a canonical snapshot after UUID deduplication.
// Invalid or conflicting controls retain a no-op marker without changing identity.
func applyParsedIdentityControl(ctx context.Context, tx *sql.Tx, event *eventapi.Event, control identityControl) (bool, error) {
	snapshot := lakeapi.IdentitySnapshot{Version: strconv.FormatInt(event.ID, 10), Traits: map[string]any{}, Links: []lakeapi.IdentityLinkSnapshot{}}
	ignored, err := applyParsedIdentityState(ctx, tx, *event, control, &snapshot)
	if err != nil {
		return false, err
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return false, err
	}
	event.IdentitySnapshot = string(encoded)
	event.DistinctID, event.Properties = "", json.RawMessage(`{}`)
	_, err = tx.ExecContext(ctx, `UPDATE events SET identity_snapshot=$1,distinct_id='',properties='{}' WHERE id=$2 AND project_id=$3`, event.IdentitySnapshot, event.ID, event.ProjectID)
	return ignored, err
}

func applyIdentityState(ctx context.Context, tx *sql.Tx, event eventapi.Event, snapshot *lakeapi.IdentitySnapshot) (bool, error) {
	control, err := parseIdentityControl(event)
	if err != nil {
		return false, err
	}
	return applyParsedIdentityState(ctx, tx, event, control, snapshot)
}

type identityControlKind uint8

const (
	identityControlIgnored identityControlKind = iota
	identityControlIdentify
	identityControlSet
)

type identityControl struct {
	kind              identityControlKind
	target, anonymous string
	set, setOnce      json.RawMessage
}

func parseIdentityControl(event eventapi.Event) (identityControl, error) {
	if !validIdentityID(event.DistinctID) {
		return identityControl{}, nil
	}
	var properties map[string]any
	if err := ingestfmt.DecodeJSON(event.Properties, &properties); err != nil {
		return identityControl{}, err
	}
	for _, key := range []string{"$set", "$set_once"} {
		if value, exists := properties[key]; exists && value == nil {
			return identityControl{}, nil
		}
	}
	set, validSet := ingestfmt.Traits(properties["$set"])
	once, validOnce := ingestfmt.Traits(properties["$set_once"])
	if !validSet || !validOnce {
		return identityControl{}, nil
	}
	control := identityControl{kind: identityControlSet, target: event.DistinctID}
	if event.Event == "$identify" {
		control.kind = identityControlIdentify
		if raw, exists := properties["$anon_distinct_id"]; exists {
			anonymous, ok := raw.(string)
			if !ok || !validIdentityID(anonymous) {
				return identityControl{}, nil
			}
			control.anonymous = anonymous
		}
	}
	control.set, _ = json.Marshal(set)
	control.setOnce, _ = json.Marshal(once)
	return control, nil
}
func applyParsedIdentityState(ctx context.Context, tx *sql.Tx, event eventapi.Event, control identityControl, snapshot *lakeapi.IdentitySnapshot) (bool, error) {
	if control.kind == identityControlIgnored {
		return true, nil
	}
	var set, once map[string]any
	if err := ingestfmt.DecodeJSON(control.set, &set); err != nil {
		return false, err
	}
	if err := ingestfmt.DecodeJSON(control.setOnce, &once); err != nil {
		return false, err
	}
	target := control.target
	boundTarget, _, err := identityLink(ctx, tx, event.ProjectID, target)
	if err != nil {
		return false, err
	}
	if boundTarget != "" {
		if control.kind == identityControlIdentify {
			return true, nil
		}
		target = boundTarget
	}
	traits, identified, err := identityProfile(ctx, tx, event.ProjectID, target)
	if err != nil {
		return false, err
	}
	var anonymous string
	var linkVersion int64
	if control.kind == identityControlIdentify {
		anonymous = control.anonymous
		if anonymous != "" && anonymous != target {
			anonymousTraits, sourceIdentified, err := identityProfile(ctx, tx, event.ProjectID, anonymous)
			if err != nil {
				return false, err
			}
			bound, version, err := identityLink(ctx, tx, event.ProjectID, anonymous)
			if err != nil {
				return false, err
			}
			if sourceIdentified || bound != "" && bound != target {
				return true, nil
			}
			linkVersion = version
			for key, value := range anonymousTraits {
				if _, exists := traits[key]; !exists {
					traits[key] = value
				}
			}
		}
		identified = true
	}
	for key, value := range once {
		if _, exists := traits[key]; !exists {
			traits[key] = value
		}
	}
	for key, value := range set {
		traits[key] = value
	}
	if _, valid := ingestfmt.Traits(traits); !valid {
		return true, nil
	}
	encoded, err := json.Marshal(traits)
	if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO identity_profiles(project_id,identified_id,is_identified,traits,version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(project_id,identified_id) DO UPDATE SET is_identified=excluded.is_identified,traits=excluded.traits,version=excluded.version`, event.ProjectID, target, identified, string(encoded), event.ID); err != nil {
		return false, err
	}
	if anonymous != "" && anonymous != target {
		if linkVersion == 0 {
			linkVersion = event.ID
			if _, err := tx.ExecContext(ctx, `INSERT INTO identity_links(project_id,anonymous_id,identified_id,version) VALUES($1,$2,$3,$4)`, event.ProjectID, anonymous, target, linkVersion); err != nil {
				return false, err
			}
		}
		snapshot.Links = append(snapshot.Links, lakeapi.IdentityLinkSnapshot{AnonymousID: anonymous, IdentifiedID: target, Version: strconv.FormatInt(linkVersion, 10)})
	}
	snapshot.IdentifiedID, snapshot.Traits = target, traits
	return false, nil
}
