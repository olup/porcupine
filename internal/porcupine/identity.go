package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

const identityMaxTraitsBytes = 32 << 10
const identityMaxTraitKeys = 128

func isIdentityControl(event string) bool { return event == "$identify" || event == "$set" }

func validIdentityID(id string) bool {
	return strings.TrimSpace(id) != "" && len(id) <= 1000 && id != cookielessSentinel && !strings.HasPrefix(id, "cookieless_")
}

func identityTraits(raw any) (map[string]any, bool) {
	if raw == nil {
		return map[string]any{}, true
	}
	traits, ok := raw.(map[string]any)
	if !ok || len(traits) > identityMaxTraitKeys {
		return nil, false
	}
	for key := range traits {
		if strings.TrimSpace(key) == "" || len(key) > 200 {
			return nil, false
		}
	}
	encoded, err := json.Marshal(traits)
	return traits, err == nil && len(encoded) <= identityMaxTraitsBytes
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
	if err := decodeIngestionJSON([]byte(raw), &traits); err != nil || traits == nil {
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

// applyIdentityControl runs after stable UUID deduplication and insertion, in the
// capture transaction. Invalid or conflicting controls retain a durable no-op
// marker and cannot change any profile or link, including a competing target.
func applyIdentityControl(ctx context.Context, tx *sql.Tx, event *Event) (bool, error) {
	if !isIdentityControl(event.Event) {
		return false, nil
	}
	snapshot := identitySnapshot{Version: strconv.FormatInt(event.ID, 10), Traits: map[string]any{}, Links: []identityLinkSnapshot{}}
	ignored, err := applyIdentityState(ctx, tx, *event, &snapshot)
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

func applyIdentityState(ctx context.Context, tx *sql.Tx, event Event, snapshot *identitySnapshot) (bool, error) {
	if !validIdentityID(event.DistinctID) {
		return true, nil
	}
	var properties map[string]any
	if err := decodeIngestionJSON(event.Properties, &properties); err != nil {
		return false, err
	}
	for _, key := range []string{"$set", "$set_once"} {
		if value, exists := properties[key]; exists && value == nil {
			return true, nil
		}
	}
	set, validSet := identityTraits(properties["$set"])
	once, validOnce := identityTraits(properties["$set_once"])
	if !validSet || !validOnce {
		return true, nil
	}
	target := event.DistinctID
	boundTarget, _, err := identityLink(ctx, tx, event.ProjectID, target)
	if err != nil {
		return false, err
	}
	if boundTarget != "" {
		if event.Event == "$identify" {
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
	if event.Event == "$identify" {
		if raw, exists := properties["$anon_distinct_id"]; exists {
			var ok bool
			anonymous, ok = raw.(string)
			if !ok || !validIdentityID(anonymous) {
				return true, nil
			}
		}
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
	if _, valid := identityTraits(traits); !valid {
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
		snapshot.Links = append(snapshot.Links, identityLinkSnapshot{AnonymousID: anonymous, IdentifiedID: target, Version: strconv.FormatInt(linkVersion, 10)})
	}
	snapshot.IdentifiedID, snapshot.Traits = target, traits
	return false, nil
}
