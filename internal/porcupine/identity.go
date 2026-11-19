package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"

	eventapi "github.com/olup/porcupine/internal/event"
	identityapi "github.com/olup/porcupine/internal/identity"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

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

type identityControl = identityapi.Command

const identityControlIgnored = identityapi.Ignored

func parseIdentityControl(event eventapi.Event) (identityControl, error) {
	return identityapi.Parse(event.Event, event.DistinctID, event.Properties)
}

func applyParsedIdentityState(ctx context.Context, tx *sql.Tx, event eventapi.Event, control identityControl, snapshot *lakeapi.IdentitySnapshot) (bool, error) {
	decision, err := identityapi.Evaluate(ctx, control, sqliteIdentityReader{tx: tx, project: event.ProjectID})
	if err != nil {
		return false, err
	}
	if decision.Ignored {
		return true, nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO identity_profiles(project_id,identified_id,is_identified,traits,version) VALUES($1,$2,$3,$4,$5) ON CONFLICT(project_id,identified_id) DO UPDATE SET is_identified=excluded.is_identified,traits=excluded.traits,version=excluded.version`, event.ProjectID, decision.Target, decision.Identified, string(decision.Traits), event.ID); err != nil {
		return false, err
	}
	if link := decision.Link; link != nil {
		version := link.Version
		if link.Create {
			version = uint64(event.ID)
			if _, err := tx.ExecContext(ctx, `INSERT INTO identity_links(project_id,anonymous_id,identified_id,version) VALUES($1,$2,$3,$4)`, event.ProjectID, link.Anonymous, link.Target, version); err != nil {
				return false, err
			}
		}
		snapshot.Links = append(snapshot.Links, lakeapi.IdentityLinkSnapshot{AnonymousID: link.Anonymous, IdentifiedID: link.Target, Version: strconv.FormatUint(version, 10)})
	}
	var traits map[string]any
	if err := ingestfmt.DecodeJSON(decision.Traits, &traits); err != nil {
		return false, err
	}
	snapshot.IdentifiedID, snapshot.Traits = decision.Target, traits
	return false, nil
}
