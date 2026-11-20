package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

var ErrDescriptionChanged = errors.New("catalog description changed or is missing")

type catalogDescriptionKey struct {
	event, kind, property string
}

func (key catalogDescriptionKey) valid() bool {
	return len(key.event) > 0 && len(key.event) <= 500 && len(key.property) <= 200 &&
		utf8.ValidString(key.event) && utf8.ValidString(key.property) &&
		!strings.ContainsRune(key.event, 0) && !strings.ContainsRune(key.property, 0)
}

type DescriptionChange struct {
	key             catalogDescriptionKey
	text            string
	confirmReviewed bool
}

func ParseDescriptionChange(event string, property *string, text string, confirmReviewed bool) (DescriptionChange, error) {
	key := catalogDescriptionKey{event: event, kind: "event"}
	if property != nil {
		key.kind, key.property = "property", *property
	}
	text = strings.TrimSpace(text)
	if !key.valid() || len(text) > 2000 || strings.ContainsRune(text, 0) {
		return DescriptionChange{}, errors.New("invalid catalog description or target")
	}
	if confirmReviewed && text == "" {
		return DescriptionChange{}, errors.New("confirming a review requires the current description text")
	}
	return DescriptionChange{key: key, text: text, confirmReviewed: confirmReviewed}, nil
}

// Apply uses the caller's admitted transaction.
func (change DescriptionChange) Apply(ctx context.Context, tx *sql.Tx, project, actor string, clock func() time.Time) error {
	key := change.key
	var err error
	switch {
	case change.confirmReviewed:
		var storedText, storedTime string
		err = tx.QueryRowContext(ctx, `SELECT description,updated_at FROM catalog_descriptions WHERE project_id=$1 AND event_name=$2 AND kind=$3 AND property_key=$4`, project, key.event, key.kind, key.property).Scan(&storedText, &storedTime)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && storedText != change.text) {
			return ErrDescriptionChanged
		}
		if err == nil {
			now := clock().UTC()
			if catalogDescriptionStale(storedTime, catalogDocumentationCutoff(now)) {
				_, err = tx.ExecContext(ctx, `UPDATE catalog_descriptions SET updated_by=$5,updated_at=$6 WHERE project_id=$1 AND event_name=$2 AND kind=$3 AND property_key=$4`,
					project, key.event, key.kind, key.property, actor, now.Format(time.RFC3339Nano))
			}
		}
	case change.text == "":
		_, err = tx.ExecContext(ctx, `DELETE FROM catalog_descriptions WHERE project_id=$1 AND event_name=$2 AND kind=$3 AND property_key=$4`, project, key.event, key.kind, key.property)
	default:
		_, err = tx.ExecContext(ctx, `INSERT INTO catalog_descriptions(project_id,event_name,kind,property_key,description,updated_by,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(project_id,event_name,kind,property_key) DO UPDATE SET
description=excluded.description,updated_by=excluded.updated_by,updated_at=excluded.updated_at
WHERE catalog_descriptions.description <> excluded.description OR catalog_descriptions.updated_by <> excluded.updated_by`,
			project, key.event, key.kind, key.property, change.text, actor, clock().UTC().Format(time.RFC3339Nano))
	}
	return err
}

// Clamp to the last day of the target month for dates such as August 31.
func catalogDocumentationCutoff(now time.Time) time.Time {
	now = now.UTC()
	firstOfMonth := now.AddDate(0, -6, 1-now.Day())
	lastDay := firstOfMonth.AddDate(0, 1, -1).Day()
	return firstOfMonth.AddDate(0, 0, min(now.Day(), lastDay)-1)
}

func catalogDescriptionStale(updatedAt string, cutoff time.Time) bool {
	updated, err := time.Parse(time.RFC3339Nano, updatedAt)
	return err != nil || updated.Before(cutoff)
}

type catalogDocumentationTarget struct {
	description **lakeapi.CatalogDescription
	status      *lakeapi.CatalogDocumentationStatus
}

func DescribePage(ctx context.Context, db *sql.DB, project, event string, page *lakeapi.EventCatalog, clock func() time.Time) error {
	targets := make(map[catalogDescriptionKey]catalogDocumentationTarget, len(page.Events)+len(page.Properties))
	for i := range page.Events {
		item := &page.Events[i]
		key := catalogDescriptionKey{event: item.Event, kind: "event"}
		if key.valid() {
			targets[key] = catalogDocumentationTarget{&item.Description, &item.DocumentationStatus}
		}
	}
	for i := range page.Properties {
		item := &page.Properties[i]
		key := catalogDescriptionKey{event: event, kind: "property", property: item.Key}
		if key.valid() {
			targets[key] = catalogDocumentationTarget{&item.Description, &item.DocumentationStatus}
		}
	}
	return describeCatalogTargets(ctx, db, project, targets, clock)
}

func DescribeProfile(ctx context.Context, db *sql.DB, project string, profile *lakeapi.EventProfile, clock func() time.Time) error {
	targets := make(map[catalogDescriptionKey]catalogDocumentationTarget, len(profile.Properties)+1)
	key := catalogDescriptionKey{event: profile.Event, kind: "event"}
	if key.valid() {
		targets[key] = catalogDocumentationTarget{&profile.Description, &profile.DocumentationStatus}
	}
	for i := range profile.Properties {
		item := &profile.Properties[i]
		key := catalogDescriptionKey{event: profile.Event, kind: "property", property: item.Key}
		if key.valid() {
			targets[key] = catalogDocumentationTarget{&item.Description, &item.DocumentationStatus}
		}
	}
	return describeCatalogTargets(ctx, db, project, targets, clock)
}

func describeCatalogTargets(ctx context.Context, db *sql.DB, project string, targets map[catalogDescriptionKey]catalogDocumentationTarget, clock func() time.Time) error {
	if len(targets) == 0 {
		return nil
	}
	cutoff := catalogDocumentationCutoff(clock())
	args := make([]any, 0, 3*len(targets)+1)
	markers := make([]string, 0, len(targets))
	for key, target := range targets {
		*target.description = nil
		*target.status = ""
		if !strings.HasPrefix(key.event, "$") && !strings.HasPrefix(key.property, "$") {
			*target.status = lakeapi.CatalogDocumentationMissing
		}
		n := len(args)
		markers = append(markers, fmt.Sprintf("($%d,$%d,$%d)", n+1, n+2, n+3))
		args = append(args, key.event, key.kind, key.property)
	}
	args = append(args, project)
	rows, err := db.QueryContext(ctx, `WITH targets(event_name,kind,property_key) AS (VALUES `+strings.Join(markers, ",")+`)
SELECT d.event_name,d.kind,d.property_key,d.description,d.updated_by,d.updated_at
FROM targets t CROSS JOIN catalog_descriptions d
WHERE d.event_name=t.event_name AND d.kind=t.kind AND d.property_key=t.property_key AND d.project_id=$`+fmt.Sprint(len(args)), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key catalogDescriptionKey
		var description lakeapi.CatalogDescription
		if err := rows.Scan(&key.event, &key.kind, &key.property, &description.Text, &description.UpdatedBy, &description.UpdatedAt); err != nil {
			return err
		}
		target := targets[key]
		*target.description = &description
		if *target.status != "" {
			*target.status = lakeapi.CatalogDocumentationCurrent
			if catalogDescriptionStale(description.UpdatedAt, cutoff) {
				*target.status = lakeapi.CatalogDocumentationStale
			}
		}
	}
	return rows.Err()
}
