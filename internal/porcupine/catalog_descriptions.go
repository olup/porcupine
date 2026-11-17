package porcupine

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

type CatalogDescriptionInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		Event       string  `json:"event" minLength:"1" maxLength:"500" doc:"Exact event name."`
		Property    *string `json:"property,omitempty" maxLength:"200" doc:"Exact top-level property key. Omit to document the event itself."`
		Description string  `json:"description" maxLength:"2000" doc:"Plain text. Use an empty string to clear the description."`
	}
}

type catalogDescriptionKey struct {
	event, kind, property string
}

func (key catalogDescriptionKey) valid() bool {
	return len(key.event) > 0 && len(key.event) <= 500 && len(key.property) <= 200 &&
		utf8.ValidString(key.event) && utf8.ValidString(key.property) &&
		!strings.ContainsRune(key.event, 0) && !strings.ContainsRune(key.property, 0)
}

func (s *Service) setCatalogDescription(ctx context.Context, input *CatalogDescriptionInput) (*struct{}, error) {
	body := input.Body
	key := catalogDescriptionKey{event: body.Event, kind: "event"}
	if body.Property != nil {
		key.kind, key.property = "property", *body.Property
	}
	description := strings.TrimSpace(body.Description)
	if !key.valid() || len(description) > 2000 || strings.ContainsRune(description, 0) {
		return nil, apiError(400, "invalid catalog description or target")
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "catalog descriptions unavailable")
	}
	defer tx.Rollback()

	// Shared deletion and query admission also take these control-row locks.
	lock := ""
	if s.shared != nil {
		lock = " FOR UPDATE OF t,p"
	}
	var project string
	err = tx.QueryRowContext(ctx, `SELECT p.id FROM projects p JOIN tenants t ON t.id=p.tenant_id WHERE p.id=$1`+lock, input.ID).Scan(&project)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(404, "project not found")
	}
	if err != nil {
		return nil, apiError(503, "catalog descriptions unavailable")
	}
	if err := requireActiveProjectSet(ctx, tx, []string{project}); err != nil {
		return nil, err
	}
	if err := s.requireExternalCurrent(ctx, tx); err != nil {
		return nil, err
	}
	if description == "" {
		_, err = tx.ExecContext(ctx, `DELETE FROM catalog_descriptions WHERE project_id=$1 AND event_name=$2 AND kind=$3 AND property_key=$4`, project, key.event, key.kind, key.property)
	} else {
		_, err = tx.ExecContext(ctx, `INSERT INTO catalog_descriptions(project_id,event_name,kind,property_key,description,updated_by,updated_at)
VALUES($1,$2,$3,$4,$5,$6,$7)
ON CONFLICT(project_id,event_name,kind,property_key) DO UPDATE SET
description=excluded.description,updated_by=excluded.updated_by,updated_at=excluded.updated_at
WHERE catalog_descriptions.description <> excluded.description OR catalog_descriptions.updated_by <> excluded.updated_by`,
			project, key.event, key.kind, key.property, description, principal(ctx).ID, s.clock().UTC().Format(time.RFC3339Nano))
	}
	if err != nil || tx.Commit() != nil {
		return nil, apiError(503, "catalog descriptions unavailable")
	}
	return nil, nil
}

func (s *Service) describeCatalogPage(ctx context.Context, project, event string, page *lakeapi.EventCatalog) error {
	targets := make(map[catalogDescriptionKey]**lakeapi.CatalogDescription, len(page.Events)+len(page.Properties))
	for i := range page.Events {
		item := &page.Events[i]
		key := catalogDescriptionKey{event: item.Event, kind: "event"}
		if key.valid() {
			targets[key] = &item.Description
		}
	}
	for i := range page.Properties {
		item := &page.Properties[i]
		key := catalogDescriptionKey{event: event, kind: "property", property: item.Key}
		if key.valid() {
			targets[key] = &item.Description
		}
	}
	if len(targets) == 0 {
		return nil
	}
	args := make([]any, 0, 3*len(targets)+1)
	markers := make([]string, 0, len(targets))
	for key := range targets {
		n := len(args)
		markers = append(markers, fmt.Sprintf("($%d,$%d,$%d)", n+1, n+2, n+3))
		args = append(args, key.event, key.kind, key.property)
	}
	args = append(args, project)
	rows, err := s.control.QueryContext(ctx, `WITH targets(event_name,kind,property_key) AS (VALUES `+strings.Join(markers, ",")+`)
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
		*targets[key] = &description
	}
	return rows.Err()
}
