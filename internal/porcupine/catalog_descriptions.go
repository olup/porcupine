package porcupine

import (
	"context"
	"database/sql"
	"errors"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/catalog"
)

type CatalogDescriptionInput struct {
	ID   string `path:"id" doc:"Project identifier"`
	Body struct {
		Event           string  `json:"event" minLength:"1" maxLength:"500" doc:"Exact event name."`
		Property        *string `json:"property,omitempty" maxLength:"200" doc:"Exact top-level property key. Omit to document the event itself."`
		Description     string  `json:"description" maxLength:"2000" doc:"Plain text. Use an empty string to clear the description."`
		ConfirmReviewed bool    `json:"confirm_reviewed,omitempty" doc:"Renew the date and author of a stale description when the supplied nonempty text matches the stored text. Current text needs no renewal. Missing or changed text returns 409."`
	}
}

func (s *Service) setCatalogDescription(ctx context.Context, input *CatalogDescriptionInput) (*struct{}, error) {
	body := input.Body
	change, err := catalog.ParseDescriptionChange(body.Event, body.Property, body.Description, body.ConfirmReviewed)
	if err != nil {
		return nil, apierr.New(400, err.Error())
	}
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierr.New(503, "catalog descriptions unavailable")
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
		return nil, apierr.New(404, "project not found")
	}
	if err != nil {
		return nil, apierr.New(503, "catalog descriptions unavailable")
	}
	if err := requireActiveProjectSet(ctx, tx, []string{project}); err != nil {
		return nil, err
	}
	if err := s.requireExternalCurrent(ctx, tx); err != nil {
		return nil, err
	}
	err = change.Apply(ctx, tx, project, principal(ctx).ID, s.clock)
	if errors.Is(err, catalog.ErrDescriptionChanged) {
		return nil, apierr.New(409, err.Error())
	}
	if err != nil || tx.Commit() != nil {
		return nil, apierr.New(503, "catalog descriptions unavailable")
	}
	return nil, nil
}
