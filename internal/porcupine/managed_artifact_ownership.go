package porcupine

import (
	"context"
	"errors"

	"github.com/olup/porcupine/internal/exportstore"
)

// Retain ownership before the last job/checkpoint reference disappears. S3
// DeleteObject may leave noncurrent versions, even during ordinary expiration.
func (s *Service) retainArtifactOwnership(ctx context.Context, project, kind string, store *exportstore.Store, key, upload string) error {
	tenant, err := s.projectAdministrationTenant(ctx, project)
	if err != nil || tenant == "" {
		return errors.New("managed artifact ownership unavailable")
	}
	_, err = s.control.ExecContext(ctx, `INSERT INTO managed_artifact_ownership(tenant_id,project_id,kind,storage_id,object_key,upload_id)
VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, tenant, project, kind, store.OwnershipID(), key, upload)
	return err
}
