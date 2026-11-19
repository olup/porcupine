package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

const hostedProjectCleanupSchema = `
CREATE TABLE hosted_project_cleanup (
 project_id TEXT PRIMARY KEY REFERENCES hosted_project_seals(project_id),
 inventory_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE hosted_project_cleanup_objects (
 project_id TEXT NOT NULL REFERENCES hosted_project_cleanup(project_id),
 kind TEXT NOT NULL,
 storage_id TEXT NOT NULL DEFAULT '',
 object_key TEXT NOT NULL,
 version_id TEXT NOT NULL DEFAULT '',
 upload_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY (project_id,kind,storage_id,object_key,version_id,upload_id)
);
`

var errHostedRecoveryFloor = errors.New("hosted project cleanup is waiting for the recovery floor")

// HostedRecoveryFloor supplies the earliest restore point still supported by the
// deployment, including secondary backups and any other recovery procedures.
// Zero or an error means the floor could not be verified. A deployment using
// this interface must never restore below a floor it has already reported.
type HostedRecoveryFloor func(context.Context) (time.Time, error)

type hostedCleanupObject struct {
	kind, storage, key, version, upload string
}

func (h *hostedRuntime) checkCleanupRecoveryFloor(ctx context.Context, observed, inventoried time.Time) error {
	config := h.native.Config
	if h.recoveryFloor == nil || config.RestoreWindow <= 0 || config.RestoreMaxReaderTime <= 0 || config.RestoreSafetyMargin <= 0 ||
		observed.Before(inventoried.Add(config.CleanupGrace)) {
		return errHostedRecoveryFloor
	}
	floor, err := h.recoveryFloor(ctx)
	if err != nil || floor.IsZero() || floor.Before(inventoried) || floor.After(observed) {
		return errHostedRecoveryFloor
	}
	return nil
}

func (s *Service) resumeHostedProjectCleanup(ctx context.Context) {
	rows, err := s.control.QueryContext(ctx, `SELECT d.project_id FROM project_deletions d
JOIN hosted_project_seals seal ON seal.project_id=d.project_id AND seal.tenant_id=d.tenant_id
WHERE d.completed=0 ORDER BY d.project_id`)
	if err != nil {
		return
	}
	var projects []string
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			rows.Close()
			return
		}
		projects = append(projects, project)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return
	}
	for _, project := range projects {
		if ctx.Err() != nil {
			return
		}
		work, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := s.cleanHostedProject(work, project)
		cancel()
		if err != nil && ctx.Err() == nil && !errors.Is(err, errHostedRecoveryFloor) && !errors.Is(err, errHostedProjectBusy) {
			slog.Warn("hosted project cleanup pending", "project_id", project)
		}
	}
}

func (s *Service) cleanHostedProject(ctx context.Context, project string) error {
	h := s.hosted
	ctx, cancel := h.incarnation.writeContext(ctx)
	defer cancel()
	key := lakeapi.ProjectWriteKey{SourceID: h.native.Config.SharedSourceID, ProjectID: project}
	if err := reclaimExpiredHostedIncarnations(ctx, s.control); err != nil {
		return err
	}
	session, err := h.flusher.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	return session.RunNative(ctx, func(ctx context.Context) error {
		seal := lakeapi.HostedProjectSeal{Scope: lakeapi.HostedProjectScope{Key: key}}
		var tenant string
		var busy bool
		err := s.control.QueryRowContext(ctx, `SELECT seal.tenant_id,seal.catalog_id,seal.stream_id,seal.final_sequence,
EXISTS(SELECT 1 FROM hosted_project_activity WHERE project_id=$1)
OR EXISTS(SELECT 1 FROM restored_artifact_holds WHERE project_id=$1)
FROM hosted_project_seals seal
JOIN project_deletions d ON d.project_id=seal.project_id AND d.tenant_id=seal.tenant_id
JOIN projects p ON p.id=d.project_id AND p.tenant_id=d.tenant_id
WHERE seal.project_id=$1 AND seal.source_id=$2 AND d.completed=0`, project, key.SourceID).
			Scan(&tenant, &seal.Scope.CatalogID, &seal.Scope.StreamID, &seal.FinalSequence, &busy)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if busy {
			return errHostedProjectBusy
		}
		seal.AcknowledgedThrough = seal.FinalSequence
		hasCatalog := seal.Scope.CatalogID != "" && seal.Scope.StreamID != ""
		if !hasCatalog && (seal.Scope.CatalogID != "" || seal.Scope.StreamID != "" || seal.FinalSequence != 0) {
			return lakeapi.ErrNativeEvidenceGap
		}

		var observed, inventoried time.Time
		err = s.control.QueryRowContext(ctx, `SELECT clock_timestamp(),inventory_at FROM hosted_project_cleanup WHERE project_id=$1`, project).
			Scan(&observed, &inventoried)
		if errors.Is(err, sql.ErrNoRows) {
			return s.recordHostedProjectInventory(ctx, session, seal, tenant, hasCatalog)
		}
		if err != nil {
			return err
		}
		if err := h.checkCleanupRecoveryFloor(ctx, observed, inventoried); err != nil {
			return err
		}
		more, err := s.sweepHostedProjectObjects(ctx, session, seal.Scope, tenant)
		if err != nil || more {
			return err
		}
		// A provider can report an abort while an already accepted part is
		// still arriving. Recheck exact owned upload scopes before metadata drop.
		outputs, scheduledBytes, err := s.hostedOutputCleanupObjects(ctx, seal.Scope, tenant)
		if err != nil {
			return err
		}
		imports, err := s.hostedImportCleanupObjects(ctx, seal.Scope)
		if err != nil {
			return err
		}
		if leftovers := append(outputs, imports...); len(leftovers) != 0 {
			return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
				return stageHostedCleanupObjects(ctx, tx, project, leftovers)
			})
		}
		if hasCatalog {
			if err := h.native.DropSealedHostedProject(ctx, seal); err != nil {
				return err
			}
		}
		return s.completeHostedProjectCleanup(ctx, session, seal.Scope, tenant, scheduledBytes)
	})
}

func (s *Service) recordHostedProjectInventory(ctx context.Context, session *lakeapi.ProjectWriteSession, seal lakeapi.HostedProjectSeal, tenant string, hasCatalog bool) error {
	h := s.hosted
	var artifacts []hostedCleanupObject
	if hasCatalog {
		native, err := h.native.InventorySealedHostedProject(ctx, seal)
		if err != nil {
			return err
		}
		keys := native.ParquetKeys
		for _, refs := range [][]lakeapi.ImmutableObjectRef{native.LiveManifests, native.ImportParsers} {
			for _, ref := range refs {
				keys = append(keys, ref.Key)
			}
		}
		for _, key := range keys {
			versions, err := h.native.HostedProjectObjectVersions(ctx, seal.Scope, key)
			if err != nil {
				return err
			}
			for _, version := range versions {
				artifacts = append(artifacts, hostedCleanupObject{kind: "analytics", key: key, version: version})
			}
		}
	}
	imports, err := s.hostedImportCleanupObjects(ctx, seal.Scope)
	if err != nil {
		return err
	}
	artifacts = append(artifacts, imports...)
	mutations, err := h.flusher.hostedMutationCleanupObjects(ctx, seal.Scope, tenant)
	if err != nil {
		return err
	}
	artifacts = append(artifacts, mutations...)
	outputs, _, err := s.hostedOutputCleanupObjects(ctx, seal.Scope, tenant)
	if err != nil {
		return err
	}
	artifacts = append(artifacts, outputs...)
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_project_cleanup(project_id) VALUES($1)`, seal.Scope.Key.ProjectID); err != nil {
			return err
		}
		return stageHostedCleanupObjects(ctx, tx, seal.Scope.Key.ProjectID, artifacts)
	})
}

func stageHostedCleanupObjects(ctx context.Context, tx *sql.Tx, project string, artifacts []hostedCleanupObject) error {
	for _, ref := range artifacts {
		if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_project_cleanup_objects
(project_id,kind,storage_id,object_key,version_id,upload_id)
VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING`, project, ref.kind, ref.storage, ref.key, ref.version, ref.upload); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) sweepHostedProjectObjects(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string) (bool, error) {
	rows, err := s.control.QueryContext(ctx, `SELECT kind,storage_id,object_key,version_id,upload_id
FROM hosted_project_cleanup_objects WHERE project_id=$1
ORDER BY kind,storage_id,object_key,version_id,upload_id LIMIT 32`, scope.Key.ProjectID)
	if err != nil {
		return false, err
	}
	var artifacts []hostedCleanupObject
	for rows.Next() {
		var ref hostedCleanupObject
		if err := rows.Scan(&ref.kind, &ref.storage, &ref.key, &ref.version, &ref.upload); err != nil {
			rows.Close()
			return false, err
		}
		artifacts = append(artifacts, ref)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return false, err
	}
	for _, ref := range artifacts {
		switch ref.kind {
		case "analytics":
			if ref.upload != "" {
				err = s.hosted.native.AbortHostedProjectObjectUpload(ctx, scope, ref.key, ref.upload)
			} else {
				err = s.hosted.native.DeleteHostedProjectObject(ctx, scope, ref.key, ref.version)
			}
		default:
			err = s.deleteHostedOutputObject(ctx, scope, tenant, ref)
		}
		if err != nil {
			return true, err
		}
		if err := session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `DELETE FROM hosted_project_cleanup_objects
WHERE project_id=$1 AND kind=$2 AND storage_id=$3 AND object_key=$4 AND version_id=$5 AND upload_id=$6`,
				scope.Key.ProjectID, ref.kind, ref.storage, ref.key, ref.version, ref.upload)
			return err
		}); err != nil {
			return true, err
		}
	}
	return len(artifacts) != 0, nil
}

func (s *Service) completeHostedProjectCleanup(ctx context.Context, session *lakeapi.ProjectWriteSession, scope lakeapi.HostedProjectScope, tenant string, scheduledBytes int64) error {
	return session.RunControlTx(ctx, func(ctx context.Context, tx *sql.Tx) error {
		project := scope.Key.ProjectID
		var pending bool
		if err := tx.QueryRowContext(ctx, `SELECT
EXISTS(SELECT 1 FROM hosted_project_cleanup_objects WHERE project_id=$1)
OR EXISTS(SELECT 1 FROM hosted_project_activity WHERE project_id=$1)
OR EXISTS(SELECT 1 FROM restored_artifact_holds WHERE project_id=$1)`, project).Scan(&pending); err != nil {
			return err
		}
		if pending {
			return errHostedProjectBusy
		}
		if err := finalizeHostedMutationsTx(ctx, tx, scope, tenant); err != nil {
			return err
		}
		if err := s.pipelines.RecordHostedScheduledUsageTx(ctx, tx, project, scheduledBytes); err != nil {
			return err
		}
		if err := output.FinalizeHostedOutputsTx(ctx, tx, tenant, project); err != nil {
			return err
		}
		return s.completeProjectDeletionTx(ctx, tx, project, tenant)
	})
}

func (s *Service) hostedOutputCleanupObjects(ctx context.Context, scope lakeapi.HostedProjectScope, tenant string) ([]hostedCleanupObject, int64, error) {
	tx, err := s.control.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	exports, err := s.exports.HostedDeletionInventory(ctx, tx, tenant, scope.Key.ProjectID)
	if err != nil {
		return nil, 0, err
	}
	pipelines, err := output.HostedPipelineDeletionInventory(ctx, tx, tenant, scope, s.hosted.native.Remote)
	if err != nil {
		return nil, 0, err
	}
	var refs []hostedCleanupObject
	scheduled, scheduledBytes, err := s.pipelines.HostedScheduledDeletionInventory(ctx, tx, tenant, scope.Key.ProjectID)
	if err != nil {
		return nil, 0, err
	}
	for _, ref := range append(append(exports, pipelines...), scheduled...) {
		refs = append(refs, hostedCleanupObject{ref.Kind, ref.StorageID, ref.Key, ref.Version, ref.Upload})
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return refs, scheduledBytes, nil
}

func (s *Service) deleteHostedOutputObject(ctx context.Context, scope lakeapi.HostedProjectScope, tenant string, ref hostedCleanupObject) error {
	artifact := output.HostedDeletionArtifact{
		Kind: ref.kind, StorageID: ref.storage, Key: ref.key, Version: ref.version, Upload: ref.upload,
	}
	if ref.kind == output.HostedDeletionPipeline {
		return output.DeleteHostedPipelineArtifact(ctx, scope, s.hosted.native.Remote, artifact)
	}
	if ref.kind == output.HostedDeletionScheduled {
		return s.pipelines.DeleteHostedScheduledArtifact(ctx, tenant, scope.Key.ProjectID, artifact)
	}
	return s.exports.DeleteHostedArtifact(ctx, artifact)
}
