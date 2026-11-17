package lake

import (
	"context"
	"database/sql"
	"errors"
	"path"
	"slices"
	"strings"

	"github.com/olup/porcupine/internal/s3client"
)

type HostedProjectDeletionInventory struct {
	ParquetKeys                  []string
	LiveManifests, ImportParsers []ImmutableObjectRef
}

func (s *DuckLakeStore) hostedDeletionRegistry(scope HostedProjectScope) (*postgresNativeRegistry, error) {
	registry, ok := s.registry.(*postgresNativeRegistry)
	if !ok || s.Remote == nil || s.SharedCatalog == nil {
		return nil, ErrHostedProjectNotReady
	}
	if _, err := s.Remote.hostedStreamObjectPrefix(scope, "effects"); err != nil {
		return nil, ErrProjectWriteFenceStale
	}
	return registry, nil
}

// InventorySealedHostedProject collects the exact managed files in retained
// DuckLake metadata and committed effect references. The caller holds the
// project session and persists the returned inventory before object GC.
func (s *DuckLakeStore) InventorySealedHostedProject(ctx context.Context, seal HostedProjectSeal) (HostedProjectDeletionInventory, error) {
	var result HostedProjectDeletionInventory
	scope := seal.Scope
	registry, err := s.hostedDeletionRegistry(scope)
	if err != nil {
		return result, err
	}
	p, release, err := s.leaseHostedProjectExisting(ctx, scope.Key.ProjectID)
	if err != nil {
		return result, err
	}
	defer release()
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var final, high, ack int64
	err = tx.QueryRowContext(ctx, `SELECT sealed_effect_seq,next_effect_seq,ack_effect_seq FROM lake.hosted_write_guard
WHERE id=1 AND source_id=? AND project_id=? AND catalog_id=? AND stream_id=? AND state='sealed' AND identity_ready
AND (SELECT count(*) FROM lake.hosted_write_guard)=1`,
		scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID).Scan(&final, &high, &ack)
	if errors.Is(err, sql.ErrNoRows) {
		return result, ErrProjectWriteFenceStale
	}
	if err != nil {
		return result, err
	}
	if final < 0 || final != seal.FinalSequence || high != final || ack != final {
		return result, ErrNativeEvidenceGap
	}
	if err := s.Remote.inventoryHostedDeletionProofs(ctx, tx, scope, final, &result); err != nil {
		return HostedProjectDeletionInventory{}, err
	}
	bounded, cancel := registry.metadataContext(ctx)
	defer cancel()
	keys, err := s.inventoryHostedDeletionFiles(bounded, scope)
	if err != nil {
		return HostedProjectDeletionInventory{}, err
	}
	result.ParquetKeys = keys
	if err := registry.authorizeHostedDeletion(bounded, seal); err != nil {
		return HostedProjectDeletionInventory{}, err
	}
	return result, nil
}

func (r *postgresNativeRegistry) authorizeHostedDeletion(ctx context.Context, seal HostedProjectSeal) error {
	scope := seal.Scope
	result, err := r.db.ExecContext(ctx, `INSERT INTO pc_native_hosted_deletion (source_id,project_id,stream_id,final_sequence)
SELECT source_id,project_id,$4,$5 FROM pc_native_projects WHERE source_id=$1 AND project_id=$2 AND catalog_id=$3 AND ready=1 AND deleted=0
ON CONFLICT(source_id,project_id) DO UPDATE SET stream_id=excluded.stream_id
WHERE pc_native_hosted_deletion.stream_id=excluded.stream_id AND pc_native_hosted_deletion.final_sequence=excluded.final_sequence`,
		scope.Key.SourceID, scope.Key.ProjectID, scope.CatalogID, scope.StreamID, seal.FinalSequence)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count != 1 {
		return ErrProjectWriteFenceStale
	}
	return err
}

func (s *DuckLakeObjectStore) inventoryHostedDeletionProofs(ctx context.Context, tx *sql.Tx, scope HostedProjectScope, final int64, result *HostedProjectDeletionInventory) error {
	effectPrefix, err := s.hostedStreamObjectPrefix(scope, "effects")
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+nativeProofColumns+" FROM lake.native_commit_proofs ORDER BY effect_seq")
	if err != nil {
		return err
	}
	defer rows.Close()
	var seq int64
	live, parsers := map[ImmutableObjectRef]bool{}, map[ImmutableObjectRef]bool{}
	for rows.Next() {
		e, err := scanNativeCommitProof(rows, scope)
		if err != nil {
			return err
		}
		if e.Sequence-1 != seq || e.Sequence > final {
			return ErrNativeEvidenceGap
		}
		seq = e.Sequence
		switch e.Kind {
		case NativeCommitLive:
			ref := e.Effects.Manifest
			if ref == (ImmutableObjectRef{}) && e.Effects.OutputCount == 0 {
				continue
			}
			suffix, ok := strings.CutPrefix(ref.Key, effectPrefix)
			parts := strings.Split(suffix, "/")
			if !ok || len(parts) != 2 || len(parts[0]) != 36 || !LakePrefixSegmentPattern.MatchString(parts[0]) ||
				!validDeletionRef(ref, effectPrefix+parts[0]+"/", MaxImmutableEffectObjectBytes) {
				return errors.New("invalid live manifest reference in hosted deletion inventory")
			}
			if !live[ref] {
				live[ref] = true
				result.LiveManifests = append(result.LiveManifests, ref)
			}
		case NativeCommitImport:
			prefix, err := s.HostedImportCheckpointPrefix(scope, e.Import.Key.JobID, e.Import.Key.FileID)
			if err != nil {
				return err
			}
			for _, ref := range []ImmutableObjectRef{e.Import.Previous.Parser, e.Import.Next.Parser} {
				if ref == (ImmutableObjectRef{}) {
					continue
				}
				if !validDeletionRef(ref, prefix, MaxImmutableImportCheckpointBytes) {
					return errors.New("invalid import parser reference in hosted deletion inventory")
				}
				if !parsers[ref] {
					parsers[ref] = true
					result.ImportParsers = append(result.ImportParsers, ref)
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if seq != final {
		return ErrNativeEvidenceGap
	}
	return nil
}

func validDeletionRef(ref ImmutableObjectRef, prefix string, maxBytes int64) bool {
	return validNativeHash(ref.SHA256) && ref.Key == prefix+ref.SHA256+".json" &&
		ref.Bytes > 0 && ref.Bytes <= maxBytes && len(ref.VersionID) <= 1024
}

func (s *DuckLakeStore) inventoryHostedDeletionFiles(ctx context.Context, scope HostedProjectScope) ([]string, error) {
	schema := SharedNativeSchema(scope.CatalogID)
	root := s.Remote.projectPath(scope.Key.ProjectID)
	// File paths descend from the schema and table visible at the start of the
	// file's lifetime. Scheduled deletions already hold a catalog-relative or
	// absolute path. Keep ended rows as long as the file is in metadata.
	rows, err := s.SharedCatalog.QueryContext(ctx, strings.ReplaceAll(`
WITH files AS (
 SELECT table_id,begin_snapshot,path,path_is_relative FROM meta.ducklake_data_file
 UNION ALL SELECT table_id,begin_snapshot,path,path_is_relative FROM meta.ducklake_delete_file
)
SELECT CASE
 WHEN NOT f.path_is_relative THEN f.path
 WHEN NOT t.path_is_relative THEN t.path || f.path
 WHEN NOT d.path_is_relative THEN d.path || t.path || f.path
 WHEN f.path_is_relative AND t.path_is_relative AND d.path_is_relative THEN $1 || d.path || t.path || f.path
END
FROM files f
LEFT JOIN meta.ducklake_table t ON t.table_id=f.table_id
 AND t.begin_snapshot<=f.begin_snapshot AND (t.end_snapshot IS NULL OR t.end_snapshot>f.begin_snapshot)
LEFT JOIN meta.ducklake_schema d ON d.schema_id=t.schema_id
 AND d.begin_snapshot<=f.begin_snapshot AND (d.end_snapshot IS NULL OR d.end_snapshot>f.begin_snapshot)
UNION ALL
SELECT CASE WHEN path_is_relative THEN $1 || path WHEN NOT path_is_relative THEN path END
FROM meta.ducklake_files_scheduled_for_deletion`, "meta.", schema+"."), root)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := map[string]struct{}{}
	for rows.Next() {
		var filePath sql.NullString
		if err := rows.Scan(&filePath); err != nil {
			return nil, err
		}
		suffix, ok := strings.CutPrefix(filePath.String, root)
		if !filePath.Valid || !ok || !safeHostedDeletionSuffix(suffix) ||
			(path.Ext(suffix) != ".parquet" && path.Ext(suffix) != ".puffin") {
			return nil, errors.New("invalid DuckLake file path in hosted deletion inventory")
		}
		keys[s.Remote.ProjectPrefix(scope.Key.ProjectID)+suffix] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	result := make([]string, 0, len(keys))
	for key := range keys {
		result = append(result, key)
	}
	slices.Sort(result)
	return result, nil
}

func safeHostedDeletionSuffix(suffix string) bool {
	return suffix != "" && suffix != "." && suffix != ".." &&
		!strings.HasPrefix(suffix, "/") && !strings.HasPrefix(suffix, "../") &&
		!strings.ContainsAny(suffix, "\\?#%\x00") && path.Clean(suffix) == suffix
}

func (s *DuckLakeStore) validateHostedProjectObject(ctx context.Context, scope HostedProjectScope, key string) error {
	if err := s.authorizeHostedProjectObjectScope(ctx, scope); err != nil {
		return err
	}
	suffix, ok := strings.CutPrefix(key, s.Remote.ProjectPrefix(scope.Key.ProjectID))
	if !ok || !safeHostedDeletionSuffix(suffix) {
		return ErrProjectWriteFenceStale
	}
	return nil
}

func (s *DuckLakeStore) authorizeHostedProjectObjectScope(ctx context.Context, scope HostedProjectScope) error {
	registry, err := s.hostedDeletionRegistry(scope)
	if err != nil {
		return err
	}
	bounded, cancel := registry.metadataContext(ctx)
	defer cancel()
	var authorized bool
	err = registry.db.QueryRowContext(bounded, `SELECT EXISTS(SELECT 1 FROM pc_native_hosted_deletion
WHERE source_id=$1 AND project_id=$2 AND stream_id=$3)`, scope.Key.SourceID, scope.Key.ProjectID, scope.StreamID).Scan(&authorized)
	if err == nil && !authorized {
		return ErrProjectWriteFenceStale
	}
	return err
}

// HostedImportCheckpointArtifacts inventories reserved parser checkpoint keys,
// including attempts that never committed native import proof.
func (s *DuckLakeStore) HostedImportCheckpointArtifacts(ctx context.Context, scope HostedProjectScope, job, file string) ([]s3client.Artifact, error) {
	if err := s.authorizeHostedProjectObjectScope(ctx, scope); err != nil {
		return nil, err
	}
	prefix, err := s.Remote.HostedImportCheckpointPrefix(scope, job, file)
	if err != nil {
		return nil, err
	}
	artifacts, err := s3client.InventoryObjectVersions(ctx, s.Remote.client, s.Remote.Config.Bucket, prefix, func(key string) bool {
		suffix, inPrefix := strings.CutPrefix(key, prefix)
		hash, isJSON := strings.CutSuffix(suffix, ".json")
		return inPrefix && isJSON && validNativeHash(hash)
	})
	if err != nil {
		return nil, analyticalS3Error(ctx)
	}
	return artifacts, nil
}

// AbortHostedProjectObjectUpload removes one recorded multipart upload.
func (s *DuckLakeStore) AbortHostedProjectObjectUpload(ctx context.Context, scope HostedProjectScope, key, uploadID string) error {
	if err := s.validateHostedProjectObject(ctx, scope, key); err != nil {
		return err
	}
	artifact := s3client.Artifact{Key: key, UploadID: uploadID}
	if err := s3client.DeleteArtifact(ctx, s.Remote.client, s.Remote.Config.Bucket, artifact); err != nil {
		return analyticalS3Error(ctx)
	}
	return nil
}

// HostedProjectObjectVersions returns versions and delete markers for one
// authorized inventory key. S3 exposes version enumeration by prefix, so
// every returned identity must equal the requested key before inclusion.
func (s *DuckLakeStore) HostedProjectObjectVersions(ctx context.Context, scope HostedProjectScope, key string) ([]string, error) {
	if err := s.validateHostedProjectObject(ctx, scope, key); err != nil {
		return nil, err
	}
	entries, err := s3client.InventoryObjectVersions(ctx, s.Remote.client, s.Remote.Config.Bucket, key, func(candidate string) bool {
		return candidate == key
	})
	if err != nil {
		return nil, analyticalS3Error(ctx)
	}
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		result = append(result, entry.VersionID)
	}
	slices.Sort(result)
	result = slices.Compact(result)
	return result, nil
}

// DeleteHostedProjectObject removes one exact version or delete marker. The
// caller verifies inventory membership and holds the project session after its
// retention floor has advanced.
func (s *DuckLakeStore) DeleteHostedProjectObject(ctx context.Context, scope HostedProjectScope, key, version string) error {
	if version == "" || len(version) > 1024 {
		return ErrProjectWriteFenceStale
	}
	if err := s.validateHostedProjectObject(ctx, scope, key); err != nil {
		return err
	}
	artifact := s3client.Artifact{Key: key, VersionID: version}
	if err := s3client.DeleteArtifact(ctx, s.Remote.client, s.Remote.Config.Bucket, artifact); err != nil {
		return analyticalS3Error(ctx)
	}
	return nil
}

// DropSealedHostedProject removes native metadata after the caller's retention
// floor and exact object GC have completed under the project session. The
// authorization recorded during inventory survives removal of the guard files.
func (s *DuckLakeStore) DropSealedHostedProject(ctx context.Context, seal HostedProjectSeal) error {
	scope := seal.Scope
	registry, err := s.hostedDeletionRegistry(scope)
	if err != nil {
		return err
	}
	ctx, cancel := registry.metadataContext(ctx)
	defer cancel()
	finish, err := s.sharedNativeInitialization(ctx, scope.Key.ProjectID)
	if err != nil {
		return err
	}
	defer finish()
	if err := s.Mu.Lock(ctx); err != nil {
		return err
	}
	defer s.Mu.Unlock()
	project := scope.Key.ProjectID
	p := s.Projects[project]
	if s.opening[project] != nil || (p != nil && len(p.leases) != 0) {
		return errors.New("DuckLake project is in use")
	}
	tx, err := registry.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var deleted bool
	err = tx.QueryRowContext(ctx, `SELECT p.deleted<>0 FROM pc_native_projects p
JOIN pc_native_hosted_deletion d USING(source_id,project_id)
WHERE p.source_id=$1 AND p.project_id=$2 AND p.catalog_id=$3 AND p.ready=1 AND d.stream_id=$4 AND d.final_sequence=$5
FOR UPDATE OF p`, scope.Key.SourceID, project, scope.CatalogID, scope.StreamID, seal.FinalSequence).Scan(&deleted)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrProjectWriteFenceStale
	}
	if err != nil {
		return err
	}
	if p != nil {
		delete(s.Projects, project)
		if err := p.DB.Close(); err != nil {
			return err
		}
	}
	schema := SharedNativeSchema(scope.CatalogID)
	if deleted {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname=$1)`, schema).Scan(&exists); err != nil {
			return err
		}
		if exists {
			return errors.New("hosted native tombstone still has a catalog schema")
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE pc_native_projects SET deleted=1 WHERE source_id=$1 AND project_id=$2`, scope.Key.SourceID, project); err != nil {
		return err
	}
	return tx.Commit()
}
