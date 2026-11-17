package output

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
)

type HostedDeletionArtifact struct {
	Kind, StorageID, Key, Version, Upload string
}

const HostedDeletionExport = "export"

var (
	errHostedOutputOwnership = errors.New("hosted output ownership cannot be proved")
	hostedExportKey          = regexp.MustCompile(`^[0-9a-f-]{36}/[0-9a-f-]{36}/part-[0-9]{6}[.](csv|jsonl)[.]gz$`)
)

// HostedDeletionInventory lists exact provider identities for live, staged and
// previously retired export keys. The caller holds a drained deletion session.
func (s *ExportJobs) HostedDeletionInventory(ctx context.Context, tx *sql.Tx, tenant, project string) ([]HostedDeletionArtifact, error) {
	rows, err := tx.QueryContext(ctx, `WITH owned AS (
		SELECT f.storage_id,
			CASE WHEN f.object_key=j.id||'/'||f.attempt_id||'/part-'||lpad(f.file_index::text,6,'0')||'.'||j.format||'.gz'
				THEN f.object_key ELSE '' END AS object_key,
			f.upload_id,p.tenant_id FROM export_files f JOIN export_jobs j ON j.id=f.job_id
			JOIN projects p ON p.id=j.project_id WHERE j.project_id=$1
		UNION SELECT d.storage_id,d.object_key,d.upload_id,coalesce(p.tenant_id,'')
			FROM project_deletion_files d LEFT JOIN projects p ON p.id=d.project_id WHERE d.project_id=$1
		UNION SELECT storage_id,object_key,upload_id,tenant_id FROM managed_artifact_ownership WHERE project_id=$1 AND kind='export'
	)
	SELECT storage_id,object_key,upload_id,tenant_id,
		EXISTS(SELECT 1 FROM export_files f JOIN export_jobs j ON j.id=f.job_id WHERE j.project_id<>$1 AND f.storage_id=o.storage_id AND f.object_key=o.object_key
		UNION ALL SELECT 1 FROM project_deletion_files d WHERE d.project_id<>$1 AND d.storage_id=o.storage_id AND d.object_key=o.object_key
		UNION ALL SELECT 1 FROM managed_artifact_ownership m WHERE m.project_id<>$1 AND m.storage_id=o.storage_id AND m.object_key=o.object_key)
	FROM owned o`, project)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make(map[HostedDeletionArtifact][]string)
	for rows.Next() {
		a := HostedDeletionArtifact{Kind: HostedDeletionExport}
		var owner, upload string
		var conflict bool
		if err := rows.Scan(&a.StorageID, &a.Key, &upload, &owner, &conflict); err != nil {
			return nil, err
		}
		if conflict || owner != tenant || !s.ownsHostedExport(a) {
			return nil, errHostedOutputOwnership
		}
		keys[a] = append(keys[a], upload)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}

	var artifacts []HostedDeletionArtifact
	for a, uploadIDs := range keys {
		identities, err := s.store.ListExactArtifacts(ctx, a.Key)
		if err != nil {
			return nil, err
		}
		listedUploads := make(map[string]bool)
		for _, identity := range identities {
			item := a
			item.Version, item.Upload = identity.VersionID, identity.UploadID
			artifacts = append(artifacts, item)
			listedUploads[item.Upload] = true
		}
		// A checkpointed ID also tests provider listings for existing uploads
		// and keeps retries safe on compatible providers that omit one.
		for _, upload := range uploadIDs {
			if upload == "" || listedUploads[upload] {
				continue
			}
			present, err := s.store.ExactUploadExists(ctx, a.Key, upload)
			if err != nil {
				return nil, err
			}
			if present {
				item := a
				item.Upload = upload
				artifacts = append(artifacts, item)
			}
		}
	}
	return artifacts, nil
}

func (s *ExportJobs) ownsHostedExport(a HostedDeletionArtifact) bool {
	return s != nil && s.retained != nil && s.store != nil && a.Kind == HostedDeletionExport &&
		a.StorageID == s.store.OwnershipID() && hostedExportKey.MatchString(a.Key)
}

func (s *ExportJobs) DeleteHostedArtifact(ctx context.Context, a HostedDeletionArtifact) error {
	if !s.ownsHostedExport(a) {
		return errHostedOutputOwnership
	}
	return s.store.DeleteExactArtifact(ctx, a.Key, a.Version, a.Upload)
}
