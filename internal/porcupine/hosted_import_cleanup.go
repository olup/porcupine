package porcupine

import (
	"context"
	"errors"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func (s *Service) hostedImportCleanupObjects(ctx context.Context, scope lakeapi.HostedProjectScope) ([]hostedCleanupObject, error) {
	rows, err := s.control.QueryContext(ctx, `SELECT f.job_id,f.id FROM import_upload_files f
JOIN import_uploads u ON u.id=f.job_id WHERE u.project_id=$1`, scope.Key.ProjectID)
	if err != nil {
		return nil, err
	}
	var files [][2]string
	for rows.Next() {
		var file [2]string
		if err := rows.Scan(&file[0], &file[1]); err != nil {
			rows.Close()
			return nil, err
		}
		files = append(files, file)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(files) != 0 && scope.StreamID == "" {
		return nil, lakeapi.ErrNativeEvidenceGap
	}
	var artifacts []hostedCleanupObject
	for _, file := range files {
		owned, err := s.hosted.native.HostedImportCheckpointArtifacts(ctx, scope, file[0], file[1])
		if err != nil {
			return nil, err
		}
		for _, ref := range owned {
			artifacts = append(artifacts, hostedCleanupObject{kind: "analytics", key: ref.Key, version: ref.VersionID, upload: ref.UploadID})
		}
	}
	return artifacts, nil
}
