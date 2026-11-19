package output

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/olup/porcupine/internal/exportstore"
)

type scheduledFileCheckpoint struct{ key, upload string }

func scheduledAttemptPrefix(pipeline string, start int64, attempt string) string {
	return fmt.Sprintf("%s/%d/%s/", pipeline, start, attempt)
}

func listScheduledFiles(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, query, run string) ([]scheduledFileCheckpoint, error) {
	rows, err := db.QueryContext(ctx, query, run)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var files []scheduledFileCheckpoint
	for rows.Next() {
		var f scheduledFileCheckpoint
		if err := rows.Scan(&f.key, &f.upload); err != nil {
			return nil, err
		}
		files = append(files, f)
	}
	return files, errors.Join(rows.Err(), rows.Close())
}

func (s *PipelineJobs) removeUnpublishedScheduledFile(ctx context.Context, store *exportstore.Store, project string, f scheduledFileCheckpoint) error {
	if err := retainArtifactOwnership(ctx, s.control, project, "pipeline_unpublished", store, f.key, f.upload); err != nil {
		return err
	}
	var err error
	if f.upload == "" {
		err = store.AbortKeyUploads(ctx, f.key)
	} else {
		err = store.Abort(ctx, f.key, f.upload)
	}
	if err != nil {
		return err
	}
	return store.Delete(ctx, f.key)
}

func (s *PipelineJobs) removeUnpublishedScheduledUploads(ctx context.Context, store *exportstore.Store, project, prefix string) error {
	if err := retainArtifactOwnership(ctx, s.control, project, "pipeline_unpublished", store, prefix, ""); err != nil {
		return err
	}
	return store.AbortAttemptUploads(ctx, prefix)
}
