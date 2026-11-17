package porcupine

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"time"
)

type storedImportUpload struct {
	importer.ImportUploadJob

	ActorID        string
	DeliveryCursor int64
}

type storedImportUploadFile struct {
	importer.ImportUploadFile

	Ordinal    int
	SourceID   string
	Format     string
	LineNumber int64
	Tail       []byte
	Discarding bool
	HashState  []byte
	LastOffset int64
	LastSize   int64
	LastSHA256 string
	Checkpoint lakeapi.ImportFileCheckpoint
}

func importCheckAuthority(ctx context.Context, query oauthQueryer, actor auth.Principal, project string, clock time.Time) error {
	if actor.External != nil {
		current, err := externalPrincipal(ctx, query, actor.External, clock)
		if err != nil {
			return err
		}
		if !auth.Authorize(current, "imports:write", project) || !auth.Authorize(current, "events:write", project) {
			return apiError(403, "import credential is no longer authorized")
		}
		return nil
	}
	var permissions, projects string
	err := query.QueryRowContext(ctx, `SELECT permissions,projects FROM access_keys WHERE id=$1 AND revoked_at IS NULL`, actor.ID).Scan(&permissions, &projects)
	if err != nil {
		return apiError(403, "import credential is no longer authorized")
	}
	var a auth.Principal
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return apiError(503, "import authorization unavailable")
	}
	if !auth.Authorize(a, "imports:write", project) || !auth.Authorize(a, "events:write", project) {
		return apiError(403, "import credential is no longer authorized")
	}
	return nil
}

func importWriteLock(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `UPDATE metadata SET value=value WHERE key='initialized'`); err != nil {
		return apiError(503, "import storage unavailable")
	}
	return nil
}

const importJobColumns = `id,project_id,actor_id,source_id,state,created_at,updated_at,completed_at,error,format,delivery_cursor`

func scanImportJob(row interface{ Scan(...any) error }) (storedImportUpload, error) {
	var job storedImportUpload
	err := row.Scan(&job.ID, &job.ProjectID, &job.ActorID, &job.SourceID, &job.State, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt, &job.Error, &job.Format, &job.DeliveryCursor)
	job.Limits = importer.Limits()
	return job, err
}

const importFileColumns = `id,path,kind,size_bytes,offset_bytes,sha256,state,ordinal,line_number,tail,discarding,hash_state,last_offset,last_size,last_sha256,rows,accepted,duplicates,ignored,unresolved,invalid,error`

func scanImportFile(row interface{ Scan(...any) error }) (storedImportUploadFile, error) {
	var file storedImportUploadFile
	err := row.Scan(&file.ID, &file.Path, &file.Kind, &file.SizeBytes, &file.OffsetBytes, &file.SHA256, &file.State, &file.Ordinal, &file.LineNumber, &file.Tail, &file.Discarding, &file.HashState, &file.LastOffset, &file.LastSize, &file.LastSHA256, &file.Counts.Rows, &file.Counts.Accepted, &file.Counts.Duplicates, &file.Counts.Ignored, &file.Counts.Unresolved, &file.Counts.Invalid, &file.Error)
	return file, err
}

func (m *importManager) loadImportJob(ctx context.Context, project, id string) (importer.ImportUploadJob, error) {
	job, err := scanImportJob(m.db.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=$1 AND id=$2`, project, id))
	if err != nil {
		return importer.ImportUploadJob{}, importLookupError(err)
	}
	rows, err := m.db.QueryContext(ctx, `SELECT `+m.importFileColumns()+` FROM import_upload_files WHERE job_id=$1 ORDER BY ordinal`, id)
	if err != nil {
		return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	for rows.Next() {
		file, scanErr := scanImportFile(rows)
		if scanErr != nil {
			rows.Close()
			return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
		}
		job.Files = append(job.Files, file.ImportUploadFile)
		addImportCounts(&job.Counts, file.Counts)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	samples, err := m.db.QueryContext(ctx, `SELECT file_id,row_number,reason FROM import_upload_samples WHERE job_id=$1 ORDER BY file_id,row_number LIMIT $2`, id, importer.MaxSamples)
	if err != nil {
		return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	defer samples.Close()
	for samples.Next() {
		var sample importer.ImportRowSample
		if err := samples.Scan(&sample.FileID, &sample.RowNumber, &sample.Reason); err != nil {
			return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
		}
		job.Samples = append(job.Samples, sample)
	}
	if samples.Err() != nil {
		return importer.ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	job.Delivery = &importer.ImportDelivery{}
	job.Delivery.Enabled = m.deliveryEnabled
	if m.hosted != nil {
		job.Delivery.Ready = job.State == "completed"
		return job.ImportUploadJob, nil
	}
	if err := m.db.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(EXISTS(SELECT 1 FROM delivery_quarantine q WHERE q.event_id=e.id)),0) FROM events e WHERE project_id=$1 AND id<=$2 AND delivered=0`, project, job.DeliveryCursor).Scan(&job.Delivery.Pending, &job.Delivery.Quarantined); err != nil {
		return importer.ImportUploadJob{}, apiError(503, "import delivery status unavailable")
	}
	job.Delivery.Ready = job.State == "completed" && job.Delivery.Enabled && job.Delivery.Pending == 0
	return job.ImportUploadJob, nil
}

func addImportCounts(target *importer.ImportCounts, delta importer.ImportCounts) {
	target.Rows += delta.Rows
	target.Accepted += delta.Accepted
	target.Duplicates += delta.Duplicates
	target.Ignored += delta.Ignored
	target.Unresolved += delta.Unresolved
	target.Invalid += delta.Invalid
}

func importLookupError(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return apiError(404, "import not found")
	}
	return apiError(503, "import storage unavailable")
}

func (m *importManager) importTime() string { return m.rules.clock().UTC().Format(time.RFC3339Nano) }
