package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"

	"github.com/olup/porcupine/internal/auth"
)

type importChunkWork struct {
	file      storedImportUploadFile
	rows      []ImportRow
	positions []int64
	samples   []ImportRowSample
}

func parseImportUploadChunk(ctx context.Context, file storedImportUploadFile, data []byte, final bool) (importChunkWork, error) {
	work := importChunkWork{file: file}
	addSample := func(reason string) {
		if len(work.samples) < importMaxSamples {
			work.samples = append(work.samples, ImportRowSample{FileID: file.ID, RowNumber: work.file.LineNumber + 1, Reason: reason})
		}
	}
	parse := func(raw []byte) error {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			return nil
		}
		work.file.Counts.Rows++
		if work.file.Counts.Rows-file.Counts.Rows > importMaxChunkRows {
			return apiError(413, "chunk exceeds 1000 JSONL rows")
		}
		row, err := parseHistoricalImportRow(raw, file.Kind, file.Format)
		if err != nil {
			work.file.Counts.Invalid++
			addSample("invalid_row")
			return nil
		}
		work.rows = append(work.rows, row)
		work.positions = append(work.positions, work.file.LineNumber+1)
		return nil
	}
	for len(data) > 0 {
		if ctx.Err() != nil {
			return work, ctx.Err()
		}
		end := bytes.IndexByte(data, '\n')
		complete := end >= 0
		if !complete {
			end = len(data)
		}
		segment := data[:end]
		if work.file.Discarding {
			if complete {
				work.file.Discarding = false
				work.file.LineNumber++
			}
		} else if len(work.file.Tail)+len(segment) > importMaxRowBytes {
			work.file.Tail = nil
			work.file.Counts.Rows++
			work.file.Counts.Invalid++
			if work.file.Counts.Rows-file.Counts.Rows > importMaxChunkRows {
				return work, apiError(413, "chunk exceeds 1000 JSONL rows")
			}
			addSample("row_too_large")
			work.file.Discarding = !complete
			if complete {
				work.file.LineNumber++
			}
		} else {
			work.file.Tail = append(work.file.Tail, segment...)
			if complete {
				if err := parse(work.file.Tail); err != nil {
					return work, err
				}
				work.file.Tail = nil
				work.file.LineNumber++
			}
		}
		data = data[end:]
		if complete {
			data = data[1:]
		}
	}
	if final {
		if !work.file.Discarding {
			if err := parse(work.file.Tail); err != nil {
				return work, err
			}
		}
		work.file.Tail = nil
		work.file.Discarding = false
	}
	return work, nil
}

func (m *importManager) commitImportChunk(ctx context.Context, project, jobID string, original storedImportUploadFile, work importChunkWork, actor auth.Principal, complete bool) error {
	if m.rules.admission != nil {
		if err := m.rules.admission.Lock(ctx); err != nil {
			return err
		}
		defer m.rules.admission.Unlock()
	}
	if m.rules.prepareAcceptance != nil {
		if err := m.rules.prepareAcceptance(ctx, project); err != nil {
			return apiError(503, "ingestion policy unavailable; retry the same chunk")
		}
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return err
	}
	job, err := scanImportJob(tx.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=$1 AND id=$2`, project, jobID))
	if err != nil {
		return importLookupError(err)
	}
	if err := importCheckAuthority(ctx, m.authorityQuery(tx), actor.ID, project); err != nil {
		return err
	}
	if job.ActorID != actor.ID && !actor.Admin() {
		return apiError(403, "only the import creator or an administrator can upload")
	}
	if job.State != "uploading" {
		return apiError(409, "import is terminal")
	}
	file, err := scanImportFile(tx.QueryRowContext(ctx, `SELECT `+importFileColumns+` FROM import_upload_files WHERE job_id=$1 AND id=$2`, jobID, original.ID))
	if err != nil {
		return importLookupError(err)
	}
	if job.SourceID != original.SourceID || job.Format != original.Format || file.State != "uploading" || file.OffsetBytes != original.OffsetBytes || !bytes.Equal(file.HashState, original.HashState) {
		return apiError(409, "file progress changed; reload import status")
	}
	var next string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM import_upload_files WHERE job_id=$1 AND state!='completed' ORDER BY ordinal LIMIT 1`, jobID).Scan(&next); err != nil {
		return apiError(503, "import progress unavailable")
	}
	if next != file.ID {
		return apiError(409, "upload files in the order returned by import status")
	}
	for i, row := range work.rows {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT import_row`); err != nil {
			return apiError(503, "import storage unavailable")
		}
		result, err := m.rules.AcceptHistoricalTx(ctx, tx, project, row)
		if errors.Is(err, errImportRow) {
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO import_row; RELEASE import_row`); err != nil {
				return apiError(503, "import storage unavailable")
			}
			work.file.Counts.Invalid++
			if len(work.samples) < importMaxSamples {
				work.samples = append(work.samples, ImportRowSample{FileID: file.ID, RowNumber: work.positions[i], Reason: "invalid_row"})
			}
			continue
		}
		if err != nil {
			var budget *tenantIngestionError
			if errors.As(err, &budget) {
				return tenantIngestionProblem(err)
			}
			return apiError(503, "import queue or storage unavailable; retry the same chunk")
		}
		if _, err := tx.ExecContext(ctx, `RELEASE import_row`); err != nil {
			return apiError(503, "import storage unavailable")
		}
		if len(work.samples) < importMaxSamples {
			reason := ""
			if result.Unresolved > 0 {
				reason = "unresolved_identity"
			} else if result.Ignored > 0 {
				reason = "ignored_row"
			}
			if reason != "" {
				work.samples = append(work.samples, ImportRowSample{FileID: file.ID, RowNumber: work.positions[i], Reason: reason})
			}
		}
		work.file.Counts.Accepted += result.Accepted
		work.file.Counts.Duplicates += result.Duplicates
		work.file.Counts.Ignored += result.Ignored
		work.file.Counts.Unresolved += result.Unresolved
	}
	if complete {
		work.file.State = "completed"
		work.file.Tail = nil
		work.file.HashState = []byte{}
	}
	if work.file.Tail == nil {
		work.file.Tail = []byte{}
	}
	_, err = tx.ExecContext(ctx, `UPDATE import_upload_files SET offset_bytes=$1,line_number=$2,tail=$3,discarding=$4,hash_state=$5,last_offset=$6,last_size=$7,last_sha256=$8,rows=$9,accepted=$10,duplicates=$11,ignored=$12,unresolved=$13,invalid=$14,state=$15 WHERE id=$16`, work.file.OffsetBytes, work.file.LineNumber, work.file.Tail, work.file.Discarding, work.file.HashState, work.file.LastOffset, work.file.LastSize, work.file.LastSHA256, work.file.Counts.Rows, work.file.Counts.Accepted, work.file.Counts.Duplicates, work.file.Counts.Ignored, work.file.Counts.Unresolved, work.file.Counts.Invalid, work.file.State, file.ID)
	if err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	for _, sample := range work.samples {
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_upload_samples(job_id,file_id,row_number,reason) SELECT $1,$2,$3,$4 WHERE (SELECT count(*) FROM import_upload_samples WHERE job_id=$5)<$6 ON CONFLICT(job_id,file_id,row_number) DO NOTHING`, jobID, sample.FileID, sample.RowNumber, sample.Reason, jobID, importMaxSamples); err != nil {
			return apiError(503, "import checkpoint unavailable")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_uploads SET updated_at=$1,delivery_cursor=CASE WHEN delivery_cursor<coalesce((SELECT max(id) FROM events WHERE project_id=$2),0) THEN coalesce((SELECT max(id) FROM events WHERE project_id=$2),0) ELSE delivery_cursor END WHERE id=$3`, m.importTime(), project, jobID); err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	if err := tx.Commit(); err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	return nil
}

func (m *importManager) failImportDigest(ctx context.Context, project, jobID, fileID string, actor auth.Principal) error {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return err
	}
	if err := importCheckAuthority(ctx, m.authorityQuery(tx), actor.ID, project); err != nil {
		return err
	}
	job, err := scanImportJob(tx.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=$1 AND id=$2`, project, jobID))
	if err != nil {
		return importLookupError(err)
	}
	if job.ActorID != actor.ID && !actor.Admin() {
		return apiError(403, "only the import creator or an administrator can complete it")
	}
	if job.State != "uploading" {
		return apiError(409, "import is terminal")
	}
	code := "sha256_mismatch_partial_data_retained"
	if _, err := tx.ExecContext(ctx, `UPDATE import_uploads SET state='failed',error=$1,updated_at=$2,completed_at=$3 WHERE id=$4`, code, m.importTime(), m.importTime(), jobID); err != nil {
		return apiError(503, "import storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_upload_files SET tail=X'',hash_state=X'',discarding=0,error=CASE WHEN id=$1 THEN $2 ELSE error END,state=CASE WHEN id=$3 THEN 'failed' ELSE state END WHERE job_id=$4`, fileID, code, fileID, jobID); err != nil {
		return apiError(503, "import storage unavailable")
	}
	if err := tx.Commit(); err != nil {
		return apiError(503, "import storage unavailable")
	}
	return nil
}

func importDigestMatches(state []byte, expected string) bool {
	h, err := restoreImportHash(state)
	return err == nil && hex.EncodeToString(h.Sum(nil)) == expected
}

// Resolve source-scoped keys before transaction-local deduplication and acceptance.
// The raw PostHog namespace is retained only for an unbound import creator.
func prepareImportUploadSource(project, source string, rows []ImportRow) error {
	for i := range rows {
		if rows[i].ingestion != nil {
			event := rows[i].Event
			event.ProjectID = project
			if err := prepareIngestionMetadata(&event, rows[i].ingestion, source); err != nil {
				return err
			}
			rows[i].Event = event
			continue
		}
		if source == "posthog" {
			continue
		}
		event := eventWithMetadata(rows[i].Event)
		event.ProjectID = project
		if rows[i].Kind == "persons" {
			event.EventType = "identify"
		}
		metadata := ingestionMetadata{Protocol: "posthog", MessageID: event.UUID, EventType: event.EventType, Context: event.Context}
		if err := prepareIngestionMetadata(&event, &metadata, source); err != nil {
			return err
		}
		rows[i].Event = event
	}
	return nil
}

// Invalid source-bound row metadata is a row-level rejection, just like a
// malformed JSONL record, rather than a permanently unresumable file chunk.
func prepareImportUploadWork(project, source string, work *importChunkWork) error {
	if source == "" || !validSourceID(source) {
		return errors.New("invalid configured import source")
	}
	rows, positions := work.rows, work.positions
	work.rows = work.rows[:0]
	work.positions = work.positions[:0]
	for i := range rows {
		if err := prepareImportUploadSource(project, source, rows[i:i+1]); err != nil {
			work.file.Counts.Invalid++
			if len(work.samples) < importMaxSamples {
				work.samples = append(work.samples, ImportRowSample{FileID: work.file.ID, RowNumber: positions[i], Reason: "invalid_metadata"})
			}
			continue
		}
		work.rows = append(work.rows, rows[i])
		work.positions = append(work.positions, positions[i])
	}
	return nil
}
