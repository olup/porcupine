package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
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
		row, err := parsePostHogImportRow(raw, file.Kind)
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

func (s *Service) commitImportChunk(ctx context.Context, project, jobID string, original storedImportUploadFile, work importChunkWork, lookup *receiptLookup, actor actor, complete bool) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return err
	}
	job, err := scanImportJob(tx.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? AND id=?`, project, jobID))
	if err != nil {
		return importLookupError(err)
	}
	if err := importCheckAuthority(ctx, tx, actor.ID, project); err != nil {
		return err
	}
	if job.ActorID != actor.ID && !actor.admin() {
		return apiError(403, "only the import creator or an administrator can upload")
	}
	if job.State != "uploading" {
		return apiError(409, "import is terminal")
	}
	file, err := scanImportFile(tx.QueryRowContext(ctx, `SELECT `+importFileColumns+` FROM import_upload_files WHERE job_id=? AND id=?`, jobID, original.ID))
	if err != nil {
		return importLookupError(err)
	}
	if job.SourceID != original.SourceID || file.State != "uploading" || file.OffsetBytes != original.OffsetBytes || !bytes.Equal(file.HashState, original.HashState) {
		return apiError(409, "file progress changed; reload import status")
	}
	var next string
	if err := tx.QueryRowContext(ctx, `SELECT id FROM import_upload_files WHERE job_id=? AND state!='completed' ORDER BY ordinal LIMIT 1`, jobID).Scan(&next); err != nil {
		return apiError(503, "import progress unavailable")
	}
	if next != file.ID {
		return apiError(409, "upload files in the order returned by import status")
	}
	if err := s.validateReceiptLookup(ctx, tx, lookup); err != nil {
		return apiError(503, "import receipt history unavailable")
	}
	for i, row := range work.rows {
		if _, err := tx.ExecContext(ctx, `SAVEPOINT import_row`); err != nil {
			return apiError(503, "import storage unavailable")
		}
		result, err := s.acceptImportRowTx(ctx, tx, project, row)
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
	_, err = tx.ExecContext(ctx, `UPDATE import_upload_files SET offset_bytes=?,line_number=?,tail=?,discarding=?,hash_state=?,last_offset=?,last_size=?,last_sha256=?,rows=?,accepted=?,duplicates=?,ignored=?,unresolved=?,invalid=?,state=? WHERE id=?`, work.file.OffsetBytes, work.file.LineNumber, work.file.Tail, work.file.Discarding, work.file.HashState, work.file.LastOffset, work.file.LastSize, work.file.LastSHA256, work.file.Counts.Rows, work.file.Counts.Accepted, work.file.Counts.Duplicates, work.file.Counts.Ignored, work.file.Counts.Unresolved, work.file.Counts.Invalid, work.file.State, file.ID)
	if err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	for _, sample := range work.samples {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO import_upload_samples(job_id,file_id,row_number,reason) SELECT ?,?,?,? WHERE (SELECT count(*) FROM import_upload_samples WHERE job_id=?)<?`, jobID, sample.FileID, sample.RowNumber, sample.Reason, jobID, importMaxSamples); err != nil {
			return apiError(503, "import checkpoint unavailable")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_uploads SET updated_at=? WHERE id=?`, s.importTime(), jobID); err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	if err := tx.Commit(); err != nil {
		return apiError(503, "import checkpoint unavailable")
	}
	return nil
}

func (s *Service) failImportDigest(ctx context.Context, project, jobID, fileID string, actor actor) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return err
	}
	if err := importCheckAuthority(ctx, tx, actor.ID, project); err != nil {
		return err
	}
	job, err := scanImportJob(tx.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? AND id=?`, project, jobID))
	if err != nil {
		return importLookupError(err)
	}
	if job.ActorID != actor.ID && !actor.admin() {
		return apiError(403, "only the import creator or an administrator can complete it")
	}
	if job.State != "uploading" {
		return apiError(409, "import is terminal")
	}
	code := "sha256_mismatch_partial_data_retained"
	if _, err := tx.ExecContext(ctx, `UPDATE import_uploads SET state='failed',error=?,updated_at=?,completed_at=? WHERE id=?`, code, s.importTime(), s.importTime(), jobID); err != nil {
		return apiError(503, "import storage unavailable")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_upload_files SET tail=X'',hash_state=X'',discarding=0,error=CASE WHEN id=? THEN ? ELSE error END,state=CASE WHEN id=? THEN 'failed' ELSE state END WHERE job_id=?`, fileID, code, fileID, jobID); err != nil {
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

// Resolve source-scoped keys before both archive receipt lookup and acceptance.
// The raw PostHog namespace is retained only for an unbound import creator.
func prepareImportUploadSource(project, source string, rows []ImportRow) error {
	if source == "posthog" {
		return nil
	}
	for i := range rows {
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
	if source == "posthog" {
		return nil
	}
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
