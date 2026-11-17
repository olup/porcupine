package porcupine

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/auth"
	eventapi "github.com/olup/porcupine/internal/event"
	"github.com/olup/porcupine/internal/importer"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type importChunkWork struct {
	file      storedImportUploadFile
	rows      []importer.Row
	positions []int64
	samples   []importer.ImportRowSample
}

const importChunkCommitTimeout = 30 * time.Second

func parseImportUploadChunk(ctx context.Context, file storedImportUploadFile, data []byte, final bool) (importChunkWork, error) {
	chunk, err := importer.ParseChunk(ctx, importer.ChunkState{FileID: file.ID, Kind: file.Kind, Format: file.Format, LineNumber: file.LineNumber, Tail: file.Tail, Discarding: file.Discarding, Counts: file.Counts}, data, final)
	if errors.Is(err, importer.ErrChunkRows) {
		return importChunkWork{}, apiError(413, err.Error())
	}
	file.LineNumber, file.Tail, file.Discarding, file.Counts = chunk.State.LineNumber, chunk.State.Tail, chunk.State.Discarding, chunk.State.Counts
	return importChunkWork{file: file, rows: chunk.Rows, positions: chunk.Positions, samples: chunk.Samples}, err
}

func (m *importManager) commitImportChunk(ctx context.Context, project, jobID string, original storedImportUploadFile, work importChunkWork, actor auth.Principal, complete bool) error {
	ctx, cancelCommit := context.WithTimeout(ctx, importChunkCommitTimeout)
	defer cancelCommit()
	release, err := m.rules.receiptGates.lock(ctx, project)
	if err != nil {
		return err
	}
	defer release()
	events := make([]eventapi.Event, 0, len(work.rows))
	for _, row := range work.rows {
		// Person snapshots have stable producer UUIDs too. Their receipts must
		// participate in the same cold deduplication as ordinary events.
		events = append(events, row.Event)
	}
	lookup, cancel := context.WithTimeout(ctx, receiptLookupTimeout)
	known, err := m.rules.coldKnown(lookup, project, events)
	cancel()
	if err != nil {
		return apiError(503, "import storage unavailable; retry the same chunk")
	}
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
		result, err := m.rules.AcceptHistoricalTx(ctx, tx, project, row, known)
		if errors.Is(err, importer.ErrRow) {
			if _, err := tx.ExecContext(ctx, `ROLLBACK TO import_row; RELEASE import_row`); err != nil {
				return apiError(503, "import storage unavailable")
			}
			work.file.Counts.Invalid++
			if len(work.samples) < importer.MaxSamples {
				work.samples = append(work.samples, importer.ImportRowSample{FileID: file.ID, RowNumber: work.positions[i], Reason: "invalid_row"})
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
		if len(work.samples) < importer.MaxSamples {
			reason := ""
			if result.Unresolved > 0 {
				reason = "unresolved_identity"
			} else if result.Ignored > 0 {
				reason = "ignored_row"
			}
			if reason != "" {
				work.samples = append(work.samples, importer.ImportRowSample{FileID: file.ID, RowNumber: work.positions[i], Reason: reason})
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_upload_samples(job_id,file_id,row_number,reason) SELECT $1,$2,$3,$4 WHERE (SELECT count(*) FROM import_upload_samples WHERE job_id=$5)<$6 ON CONFLICT(job_id,file_id,row_number) DO NOTHING`, jobID, sample.FileID, sample.RowNumber, sample.Reason, jobID, importer.MaxSamples); err != nil {
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
	h, err := importer.RestoreHash(state)
	return err == nil && hex.EncodeToString(h.Sum(nil)) == expected
}

// Resolve source-scoped keys before transaction-local deduplication and acceptance.
// The raw PostHog namespace is retained only for an unbound import creator.
func prepareImportUploadSource(project, source string, rows []importer.Row) error {
	for i := range rows {
		if rows[i].Ingestion != nil {
			event := rows[i].Event
			event.ProjectID = project
			if err := prepareIngestionMetadata(&event, rows[i].Ingestion, source); err != nil {
				return err
			}
			rows[i].Event = event
			continue
		}
		if source == "posthog" {
			continue
		}
		event := lakeapi.EventWithMetadata(rows[i].Event)
		event.ProjectID = project
		if rows[i].Kind == "persons" {
			event.EventType = "identify"
		}
		metadata := ingestfmt.Metadata{Protocol: "posthog", MessageID: event.UUID, EventType: event.EventType, Context: event.Context}
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
			if len(work.samples) < importer.MaxSamples {
				work.samples = append(work.samples, importer.ImportRowSample{FileID: work.file.ID, RowNumber: positions[i], Reason: "invalid_metadata"})
			}
			continue
		}
		work.rows = append(work.rows, rows[i])
		work.positions = append(work.positions, positions[i])
	}
	return nil
}
