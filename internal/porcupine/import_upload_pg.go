package porcupine

import (
	"cmp"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/importer"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
)

// Constructed only by an explicit hosted integration. The current owner-routed
// Service continues to use its journal manager.
func newHostedImportManager(rules *eventRules, flusher *hostedLiveFlusher) *importManager {
	m := &importManager{db: flusher.publisher.control, control: flusher.publisher.control, rules: rules, deliveryEnabled: true, hosted: flusher}
	flusher.publisher.checkpointImport = checkpointHostedImport
	return m
}

type heldImportProject struct {
	session *lakeapi.ProjectWriteSession
	scope   lakeapi.HostedProjectScope
	fence   lakeapi.ProjectWriteFence
	tenant  string
}
type heldImportProjectKey struct{}

func (m *importManager) withHostedImport(ctx context.Context, project string, fn func(context.Context) (*ImportUploadOutput, error)) (*ImportUploadOutput, error) {
	ctx, cancel := context.WithTimeout(ctx, importChunkCommitTimeout)
	defer cancel()
	if err := importCheckAuthority(ctx, m.control, principal(ctx), project, m.rules.clock()); err != nil {
		return nil, err
	}
	f := m.hosted
	key := lakeapi.ProjectWriteKey{SourceID: f.native.Config.SharedSourceID, ProjectID: project}
	session, err := f.coordinator.Acquire(ctx, key)
	if err != nil {
		return nil, hostedImportError(err)
	}
	defer session.Release()
	var result *ImportUploadOutput
	err = f.runActiveHostedProject(ctx, session, project, func(ctx context.Context) error {
		p := heldImportProject{session: session}
		if err := m.db.QueryRowContext(ctx, `SELECT tenant_id FROM projects p WHERE p.id=$1 AND NOT EXISTS(SELECT 1 FROM project_deletions d WHERE d.project_id=p.id) AND NOT EXISTS(SELECT 1 FROM tenant_deletions d WHERE d.tenant_id=p.tenant_id)`, project).Scan(&p.tenant); err != nil {
			return importLookupError(err)
		}
		var err error
		p.scope, err = f.native.LookupHostedProjectExisting(ctx, key)
		if err != nil {
			return err
		}
		p.fence = session.Fence(p.scope.StreamID)
		if err = f.native.AdvanceRecoveryFence(ctx, p.fence); err != nil {
			return err
		}
		if err = f.reconcileHostedWrites(ctx, session, p.scope, p.fence, p.tenant); err != nil {
			return err
		}
		if err := importCheckAuthority(ctx, m.control, principal(ctx), project, m.rules.clock()); err != nil {
			return err
		}
		result, err = fn(context.WithValue(ctx, heldImportProjectKey{}, p))
		return err
	})
	return result, hostedImportError(err)
}

func hostedImportError(err error) error {
	if err == nil {
		return nil
	}
	var httpErr *managementError
	var budget *tenantIngestionError
	switch {
	case errors.As(err, &httpErr):
		return err
	case errors.As(err, &budget):
		return tenantIngestionProblem(err)
	case errors.Is(err, lakeapi.ErrImportChunkConflict):
		return apiError(409, "file progress changed; reload import status")
	default:
		return apiError(503, "import storage unavailable; retry the same request")
	}
}

const hostedImportAdmissionLock int64 = 50938433701251924

func (m *importManager) writeLock(ctx context.Context, tx *sql.Tx) error {
	if m.hosted == nil {
		return importWriteLock(ctx, tx)
	}
	// A transaction lock makes the instance-wide active-job cap exact across
	// workers. File progress and terminal requests use the project session.
	_, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, hostedImportAdmissionLock)
	return hostedImportError(err)
}

func (m *importManager) importFileColumns() string {
	if m.hosted == nil {
		return importFileColumns
	}
	// Always return the same read model. PG has no raw bytes/rolling hash columns.
	return strings.Replace(importFileColumns, "tail,discarding,hash_state", "'\\x'::bytea,false,'\\x'::bytea", 1)
}

func (m *importManager) clearImportParserSQL() string {
	if m.hosted == nil {
		return "tail=X'',hash_state=X'',discarding=0"
	}
	return "parser_key='',parser_version='',parser_sha256='',parser_bytes=0"
}

type hostedImportParser struct {
	Version                        int
	Generation, Offset, LineNumber int64
	Tail                           []byte
	HashState                      []byte
	Discarding                     bool
}

func (m *importManager) openHostedImportParser(ctx context.Context, job string, file *storedImportUploadFile) error {
	p := ctx.Value(heldImportProjectKey{}).(heldImportProject)
	checkpoint, err := hostedImportCheckpoint(ctx, m.db, job, *file)
	if err != nil {
		return err
	}
	file.Checkpoint = checkpoint
	if checkpoint.Generation == 0 {
		file.HashState, err = importer.HashState(sha256.New())
		return err
	}
	raw, err := m.hosted.native.Remote.ReadImmutableImportCheckpoint(ctx, p.scope, job, file.ID, checkpoint.Parser)
	if err != nil {
		return err
	}
	var parser hostedImportParser
	if err := decodeHostedJSON(raw, &parser); err != nil {
		return err
	}
	if parser.Version != 1 || parser.Generation != checkpoint.Generation || parser.Offset != checkpoint.Offset || parser.LineNumber != checkpoint.LineNumber || len(parser.Tail) > importer.MaxRowBytes || (parser.Discarding && len(parser.Tail) != 0) {
		return errors.New("invalid hosted import parser")
	}
	file.Tail, file.HashState, file.Discarding = parser.Tail, parser.HashState, parser.Discarding
	return nil
}

func hostedImportCheckpoint(ctx context.Context, q oauthQueryer, job string, file storedImportUploadFile) (lakeapi.ImportFileCheckpoint, error) {
	checkpoint := lakeapi.ImportFileCheckpoint{Offset: file.OffsetBytes, LineNumber: file.LineNumber, Counts: file.Counts, Completed: file.State == "completed"}
	err := q.QueryRowContext(ctx, `SELECT generation,parser_key,parser_version,parser_sha256,parser_bytes FROM import_upload_files WHERE job_id=$1 AND id=$2`, job, file.ID).Scan(&checkpoint.Generation, &checkpoint.Parser.Key, &checkpoint.Parser.VersionID, &checkpoint.Parser.SHA256, &checkpoint.Parser.Bytes)
	if err != nil {
		return checkpoint, err
	}
	rows, err := q.QueryContext(ctx, `SELECT row_number,reason FROM import_upload_samples WHERE job_id=$1 AND file_id=$2 ORDER BY row_number`, job, file.ID)
	if err != nil {
		return checkpoint, err
	}
	defer rows.Close()
	for rows.Next() {
		sample := importer.ImportRowSample{FileID: file.ID}
		if err := rows.Scan(&sample.RowNumber, &sample.Reason); err != nil {
			return checkpoint, err
		}
		checkpoint.Samples = append(checkpoint.Samples, sample)
	}
	return checkpoint, rows.Err()
}

func (m *importManager) commitHostedImportChunk(ctx context.Context, project, jobID string, file storedImportUploadFile, work importChunkWork, actor auth.Principal, complete bool) error {
	p := ctx.Value(heldImportProjectKey{}).(heldImportProject)
	key := lakeapi.ImportChunkKey{Scope: p.scope, JobID: jobID, FileID: file.ID, Start: file.OffsetBytes, ExpectedEnd: work.file.OffsetBytes, Phase: "bytes"}
	inputHash := work.file.LastSHA256
	parser := hostedImportParser{Version: 1, Generation: file.Checkpoint.Generation + 1, Offset: work.file.OffsetBytes, LineNumber: work.file.LineNumber}
	if complete {
		key.Phase = "finalize"
		sum := sha256.Sum256(nil)
		inputHash = hex.EncodeToString(sum[:])
	} else {
		parser.Tail = work.file.Tail
		parser.HashState = work.file.HashState
		parser.Discarding = work.file.Discarding
	}
	var manifest string
	if err := m.db.QueryRowContext(ctx, `SELECT request_hash FROM import_uploads WHERE project_id=$1 AND id=$2`, project, jobID).Scan(&manifest); err != nil {
		return err
	}
	raw, err := json.Marshal(parser)
	if err != nil {
		return err
	}
	ref, err := m.hosted.native.Remote.PutImmutableImportCheckpoint(ctx, p.scope, jobID, file.ID, raw)
	if err != nil {
		return err
	}
	delta := importer.ImportCounts{Rows: work.file.Counts.Rows - file.Counts.Rows, Invalid: work.file.Counts.Invalid - file.Counts.Invalid}
	slices.SortFunc(work.failures, func(a, b importer.RowFailure) int { return cmp.Compare(a.Position, b.Position) })
	input := lakeapi.ImportChunkInput{Key: key, TenantID: p.tenant, ManifestSHA256: manifest, SourceID: file.SourceID, Format: file.Format, InputSHA256: inputHash, Previous: file.Checkpoint, Parser: ref, LineNumber: work.file.LineNumber, Parsed: delta, Samples: work.samples, Failures: work.failures}
	for i, row := range work.rows {
		row.Event.ProjectID = project
		row.Event.SourceID = file.SourceID
		input.Candidates = append(input.Candidates, lakeapi.ImportCandidate{Position: work.positions[i], Row: row})
	}
	_, err = m.hosted.commitImportHeld(ctx, p.session, p.fence, input, func(ctx context.Context) error {
		if err := importCheckAuthority(ctx, m.control, actor, project, m.rules.clock()); err != nil {
			return err
		}
		if len(input.Candidates) > 0 {
			return m.hosted.policy.admit(ctx, p.tenant)
		}
		return nil
	})
	return err
}

func checkpointHostedImport(ctx context.Context, tx *sql.Tx, evidence lakeapi.NativeCommitEvidence) error {
	o := evidence.Import
	key := o.Key
	var manifest, source, format string
	err := tx.QueryRowContext(ctx, `SELECT request_hash,source_id,format FROM import_uploads WHERE project_id=$1 AND id=$2 AND state='uploading' FOR UPDATE`, key.Scope.Key.ProjectID, key.JobID).Scan(&manifest, &source, &format)
	if err != nil {
		return err
	}
	if manifest != o.ManifestSHA256 || source != o.SourceID || format != o.Format {
		return output.ErrPipelineConflict
	}
	var file storedImportUploadFile
	file.ID = key.FileID
	err = tx.QueryRowContext(ctx, `SELECT state,size_bytes,offset_bytes,line_number,rows,accepted,duplicates,ignored,unresolved,invalid FROM import_upload_files WHERE job_id=$1 AND id=(SELECT id FROM import_upload_files WHERE job_id=$1 AND state!='completed' ORDER BY ordinal LIMIT 1) AND id=$2`, key.JobID, key.FileID).Scan(&file.State, &file.SizeBytes, &file.OffsetBytes, &file.LineNumber, &file.Counts.Rows, &file.Counts.Accepted, &file.Counts.Duplicates, &file.Counts.Ignored, &file.Counts.Unresolved, &file.Counts.Invalid)
	if err != nil {
		return err
	}
	prior, err := hostedImportCheckpoint(ctx, tx, key.JobID, file)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(prior, o.Previous) || file.State != "uploading" || o.Next.Offset > file.SizeBytes || (o.Next.Completed && o.Next.Offset != file.SizeBytes) {
		return output.ErrPipelineConflict
	}
	next := o.Next
	state := "uploading"
	if next.Completed {
		state = "completed"
		next.Parser = lakeapi.ImmutableObjectRef{}
	}
	updated, err := tx.ExecContext(ctx, `UPDATE import_upload_files SET offset_bytes=$1,line_number=$2,generation=$3,state=$4,parser_key=$5,parser_version=$6,parser_sha256=$7,parser_bytes=$8,rows=$9,accepted=$10,duplicates=$11,ignored=$12,unresolved=$13,invalid=$14,last_offset=CASE WHEN $15='bytes' THEN $16 ELSE last_offset END,last_size=CASE WHEN $15='bytes' THEN $17 ELSE last_size END,last_sha256=CASE WHEN $15='bytes' THEN $18 ELSE last_sha256 END WHERE job_id=$19 AND id=$20 AND generation=$21`, next.Offset, next.LineNumber, next.Generation, state, next.Parser.Key, next.Parser.VersionID, next.Parser.SHA256, next.Parser.Bytes, next.Counts.Rows, next.Counts.Accepted, next.Counts.Duplicates, next.Counts.Ignored, next.Counts.Unresolved, next.Counts.Invalid, key.Phase, key.Start, key.ExpectedEnd-key.Start, o.InputSHA256, key.JobID, key.FileID, prior.Generation)
	if err != nil {
		return err
	}
	if n, err := updated.RowsAffected(); err != nil || n != 1 {
		return output.ErrPipelineConflict
	}
	for _, sample := range o.Samples {
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_upload_samples(job_id,file_id,row_number,reason) VALUES($1,$2,$3,$4)`, key.JobID, sample.FileID, sample.RowNumber, sample.Reason); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE import_uploads SET updated_at=$1 WHERE id=$2`, o.AccountedAt.UTC().Format(time.RFC3339Nano), key.JobID)
	return err
}
