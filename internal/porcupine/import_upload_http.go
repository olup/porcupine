package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
)

type ImportUploadCreateInput struct {
	ID   string `path:"id"`
	Body struct {
		IdempotencyKey string                 `json:"idempotency_key" minLength:"1" maxLength:"200"`
		Files          []ImportUploadFileSpec `json:"files" minItems:"1" maxItems:"4096"`
	}
}
type ImportUploadInput struct {
	ID       string `path:"id"`
	ImportID string `path:"import_id"`
}
type ImportUploadFileInput struct {
	ID       string `path:"id"`
	ImportID string `path:"import_id"`
	FileID   string `path:"file_id"`
}
type ImportUploadChunkInput struct {
	ID       string `path:"id"`
	ImportID string `path:"import_id"`
	FileID   string `path:"file_id"`
	Offset   string `header:"Upload-Offset" required:"true"`
	SHA256   string `header:"X-Chunk-SHA256" required:"true"`
	RawBody  []byte `contentType:"application/octet-stream"`
}
type ImportUploadOutput struct{ Body ImportUploadJob }
type ImportUploadListInput struct {
	ID     string `path:"id"`
	Limit  int    `query:"limit" default:"20" minimum:"1" maximum:"100"`
	Offset int    `query:"offset" default:"0" minimum:"0"`
}
type ImportUploadList struct {
	Jobs       []ImportUploadJob `json:"jobs"`
	Limit      int               `json:"limit"`
	Offset     int               `json:"offset"`
	NextOffset *int              `json:"next_offset,omitempty"`
}
type ImportUploadListOutput struct{ Body ImportUploadList }

func (s *Service) registerImportUploads(api huma.API, op func(string, string, string, string, operationAccess, int) huma.Operation) {
	access := operationAccess{Permission: "imports:write", AdditionalPermissions: []string{"events:write"}, ProjectParameter: "id"}
	operation := func(id, method, path, summary string, status int) huma.Operation {
		o := op(id, method, path, summary, access, status)
		o.Metadata["http_only"] = true
		o.Middlewares = append(o.Middlewares, func(ctx huma.Context, next func(huma.Context)) {
			req, w := humago.Unwrap(ctx)
			if method == "PUT" || id == "create-posthog-import" || id == "complete-posthog-import-file" {
				select {
				case s.importSlots <- struct{}{}:
					defer func() { <-s.importSlots }()
				default:
					w.Header().Set("Retry-After", "1")
					problem(w, 429, "import request concurrency limit reached")
					return
				}
			}
			if s.oauth != nil {
				raw, err := bearerCredential(req)
				if err != nil || !strings.HasPrefix(raw, "pc_at_") {
					s.authChallenge(w, s.oauth.apiResource, true)
					problem(w, 401, "imports require an API OAuth access token")
					return
				}
				if _, err := s.authenticateResource(ctx.Context(), raw, s.oauth.apiResource); err != nil {
					s.authChallenge(w, s.oauth.apiResource, true)
					problem(w, 401, "imports require an API OAuth access token")
					return
				}
			}
			if method == "PUT" {
				if strings.ToLower(strings.TrimSpace(strings.Split(ctx.Header("Content-Type"), ";")[0])) != "application/octet-stream" {
					problem(w, 415, "upload chunks require application/octet-stream")
					return
				}
				for _, header := range []string{"Upload-Offset", "X-Chunk-SHA256"} {
					if len(req.Header.Values(header)) != 1 {
						problem(w, 400, "upload headers must occur exactly once")
						return
					}
				}
			}
			next(ctx)
		})
		return o
	}
	base := "/api/v1/projects/{id}/imports"
	create := operation("create-posthog-import", "POST", base, "Create a resumable PostHog JSONL import manifest; persons files precede events", 201)
	create.MaxBodyBytes = 4 << 20
	huma.Register(api, create, s.createImportUpload)
	huma.Register(api, operation("list-posthog-imports", "GET", base, "List bounded project import progress summaries", 200), s.listImportUploads)
	huma.Register(api, operation("get-posthog-import", "GET", base+"/{import_id}", "Read authoritative per-file byte offsets, counts and safe row samples", 200), s.getImportUpload)
	upload := operation("upload-posthog-import-chunk", "PUT", base+"/{import_id}/files/{file_id}", "Upload raw JSONL bytes with exact byte offset and SHA-256; cursor and accepted rows commit atomically", 200)
	upload.MaxBodyBytes = importMaxChunkBytes
	huma.Register(api, upload, s.uploadImportChunk)
	huma.Register(api, operation("complete-posthog-import-file", "POST", base+"/{import_id}/files/{file_id}/complete", "Validate final file SHA-256 and process the final JSONL row", 200), s.completeImportFile)
	huma.Register(api, operation("complete-posthog-import", "POST", base+"/{import_id}/complete", "Complete an import after every file completes; downstream delivery remains asynchronous", 200), s.completeImportUpload)
	huma.Register(api, operation("cancel-posthog-import", "DELETE", base+"/{import_id}", "Cancel future upload acceptance and clear private partial rows; already accepted records remain", 200), s.cancelImportUpload)
}

func (s *Service) createImportUpload(ctx context.Context, input *ImportUploadCreateInput) (*ImportUploadOutput, error) {
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(input.Body.IdempotencyKey) == "" || len(input.Body.IdempotencyKey) > 200 {
		return nil, apiError(400, "idempotency_key must contain 1 to 200 bytes")
	}
	files, err := normalizeImportManifest(input.Body.Files)
	if err != nil {
		return nil, err
	}
	manifest, _ := json.Marshal(files)
	requestHash, keyHash := hash(string(manifest)), hash(input.Body.IdempotencyKey)
	a := principal(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return nil, err
	}
	if err := importCheckAuthority(ctx, tx, a.ID, input.ID); err != nil {
		return nil, err
	}
	var id, previousHash string
	err = tx.QueryRowContext(ctx, `SELECT id,request_hash FROM import_uploads WHERE project_id=? AND actor_id=? AND idempotency_hash=?`, input.ID, a.ID, keyHash).Scan(&id, &previousHash)
	if err == nil {
		if previousHash != requestHash {
			return nil, apiError(409, "idempotency_key was used with a different manifest")
		}
		tx.Rollback()
		job, err := s.loadImportJob(ctx, input.ID, id)
		if err != nil {
			return nil, err
		}
		return &ImportUploadOutput{Body: job}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, apiError(503, "import storage unavailable")
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM import_uploads WHERE state='uploading'`).Scan(&active); err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	if active >= importMaxActiveJobs {
		return nil, apiError(503, "active import limit reached")
	}
	id = newID()
	source := a.SourceID
	if source == "" {
		source = "posthog"
	}
	now := s.importTime()
	if _, err := tx.ExecContext(ctx, `INSERT INTO import_uploads(id,project_id,actor_id,source_id,idempotency_hash,request_hash,state,created_at,updated_at) VALUES(?,?,?,?,?,?,'uploading',?,?)`, id, input.ID, a.ID, source, keyHash, requestHash, now, now); err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	hashState, err := importHashState(sha256.New())
	if err != nil {
		return nil, apiError(503, "import checksum unavailable")
	}
	for index, file := range files {
		if _, err := tx.ExecContext(ctx, `INSERT INTO import_upload_files(id,job_id,ordinal,path,kind,size_bytes,sha256,hash_state) VALUES(?,?,?,?,?,?,?,?)`, newID(), id, index, file.Path, file.Kind, file.SizeBytes, file.SHA256, hashState); err != nil {
			return nil, apiError(503, "import manifest unavailable")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "import manifest unavailable")
	}
	job, err := s.loadImportJob(ctx, input.ID, id)
	if err != nil {
		return nil, err
	}
	return &ImportUploadOutput{Body: job}, nil
}

func (s *Service) getImportUpload(ctx context.Context, input *ImportUploadInput) (*ImportUploadOutput, error) {
	job, err := s.loadImportJob(ctx, input.ID, input.ImportID)
	if err != nil {
		return nil, err
	}
	return &ImportUploadOutput{Body: job}, nil
}

func (s *Service) listImportUploads(ctx context.Context, input *ImportUploadListInput) (*ImportUploadListOutput, error) {
	if input.Limit < 1 || input.Limit > 100 || input.Offset < 0 {
		return nil, apiError(400, "invalid import pagination")
	}
	if err := s.analyticsProject(ctx, input.ID); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? ORDER BY rowid DESC LIMIT ? OFFSET ?`, input.ID, input.Limit+1, input.Offset)
	if err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	jobs := []ImportUploadJob{}
	for rows.Next() {
		job, err := scanImportJob(rows)
		if err != nil {
			rows.Close()
			return nil, apiError(503, "import storage unavailable")
		}
		jobs = append(jobs, job.ImportUploadJob)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	result := ImportUploadList{Jobs: []ImportUploadJob{}, Limit: input.Limit, Offset: input.Offset}
	if len(jobs) > input.Limit {
		next := input.Offset + input.Limit
		result.NextOffset = &next
		jobs = jobs[:input.Limit]
	}
	for _, job := range jobs {
		if err := s.db.QueryRowContext(ctx, `SELECT coalesce(sum(rows),0),coalesce(sum(accepted),0),coalesce(sum(duplicates),0),coalesce(sum(ignored),0),coalesce(sum(unresolved),0),coalesce(sum(invalid),0) FROM import_upload_files WHERE job_id=?`, job.ID).Scan(&job.Counts.Rows, &job.Counts.Accepted, &job.Counts.Duplicates, &job.Counts.Ignored, &job.Counts.Unresolved, &job.Counts.Invalid); err != nil {
			return nil, apiError(503, "import progress unavailable")
		}
		result.Jobs = append(result.Jobs, job)
	}
	return &ImportUploadListOutput{Body: result}, nil
}

func (s *Service) loadImportFileForWrite(ctx context.Context, project, jobID, fileID string, a actor) (storedImportUploadFile, error) {
	job, err := scanImportJob(s.db.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? AND id=?`, project, jobID))
	if err != nil {
		return storedImportUploadFile{}, importLookupError(err)
	}
	if job.ActorID != a.ID && !a.admin() {
		return storedImportUploadFile{}, apiError(403, "only the import creator or an administrator can upload")
	}
	if job.State != "uploading" && job.State != "completed" {
		return storedImportUploadFile{}, apiError(409, "import is terminal")
	}
	file, err := scanImportFile(s.db.QueryRowContext(ctx, `SELECT `+importFileColumns+` FROM import_upload_files WHERE job_id=? AND id=?`, jobID, fileID))
	if err != nil {
		return file, importLookupError(err)
	}
	if file.State == "uploading" {
		var next string
		if err := s.db.QueryRowContext(ctx, `SELECT id FROM import_upload_files WHERE job_id=? AND state!='completed' ORDER BY ordinal LIMIT 1`, jobID).Scan(&next); err != nil {
			return file, apiError(503, "import progress unavailable")
		}
		if next != file.ID {
			return file, apiError(409, "upload files in the order returned by import status")
		}
	}
	file.SourceID = job.SourceID
	return file, nil
}

func (s *Service) uploadImportChunk(ctx context.Context, input *ImportUploadChunkInput) (*ImportUploadOutput, error) {
	offset, err := strconv.ParseUint(input.Offset, 10, 63)
	if err != nil || input.Offset == "" || strings.ContainsAny(input.Offset, "+ -\t\r\n") {
		return nil, apiError(400, "Upload-Offset must be a nonnegative decimal byte offset")
	}
	if len(input.RawBody) == 0 || len(input.RawBody) > importMaxChunkBytes {
		return nil, apiError(413, "chunk must contain 1 to 262144 bytes")
	}
	sum := sha256.Sum256(input.RawBody)
	digest := hex.EncodeToString(sum[:])
	if !validImportSHA256(input.SHA256) || digest != strings.ToLower(input.SHA256) {
		return nil, apiError(400, "chunk SHA-256 mismatch")
	}
	a := principal(ctx)
	file, err := s.loadImportFileForWrite(ctx, input.ID, input.ImportID, input.FileID, a)
	if err != nil {
		return nil, err
	}
	if file.LastOffset == int64(offset) && file.LastSize == int64(len(input.RawBody)) && file.LastSHA256 == digest {
		job, err := s.loadImportJob(ctx, input.ID, input.ImportID)
		if err != nil {
			return nil, err
		}
		return &ImportUploadOutput{Body: job}, nil
	}
	if file.State != "uploading" || file.OffsetBytes != int64(offset) {
		return nil, apiError(409, "upload offset conflicts with durable progress; reload import status")
	}
	if int64(len(input.RawBody)) > file.SizeBytes-file.OffsetBytes {
		return nil, apiError(413, "chunk exceeds manifest file size")
	}
	work, err := parseImportUploadChunk(ctx, file, input.RawBody, false)
	if err != nil {
		return nil, err
	}
	h, err := restoreImportHash(file.HashState)
	if err != nil {
		return nil, apiError(503, "import checksum unavailable")
	}
	h.Write(input.RawBody)
	work.file.HashState, err = importHashState(h)
	if err != nil {
		return nil, apiError(503, "import checksum unavailable")
	}
	work.file.OffsetBytes += int64(len(input.RawBody))
	work.file.LastOffset = int64(offset)
	work.file.LastSize = int64(len(input.RawBody))
	work.file.LastSHA256 = digest
	if err := prepareImportUploadWork(input.ID, file.SourceID, &work); err != nil {
		return nil, apiError(400, "invalid configured import source metadata")
	}
	lookup, err := s.prepareImportRows(ctx, input.ID, work.rows)
	if err != nil {
		return nil, apiError(503, "import receipt history unavailable")
	}
	defer s.releaseReceiptLookup(lookup)
	if err := s.commitImportChunk(ctx, input.ID, input.ImportID, file, work, lookup, a, false); err != nil {
		return nil, err
	}
	job, err := s.loadImportJob(ctx, input.ID, input.ImportID)
	if err != nil {
		return nil, err
	}
	return &ImportUploadOutput{Body: job}, nil
}

func (s *Service) completeImportFile(ctx context.Context, input *ImportUploadFileInput) (*ImportUploadOutput, error) {
	a := principal(ctx)
	file, err := s.loadImportFileForWrite(ctx, input.ID, input.ImportID, input.FileID, a)
	if err != nil {
		return nil, err
	}
	if file.State == "completed" {
		job, err := s.loadImportJob(ctx, input.ID, input.ImportID)
		if err != nil {
			return nil, err
		}
		return &ImportUploadOutput{Body: job}, nil
	}
	if file.OffsetBytes != file.SizeBytes {
		return nil, apiError(409, "file upload is incomplete")
	}
	if !importDigestMatches(file.HashState, file.SHA256) {
		if err := s.failImportDigest(ctx, input.ID, input.ImportID, file.ID, a); err != nil {
			return nil, err
		}
		return nil, apiError(409, "file SHA-256 mismatch; previously accepted records remain queued")
	}
	work, err := parseImportUploadChunk(ctx, file, nil, true)
	if err != nil {
		return nil, err
	}
	if err := prepareImportUploadWork(input.ID, file.SourceID, &work); err != nil {
		return nil, apiError(400, "invalid configured import source metadata")
	}
	lookup, err := s.prepareImportRows(ctx, input.ID, work.rows)
	if err != nil {
		return nil, apiError(503, "import receipt history unavailable")
	}
	defer s.releaseReceiptLookup(lookup)
	if err := s.commitImportChunk(ctx, input.ID, input.ImportID, file, work, lookup, a, true); err != nil {
		return nil, err
	}
	job, err := s.loadImportJob(ctx, input.ID, input.ImportID)
	if err != nil {
		return nil, err
	}
	return &ImportUploadOutput{Body: job}, nil
}

func (s *Service) completeImportUpload(ctx context.Context, input *ImportUploadInput) (*ImportUploadOutput, error) {
	return s.finishImportUpload(ctx, input, false)
}
func (s *Service) cancelImportUpload(ctx context.Context, input *ImportUploadInput) (*ImportUploadOutput, error) {
	return s.finishImportUpload(ctx, input, true)
}
func (s *Service) finishImportUpload(ctx context.Context, input *ImportUploadInput, cancel bool) (*ImportUploadOutput, error) {
	a := principal(ctx)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, apiError(503, "import storage unavailable")
	}
	defer tx.Rollback()
	if err := importWriteLock(ctx, tx); err != nil {
		return nil, err
	}
	if err := importCheckAuthority(ctx, tx, a.ID, input.ID); err != nil {
		return nil, err
	}
	job, err := scanImportJob(tx.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? AND id=?`, input.ID, input.ImportID))
	if err != nil {
		return nil, importLookupError(err)
	}
	if job.ActorID != a.ID && !a.admin() {
		return nil, apiError(403, "only the import creator or an administrator can complete or cancel it")
	}
	target := "completed"
	if cancel {
		target = "cancelled"
	}
	if job.State == target {
		tx.Rollback()
		result, err := s.loadImportJob(ctx, input.ID, input.ImportID)
		if err != nil {
			return nil, err
		}
		return &ImportUploadOutput{Body: result}, nil
	}
	if job.State != "uploading" {
		return nil, apiError(409, "import is terminal")
	}
	if !cancel {
		var unfinished int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM import_upload_files WHERE job_id=? AND state!='completed'`, job.ID).Scan(&unfinished); err != nil {
			return nil, apiError(503, "import progress unavailable")
		}
		if unfinished > 0 {
			return nil, apiError(409, "complete every manifest file before completing the import")
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_uploads SET state=?,updated_at=?,completed_at=? WHERE id=?`, target, s.importTime(), s.importTime(), job.ID); err != nil {
		return nil, apiError(503, "import checkpoint unavailable")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE import_upload_files SET tail=X'',hash_state=X'',discarding=0 WHERE job_id=?`, job.ID); err != nil {
		return nil, apiError(503, "import cleanup unavailable")
	}
	if err := tx.Commit(); err != nil {
		return nil, apiError(503, "import checkpoint unavailable")
	}
	result, err := s.loadImportJob(ctx, input.ID, input.ImportID)
	if err != nil {
		return nil, err
	}
	return &ImportUploadOutput{Body: result}, nil
}
