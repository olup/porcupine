package porcupine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	stdhash "hash"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"
)

const importMaxChunkBytes = 256 << 10
const importMaxRowBytes = 1 << 20
const importMaxChunkRows = 1000
const importMaxFiles = 4096
const importMaxActiveJobs = 4
const importMaxSamples = 20

// Import progress is metadata only. The one incomplete row and hash state are
// bounded private resumability state, removed on completion or cancellation.
type ImportCounts struct {
	Rows       int64 `json:"rows"`
	Accepted   int64 `json:"accepted"`
	Duplicates int64 `json:"duplicates"`
	Ignored    int64 `json:"ignored"`
	Unresolved int64 `json:"unresolved"`
	Invalid    int64 `json:"invalid"`
}

type ImportUploadLimits struct {
	MaxChunkBytes         int `json:"max_chunk_bytes"`
	MaxRowsPerChunk       int `json:"max_rows_per_chunk"`
	MaxRowBytes           int `json:"max_row_bytes"`
	MaxFiles              int `json:"max_files"`
	MaxActiveJobs         int `json:"max_active_jobs"`
	MaxSamples            int `json:"max_samples"`
	MaxConcurrentRequests int `json:"max_concurrent_requests"`
}

func importUploadLimits() ImportUploadLimits {
	return ImportUploadLimits{importMaxChunkBytes, importMaxChunkRows, importMaxRowBytes, importMaxFiles, importMaxActiveJobs, importMaxSamples, 2}
}

type ImportRowSample struct {
	FileID    string `json:"file_id"`
	RowNumber int64  `json:"row_number"`
	Reason    string `json:"reason"`
}

type ImportUploadFileSpec struct {
	Path      string `json:"path" maxLength:"1024"`
	Kind      string `json:"kind" enum:"events,persons"`
	SizeBytes int64  `json:"size_bytes" minimum:"0"`
	SHA256    string `json:"sha256" minLength:"64" maxLength:"64"`
}

type ImportUploadFile struct {
	ID          string       `json:"id"`
	Path        string       `json:"path"`
	Kind        string       `json:"kind"`
	SizeBytes   int64        `json:"size_bytes"`
	OffsetBytes int64        `json:"offset_bytes"`
	SHA256      string       `json:"sha256"`
	State       string       `json:"state"`
	Counts      ImportCounts `json:"counts"`
	Error       string       `json:"error,omitempty"`
}

type ImportUploadJob struct {
	ID          string             `json:"id"`
	ProjectID   string             `json:"project_id"`
	SourceID    string             `json:"source_id"`
	State       string             `json:"state"`
	Counts      ImportCounts       `json:"counts"`
	Files       []ImportUploadFile `json:"files,omitempty"`
	Samples     []ImportRowSample  `json:"samples,omitempty"`
	Limits      ImportUploadLimits `json:"limits"`
	Error       string             `json:"error,omitempty"`
	CreatedAt   string             `json:"created_at"`
	UpdatedAt   string             `json:"updated_at"`
	CompletedAt string             `json:"completed_at,omitempty"`
}

type storedImportUpload struct {
	ImportUploadJob
	ActorID string
}

type storedImportUploadFile struct {
	ImportUploadFile
	Ordinal    int
	SourceID   string
	LineNumber int64
	Tail       []byte
	Discarding bool
	HashState  []byte
	LastOffset int64
	LastSize   int64
	LastSHA256 string
}

func (s *Service) initializeImports(ctx context.Context) error {
	var exists bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('events') WHERE name='is_imported')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE events ADD COLUMN is_imported INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS import_uploads (
id TEXT PRIMARY KEY,project_id TEXT NOT NULL REFERENCES projects(id),actor_id TEXT NOT NULL REFERENCES access_keys(id),source_id TEXT NOT NULL DEFAULT 'posthog',
idempotency_hash TEXT NOT NULL,request_hash TEXT NOT NULL,state TEXT NOT NULL,
created_at TEXT NOT NULL,updated_at TEXT NOT NULL,completed_at TEXT NOT NULL DEFAULT '',error TEXT NOT NULL DEFAULT '',
UNIQUE(project_id,actor_id,idempotency_hash));
CREATE INDEX IF NOT EXISTS import_uploads_project ON import_uploads(project_id,created_at,id);
CREATE INDEX IF NOT EXISTS import_uploads_active ON import_uploads(state);
CREATE TABLE IF NOT EXISTS import_upload_files (
id TEXT PRIMARY KEY,job_id TEXT NOT NULL REFERENCES import_uploads(id),ordinal INTEGER NOT NULL,path TEXT NOT NULL,kind TEXT NOT NULL,
size_bytes INTEGER NOT NULL,offset_bytes INTEGER NOT NULL DEFAULT 0,sha256 TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'uploading',
line_number INTEGER NOT NULL DEFAULT 0,tail BLOB NOT NULL DEFAULT X'',discarding INTEGER NOT NULL DEFAULT 0,hash_state BLOB NOT NULL,
last_offset INTEGER NOT NULL DEFAULT -1,last_size INTEGER NOT NULL DEFAULT 0,last_sha256 TEXT NOT NULL DEFAULT '',
rows INTEGER NOT NULL DEFAULT 0,accepted INTEGER NOT NULL DEFAULT 0,duplicates INTEGER NOT NULL DEFAULT 0,
ignored INTEGER NOT NULL DEFAULT 0,unresolved INTEGER NOT NULL DEFAULT 0,invalid INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',
UNIQUE(job_id,path),UNIQUE(job_id,ordinal));
CREATE INDEX IF NOT EXISTS import_upload_files_order ON import_upload_files(job_id,ordinal);
CREATE TABLE IF NOT EXISTS import_upload_samples (
job_id TEXT NOT NULL REFERENCES import_uploads(id),file_id TEXT NOT NULL REFERENCES import_upload_files(id),row_number INTEGER NOT NULL,reason TEXT NOT NULL,
PRIMARY KEY(job_id,file_id,row_number)) WITHOUT ROWID;`); err != nil {
		return err
	}
	var sourceColumn bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pragma_table_info('import_uploads') WHERE name='source_id')`).Scan(&sourceColumn); err != nil {
		return err
	}
	if !sourceColumn {
		if _, err := s.db.ExecContext(ctx, `ALTER TABLE import_uploads ADD COLUMN source_id TEXT NOT NULL DEFAULT 'posthog'`); err != nil {
			return err
		}
	}
	return s.initializeImportRows(ctx)
}

func normalizeImportManifest(files []ImportUploadFileSpec) ([]ImportUploadFileSpec, error) {
	if len(files) == 0 || len(files) > importMaxFiles {
		return nil, apiError(400, "manifest must contain 1 to 4096 files")
	}
	files = append([]ImportUploadFileSpec(nil), files...)
	seen := map[string]bool{}
	for i := range files {
		file := &files[i]
		if file.Path == "" || len(file.Path) > 1024 || strings.Contains(file.Path, "\\") || path.IsAbs(file.Path) || path.Clean(file.Path) != file.Path || file.Path == "." || strings.HasPrefix(file.Path, "../") || file.Path == ".." || strings.ContainsFunc(file.Path, unicode.IsControl) {
			return nil, apiError(400, "manifest paths must be clean relative file labels")
		}
		if file.Kind != "events" && file.Kind != "persons" || file.SizeBytes < 0 || !validImportSHA256(file.SHA256) || seen[file.Path] {
			return nil, apiError(400, "invalid file kind, size, SHA-256 or duplicate path")
		}
		file.SHA256 = strings.ToLower(file.SHA256)
		seen[file.Path] = true
	}
	sort.Slice(files, func(i, j int) bool {
		if files[i].Kind != files[j].Kind {
			return files[i].Kind == "persons"
		}
		return files[i].Path < files[j].Path
	})
	return files, nil
}

func validImportSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(value) == 64 && len(decoded) == 32
}

func importHashState(h stdhash.Hash) ([]byte, error) {
	return h.(encoding.BinaryMarshaler).MarshalBinary()
}
func restoreImportHash(state []byte) (stdhash.Hash, error) {
	h := sha256.New()
	if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		return nil, err
	}
	return h, nil
}

func importCheckAuthority(ctx context.Context, tx *sql.Tx, actorID, project string) error {
	var permissions, projects string
	err := tx.QueryRowContext(ctx, `SELECT permissions,projects FROM access_keys WHERE id=? AND revoked_at IS NULL`, actorID).Scan(&permissions, &projects)
	if err != nil {
		return apiError(403, "import credential is no longer authorized")
	}
	var a actor
	if json.Unmarshal([]byte(permissions), &a.Permissions) != nil || json.Unmarshal([]byte(projects), &a.Projects) != nil {
		return apiError(503, "import authorization unavailable")
	}
	if !authorize(a, "imports:write", project) || !authorize(a, "events:write", project) {
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

const importJobColumns = `id,project_id,actor_id,source_id,state,created_at,updated_at,completed_at,error`

func scanImportJob(row interface{ Scan(...any) error }) (storedImportUpload, error) {
	var job storedImportUpload
	err := row.Scan(&job.ID, &job.ProjectID, &job.ActorID, &job.SourceID, &job.State, &job.CreatedAt, &job.UpdatedAt, &job.CompletedAt, &job.Error)
	job.Limits = importUploadLimits()
	return job, err
}

const importFileColumns = `id,path,kind,size_bytes,offset_bytes,sha256,state,ordinal,line_number,tail,discarding,hash_state,last_offset,last_size,last_sha256,rows,accepted,duplicates,ignored,unresolved,invalid,error`

func scanImportFile(row interface{ Scan(...any) error }) (storedImportUploadFile, error) {
	var file storedImportUploadFile
	err := row.Scan(&file.ID, &file.Path, &file.Kind, &file.SizeBytes, &file.OffsetBytes, &file.SHA256, &file.State, &file.Ordinal, &file.LineNumber, &file.Tail, &file.Discarding, &file.HashState, &file.LastOffset, &file.LastSize, &file.LastSHA256, &file.Counts.Rows, &file.Counts.Accepted, &file.Counts.Duplicates, &file.Counts.Ignored, &file.Counts.Unresolved, &file.Counts.Invalid, &file.Error)
	return file, err
}

func (s *Service) loadImportJob(ctx context.Context, project, id string) (ImportUploadJob, error) {
	job, err := scanImportJob(s.db.QueryRowContext(ctx, `SELECT `+importJobColumns+` FROM import_uploads WHERE project_id=? AND id=?`, project, id))
	if err != nil {
		return ImportUploadJob{}, importLookupError(err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+importFileColumns+` FROM import_upload_files WHERE job_id=? ORDER BY ordinal`, id)
	if err != nil {
		return ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	for rows.Next() {
		file, scanErr := scanImportFile(rows)
		if scanErr != nil {
			rows.Close()
			return ImportUploadJob{}, apiError(503, "import storage unavailable")
		}
		job.Files = append(job.Files, file.ImportUploadFile)
		addImportCounts(&job.Counts, file.Counts)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	samples, err := s.db.QueryContext(ctx, `SELECT file_id,row_number,reason FROM import_upload_samples WHERE job_id=? ORDER BY file_id,row_number LIMIT ?`, id, importMaxSamples)
	if err != nil {
		return ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	defer samples.Close()
	for samples.Next() {
		var sample ImportRowSample
		if err := samples.Scan(&sample.FileID, &sample.RowNumber, &sample.Reason); err != nil {
			return ImportUploadJob{}, apiError(503, "import storage unavailable")
		}
		job.Samples = append(job.Samples, sample)
	}
	if samples.Err() != nil {
		return ImportUploadJob{}, apiError(503, "import storage unavailable")
	}
	return job.ImportUploadJob, nil
}

func addImportCounts(target *ImportCounts, delta ImportCounts) {
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

func (s *Service) importTime() string { return s.clock().UTC().Format(time.RFC3339Nano) }
