package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
	"github.com/olup/porcupine/internal/errtrack"
)

const sourceMapSchema = `CREATE TABLE project_source_maps(
	project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
	selector TEXT NOT NULL,
	release TEXT NOT NULL,
	sha256 TEXT NOT NULL,
	content BYTEA NOT NULL,
	PRIMARY KEY(project_id,selector)
);`

type SourceMapUploadInput struct {
	ID       string `path:"id"`
	BuildID  string `header:"X-Source-Map-Build-ID" maxLength:"128"`
	AssetURL string `header:"X-Source-Map-Asset-URL" maxLength:"2048"`
	ChunkID  string `header:"X-Source-Map-Chunk-ID" maxLength:"128"`
	Release  string `header:"X-Source-Map-Release" maxLength:"128"`
	RawBody  []byte `contentType:"application/octet-stream"`
}

type SourceMapUploadOutput struct {
	Body struct {
		Digest string `json:"sha256"`
	}
}

type ErrorStackInput struct {
	ID      string `path:"id"`
	EventID string `path:"event_id" maxLength:"19" doc:"Decimal id of a delivered exception event"`
}

type ErrorStackFrame struct {
	Generated errtrack.Frame             `json:"generated"`
	Original  *errtrack.OriginalPosition `json:"original,omitempty"`
}

type ErrorStackException struct {
	Type   string            `json:"type,omitempty"`
	Value  string            `json:"value,omitempty"`
	Frames []ErrorStackFrame `json:"frames"`
}

type ErrorStackOutput struct {
	Body struct {
		EventID     string                `json:"event_id"`
		BuildID     string                `json:"build_id,omitempty"`
		Release     string                `json:"release,omitempty"`
		Fingerprint string                `json:"fingerprint"`
		Degraded    bool                  `json:"degraded,omitempty"`
		Exceptions  []ErrorStackException `json:"exceptions"`
		MapLimit    bool                  `json:"map_limit_reached,omitempty" doc:"Some frames exceeded the number of source maps that can be looked up in one request."`
	}
}

func (s *Service) registerSourceMaps(api huma.API, op func(string, string, string, string, auth.OperationAccess, int) huma.Operation) {
	capacity := func(ctx huma.Context, next func(huma.Context)) {
		select {
		case s.sourceMapSlots <- struct{}{}:
			defer func() { <-s.sourceMapSlots }()
			next(ctx)
		default:
			_, w := humago.Unwrap(ctx)
			w.Header().Set("Retry-After", "1")
			problem(w, 429, "source map request concurrency limit reached")
		}
	}
	upload := op("put-source-map", http.MethodPut, "/api/v1/projects/{id}/source-maps", "Upload a private Source Map v3 for a build asset or a PostHog chunk ID", auth.OperationAccess{Permission: "sourcemaps:write", ProjectParameter: "id"}, 200)
	upload.MaxBodyBytes = errtrack.MaxSourceMapBytes
	upload.Errors = append(upload.Errors, 409, 413, 429)
	upload.Metadata["http_only"] = true
	upload.Middlewares = append(upload.Middlewares, capacity)
	huma.Register(api, upload, s.putSourceMap)
	stack := op("get-error-stack", http.MethodGet, "/api/v1/projects/{id}/errors/{event_id}/stack", "Read a delivered exception and resolve its stack frames against uploaded private source maps", auth.OperationAccess{Permission: "events:read", ProjectParameter: "id"}, 200)
	stack.Errors = append(stack.Errors, 429)
	stack.Middlewares = append(stack.Middlewares, capacity)
	huma.Register(api, stack, s.getErrorStack)
}

func assetMapSelector(buildID, assetURL string) string {
	return "asset:" + buildID + ":" + assetURL
}

func (s *Service) putSourceMap(ctx context.Context, input *SourceMapUploadInput) (*SourceMapUploadOutput, error) {
	var selector string
	switch {
	case errtrack.ValidID(input.ChunkID) && input.BuildID == "" && input.AssetURL == "":
		selector = "chunk:" + input.ChunkID
	case input.ChunkID == "" && errtrack.ValidID(input.BuildID):
		asset, ok := errtrack.AssetURL(input.AssetURL)
		if !ok {
			return nil, apierr.New(400, "source map asset URL must be an absolute HTTP(S) URL")
		}
		selector = assetMapSelector(input.BuildID, asset)
	default:
		return nil, apierr.New(400, "supply either a chunk ID or a build ID and asset URL")
	}
	if input.Release != "" && !errtrack.ValidID(input.Release) {
		return nil, apierr.New(400, "invalid source map release")
	}
	canonical, err := errtrack.PrepareSourceMap(input.RawBody)
	if err != nil {
		return nil, apierr.New(400, "invalid or unsupported Source Map v3")
	}
	hash := sha256.Sum256(canonical)
	digest := hex.EncodeToString(hash[:])
	var compressed bytes.Buffer
	writer, err := gzip.NewWriterLevel(&compressed, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	_, err = writer.Write(canonical)
	if err := errors.Join(err, writer.Close()); err != nil {
		return nil, err
	}
	if err := s.storeSourceMap(ctx, input.ID, selector, input.Release, digest, compressed.Bytes()); err != nil {
		return nil, err
	}
	result := &SourceMapUploadOutput{}
	result.Body.Digest = digest
	return result, nil
}

func (s *Service) storeSourceMap(ctx context.Context, project, selector, release, digest string, content []byte) error {
	tx, err := s.control.BeginTx(ctx, nil)
	if err != nil {
		return apierr.New(503, "source map storage unavailable")
	}
	defer tx.Rollback()
	if s.shared != nil {
		if _, err := lockActiveHostedProject(ctx, tx, project); err != nil {
			if errors.Is(err, errHostedProjectFenced) {
				return apierr.New(404, "project not found")
			}
			return apierr.New(503, "source map storage unavailable")
		}
	}
	// The conditional insert also synchronizes with deletion on SQLite.
	_, err = tx.ExecContext(ctx, `INSERT INTO project_source_maps(project_id,selector,release,sha256,content)
SELECT p.id,$2,$3,$4,$5 FROM projects p WHERE p.id=$1
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=p.id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=p.tenant_id)
ON CONFLICT(project_id,selector) DO NOTHING`, project, selector, release, digest, content)
	if err != nil {
		return apierr.New(503, "source map storage unavailable")
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT sm.sha256 FROM project_source_maps sm JOIN projects p ON p.id=sm.project_id
WHERE sm.project_id=$1 AND sm.selector=$2
AND NOT EXISTS(SELECT 1 FROM project_deletions WHERE project_id=p.id)
AND NOT EXISTS(SELECT 1 FROM tenant_deletions WHERE tenant_id=p.tenant_id)`, project, selector).Scan(&existing)
	if errors.Is(err, sql.ErrNoRows) {
		return apierr.New(404, "project not found")
	}
	if err != nil {
		return apierr.New(503, "source map storage unavailable")
	}
	if existing != digest {
		return apierr.New(409, "this build asset or chunk ID already has a different source map")
	}
	if err := tx.Commit(); err != nil {
		return apierr.New(503, "source map storage unavailable")
	}
	return nil
}

func (s *Service) getErrorStack(ctx context.Context, input *ErrorStackInput) (*ErrorStackOutput, error) {
	id, err := strconv.ParseInt(input.EventID, 10, 64)
	if err != nil || id < 1 || strconv.FormatInt(id, 10) != input.EventID {
		return nil, apierr.New(400, "event_id must be a positive decimal event ID")
	}
	reader := s.interactiveReader()
	if reader == nil {
		return nil, apierr.New(503, "analytics storage is not configured")
	}
	query, _, err := s.queries.SQL(ctx, reader, input.ID,
		"SELECT properties FROM events WHERE id = CAST($id AS BIGINT) AND event = '$exception' LIMIT 1",
		map[string]string{"id": input.EventID}, nil)
	if err != nil {
		return nil, err
	}
	if len(query.Data) == 0 {
		return nil, apierr.New(404, "exception event not found")
	}
	var raw string
	if err := json.Unmarshal(query.Data[0]["properties"], &raw); err != nil {
		return nil, apierr.New(500, "exception properties unavailable")
	}
	var properties map[string]any
	if err := json.Unmarshal([]byte(raw), &properties); err != nil {
		return nil, apierr.New(500, "exception properties unavailable")
	}
	normalized := errtrack.Normalize(properties)
	out := &ErrorStackOutput{}
	out.Body.EventID = input.EventID
	out.Body.BuildID, out.Body.Release = normalized.BuildID, normalized.Release
	out.Body.Fingerprint = normalized.Fingerprint
	out.Body.Degraded = normalized.Degraded || properties["$exception_degraded"] == true
	out.Body.Exceptions = make([]ErrorStackException, 0, len(normalized.List))
	for _, exception := range normalized.List {
		item := ErrorStackException{Type: exception.Type, Value: exception.Value, Frames: []ErrorStackFrame{}}
		if exception.Stacktrace != nil {
			for _, frame := range exception.Stacktrace.Frames {
				item.Frames = append(item.Frames, ErrorStackFrame{Generated: frame})
			}
		}
		out.Body.Exceptions = append(out.Body.Exceptions, item)
	}
	// First resolve chunks, then fall back to exact build asset selectors.
	// Process each distinct map once and release its parsed index before reading another.
	const maxLookups = 8
	lookups := 0
	for _, useChunk := range []bool{true, false} {
		framesBySelector := map[string][]*ErrorStackFrame{}
		var orderedSelectors []string
		for _, exception := range out.Body.Exceptions {
			for i := range exception.Frames {
				frame := &exception.Frames[i]
				if frame.Original != nil || frame.Generated.Line < 1 || frame.Generated.Column < 1 {
					continue
				}
				selector := ""
				if useChunk && frame.Generated.ChunkID != "" {
					selector = "chunk:" + frame.Generated.ChunkID
				}
				if !useChunk && normalized.BuildID != "" {
					if asset, ok := errtrack.AssetURL(frame.Generated.Filename); ok {
						selector = assetMapSelector(normalized.BuildID, asset)
					}
				}
				if selector != "" {
					if len(framesBySelector[selector]) == 0 {
						orderedSelectors = append(orderedSelectors, selector)
					}
					framesBySelector[selector] = append(framesBySelector[selector], frame)
				}
			}
		}
		for _, selector := range orderedSelectors {
			if lookups == maxLookups {
				out.Body.MapLimit = true
				break
			}
			lookups++
			source, err := s.readSourceMap(ctx, input.ID, selector)
			if err != nil {
				return nil, err
			}
			if source == nil {
				continue
			}
			for _, frame := range framesBySelector[selector] {
				if original, ok := source.Original(frame.Generated.Line, frame.Generated.Column); ok {
					frame.Original = &original
				}
			}
		}
	}
	return out, nil
}

func (s *Service) readSourceMap(ctx context.Context, project, selector string) (*errtrack.SourceMap, error) {
	var compressed []byte
	err := s.control.QueryRowContext(ctx, `SELECT content FROM project_source_maps WHERE project_id=$1 AND selector=$2`, project, selector).Scan(&compressed)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, apierr.New(503, "source map storage unavailable")
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, apierr.New(500, "stored source map is unreadable")
	}
	defer reader.Close()
	content, err := io.ReadAll(io.LimitReader(reader, errtrack.MaxSourceMapBytes+1))
	if err != nil {
		return nil, apierr.New(500, "stored source map is unreadable")
	}
	source, err := errtrack.ParseSourceMap(content)
	if err != nil {
		return nil, apierr.New(500, "stored source map is unreadable")
	}
	return source, nil
}
