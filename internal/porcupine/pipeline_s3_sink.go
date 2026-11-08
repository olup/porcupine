package porcupine

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	stdhash "hash"
	"io"

	"github.com/olup/porcupine/internal/exportstore"
	"golang.org/x/time/rate"
)

type pipelineS3File struct {
	Key          string `json:"key"`
	Rows         int64  `json:"rows"`
	Bytes        int64  `json:"bytes"`
	DecodedBytes int64  `json:"decoded_bytes"`
	SHA256       string `json:"sha256"`
}
type pipelineS3Manifest struct {
	Version       int                     `json:"version"`
	ProjectID     string                  `json:"project_id"`
	PipelineID    string                  `json:"pipeline_id"`
	RunID         string                  `json:"run_id"`
	WindowStartMS int64                   `json:"window_start_ms"`
	WindowEndMS   int64                   `json:"window_end_ms"`
	Format        string                  `json:"format"`
	Rows          int64                   `json:"rows"`
	Columns       []AnalyticsResultColumn `json:"columns"`
	Files         []pipelineS3File        `json:"files"`
}
type pipelineS3Sink struct {
	ctx                       context.Context
	s                         *Service
	work                      *s3PipelineWork
	store                     *exportstore.Store
	config                    ExportConfig
	encoder                   exportSink
	limiter                   *rate.Limiter
	current                   *pipelineS3File
	upload                    *exportstore.Upload
	gzip                      *gzip.Writer
	digest                    stdhash.Hash
	files                     []pipelineS3File
	decoded, compressed, rows int64
}

func newPipelineS3Sink(ctx context.Context, s *Service, w *s3PipelineWork, store *exportstore.Store) *pipelineS3Sink {
	sink := &pipelineS3Sink{ctx: ctx, s: s, work: w, store: store, config: s.pipelines.config.ExportLimits, files: []pipelineS3File{}}
	sink.encoder.job.Format = w.spec.Format
	sink.limiter = rate.NewLimiter(rate.Limit(sink.config.BandwidthBytesPerSecond), 64<<10)
	return sink
}
func (w *pipelineS3Sink) columns(columns []AnalyticsResultColumn) error {
	return w.encoder.setColumns(columns)
}
func (w *pipelineS3Sink) row(values []json.RawMessage) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	row, err := w.encoder.encodeRow(values)
	if err != nil {
		return err
	}
	if w.current != nil && w.current.Rows > 0 && w.current.DecodedBytes+int64(len(row)) > w.config.FileBytes {
		if err = w.finishFile(); err != nil {
			return err
		}
	}
	if w.current == nil {
		if err = w.beginFile(); err != nil {
			return err
		}
	}
	if err = w.writeDecoded(row); err != nil {
		return err
	}
	w.current.Rows++
	w.rows++
	return nil
}
func (w *pipelineS3Sink) beginFile() error {
	if len(w.files) >= 10000 {
		return errExportOutputLimit
	}
	key := fmt.Sprintf("%s/%d/%s/part-%06d.%s.gz", w.work.pipeline.ID, w.work.start, w.work.attemptID, len(w.files)+1, w.work.spec.Format)
	if _, err := w.s.db.ExecContext(w.ctx, `INSERT INTO pipeline_s3_files(run_id,object_key) VALUES(?,?)`, w.work.id, key); err != nil {
		return err
	}
	upload, err := w.store.Begin(w.ctx, key)
	if err != nil {
		return err
	}
	if _, err = w.s.db.ExecContext(w.ctx, `UPDATE pipeline_s3_files SET upload_id=? WHERE run_id=? AND object_key=?`, upload.ID(), w.work.id, key); err != nil {
		// The durable key still permits deletion after an ambiguous Complete. Abort
		// this newly-created ID immediately if checkpointing it failed.
		cleanup, cancel := context.WithTimeout(context.Background(), 10e9)
		defer cancel()
		_ = upload.Abort(cleanup)
		return err
	}
	w.current = &pipelineS3File{Key: w.work.spec.Destination.Prefix + "/" + key}
	w.upload = upload
	w.digest = sha256.New()
	w.gzip, err = gzip.NewWriterLevel((*pipelineS3Compressed)(w), gzip.BestSpeed)
	if err != nil {
		return err
	}
	if len(w.encoder.header) > 0 {
		return w.writeDecoded(w.encoder.header)
	}
	return nil
}
func (w *pipelineS3Sink) writeDecoded(data []byte) error {
	if int64(len(data)) > w.config.MaxOutputBytes-w.decoded {
		return errExportOutputLimit
	}
	n, err := w.gzip.Write(data)
	w.decoded += int64(n)
	w.current.DecodedBytes += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

type pipelineS3Compressed pipelineS3Sink

func (w *pipelineS3Compressed) Write(data []byte) (int, error) {
	if int64(len(data)) > w.config.MaxOutputBytes-w.compressed {
		return 0, errExportOutputLimit
	}
	written := 0
	for len(data) > 0 {
		size := min(len(data), w.limiter.Burst())
		if err := w.limiter.WaitN(w.ctx, size); err != nil {
			return written, err
		}
		n, err := w.upload.Write(data[:size])
		if n > 0 {
			w.digest.Write(data[:n])
			w.current.Bytes += int64(n)
			w.compressed += int64(n)
			written += n
			data = data[n:]
		}
		if err != nil {
			return written, err
		}
		if n != size {
			return written, io.ErrShortWrite
		}
	}
	return written, nil
}
func (w *pipelineS3Sink) finishFile() error {
	if w.current == nil {
		return nil
	}
	if err := w.gzip.Close(); err != nil {
		return err
	}
	if err := w.upload.Complete(w.ctx); err != nil {
		return err
	}
	w.current.SHA256 = hex.EncodeToString(w.digest.Sum(nil))
	w.files = append(w.files, *w.current)
	w.current = nil
	w.upload = nil
	w.gzip = nil
	return nil
}
func (w *pipelineS3Sink) finish() (pipelineS3Manifest, error) {
	if err := w.finishFile(); err != nil {
		return pipelineS3Manifest{}, err
	}
	return pipelineS3Manifest{Version: 1, ProjectID: w.work.pipeline.ProjectID, PipelineID: w.work.pipeline.ID, RunID: w.work.id, WindowStartMS: w.work.start, WindowEndMS: w.work.end, Format: w.work.spec.Format, Rows: w.rows, Columns: w.encoder.columns, Files: w.files}, nil
}
