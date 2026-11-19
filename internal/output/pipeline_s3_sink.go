package output

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
)

type pipelineS3File struct {
	Key          string `json:"key"`
	Rows         int64  `json:"rows"`
	Bytes        int64  `json:"bytes"`
	DecodedBytes int64  `json:"decoded_bytes"`
	SHA256       string `json:"sha256"`
}
type pipelineS3Manifest struct {
	Version       int                             `json:"version"`
	ProjectID     string                          `json:"project_id"`
	PipelineID    string                          `json:"pipeline_id"`
	RunID         string                          `json:"run_id"`
	WindowStartMS int64                           `json:"window_start_ms"`
	WindowEndMS   int64                           `json:"window_end_ms"`
	Format        string                          `json:"format"`
	Rows          int64                           `json:"rows"`
	Columns       []lakeapi.AnalyticsResultColumn `json:"columns"`
	Files         []pipelineS3File                `json:"files"`
}
type pipelineS3Sink struct {
	*outputSink
	s      *PipelineJobs
	work   *s3PipelineWork
	hosted *hostedScheduledWork
	store  *exportstore.Store
	parts  []pipelineS3File
}

func newPipelineS3Sink(ctx context.Context, s *PipelineJobs, w *s3PipelineWork, hosted *hostedScheduledWork, store *exportstore.Store) *pipelineS3Sink {
	config, maxFiles := s.config.ExportLimits, maxOutputFiles
	if hosted != nil {
		config.MaxOutputBytes = hosted.reservedBytes
		maxFiles = hostedScheduledMaxFiles
	}
	prefix := scheduledAttemptPrefix(w.pipeline.ID, w.start, w.attemptID)
	sink := &pipelineS3Sink{outputSink: newOutputSink(ctx, config, w.spec.Format, prefix),
		s: s, work: w, hosted: hosted, store: store, parts: []pipelineS3File{}}
	sink.maxFiles, sink.limitCompressed = maxFiles, true
	sink.outputSink.beginUpload, sink.completeUpload = sink.startUpload, sink.finishUpload
	return sink
}

func (w *pipelineS3Sink) startUpload(_ int, key string) (*exportstore.Upload, error) {
	if w.hosted != nil {
		return w.beginHostedUpload(key)
	}
	checkpoint, checkpointErr := w.s.db.ExecContext(w.ctx, `INSERT INTO pipeline_s3_files(run_id,object_key) SELECT $1,$2 WHERE EXISTS(SELECT 1 FROM pipeline_s3_runs WHERE id=$1 AND state='running' AND claim_id=$3)`, w.work.id, key, w.work.claimID)
	if err := pipelineCheckpoint(checkpoint, checkpointErr); err != nil {
		return nil, err
	}
	upload, err := w.store.Begin(w.ctx, key)
	if err != nil {
		return nil, err
	}
	checkpoint, checkpointErr = w.s.db.ExecContext(w.ctx, `UPDATE pipeline_s3_files SET upload_id=$1 WHERE run_id=$2 AND object_key=$3 AND EXISTS(SELECT 1 FROM pipeline_s3_runs WHERE id=$2 AND state='running' AND claim_id=$4)`, upload.ID(), w.work.id, key, w.work.claimID)
	if err = pipelineCheckpoint(checkpoint, checkpointErr); err != nil {
		// The durable key still permits deletion after an ambiguous Complete. Abort
		// this newly-created ID immediately if checkpointing it failed.
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = upload.Abort(cleanup)
		return nil, err
	}
	return upload, nil
}
func (w *pipelineS3Sink) finishUpload(file *outputFile) error {
	var err error
	if w.hosted == nil {
		err = file.upload.Complete(w.ctx)
	} else {
		err = w.s.withHostedScheduledRun(w.ctx, w.hosted, func(*sql.Tx) error { return file.upload.Complete(w.ctx) })
	}
	if err != nil {
		return err
	}
	w.parts = append(w.parts, pipelineS3File{Key: w.work.spec.Destination.Prefix + "/" + file.key,
		Rows: file.rows, Bytes: file.bytes, DecodedBytes: file.decoded, SHA256: file.sha256})
	return nil
}

func (w *pipelineS3Sink) finishManifest() (pipelineS3Manifest, error) {
	if err := w.finish(); err != nil {
		return pipelineS3Manifest{}, err
	}
	return pipelineS3Manifest{Version: 1, ProjectID: w.work.pipeline.ProjectID, PipelineID: w.work.pipeline.ID, RunID: w.work.id, WindowStartMS: w.work.start, WindowEndMS: w.work.end, Format: w.work.spec.Format, Rows: w.rows, Columns: w.columns, Files: w.parts}, nil
}

var errPipelinePressure = errors.New("export delivery pressure")

func pipelineCheckpoint(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return errors.New("attempt_conflict")
	}
	return nil
}
