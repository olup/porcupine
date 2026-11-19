package output

import (
	"context"
	"encoding/json"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
)

type exportSink struct {
	*outputSink
	s            *ExportJobs
	job          storedExport
	lastProgress time.Time
}

func newExportSink(ctx context.Context, s *ExportJobs, job storedExport) *exportSink {
	w := &exportSink{outputSink: newOutputSink(ctx, s.config, job.Format, job.ID+"/"+job.AttemptID+"/"),
		s: s, job: job, lastProgress: time.Now()}
	w.writeEmptyFile = true
	w.beginUpload, w.completeUpload, w.outputSink.progress = w.beginExportUpload, w.completeExportUpload, w.recordProgress
	return w
}

func (w *exportSink) writeRow(values []json.RawMessage) error {
	if err := w.outputSink.writeRow(values); err != nil {
		return err
	}
	if w.rows%1000 == 0 || time.Since(w.lastProgress) >= time.Second {
		return w.recordProgress()
	}
	return nil
}

func (w *exportSink) beginExportUpload(index int, key string) (*exportstore.Upload, error) {
	err := w.s.withExportAttempt(w.ctx, w.job, func(db exportExecer) error {
		_, err := db.ExecContext(w.ctx, `INSERT INTO export_files(job_id,attempt_id,file_index,object_key,storage_id) VALUES($1,$2,$3,$4,$5)`, w.job.ID, w.job.AttemptID, index, key, w.s.store.OwnershipID())
		return err
	})
	if err != nil {
		return nil, errExportSink
	}
	var upload *exportstore.Upload
	err = w.s.withExportAttempt(w.ctx, w.job, func(db exportExecer) error {
		var err error
		upload, err = w.s.store.Begin(w.ctx, key)
		if err != nil {
			return err
		}
		return verifyExportMutation(db.ExecContext(w.ctx, `UPDATE export_files SET upload_id=$1 WHERE job_id=$2 AND attempt_id=$3 AND file_index=$4`, upload.ID(), w.job.ID, w.job.AttemptID, index))
	})
	if err != nil && upload != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = upload.Abort(cleanup)
	}
	return upload, err
}

func (w *exportSink) completeExportUpload(file *outputFile) error {
	return w.s.withExportAttempt(w.ctx, w.job, func(db exportExecer) error {
		if err := verifyExportMutation(db.ExecContext(w.ctx, `UPDATE export_files SET rows=$1,bytes=$2,decoded_bytes=$3,sha256=$4 WHERE job_id=$5 AND attempt_id=$6 AND file_index=$7`, file.rows, file.bytes, file.decoded, file.sha256, w.job.ID, w.job.AttemptID, file.index)); err != nil {
			return errExportSink
		}
		if err := file.upload.Complete(w.ctx); err != nil {
			return err
		}
		return verifyExportMutation(db.ExecContext(w.ctx, `UPDATE export_files SET complete=1 WHERE job_id=$1 AND attempt_id=$2 AND file_index=$3`, w.job.ID, w.job.AttemptID, file.index))
	})
}

func (w *exportSink) recordProgress() error {
	result, err := w.s.db.ExecContext(w.ctx, `UPDATE export_jobs SET rows=$1,bytes=$2,decoded_bytes=$3,file_count=$4 WHERE id=$5 AND attempt_id=$6 AND state='running'`+w.s.leaseGuard(), w.rows, w.compressed, w.decoded, w.files, w.job.ID, w.job.AttemptID)
	w.lastProgress = time.Now()
	if err != nil {
		return errExportSink
	}
	if w.s.retained != nil && verifyExportMutation(result, nil) != nil {
		return errExportSink
	}
	return nil
}
