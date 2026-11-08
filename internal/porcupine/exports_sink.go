package porcupine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	hashpkg "hash"
	"io"
	"time"

	"github.com/olup/porcupine/internal/exportstore"
	"golang.org/x/time/rate"
)

var (
	errExportOutputLimit = errors.New("export output limit")
	errExportSink        = errors.New("export object write failed")
)

type exportSink struct {
	s                         *Service
	ctx                       context.Context
	job                       storedExport
	limiter                   *rate.Limiter
	columns                   []AnalyticsResultColumn
	names                     [][]byte
	header                    []byte
	current                   *exportSinkFile
	rows, decoded, compressed int64
	files                     int
	lastProgress              time.Time
}

type exportSinkFile struct {
	artifact exportArtifact
	upload   *exportstore.Upload
	gzip     *gzip.Writer
	digest   hashpkg.Hash
}

func newExportSink(ctx context.Context, s *Service, job storedExport) *exportSink {
	return &exportSink{s: s, ctx: ctx, job: job, limiter: rate.NewLimiter(rate.Limit(s.exports.config.BandwidthBytesPerSecond), 64<<10), lastProgress: time.Now()}
}

func (w *exportSink) setColumns(columns []AnalyticsResultColumn) error {
	if w.columns != nil {
		return errExportSink
	}
	w.columns = append([]AnalyticsResultColumn{}, columns...)
	seen := map[string]bool{}
	for _, column := range columns {
		if w.job.Format == "jsonl" && seen[column.Name] {
			return errExportSink
		}
		seen[column.Name] = true
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		encoder.SetEscapeHTML(false)
		if err := encoder.Encode(column.Name); err != nil {
			return errExportSink
		}
		w.names = append(w.names, bytes.TrimSuffix(encoded.Bytes(), []byte("\n")))
	}
	if w.job.Format == "csv" {
		var header bytes.Buffer
		writer := csv.NewWriter(&header)
		names := make([]string, len(columns))
		for i, column := range columns {
			names[i] = column.Name
		}
		if err := writer.Write(names); err != nil {
			return errExportSink
		}
		writer.Flush()
		if writer.Error() != nil {
			return errExportSink
		}
		w.header = header.Bytes()
	} else if w.job.Format != "jsonl" {
		return errExportSink
	}
	return nil
}

func (w *exportSink) encodeRow(values []json.RawMessage) ([]byte, error) {
	if w.columns == nil || len(values) != len(w.columns) {
		return nil, errExportSink
	}
	var row bytes.Buffer
	if w.job.Format == "jsonl" {
		row.WriteByte('{')
		for i, value := range values {
			if i > 0 {
				row.WriteByte(',')
			}
			row.Write(w.names[i])
			row.WriteByte(':')
			row.Write(value)
		}
		row.WriteString("}\n")
	} else {
		fields := make([]string, len(values))
		for i, value := range values {
			if bytes.Equal(value, []byte("null")) {
				continue
			}
			if len(value) > 0 && value[0] == '"' {
				if json.Unmarshal(value, &fields[i]) != nil {
					return nil, errExportSink
				}
			} else {
				fields[i] = string(value)
			}
		}
		writer := csv.NewWriter(&row)
		if err := writer.Write(fields); err != nil {
			return nil, errExportSink
		}
		writer.Flush()
		if writer.Error() != nil {
			return nil, errExportSink
		}
	}
	return row.Bytes(), nil
}

func (w *exportSink) writeRow(values []json.RawMessage) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	row, err := w.encodeRow(values)
	if err != nil {
		return err
	}
	if w.current != nil && w.current.artifact.Rows > 0 && w.current.artifact.DecodedBytes+int64(len(row)) > w.s.exports.config.FileBytes {
		if err = w.completeFile(); err != nil {
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
	w.current.artifact.Rows++
	w.rows++
	if w.rows%1000 == 0 || time.Since(w.lastProgress) >= time.Second {
		return w.progress()
	}
	return nil
}

func (w *exportSink) beginFile() error {
	if w.files >= 10000 {
		return errExportOutputLimit
	}
	if int64(len(w.header)) > w.s.exports.config.MaxOutputBytes-w.decoded {
		return errExportOutputLimit
	}
	w.files++
	artifact := exportArtifact{Index: w.files, Key: fmt.Sprintf("%s/%s/part-%06d.%s.gz", w.job.ID, w.job.AttemptID, w.files, w.job.Format)}
	if _, err := w.s.db.ExecContext(w.ctx, `INSERT INTO export_files(job_id,attempt_id,file_index,object_key) VALUES(?,?,?,?)`, w.job.ID, w.job.AttemptID, artifact.Index, artifact.Key); err != nil {
		return errExportSink
	}
	upload, err := w.s.exports.store.Begin(w.ctx, artifact.Key)
	if err != nil {
		return err
	}
	artifact.UploadID = upload.ID()
	if _, err = w.s.db.ExecContext(w.ctx, `UPDATE export_files SET upload_id=? WHERE job_id=? AND attempt_id=? AND file_index=?`, artifact.UploadID, w.job.ID, w.job.AttemptID, artifact.Index); err != nil {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		upload.Abort(cleanup)
		return errExportSink
	}
	file := &exportSinkFile{artifact: artifact, upload: upload, digest: sha256.New()}
	compressed := &exportCompressedWriter{sink: w, file: file}
	file.gzip, err = gzip.NewWriterLevel(compressed, gzip.BestSpeed)
	if err != nil {
		return errExportSink
	}
	w.current = file
	if len(w.header) > 0 {
		return w.writeDecoded(w.header)
	}
	return nil
}

func (w *exportSink) writeDecoded(data []byte) error {
	if int64(len(data)) > w.s.exports.config.MaxOutputBytes-w.decoded {
		return errExportOutputLimit
	}
	n, err := w.current.gzip.Write(data)
	w.decoded += int64(n)
	w.current.artifact.DecodedBytes += int64(n)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

type exportCompressedWriter struct {
	sink *exportSink
	file *exportSinkFile
}

func (w *exportCompressedWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		size := min(len(data), w.sink.limiter.Burst())
		if err := w.sink.limiter.WaitN(w.sink.ctx, size); err != nil {
			return written, err
		}
		n, err := w.file.upload.Write(data[:size])
		if n > 0 {
			w.file.digest.Write(data[:n])
			w.file.artifact.Bytes += int64(n)
			w.sink.compressed += int64(n)
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

func (w *exportSink) completeFile() error {
	file := w.current
	if file == nil {
		return nil
	}
	if err := file.gzip.Close(); err != nil {
		return err
	}
	file.artifact.SHA256 = hex.EncodeToString(file.digest.Sum(nil))
	if _, err := w.s.db.ExecContext(w.ctx, `UPDATE export_files SET rows=?,bytes=?,decoded_bytes=?,sha256=? WHERE job_id=? AND attempt_id=? AND file_index=?`, file.artifact.Rows, file.artifact.Bytes, file.artifact.DecodedBytes, file.artifact.SHA256, w.job.ID, w.job.AttemptID, file.artifact.Index); err != nil {
		return errExportSink
	}
	if err := file.upload.Complete(w.ctx); err != nil {
		return err
	}
	if _, err := w.s.db.ExecContext(w.ctx, `UPDATE export_files SET complete=1 WHERE job_id=? AND attempt_id=? AND file_index=?`, w.job.ID, w.job.AttemptID, file.artifact.Index); err != nil {
		return errExportSink
	}
	w.current = nil
	return w.progress()
}

// finish is called only after the complete source envelope and response close succeed.
func (w *exportSink) finish() error {
	if w.current == nil && w.files == 0 {
		if err := w.beginFile(); err != nil {
			return err
		}
	}
	return w.completeFile()
}

func (w *exportSink) progress() error {
	_, err := w.s.db.ExecContext(w.ctx, `UPDATE export_jobs SET rows=?,bytes=?,decoded_bytes=?,file_count=? WHERE id=? AND attempt_id=? AND state='running'`, w.rows, w.compressed, w.decoded, w.files, w.job.ID, w.job.AttemptID)
	w.lastProgress = time.Now()
	if err != nil {
		return errExportSink
	}
	return nil
}
