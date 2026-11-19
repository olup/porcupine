package output

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/olup/porcupine/internal/delivery"
	"github.com/olup/porcupine/internal/exportstore"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"golang.org/x/time/rate"
)

var (
	errExportOutputLimit = errors.New("export output limit")
	errExportSink        = errors.New("export object write failed")
)

const maxOutputFiles = 10000

// outputSink writes a stream of rows into bounded gzip files. The caller owns
// the durable checkpoints that enclose starting and completing each upload.
type outputSink struct {
	ctx                       context.Context
	config                    ExportConfig
	format, prefix            string
	maxFiles                  int
	writeEmptyFile            bool
	limitCompressed           bool
	beginUpload               func(int, string) (*exportstore.Upload, error)
	completeUpload            func(*outputFile) error
	progress                  func() error
	encoder                   *delivery.RowEncoder
	columns                   []lakeapi.AnalyticsResultColumn
	limiter                   *rate.Limiter
	current                   *outputFile
	rows, decoded, compressed int64
	files                     int
}

type outputFile struct {
	index                int
	key, sha256          string
	rows, bytes, decoded int64
	upload               *exportstore.Upload
	gzip                 *gzip.Writer
	digest               hash.Hash
}

func newOutputSink(ctx context.Context, config ExportConfig, format, prefix string) *outputSink {
	return &outputSink{ctx: ctx, config: config, format: format, prefix: prefix, maxFiles: maxOutputFiles,
		limiter: rate.NewLimiter(rate.Limit(config.BandwidthBytesPerSecond), 64<<10)}
}

func (w *outputSink) setColumns(columns []lakeapi.AnalyticsResultColumn) error {
	if w.encoder != nil {
		return errExportSink
	}
	names := make([]string, len(columns))
	for i, column := range columns {
		names[i] = column.Name
	}
	encoder, err := delivery.NewRowEncoder(w.format, names)
	if err != nil {
		return errExportSink
	}
	w.encoder = encoder
	w.columns = append([]lakeapi.AnalyticsResultColumn{}, columns...)
	return nil
}

func (w *outputSink) writeRow(values []json.RawMessage) error {
	if err := w.ctx.Err(); err != nil {
		return err
	}
	row, err := w.encoder.Encode(values)
	if err != nil {
		return err
	}
	if w.current != nil && w.current.rows > 0 && w.current.decoded+int64(len(row)) > w.config.FileBytes {
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
	w.current.rows++
	w.rows++
	return nil
}

func (w *outputSink) beginFile() error {
	if w.files >= w.maxFiles {
		return errExportOutputLimit
	}
	// An export can consist of just a CSV header. Check its budget before
	// making the upload checkpoint in that case.
	if w.writeEmptyFile && int64(len(w.encoder.Header())) > w.config.MaxOutputBytes-w.decoded {
		return errExportOutputLimit
	}
	key := fmt.Sprintf("%spart-%06d.%s.gz", w.prefix, w.files+1, w.format)
	upload, err := w.beginUpload(w.files+1, key)
	if err != nil {
		return err
	}
	w.files++
	w.current = &outputFile{index: w.files, key: key, upload: upload, digest: sha256.New()}
	w.current.gzip, err = gzip.NewWriterLevel((*compressedOutput)(w), gzip.BestSpeed)
	if err != nil {
		return err
	}
	if header := w.encoder.Header(); len(header) > 0 {
		return w.writeDecoded(header)
	}
	return nil
}

func (w *outputSink) writeDecoded(data []byte) error {
	if int64(len(data)) > w.config.MaxOutputBytes-w.decoded {
		return errExportOutputLimit
	}
	n, err := w.current.gzip.Write(data)
	w.decoded += int64(n)
	w.current.decoded += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

type compressedOutput outputSink

func (w *compressedOutput) Write(data []byte) (int, error) {
	if w.limitCompressed && int64(len(data)) > w.config.MaxOutputBytes-w.compressed {
		return 0, errExportOutputLimit
	}
	written := 0
	for len(data) > 0 {
		size := min(len(data), w.limiter.Burst())
		if err := w.limiter.WaitN(w.ctx, size); err != nil {
			return written, err
		}
		n, err := w.current.upload.Write(data[:size])
		if n > 0 {
			w.current.digest.Write(data[:n])
			w.current.bytes += int64(n)
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

func (w *outputSink) completeFile() error {
	file := w.current
	if file == nil {
		return nil
	}
	if err := file.gzip.Close(); err != nil {
		return err
	}
	file.sha256 = hex.EncodeToString(file.digest.Sum(nil))
	if err := w.completeUpload(file); err != nil {
		return err
	}
	w.current = nil
	if w.progress != nil {
		return w.progress()
	}
	return nil
}

func (w *outputSink) finish() error {
	if w.writeEmptyFile && w.current == nil && w.files == 0 {
		if err := w.beginFile(); err != nil {
			return err
		}
	}
	return w.completeFile()
}
