//go:build ducklake && (linux || darwin)

package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type duckLakePipeReader struct {
	ctx  context.Context
	file *os.File
}

func (r duckLakePipeReader) Read(buffer []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		n, err := r.file.Read(buffer)
		if n > 0 || !errors.Is(err, syscall.EAGAIN) {
			return n, err
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-r.ctx.Done():
			timer.Stop()
			return 0, r.ctx.Err()
		case <-timer.C:
		}
	}
}

// COPY streams directly into an operator-owned FIFO. No SQL result or full
// export spool is retained; closing the descriptors unblocks a failed writer.
func copyDuckLakeExport(parent context.Context, conn *sql.Conn, directory, canonical string, args []any, consume func(io.Reader) error) error {
	if err := os.MkdirAll(filepath.Join(directory, "temp"), 0700); err != nil {
		return err
	}
	directory, err := os.MkdirTemp(filepath.Join(directory, "temp"), "export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "rows.jsonl")
	if err = syscall.Mkfifo(path, 0600); err != nil {
		return err
	}
	anchor, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	defer anchor.Close()
	reader, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	defer reader.Close()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, copyErr := conn.ExecContext(ctx, "COPY ("+canonical+") TO "+lakeLiteral(path)+" (FORMAT JSON, ARRAY false, COMPRESSION uncompressed)", args...)
		anchor.Close()
		done <- copyErr
	}()
	readErr := consume(duckLakePipeReader{ctx: ctx, file: reader})
	if readErr != nil {
		cancel()
		// Keep a reader present until COPY exits: cancelling immediately before
		// its blocking FIFO open must not leave the writer waiting for a reader.
		_, _ = io.Copy(io.Discard, duckLakePipeReader{ctx: context.Background(), file: reader})
	}
	copyErr := <-done
	if parent.Err() != nil {
		return parent.Err()
	}
	if readErr != nil {
		return readErr
	}
	if copyErr != nil {
		return copyErr
	}
	return nil
}
