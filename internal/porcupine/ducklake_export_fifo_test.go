//go:build ducklake && (linux || darwin)

package porcupine

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestDuckLakeCopyFIFOProbe(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 1)
	p, release, err := store.leaseProject(context.Background(), "fifo-probe")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	conn, err := p.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var bytes int64
	err = copyDuckLakeExport(ctx, conn, p.directory, "SELECT i, CAST(i AS VARCHAR) AS v FROM range(10000) r(i)", nil, func(reader io.Reader) error { var err error; bytes, err = io.Copy(io.Discard, reader); return err })
	if err != nil || bytes == 0 {
		t.Fatalf("native COPY stream: bytes=%d err=%v", bytes, err)
	}
	t.Logf("native COPY streamed %d bytes", bytes)
}

func TestDuckLakeCopyFIFOBlockedWriterCancellationProbe(t *testing.T) {
	store := openLakeStorageTest(t, t.TempDir(), 1)
	p, release, err := store.leaseProject(context.Background(), "fifo-cancel")
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	conn, err := p.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- copyDuckLakeExport(ctx, conn, p.directory, "SELECT i, repeat('x',4096) AS v FROM range(10000000) r(i)", nil, func(reader io.Reader) error {
			if _, err := io.ReadFull(reader, make([]byte, 32<<10)); err != nil {
				return err
			}
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-started:
	case err := <-done:
		t.Fatalf("COPY stopped before backpressure: %v", err)
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("COPY did not produce prefix")
	}
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unexpectedly completed enormous unread export")
		}
		t.Logf("blocked writer cancelled in %s: %v", time.Since(start), err)
	case <-time.After(3 * time.Second):
		t.Fatal("blocked native writer did not stop")
	}
	var n int
	if err = p.db.QueryRow("SELECT 1").Scan(&n); err != nil || n != 1 {
		t.Fatalf("connection after cancelled COPY: %d %v", n, err)
	}
}
