package acceptance

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Measures memory admission and cloning only. A no-op writer is attached but
// thresholds keep it idle; pending fixtures are discarded outside the timer.
// This does not measure durable commits, hosted throughput, S3, or retained RSS.
func BenchmarkBufferedMemoryAdmission(b *testing.B) {
	for _, n := range []int{1, 100, 1000} {
		for _, projects := range []int{1, 128} {
			b.Run(fmt.Sprintf("rows=%d/projects=%d", n, projects), func(b *testing.B) {
				c := testBufferConfig()
				c.MaxBytes = 256 << 20
				c.ProjectMaxBytes = c.MaxBytes
				c.FlushBytes = c.MaxBytes
				c.MaxEvents = 1000000
				c.ProjectMaxEvents = c.MaxEvents
				c.FlushInterval = time.Hour
				buffer := newTestBuffer(b, c, FlushFunc(func(context.Context, FlushBatch) error { return nil }))
				batches := make([]Batch, projects)
				for i := range batches {
					batches[i] = testBufferBatch(fmt.Sprintf("project-%03d", i), n)
				}
				charge := bufferedInputBytes(batches[0].Events[0])
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if i%128 == 0 {
						b.StopTimer()
						buffer.mu.Lock()
						clear(buffer.projects)
						buffer.bytes = 0
						buffer.events = 0
						buffer.mu.Unlock()
						b.StartTimer()
					}
					if _, err := buffer.Accept(b.Context(), batches[i%projects], Peer{}, true); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(n), "rows/op")
				b.ReportMetric(float64(charge), "charged-bytes/row")
			})
		}
	}
}
