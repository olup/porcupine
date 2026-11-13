//go:build ducklake

package porcupine

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Stage the same 3,000 events per window, then vary the worker's row/byte caps.
// Production defaults remain 100 events and 4 MiB.
func BenchmarkDuckLakeBatchWindows(b *testing.B) {
	cases := []struct {
		name                  string
		rows, bytes, memoryMB int
	}{
		{"max100", 100, 4 << 20, 256}, {"max500", 500, 4 << 20, 256},
		{"max1000", 1000, 4 << 20, 256}, {"max3000", 3000, 4 << 20, 256},
		{"max3000bytes32MiB", 3000, 32 << 20, 512},
	}
	for _, candidate := range cases {
		b.Run(candidate.name, func(b *testing.B) {
			if b.N > 30 {
				b.Fatal("use a fixed iteration count of at most 30")
			}
			s, project, token := comparisonServiceMemory(b, "ducklake", candidate.memoryMB)
			s.deliveryMaxEvents = candidate.rows
			s.deliveryMaxBytes = candidate.bytes
			s.queueMax = 10000
			handler := s.Handler()
			comparisonQuery(b, handler, project, "SELECT count(*) AS total FROM events")
			payloads := comparisonPayloads(b.N * 30)
			chunks, largest := 0, 0
			var longest time.Duration
			slow := 0
			b.ResetTimer()
			for window := range b.N {
				started := time.Now()
				for _, payload := range payloads[window*30 : (window+1)*30] {
					comparisonAccept(b, handler, token, payload)
				}
				pending := 3000
				for pending > 0 {
					if err := s.flushDelivery(context.Background()); err != nil {
						b.Fatal(err)
					}
					var remaining int
					if err := s.db.QueryRow("SELECT count(*) FROM events").Scan(&remaining); err != nil {
						b.Fatal(err)
					}
					if remaining >= pending {
						b.Fatal("delivery stopped progressing")
					}
					largest = max(largest, pending-remaining)
					pending = remaining
					chunks++
				}
				elapsed := time.Since(started)
				longest = max(longest, elapsed)
				if elapsed > time.Second {
					slow++
				}
			}
			b.StopTimer()
			b.ReportMetric(3000, "events/op")
			b.ReportMetric(float64(b.N*3000)/b.Elapsed().Seconds(), "events/s")
			b.ReportMetric(float64(chunks)/float64(b.N), "chunks/window")
			b.ReportMetric(float64(largest), "max_events/chunk")
			b.ReportMetric(float64(longest)/float64(time.Millisecond), "max_window_ms")
			b.ReportMetric(float64(slow), "windows_over_1s")
			result := comparisonQuery(b, handler, project, "SELECT count(*) AS total FROM events")
			if len(result.Data) != 1 || string(result.Data[0]["total"]) != fmt.Sprintf("\"%d\"", b.N*3000) {
				b.Fatal("incorrect final analytical count")
			}
		})
	}
}
