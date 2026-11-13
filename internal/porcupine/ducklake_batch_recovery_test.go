//go:build ducklake

package porcupine

import (
	"context"
	"strings"
	"testing"
)

func TestDuckLakeLargeBatchFailureRecovery(t *testing.T) {
	s, project, token := comparisonServiceMemory(t, "ducklake", 128)
	s.queueMax = 10000
	s.deliveryMaxEvents, s.deliveryMaxBytes = 3000, 32<<20
	handler := s.Handler()
	comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	payloads := comparisonPayloads(30)
	for _, payload := range payloads {
		comparisonAccept(t, handler, token, payload)
	}
	err := s.flushDelivery(context.Background())
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "out of memory") {
		t.Fatalf("expected bounded native allocation failure, got %v", err)
	}
	count, _ := queueUsage(t, s)
	if count != 3000 {
		t.Fatalf("failed analytical write lost queued events: %d", count)
	}
	result := comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"0"` {
		t.Fatal("failed batch partially committed")
	}
	s.deliveryMaxEvents, s.deliveryMaxBytes = 100, 4<<20
	for attempts := 0; count > 0 && attempts < 30; attempts++ {
		if err := s.flushDelivery(context.Background()); err != nil {
			t.Fatal(err)
		}
		count, _ = queueUsage(t, s)
	}
	if count != 0 {
		t.Fatal("smaller batches did not drain the accepted events")
	}
	comparisonAccept(t, handler, token, payloads[0])
	result = comparisonQuery(t, handler, project, "SELECT count(*) AS total FROM events")
	if len(result.Data) != 1 || string(result.Data[0]["total"]) != `"3000"` {
		t.Fatal("recovery or retry changed the analytical count")
	}
	count, _ = queueUsage(t, s)
	if count != 0 {
		t.Fatal("retry requeued already projected events")
	}
}
