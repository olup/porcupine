package lake

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestHostedListEventsDeadline(t *testing.T) {
	t.Parallel()
	reader := HostedReader{store: &DuckLakeStore{}}
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()

	_, err := reader.ListEvents(ctx, "project", 10, 1024)
	if queryStatus(err) != http.StatusTooManyRequests {
		t.Fatalf("expired event list returned %v, want a typed time-limit error", err)
	}
}
