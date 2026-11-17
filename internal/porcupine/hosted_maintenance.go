package porcupine

import (
	"context"
	"errors"
	"log/slog"
	"time"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

type hostedMaintenance struct {
	stop context.CancelFunc
	done chan struct{}
}

func (s *Service) startHostedMaintenance(interval time.Duration) *hostedMaintenance {
	h := s.hosted
	ctx, stop := context.WithCancel(context.Background())
	worker := &hostedMaintenance{stop: stop, done: make(chan struct{})}
	go func() {
		defer close(worker.done)
		ticker := time.NewTicker(max(interval, 5*time.Second))
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := h.native.CompactHostedDue(ctx, s.clock(), h.flusher.coordinator, s.compactHostedProject); err != nil && ctx.Err() == nil {
					slog.Warn("hosted project compaction deferred")
				}
			}
		}
	}()
	return worker
}

func (s *Service) compactHostedProject(ctx context.Context, session *lakeapi.ProjectWriteSession) (stats lakeapi.DuckLakeMaintenanceStats, err error) {
	h := s.hosted
	err = h.flusher.runActiveHostedProject(ctx, session, session.Fence("").Key.ProjectID, func(ctx context.Context) error {
		stats, err = h.native.MaintainHostedProject(ctx, session)
		return err
	})
	if errors.Is(err, errHostedProjectFenced) {
		err = nil
	}
	return stats, err
}
