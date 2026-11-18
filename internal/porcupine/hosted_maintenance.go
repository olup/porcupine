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

// Each tick runs at most one compaction and one bounded history cleanup pass,
// each for one claimed project under the coordinator session and write fence.
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
				if err := h.native.CleanupHostedDue(ctx, s.clock(), h.flusher.coordinator, s.cleanupHostedHistory); err != nil && ctx.Err() == nil {
					slog.Warn("hosted history cleanup deferred")
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

// cleanupHostedHistory reads the operator's recovery floor for every pass. A
// missing, unreadable or future floor deletes nothing.
func (s *Service) cleanupHostedHistory(ctx context.Context, session *lakeapi.ProjectWriteSession, now time.Time) (stats lakeapi.HostedCleanupStats, err error) {
	h := s.hosted
	if h.recoveryFloor == nil {
		return stats, lakeapi.ErrHostedCleanupGuard
	}
	floor, err := h.recoveryFloor(ctx)
	if err != nil {
		return stats, errors.Join(lakeapi.ErrHostedCleanupGuard, err)
	}
	err = h.flusher.runActiveHostedProject(ctx, session, session.Fence("").Key.ProjectID, func(ctx context.Context) error {
		stats, err = h.native.CleanupHostedProject(ctx, session, now, floor)
		return err
	})
	if errors.Is(err, errHostedProjectFenced) {
		err = nil
	}
	return stats, err
}
