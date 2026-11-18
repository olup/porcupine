package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/output"
	"github.com/olup/porcupine/internal/usage"
)

type hostedRuntime struct {
	maintenance   *hostedMaintenance
	native        *lakeapi.DuckLakeStore
	flusher       *hostedLiveFlusher
	buffer        *bufferedAcceptance
	stopBuffer    context.CancelFunc
	stopRecovery  context.CancelFunc
	recoveryDone  chan struct{}
	scratch       string
	recoveryFloor HostedRecoveryFloor
	nativeGrace   time.Duration
}

// openHostedProfile binds the shared providers before starting background work.
func openHostedProfile(c Config) (_ *Service, err error) {
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	config, err := normalizeInteractiveQueries(c.InteractiveQueries)
	if err != nil {
		return nil, err
	}
	s, err := newServiceRuntime(c, config)
	if err != nil {
		return nil, err
	}
	h := &hostedRuntime{recoveryFloor: c.HostedRecoveryFloor, nativeGrace: hostedNativeReturnGrace}
	s.hosted = h
	s.shared = &sharedRuntime{workerID: eventapi.NewID()}
	control, err := openSharedControl(ctx, *c.Shared)
	if err != nil {
		return nil, err
	}
	s.control = control
	defer func() {
		if err != nil {
			err = errors.Join(err, s.closeHosted())
		}
	}()
	if err := initializeSharedControl(ctx, control, c.BootstrapToken, config, s.externalAuthEnabled()); err != nil {
		return nil, err
	}
	lakeConfig := *c.DuckLake
	lakeConfig.Hosted = true
	if err := control.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='shared_source_id'").Scan(&lakeConfig.SharedSourceID); err != nil {
		return nil, errors.New("hosted analytical source identity unavailable")
	}
	if lakeConfig.Directory == "" {
		h.scratch, err = os.MkdirTemp("", "porcupine-ducklake-")
		if err != nil {
			return nil, err
		}
		lakeConfig.Directory = h.scratch
	}
	if err := s.initializeDuckLake(lakeConfig); err != nil {
		return nil, err
	}
	h.native = s.lake.(*lakeapi.DuckLakeStore)
	if err := s.initializeOAuthClients(ctx); err != nil {
		return nil, err
	}
	if err := s.initializeOAuth(ctx); err != nil {
		return nil, err
	}
	if err := s.initializeUsage(ctx, c.Usage); err != nil {
		return nil, err
	}
	if err := s.initializeHostedQueries(h.native, registerHostedProjectActivityTx, finishHostedProjectActivity); err != nil {
		return nil, err
	}
	coordinator, err := lakeapi.NewPostgresProjectCoordinator(control, lakeapi.DefaultProjectCoordinatorConfig())
	if err != nil {
		return nil, err
	}
	h.flusher, err = newHostedLiveFlusher(control, h.native, coordinator, s.usage.store)
	if err != nil {
		return nil, err
	}
	s.imports = newHostedImportManager(s.eventRules, h.flusher)
	s.hostedMutations = h.flusher
	s.sealHostedProject = h.flusher.sealProject
	endActivity := h.flusher.finishProjectActivity
	if err := s.configureHostedPipelines(c.Pipelines, h.flusher, hostedPipelineExecution{
		snapshotCut: h.exportSnapshotCut, deleteGate: hostedOutputBlocked, beginAttemptTx: registerHostedProjectActivityTx, endAttempt: endActivity,
	}); err != nil {
		return nil, err
	}
	h.flusher.publisher.cancelMutationWebhook = h.flusher.publisher.effects.CancelHostedWebhookEventTx
	if err := s.pipelines.Initialize(ctx); err != nil {
		return nil, err
	}
	if err := s.configureHostedExports(c.Exports, registerHostedProjectActivityTx, endActivity, h.exportSnapshotCut); err != nil {
		return nil, err
	}
	buffer := defaultBufferConfig()
	buffer.clock = s.clock
	if c.QueueMaxEvents > 0 {
		buffer.maxEvents = c.QueueMaxEvents
		buffer.projectMaxEvents = min(buffer.projectMaxEvents, buffer.maxEvents)
	}
	if c.QueueMaxBytes > 0 {
		buffer.maxBytes = c.QueueMaxBytes
		buffer.projectMaxBytes = min(buffer.projectMaxBytes, buffer.maxBytes)
		buffer.flushBytes = min(buffer.flushBytes, buffer.projectMaxBytes)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	h.stopBuffer = cancel
	h.buffer, err = newBufferedAcceptance(lifetime, buffer, h.flusher, hostedCookielessKeys{control})
	if err != nil {
		return nil, err
	}
	s.acceptance = h.buffer
	s.startDeletionRecovery()
	s.startHostedRecovery(lakeConfig.MaintenanceInterval)
	h.maintenance = s.startHostedMaintenance(lakeConfig.MaintenanceInterval)
	s.pipelines.Start()
	s.exports.Start()
	s.startUsage()
	return s, nil
}

func hostedOutputBlocked(ctx context.Context, tx *sql.Tx, scope lakeapi.HostedProjectScope, tenant string) (bool, error) {
	owner, err := lockActiveHostedProject(ctx, tx, scope.Key.ProjectID)
	if errors.Is(err, errHostedProjectFenced) {
		return true, nil
	}
	if err != nil || owner != tenant {
		return true, err
	}
	return hostedReadyDelete(ctx, tx, scope, tenant)
}

func (h *hostedRuntime) exportSnapshotCut(ctx context.Context, project string, publish func(context.Context, output.ExportControlTx) error) error {
	return h.flusher.withHostedMutationCut(ctx, project, func(ctx context.Context, session *lakeapi.ProjectWriteSession, _ lakeapi.HostedProjectScope, _ string, _ int64) error {
		return publish(ctx, session.RunControlTx)
	})
}

func (s *Service) initializeHostedProject(ctx context.Context, project string) error {
	h := s.hosted
	key := lakeapi.ProjectWriteKey{SourceID: h.native.Config.SharedSourceID, ProjectID: project}
	session, err := h.flusher.coordinator.Acquire(ctx, key)
	if err != nil {
		return err
	}
	defer session.Release()
	return h.flusher.runActiveHostedProject(ctx, session, project, func(ctx context.Context) error {
		if err := s.requireProjectActive(ctx, project); err != nil {
			return err
		}
		_, err := h.native.InitializeHostedProject(ctx, key)
		return err
	})
}

func (s *Service) startHostedRecovery(interval time.Duration) {
	if interval == 0 {
		interval = 2 * time.Second
	}
	h := s.hosted
	ctx, stop := context.WithCancel(context.Background())
	h.stopRecovery, h.recoveryDone = stop, make(chan struct{})
	go func() {
		defer close(h.recoveryDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		var after string
		for {
			work, cancel := context.WithTimeout(ctx, 30*time.Second)
			h.flusher.flushReturnedProjectActivities(work)
			records, err := h.native.ScanHostedProjects(work, after, 32)
			if err == nil {
				if len(records) == 0 {
					after = ""
				}
				for _, record := range records {
					after = record.Key.ProjectID
					if !record.Ready || record.Deleted || s.requireProjectActive(work, after) != nil {
						continue
					}
					if err := h.flusher.RecoverProject(work, record.Key); err != nil && work.Err() == nil {
						slog.Warn("hosted project reconciliation pending", "project_id", after)
					}
				}
			}
			cancel()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// Shutdown bounds. The drain deadline covers the flush of accepted batches.
// The return grace then covers canceled analytical calls and maintenance.
const (
	hostedDrainTimeout      = 30 * time.Second
	hostedNativeReturnGrace = 5 * time.Second
	hostedNativeReturnPoll  = 20 * time.Millisecond
)

func (s *Service) closeHosted() error {
	h := s.hosted
	// Admission stops and analytical work is canceled without waiting for
	// native calls to return, so a stuck call cannot delay the buffer drain.
	s.BeginDrain()
	var drainErr error
	if h.buffer != nil {
		ctx, stop := context.WithTimeout(context.Background(), hostedDrainTimeout)
		drainErr = h.buffer.Drain(ctx)
		stop()
	}
	if h.stopBuffer != nil {
		h.stopBuffer()
	}
	if err := errors.Join(drainErr, s.awaitHostedNativeReturn(h.nativeGrace)); err != nil {
		// In-flight flushes, analytics or maintenance still own native
		// connections, and the permit keeper still needs PostgreSQL. The server
		// process exits after reporting the incomplete close.
		return err
	}
	if h.stopRecovery != nil {
		h.stopRecovery()
		<-h.recoveryDone
	}
	if s.deletionCancel != nil {
		s.deletionCancel()
		<-s.deletionDone
	}
	if s.pipelines != nil {
		s.pipelines.Close()
	}
	if s.exports != nil {
		s.exports.Close()
	}
	if s.usage != nil && s.usage.done != nil {
		s.stopUsage()
	}
	var lakeErr error
	if s.lake != nil {
		lakeErr = s.lake.Close()
	}
	var cleanupErr error
	if h.scratch != "" {
		cleanupErr = os.RemoveAll(h.scratch)
	}
	return errors.Join(lakeErr, s.control.Close(), cleanupErr)
}

// awaitHostedNativeReturn waits, up to grace, for canceled analytical calls and
// maintenance to return. Calls that ignore cancellation are reported, not
// waited for: their permits keep renewing until the process exits.
func (s *Service) awaitHostedNativeReturn(grace time.Duration) error {
	var maintenance <-chan struct{}
	if s.hosted.maintenance != nil {
		maintenance = s.hosted.maintenance.done
	}
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	poll := time.NewTicker(hostedNativeReturnPoll)
	defer poll.Stop()
	for {
		running := false
		if maintenance != nil {
			select {
			case <-maintenance:
			default:
				running = true
			}
		}
		active := s.sharedQueries.status().Active
		if !running && active == 0 {
			return nil
		}
		select {
		case <-deadline.C:
			var err error
			if running {
				err = errors.New("hosted maintenance is still running")
			}
			if active != 0 {
				err = errors.Join(err, fmt.Errorf("hosted analytics execution is still active (%d calls); permit renewal continues until they return", active))
			}
			return err
		case <-poll.C:
		}
	}
}

func (s *Service) initializeHostedUsage(ctx context.Context, c *usage.Config) error {
	config, err := usage.Normalize(c)
	if err != nil {
		return err
	}
	if _, err = usageMetadata(ctx, s.control, "usage_epoch", usage.NewID()); err != nil {
		return err
	}
	if _, err = usageMetadata(ctx, s.control, "usage_available_since", usage.Stamp(s.clock())); err != nil {
		return err
	}
	s.usage = &usageRuntime{store: usage.NewStore(s.control, true, config, s.clock), config: config, clock: s.clock}
	return nil
}

func (s *Service) BeginDrain() {
	s.BeginQueryDrain()
	if s.hosted != nil && s.hosted.maintenance != nil {
		s.hosted.maintenance.stop()
	}
	if s.hosted != nil && s.hosted.buffer != nil {
		s.hosted.buffer.stopAdmission()
	}
}
