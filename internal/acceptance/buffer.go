package acceptance

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/identity"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
	"github.com/olup/porcupine/internal/observability"
)

// FlushFunc receives validated commands. The writer must retain the batch ID
// across retries, deduplicate under the project write guard, and recheck
// tenant and project deletion fences. Success requires a durable commit
// with recoverable effect evidence.
// A transient error leaves ownership and capacity with the buffer, including
// after a commit whose response was lost. A durable deletion fence discards
// pending input for that project. The writer must not retain or mutate the batch.
type FlushFunc func(context.Context, FlushBatch) error

// ErrBatchTooLarge requests a split and proves the batch never reached native commit.
var ErrBatchTooLarge = errors.New("hosted live batch exceeds output limits")

// ErrProjectFenced marks a durable deletion fence for hosted writes.
var ErrProjectFenced = errors.New("hosted project is fenced")

// ErrProjectQuarantined signals project-scoped backpressure during hosted recovery.
var ErrProjectQuarantined = &Failure{Message: "project ingestion is quarantined", Retry: true, Pressure: true}

type BufferedEvent struct {
	Input      Input
	AdmittedAt time.Time
	requestEnd bool
}
type FlushBatch struct {
	ID, ProjectID string
	Events        []BufferedEvent
	oldest        time.Time
	bytes         int64
}

// Keep live IDs above historical UUIDs and import IDs so DuckLake can skip
// older proof files by operation_id, including files with legacy IDs.
func newLiveBatchID() string {
	return "live-" + uuid.Must(uuid.NewV7()).String()
}

func (b FlushBatch) CanSplit() bool {
	for _, event := range b.Events[:max(0, len(b.Events)-1)] {
		if event.requestEnd {
			return true
		}
	}
	return false
}

// DailyKeyFunc resolves durable shared daily keys. The buffer does not retain
// raw peer addresses, user agents, or daily secrets after admission.
type DailyKeyFunc func(context.Context, string) ([]byte, error)

type BufferConfig struct {
	MaxBytes, ProjectMaxBytes, FlushBytes                int64
	MaxEvents, ProjectMaxEvents, MaxProjects, MaxFlushes int
	FlushInterval, RetryMin, RetryMax                    time.Duration
	Clock                                                func() time.Time
	Metrics                                              *observability.Registry
	// ClassifyFlushError returns a quarantine code or a code for a request rejected before commit.
	ClassifyFlushError func(error) (projectCode, requestCode string)
	// FlushRetryReason returns a fixed metric label for an error the buffer will retry.
	FlushRetryReason func(error) string
}

func DefaultBufferConfig() BufferConfig {
	return BufferConfig{MaxBytes: 64 << 20, ProjectMaxBytes: 8 << 20, FlushBytes: 5 << 20,
		MaxEvents: 100000, ProjectMaxEvents: 10000, MaxProjects: 1024, MaxFlushes: 4,
		FlushInterval: 2 * time.Second, RetryMin: 100 * time.Millisecond, RetryMax: 5 * time.Second, Clock: time.Now}
}
func (c BufferConfig) validate() error {
	if c.MaxBytes <= 0 || c.ProjectMaxBytes <= 0 || c.FlushBytes <= 0 || c.MaxEvents <= 0 || c.ProjectMaxEvents <= 0 || c.MaxProjects <= 0 || c.MaxFlushes <= 0 || c.FlushInterval <= 0 || c.RetryMin <= 0 || c.RetryMax < c.RetryMin || c.Clock == nil {
		return errors.New("buffer limits, flush interval, retry bounds and clock must be configured")
	}
	if c.ProjectMaxBytes > c.MaxBytes || c.ProjectMaxEvents > c.MaxEvents || c.FlushBytes > c.ProjectMaxBytes {
		return errors.New("project buffer limits and flush threshold exceed their enclosing capacity")
	}
	return nil
}

type bufferedProject struct {
	pending      []BufferedEvent
	pendingBytes int64
	oldest       time.Time
	ready        []*FlushBatch
	running      bool
	retryDelay   time.Duration
	retryAt      time.Time
	quarantine   string
}
type BufferSnapshot struct {
	Bytes                          int64
	Events, projects, Inflight     int
	Oldest                         time.Time
	draining, failed, Stopped      bool
	CapacityEvents                 int
	CapacityBytes                  int64
	abandoned                      int
	DroppedRequests, DroppedEvents uint64
	QuarantinedProjects            map[string]string
}
type Buffer struct {
	mu                             sync.Mutex
	config                         BufferConfig
	writer                         FlushFunc
	journal                        *journal
	keys                           DailyKeyFunc
	lockAdmission                  func(context.Context, string) (func(), error)
	projects                       map[string]*bufferedProject
	bytes                          int64
	events, running                int
	droppedRequests, droppedEvents uint64
	draining                       bool
	stopped                        bool
	notify                         chan struct{}
	completed                      chan flushCompletion
	done                           chan struct{}
	ctx                            context.Context
}
type flushCompletion struct {
	project string
	id      string
	err     error
	elapsed time.Duration
}

// The byte budget charges owned payloads plus conservative allowances for structs,
// allocator metadata and slice capacity. It is not a hard process-RSS limit.
const bufferedEventOverhead int64 = 1024
const bufferedProjectOverhead int64 = 512

func NewBuffer(ctx context.Context, c BufferConfig, writer FlushFunc, lockAdmission func(context.Context, string) (func(), error), keys DailyKeyFunc) (*Buffer, error) {
	return newBuffer(ctx, c, writer, lockAdmission, keys, nil, ctx)
}

// NewJournalBuffer takes ownership of db on success. The caller initializes
// the journal, passes the release for its exclusive writer lock, and closes db on failure.
func NewJournalBuffer(startup, ctx context.Context, c BufferConfig, writer FlushFunc, lockAdmission func(context.Context, string) (func(), error), keys DailyKeyFunc, db *sql.DB, unlock func()) (*Buffer, error) {
	return newBuffer(ctx, c, writer, lockAdmission, keys, &journal{db: db, unlock: unlock}, startup)
}

func newBuffer(ctx context.Context, c BufferConfig, writer FlushFunc, lockAdmission func(context.Context, string) (func(), error), keys DailyKeyFunc, journal *journal, startup context.Context) (*Buffer, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if writer == nil {
		return nil, errors.New("buffered acceptance requires a durable batch writer")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := &Buffer{
		config: c, writer: writer, journal: journal, lockAdmission: lockAdmission, keys: keys, ctx: ctx,
		projects: make(map[string]*bufferedProject), notify: make(chan struct{}, 1),
		completed: make(chan flushCompletion, c.MaxFlushes), done: make(chan struct{}),
	}
	if journal != nil {
		if err := journal.load(startup, b); err != nil {
			return nil, err
		}
	}
	go b.run()
	return b, nil
}
func (b *Buffer) Durability() Durability {
	if b.journal != nil {
		return Durable
	}
	return Buffered
}

// CheckJournal distinguishes a worker with no local journal from a failed probe.
func (b *Buffer) CheckJournal(ctx context.Context) (configured bool, err error) {
	if b.journal == nil {
		return false, nil
	}
	return true, b.journal.db.PingContext(ctx)
}

func (b *Buffer) wake() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
func (b *Buffer) Accept(ctx context.Context, batch Batch, peer Peer, withReceipts bool) (Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	_, err := b.projectForAdmissionLocked(batch.ProjectID)
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	admittedAt := b.config.Clock().UTC()
	// A fixed-size pseudonym lets capacity be checked before key-provider work.
	// The admission day is fixed even if key resolution crosses UTC midnight.
	day := admittedAt.Format("2006-01-02")
	cookielessID := ""
	if batch.Cookieless {
		cookielessID = "cookieless_" + strings.Repeat("0", 64)
	}
	inputs := make([]Input, len(batch.Events))
	var size int64
	for i, input := range batch.Events {
		if input.Event.ProjectID != batch.ProjectID || batch.ProjectID == "" || input.Event.ID != 0 || input.Event.Imported || input.Event.IdentitySnapshot != "" || identity.IsControl(input.Event.Event) != (input.Identity != nil) {
			return nil, errors.New("invalid validated live batch scope or state")
		}
		if input.Event.DistinctID == ingestfmt.CookielessSentinel && !identity.IsControl(input.Event.Event) {
			if cookielessID == "" {
				return nil, errors.New("unresolved cookieless input")
			}
			input.Event = ResolveCookieless(input.Event, cookielessID, day)
		}
		charge := bufferedInputBytes(input)
		if charge > b.config.MaxBytes-size {
			return nil, &Failure{Message: "buffer is full", Retry: true, Pressure: true}
		}
		size += charge
		inputs[i] = input
	}
	if len(inputs) == 0 {
		return nil, errors.New("empty validated live batch")
	}
	if batch.Cookieless {
		b.mu.Lock()
		_, _, err := b.checkCapacityLocked(batch.ProjectID, size, len(inputs))
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if b.keys == nil {
			return nil, &Failure{Message: "shared cookieless key provider unavailable", Retry: true}
		}
		secret, err := b.keys(ctx, day)
		if err != nil || len(secret) != 32 {
			return nil, &Failure{Message: "shared cookieless key unavailable", Retry: true}
		}
		cookielessID = CookielessDistinctID(secret, batch.ProjectID, peer.Address, peer.UserAgent)
		for i, original := range batch.Events {
			if original.Event.DistinctID == ingestfmt.CookielessSentinel && !identity.IsControl(original.Event.Event) {
				inputs[i].Event.DistinctID = cookielessID
				inputs[i].Event.Properties = CookielessProperties(original.Event.Properties, cookielessID, day)
			}
		}
	}
	if b.lockAdmission != nil {
		release, err := b.lockAdmission(ctx, batch.ProjectID)
		if err != nil {
			switch {
			case errors.Is(err, ErrProjectFenced):
				return nil, apierr.New(404, "project not found")
			case errors.Is(err, ErrProjectQuarantined):
				return nil, ErrProjectQuarantined
			}
			return nil, &Failure{Message: "project admission unavailable", Retry: true}
		}
		defer release()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, overhead, err := b.checkCapacityLocked(batch.ProjectID, size, len(inputs))
	if err != nil {
		return nil, err
	}
	owned := make([]BufferedEvent, len(inputs))
	var receipts []BufferedReceipt
	if withReceipts {
		receipts = make([]BufferedReceipt, len(inputs))
	}
	for i, input := range inputs {
		input = cloneValidatedLiveEvent(input)
		owned[i] = BufferedEvent{Input: input, AdmittedAt: admittedAt, requestEnd: i == len(inputs)-1}
		if withReceipts {
			receipts[i] = BufferedReceipt{MessageID: input.Event.MessageID, UUID: input.Event.UUID}
		}
	}
	if b.journal != nil {
		if err := b.journal.append(ctx, batch.ProjectID, owned); err != nil {
			return nil, &Failure{Message: "worker ingestion journal unavailable", Retry: true}
		}
	}
	if p == nil {
		p = &bufferedProject{}
		b.projects[strings.Clone(batch.ProjectID)] = p
	}
	if len(p.pending) == 0 {
		p.oldest = admittedAt
	}
	p.pending = append(p.pending, owned...)
	p.pendingBytes += size
	b.bytes += size + overhead
	b.events += len(inputs)
	b.wake()
	return BufferedResult{Received: len(inputs), Receipts: receipts, Journaled: b.journal != nil}, nil
}
func bufferedInputBytes(input Input) int64 {
	e := input.Event
	size := bufferedEventOverhead + int64(len(e.ProjectID)+len(e.UUID)+len(e.Event)+len(e.DistinctID)+len(e.Properties)+len(e.Timestamp)+len(e.ReceivedAt)+len(e.SourceID)+len(e.MessageID)+len(e.EventType)+len(e.Context))
	if c := input.Identity; c != nil {
		size += int64(len(c.Target) + len(c.Anonymous) + len(c.Set) + len(c.SetOnce))
	}
	return size
}
func cloneValidatedLiveEvent(input Input) Input {
	e := &input.Event
	e.ProjectID = strings.Clone(e.ProjectID)
	e.UUID = strings.Clone(e.UUID)
	e.Event = strings.Clone(e.Event)
	e.DistinctID = strings.Clone(e.DistinctID)
	e.Timestamp = strings.Clone(e.Timestamp)
	e.ReceivedAt = strings.Clone(e.ReceivedAt)
	e.SourceID = strings.Clone(e.SourceID)
	e.MessageID = strings.Clone(e.MessageID)
	e.EventType = strings.Clone(e.EventType)
	e.Properties = append([]byte(nil), e.Properties...)
	e.Context = append([]byte(nil), e.Context...)
	if input.Identity != nil {
		c := *input.Identity
		c.Target = strings.Clone(c.Target)
		c.Anonymous = strings.Clone(c.Anonymous)
		c.Set = append([]byte(nil), c.Set...)
		c.SetOnce = append([]byte(nil), c.SetOnce...)
		input.Identity = &c
	}
	return input
}
func (b *Buffer) Snapshot() BufferSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := BufferSnapshot{
		Bytes: b.bytes, Events: b.events, projects: len(b.projects), Inflight: b.running,
		Stopped: b.stopped, DroppedRequests: b.droppedRequests, DroppedEvents: b.droppedEvents,
		draining:       b.draining,
		CapacityEvents: b.config.MaxEvents, CapacityBytes: b.config.MaxBytes,
	}
	stopping := b.ctx.Err() != nil
	if stopping {
		s.abandoned = b.events
	}
	for project, p := range b.projects {
		if p.quarantine != "" {
			if s.QuarantinedProjects == nil {
				s.QuarantinedProjects = make(map[string]string)
			}
			s.QuarantinedProjects[project] = p.quarantine
		}
		s.failed = s.failed || p.retryDelay > 0
		if stopping && p.running {
			s.abandoned -= len(p.ready[0].Events)
		}
		oldest := p.oldest
		if len(p.ready) > 0 && !p.ready[0].oldest.IsZero() && (oldest.IsZero() || p.ready[0].oldest.Before(oldest)) {
			oldest = p.ready[0].oldest
		}
		if !oldest.IsZero() && (s.Oldest.IsZero() || oldest.Before(s.Oldest)) {
			s.Oldest = oldest
		}
	}
	return s
}
func (b *Buffer) Drain(ctx context.Context) error {
	b.StopAdmission()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		if b.Snapshot().Events != 0 {
			return errors.New("buffer stopped with uncommitted events")
		}
		return nil
	}
}
func (b *Buffer) run() {
	defer func() {
		if b.journal != nil {
			_ = b.journal.db.Close()
			b.journal.unlock()
		}
		b.mu.Lock()
		b.stopped = true
		b.mu.Unlock()
		close(b.done)
	}()
	// Leave room for new requests while a batch commits, including when a request crosses the trigger.
	flushEvents := max(1, b.config.ProjectMaxEvents/3)
	for {
		b.mu.Lock()
		stopping := b.ctx.Err() != nil
		if stopping {
			b.draining = true
		}
		now := b.config.Clock()
		wait := b.config.FlushInterval
		for project, p := range b.projects {
			if stopping || b.ctx.Err() != nil {
				stopping = true
				b.draining = true
				break
			}
			if p.running {
				continue
			}
			if now.Before(p.retryAt) {
				wait = min(wait, p.retryAt.Sub(now))
				continue
			}
			if len(p.ready) == 0 && len(p.pending) > 0 {
				due := p.oldest.Add(b.config.FlushInterval)
				if b.draining || p.pendingBytes >= b.config.FlushBytes || len(p.pending) >= flushEvents || !now.Before(due) {
					batch := &FlushBatch{ID: newLiveBatchID(), ProjectID: project, Events: p.pending, oldest: p.oldest, bytes: p.pendingBytes}
					if b.journal != nil {
						if err := b.journal.seal(b.ctx, batch); err != nil {
							b.retryJournal(p, "seal")
							wait = min(wait, p.retryDelay)
							continue
						}
					}
					p.ready = []*FlushBatch{batch}
					p.pending = nil
					p.pendingBytes = 0
					p.oldest = time.Time{}
				} else {
					wait = min(wait, due.Sub(now))
					continue
				}
			}
			if len(p.ready) == 0 {
				continue
			}
			if b.running >= b.config.MaxFlushes {
				continue
			}
			batch := *p.ready[0]
			p.running = true
			b.running++
			go func() {
				started := time.Now()
				err := b.writer(b.ctx, batch)
				b.completed <- flushCompletion{project: batch.ProjectID, id: batch.ID, err: err, elapsed: time.Since(started)}
			}()
		}
		drained := b.draining && b.events == 0 || stopping && b.running == 0
		b.mu.Unlock()
		if drained {
			return
		}
		if stopping {
			b.complete(<-b.completed)
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-b.ctx.Done():
			timer.Stop()
		case <-b.notify:
			timer.Stop()
		case completion := <-b.completed:
			timer.Stop()
			b.complete(completion)
		case <-timer.C:
		}
	}
}
func (b *Buffer) complete(result flushCompletion) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.projects[result.project]
	if p == nil || len(p.ready) == 0 || p.ready[0].ID != result.id || !p.running {
		return
	}
	p.running = false
	b.running--
	observe := func(outcome string) {
		if m := b.config.Metrics; m != nil {
			m.ObserveFlush(outcome, result.elapsed)
		}
	}
	if errors.Is(result.err, ErrProjectFenced) {
		if b.journal != nil {
			if err := b.journal.remove(p.ready[0], true); err != nil {
				b.retryJournal(p, "fence")
				return
			}
		}
		events := len(p.pending)
		b.bytes -= p.pendingBytes + bufferedProjectOverhead + int64(len(result.project))
		for _, batch := range p.ready {
			events += len(batch.Events)
			b.bytes -= batch.bytes
		}
		b.events -= events
		delete(b.projects, result.project)
		b.countDiscarded(events, "project_fenced")
		observe("dropped")
		return
	}
	projectCode, requestCode := "", ""
	if b.config.ClassifyFlushError != nil && b.ctx.Err() == nil {
		projectCode, requestCode = b.config.ClassifyFlushError(result.err)
	}
	if projectCode == "" && (requestCode != "" || errors.Is(result.err, ErrBatchTooLarge)) {
		if left, right, ok := splitBufferedLiveBatch(*p.ready[0]); ok {
			if b.journal != nil {
				if err := b.journal.split(b.ctx, result.id, left, right); err != nil {
					b.retryJournal(p, "split")
					return
				}
			}
			clear(p.ready[0].Events[len(left.Events):])
			observe("split")
			p.ready[0] = right
			p.ready = append([]*FlushBatch{left}, p.ready...)
			p.retryAt = time.Time{}
			return
		}
	}
	if b.journal != nil && requestCode != "" {
		// An acknowledged request that cannot be projected needs operator
		// attention. Keep its payload and isolate its project.
		projectCode, requestCode = requestCode, ""
	}
	if b.journal != nil && result.err == nil {
		if err := b.journal.remove(p.ready[0], false); err != nil {
			b.retryJournal(p, "acknowledge")
			return
		}
	}
	switch {
	case requestCode != "":
		events := len(p.ready[0].Events)
		b.droppedRequests++
		b.droppedEvents += uint64(events)
		b.countDiscarded(events, requestCode)
		slog.Error("hosted ingestion discarded an isolated request", "project_id", result.project, "batch_id", result.id, "reason", requestCode, "discarded_events", events)
		observe("dropped")
	case result.err == nil:
		p.quarantine = ""
		observe("ok")
	default:
		switch {
		case projectCode != "" && p.quarantine != projectCode:
			p.quarantine = projectCode
			if m := b.config.Metrics; m != nil {
				m.Add("porcupine_flush_quarantined_projects_total", 1, observability.Label{Name: "reason", Value: projectCode})
			}
			slog.Error("hosted ingestion quarantined project", "project_id", result.project, "batch_id", result.id, "reason", projectCode)
			observe("quarantined")
		case b.ctx.Err() != nil:
			observe("cancelled")
		default:
			observe("retry")
			if m := b.config.Metrics; m != nil && b.config.FlushRetryReason != nil {
				m.Add("porcupine_flush_retries_total", 1, observability.Label{Name: "reason", Value: b.config.FlushRetryReason(result.err)})
			}
		}
		b.retry(p)
		return
	}
	b.bytes -= p.ready[0].bytes
	b.events -= len(p.ready[0].Events)
	p.ready[0] = nil
	p.ready = p.ready[1:]
	p.retryAt, p.retryDelay = time.Time{}, 0
	if len(p.pending) == 0 && len(p.ready) == 0 {
		delete(b.projects, result.project)
		b.bytes -= bufferedProjectOverhead + int64(len(result.project))
	}
}

func (b *Buffer) retryJournal(p *bufferedProject, operation string) {
	if p.retryDelay == 0 {
		slog.Error("worker ingestion journal unavailable", "operation", operation)
	}
	b.retry(p)
}

func (b *Buffer) retry(p *bufferedProject) {
	p.retryDelay += min(p.retryDelay, b.config.RetryMax-p.retryDelay)
	p.retryDelay = max(b.config.RetryMin, p.retryDelay)
	p.retryAt = b.config.Clock().Add(p.retryDelay)
}

func (b *Buffer) countDiscarded(events int, reason string) {
	if m := b.config.Metrics; m != nil {
		m.Add("porcupine_flush_dropped_events_total", uint64(events), observability.Label{Name: "reason", Value: reason})
	}
}

// Preserve admission order and divide as close to the midpoint as request boundaries permit.
func splitBufferedLiveBatch(batch FlushBatch) (left, right *FlushBatch, ok bool) {
	mid := len(batch.Events) / 2
	cut := 0
	for i, event := range batch.Events[:len(batch.Events)-1] {
		if !event.requestEnd {
			continue
		}
		if i+1 <= mid || cut == 0 || i+1-mid < mid-cut {
			cut = i + 1
		}
		if i+1 >= mid {
			break
		}
	}
	if cut == 0 {
		return nil, nil, false
	}

	first, second := batch, batch
	first.ID, second.ID = newLiveBatchID(), newLiveBatchID()
	first.Events = batch.Events[:cut:cut]
	// The right child must not keep references to payloads released with the left.
	second.Events = append([]BufferedEvent(nil), batch.Events[cut:]...)
	second.oldest = second.Events[0].AdmittedAt
	first.bytes = 0
	for _, event := range first.Events {
		first.bytes += bufferedInputBytes(event.Input)
	}
	second.bytes = batch.bytes - first.bytes
	return &first, &second, true
}

func (b *Buffer) projectForAdmissionLocked(projectID string) (*bufferedProject, error) {
	p := b.projects[projectID]
	switch {
	case b.ctx.Err() != nil || b.draining:
		return nil, &Failure{Message: "buffer is unavailable or draining", Retry: true, Pressure: true}
	case p != nil && p.quarantine != "":
		return nil, ErrProjectQuarantined
	case p != nil && p.retryDelay > 0:
		return nil, &Failure{Message: "project buffer is retrying a flush", Retry: true, Pressure: true}
	}
	return p, nil
}

func (b *Buffer) checkCapacityLocked(projectID string, size int64, count int) (*bufferedProject, int64, error) {
	p, err := b.projectForAdmissionLocked(projectID)
	if err != nil {
		return nil, 0, err
	}
	projectSize, overhead, projectEvents := int64(0), int64(0), 0
	if p == nil {
		if len(b.projects) >= b.config.MaxProjects {
			return nil, 0, &Failure{Message: "buffer is full", Retry: true, Pressure: true}
		}
		overhead = bufferedProjectOverhead + int64(len(projectID))
	} else {
		projectSize = p.pendingBytes
		projectEvents = len(p.pending)
		for _, batch := range p.ready {
			projectSize += batch.bytes
			projectEvents += len(batch.Events)
		}
	}
	if size > b.config.MaxBytes-b.bytes-overhead || count > b.config.MaxEvents-b.events || size > b.config.ProjectMaxBytes-projectSize || count > b.config.ProjectMaxEvents-projectEvents {
		return nil, 0, &Failure{Message: "buffer is full", Retry: true, Pressure: true}
	}
	return p, overhead, nil
}

func (b *Buffer) StopAdmission() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	b.wake()
}

func (b *Buffer) Available() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx.Err() == nil && !b.draining && b.bytes < b.config.MaxBytes && b.events < b.config.MaxEvents
}
