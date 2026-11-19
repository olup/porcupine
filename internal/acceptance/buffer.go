package acceptance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/olup/porcupine/internal/apierr"
	eventapi "github.com/olup/porcupine/internal/event"
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
	failures     int
	retryAt      time.Time
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
}
type Buffer struct {
	mu              sync.Mutex
	config          BufferConfig
	writer          FlushFunc
	keys            DailyKeyFunc
	lockAdmission   func(context.Context, string) (func(), error)
	projects        map[string]*bufferedProject
	bytes           int64
	events, running int
	draining        bool
	stopped         bool
	notify          chan struct{}
	completed       chan flushCompletion
	done            chan struct{}
	ctx             context.Context
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
		config: c, writer: writer, lockAdmission: lockAdmission, keys: keys, ctx: ctx,
		projects: make(map[string]*bufferedProject), notify: make(chan struct{}, 1),
		completed: make(chan flushCompletion, c.MaxFlushes), done: make(chan struct{}),
	}
	go b.run()
	return b, nil
}
func (*Buffer) Durability() Durability { return Buffered }
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
	project := b.projects[batch.ProjectID]
	unavailable := b.ctx.Err() != nil || b.draining || project != nil && project.failures > 0
	b.mu.Unlock()
	if unavailable {
		return nil, &Failure{Message: "buffer is unavailable or draining", Retry: true, Pressure: true}
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
			if errors.Is(err, ErrProjectFenced) {
				return nil, apierr.New(404, "project not found")
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
	newProject := p == nil
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
	if newProject {
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
	return BufferedResult{Received: len(inputs), Receipts: receipts}, nil
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
		Stopped:        b.stopped,
		draining:       b.draining,
		CapacityEvents: b.config.MaxEvents, CapacityBytes: b.config.MaxBytes,
	}
	stopping := b.ctx.Err() != nil
	if stopping {
		s.abandoned = b.events
	}
	for _, p := range b.projects {
		s.failed = s.failed || p.failures > 0
		if stopping && p.running {
			s.abandoned -= len(p.ready[0].Events)
		}
		for _, at := range []time.Time{p.oldest, readyOldest(p.ready)} {
			if !at.IsZero() && (s.Oldest.IsZero() || at.Before(s.Oldest)) {
				s.Oldest = at
			}
		}
	}
	return s
}
func readyOldest(batches []*FlushBatch) time.Time {
	if len(batches) == 0 {
		return time.Time{}
	}
	return batches[0].oldest
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
	defer func() { b.mu.Lock(); b.stopped = true; b.mu.Unlock(); close(b.done) }()
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
			if len(p.ready) == 0 && len(p.pending) > 0 {
				due := p.oldest.Add(b.config.FlushInterval)
				if b.draining || p.pendingBytes >= b.config.FlushBytes || !now.Before(due) {
					p.ready = []*FlushBatch{{ID: eventapi.NewID(), ProjectID: project, Events: p.pending, oldest: p.oldest, bytes: p.pendingBytes}}
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
			if now.Before(p.retryAt) {
				wait = min(wait, p.retryAt.Sub(now))
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
		dropped := len(p.pending)
		b.bytes -= p.pendingBytes + bufferedProjectOverhead + int64(len(result.project))
		b.events -= len(p.pending)
		for _, batch := range p.ready {
			b.bytes -= batch.bytes
			b.events -= len(batch.Events)
			dropped += len(batch.Events)
		}
		observe("dropped")
		if m := b.config.Metrics; m != nil {
			m.Add("porcupine_flush_dropped_events_total", uint64(dropped), observability.Label{Name: "reason", Value: "project_fenced"})
		}
		delete(b.projects, result.project)
		return
	}
	if errors.Is(result.err, ErrBatchTooLarge) {
		if left, right, ok := splitBufferedLiveBatch(*p.ready[0]); ok {
			observe("split")
			p.ready[0] = right
			p.ready = append([]*FlushBatch{left}, p.ready...)
			p.retryAt = time.Time{}
			return
		}
	}
	if result.err != nil {
		if b.ctx.Err() != nil {
			observe("cancelled")
		} else {
			observe("retry")
		}
		p.failures++
		delay := b.config.RetryMin
		for n := 1; n < p.failures && delay < b.config.RetryMax; n++ {
			delay = min(b.config.RetryMax, delay+min(delay, b.config.RetryMax-delay))
		}
		p.retryAt = b.config.Clock().Add(delay)
		return
	} else {
		observe("ok")
	}
	b.bytes -= p.ready[0].bytes
	b.events -= len(p.ready[0].Events)
	p.ready[0] = nil
	p.ready = p.ready[1:]
	p.failures = 0
	p.retryAt = time.Time{}
	if len(p.pending) == 0 && len(p.ready) == 0 {
		delete(b.projects, result.project)
		b.bytes -= bufferedProjectOverhead + int64(len(result.project))
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
	first.ID, second.ID = eventapi.NewID(), eventapi.NewID()
	first.Events = batch.Events[:cut:cut]
	// The right child must not keep references to payloads released with the left.
	second.Events = append([]BufferedEvent(nil), batch.Events[cut:]...)
	clear(batch.Events[cut:])
	second.oldest = second.Events[0].AdmittedAt
	first.bytes = 0
	for _, event := range first.Events {
		first.bytes += bufferedInputBytes(event.Input)
	}
	second.bytes = batch.bytes - first.bytes
	return &first, &second, true
}

func (b *Buffer) checkCapacityLocked(projectID string, size int64, count int) (*bufferedProject, int64, error) {
	if b.ctx.Err() != nil || b.draining {
		return nil, 0, &Failure{Message: "buffer is unavailable or draining", Retry: true, Pressure: true}
	}
	p := b.projects[projectID]
	newProject := p == nil
	projectSize, projectEvents := int64(0), 0
	if newProject {
		if len(b.projects) >= b.config.MaxProjects {
			return nil, 0, &Failure{Message: "buffer is full", Retry: true, Pressure: true}
		}
	} else {
		projectSize = p.pendingBytes
		projectEvents = len(p.pending)
		if p.failures > 0 {
			return nil, 0, &Failure{Message: "project buffer is retrying a flush", Retry: true, Pressure: true}
		}
		for _, batch := range p.ready {
			projectSize += batch.bytes
			projectEvents += len(batch.Events)
		}
	}
	overhead := int64(0)
	if newProject {
		overhead = bufferedProjectOverhead + int64(len(projectID))
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
