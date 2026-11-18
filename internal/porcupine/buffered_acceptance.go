package porcupine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	ingestfmt "github.com/olup/porcupine/internal/ingest"
)

// This boundary carries validated commands, not committed analytical events.
// FlushLive must retain the batch ID across retries, deduplicate under the project
// write guard, recheck authoritative tenant/project deletion fences, and durably
// commit recoverable effect evidence before returning nil.
// A transient error leaves ownership and capacity with the buffer, including
// after a commit whose response was lost. A durable deletion fence discards
// pending input for that project. The writer must not retain or mutate the batch.
type liveBatchFlusher interface {
	FlushLive(context.Context, bufferedFlushBatch) error
}

// This error proves the batch cannot fit and never reached native commit.
var errHostedLiveBatchTooLarge = errors.New("hosted live batch exceeds output limits")

type bufferedLiveEvent struct {
	input      validatedLiveEvent
	admittedAt time.Time
	requestEnd bool
}
type bufferedFlushBatch struct {
	id, projectID string
	events        []bufferedLiveEvent
	oldest        time.Time
	bytes         int64
}

// Implementations resolve durable shared daily keys. The buffer does not retain
// raw peer addresses, user agents, or daily secrets after admission.
type cookielessKeyResolver interface {
	CookielessKey(context.Context, string) ([]byte, error)
}
type bufferConfig struct {
	maxBytes, projectMaxBytes, flushBytes                int64
	maxEvents, projectMaxEvents, maxProjects, maxFlushes int
	flushInterval, retryMin, retryMax                    time.Duration
	clock                                                func() time.Time
}

func defaultBufferConfig() bufferConfig {
	return bufferConfig{maxBytes: 64 << 20, projectMaxBytes: 8 << 20, flushBytes: 5 << 20,
		maxEvents: 100000, projectMaxEvents: 10000, maxProjects: 1024, maxFlushes: 4,
		flushInterval: 2 * time.Second, retryMin: 100 * time.Millisecond, retryMax: 5 * time.Second, clock: time.Now}
}
func (c bufferConfig) validate() error {
	if c.maxBytes <= 0 || c.projectMaxBytes <= 0 || c.flushBytes <= 0 || c.maxEvents <= 0 || c.projectMaxEvents <= 0 || c.maxProjects <= 0 || c.maxFlushes <= 0 || c.flushInterval <= 0 || c.retryMin <= 0 || c.retryMax < c.retryMin || c.clock == nil {
		return errors.New("buffer limits, flush interval, retry bounds and clock must be configured")
	}
	if c.projectMaxBytes > c.maxBytes || c.projectMaxEvents > c.maxEvents || c.flushBytes > c.projectMaxBytes {
		return errors.New("project buffer limits and flush threshold exceed their enclosing capacity")
	}
	return nil
}

type bufferedProject struct {
	pending      []bufferedLiveEvent
	pendingBytes int64
	oldest       time.Time
	ready        []*bufferedFlushBatch
	running      bool
	failures     int
	retryAt      time.Time
}
type bufferSnapshot struct {
	bytes                          int64
	events, projects, inflight     int
	oldest                         time.Time
	draining, failed, stopped      bool
	abandoned                      int
	droppedRequests, droppedEvents uint64
}
type bufferedAcceptance struct {
	mu                             sync.Mutex
	config                         bufferConfig
	writer                         liveBatchFlusher
	keys                           cookielessKeyResolver
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
}

// The byte budget charges owned payloads plus conservative allowances for structs,
// allocator metadata and slice capacity. It is not a hard process-RSS limit.
const bufferedEventOverhead int64 = 1024
const bufferedProjectOverhead int64 = 512

func newBufferedAcceptance(ctx context.Context, c bufferConfig, writer liveBatchFlusher, keys cookielessKeyResolver) (*bufferedAcceptance, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	if writer == nil {
		return nil, errors.New("buffered acceptance requires a durable batch writer")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := &bufferedAcceptance{config: c, writer: writer, keys: keys, projects: make(map[string]*bufferedProject), notify: make(chan struct{}, 1), completed: make(chan flushCompletion, c.maxFlushes), done: make(chan struct{}), ctx: ctx}
	go b.run()
	return b, nil
}
func (*bufferedAcceptance) Durability() acceptanceDurability { return acceptanceBuffered }
func (b *bufferedAcceptance) wake() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
func (b *bufferedAcceptance) Accept(ctx context.Context, batch preparedLiveBatch, peer ingestionPeer, withReceipts bool) (liveAcceptanceResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	project := b.projects[batch.projectID]
	unavailable := b.ctx.Err() != nil || b.draining || project != nil && project.failures > 0
	b.mu.Unlock()
	if unavailable {
		return nil, &acceptanceFailure{message: "buffer is unavailable or draining", retry: true}
	}
	admittedAt := b.config.clock().UTC()
	// A fixed-size pseudonym lets capacity be checked before key-provider work.
	// The admission day is fixed even if key resolution crosses UTC midnight.
	day := admittedAt.Format("2006-01-02")
	cookielessID := ""
	if batch.cookieless {
		cookielessID = "cookieless_" + strings.Repeat("0", 64)
	}
	inputs := make([]validatedLiveEvent, len(batch.events))
	var size int64
	for i, input := range batch.events {
		if input.event.ProjectID != batch.projectID || batch.projectID == "" || input.event.ID != 0 || input.event.Imported || input.event.IdentitySnapshot != "" || isIdentityControl(input.event.Event) != (input.identity != nil) {
			return nil, errors.New("invalid validated live batch scope or state")
		}
		if input.event.DistinctID == ingestfmt.CookielessSentinel && !isIdentityControl(input.event.Event) {
			if cookielessID == "" {
				return nil, errors.New("unresolved cookieless input")
			}
			input.event.DistinctID = cookielessID
			input.event.Properties = cookielessProperties(input.event.Properties, cookielessID, day)
			if input.event.SourceID != "" {
				input.event.Context = ingestfmt.ScrubContext(input.event.Context)
			}
		}
		charge := bufferedInputBytes(input)
		if charge > b.config.maxBytes-size {
			return nil, &acceptanceFailure{message: "buffer is full", retry: true}
		}
		size += charge
		inputs[i] = input
	}
	if len(inputs) == 0 {
		return nil, errors.New("empty validated live batch")
	}
	if batch.cookieless {
		b.mu.Lock()
		_, _, err := b.checkCapacityLocked(batch.projectID, size, len(inputs))
		b.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if b.keys == nil {
			return nil, &acceptanceFailure{message: "shared cookieless key provider unavailable", retry: true}
		}
		secret, err := b.keys.CookielessKey(ctx, day)
		if err != nil || len(secret) != 32 {
			return nil, &acceptanceFailure{message: "shared cookieless key unavailable", retry: true}
		}
		cookielessID = cookielessDistinctID(secret, batch.projectID, peer.address, peer.userAgent)
		for i, original := range batch.events {
			if original.event.DistinctID == ingestfmt.CookielessSentinel && !isIdentityControl(original.event.Event) {
				inputs[i].event.DistinctID = cookielessID
				inputs[i].event.Properties = cookielessProperties(original.event.Properties, cookielessID, day)
			}
		}
	}
	if guard, ok := b.writer.(interface {
		lockLiveAdmission(context.Context, string) (func(), error)
	}); ok {
		release, err := guard.lockLiveAdmission(ctx, batch.projectID)
		if err != nil {
			if errors.Is(err, errHostedProjectFenced) {
				return nil, apiError(404, "project not found")
			}
			return nil, &acceptanceFailure{message: "project admission unavailable", retry: true}
		}
		defer release()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, overhead, err := b.checkCapacityLocked(batch.projectID, size, len(inputs))
	if err != nil {
		return nil, err
	}
	newProject := p == nil
	owned := make([]bufferedLiveEvent, len(inputs))
	var receipts []bufferedReceipt
	if withReceipts {
		receipts = make([]bufferedReceipt, len(inputs))
	}
	for i, input := range inputs {
		input = cloneValidatedLiveEvent(input)
		owned[i] = bufferedLiveEvent{input: input, admittedAt: admittedAt, requestEnd: i == len(inputs)-1}
		if withReceipts {
			receipts[i] = bufferedReceipt{MessageID: input.event.MessageID, UUID: input.event.UUID}
		}
	}
	if newProject {
		p = &bufferedProject{}
		b.projects[strings.Clone(batch.projectID)] = p
	}
	if len(p.pending) == 0 {
		p.oldest = admittedAt
	}
	p.pending = append(p.pending, owned...)
	p.pendingBytes += size
	b.bytes += size + overhead
	b.events += len(inputs)
	b.wake()
	return bufferedLiveResult{received: len(inputs), receipts: receipts}, nil
}
func bufferedInputBytes(input validatedLiveEvent) int64 {
	e := input.event
	size := bufferedEventOverhead + int64(len(e.ProjectID)+len(e.UUID)+len(e.Event)+len(e.DistinctID)+len(e.Properties)+len(e.Timestamp)+len(e.ReceivedAt)+len(e.SourceID)+len(e.MessageID)+len(e.EventType)+len(e.Context))
	if c := input.identity; c != nil {
		size += int64(len(c.Target) + len(c.Anonymous) + len(c.Set) + len(c.SetOnce))
	}
	return size
}
func cloneValidatedLiveEvent(input validatedLiveEvent) validatedLiveEvent {
	e := &input.event
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
	if input.identity != nil {
		c := *input.identity
		c.Target = strings.Clone(c.Target)
		c.Anonymous = strings.Clone(c.Anonymous)
		c.Set = append([]byte(nil), c.Set...)
		c.SetOnce = append([]byte(nil), c.SetOnce...)
		input.identity = &c
	}
	return input
}
func (b *bufferedAcceptance) failedLocked() bool {
	for _, p := range b.projects {
		if p.failures > 0 {
			return true
		}
	}
	return false
}
func (b *bufferedAcceptance) snapshot() bufferSnapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := bufferSnapshot{bytes: b.bytes, events: b.events, projects: len(b.projects), inflight: b.running, draining: b.draining, failed: b.failedLocked(), stopped: b.stopped, droppedRequests: b.droppedRequests, droppedEvents: b.droppedEvents}
	stopping := b.ctx.Err() != nil
	if stopping {
		s.abandoned = b.events
	}
	for _, p := range b.projects {
		if stopping && p.running {
			s.abandoned -= len(p.ready[0].events)
		}
		for _, at := range []time.Time{p.oldest, readyOldest(p.ready)} {
			if !at.IsZero() && (s.oldest.IsZero() || at.Before(s.oldest)) {
				s.oldest = at
			}
		}
	}
	return s
}
func readyOldest(batches []*bufferedFlushBatch) time.Time {
	if len(batches) == 0 {
		return time.Time{}
	}
	return batches[0].oldest
}
func (b *bufferedAcceptance) Drain(ctx context.Context) error {
	b.stopAdmission()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		if b.snapshot().events != 0 {
			return errors.New("buffer stopped with uncommitted events")
		}
		return nil
	}
}
func (b *bufferedAcceptance) run() {
	defer func() { b.mu.Lock(); b.stopped = true; b.mu.Unlock(); close(b.done) }()
	for {
		b.mu.Lock()
		stopping := b.ctx.Err() != nil
		if stopping {
			b.draining = true
		}
		now := b.config.clock()
		wait := b.config.flushInterval
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
				due := p.oldest.Add(b.config.flushInterval)
				if b.draining || p.pendingBytes >= b.config.flushBytes || !now.Before(due) {
					p.ready = []*bufferedFlushBatch{{id: eventapi.NewID(), projectID: project, events: p.pending, oldest: p.oldest, bytes: p.pendingBytes}}
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
			if b.running >= b.config.maxFlushes {
				continue
			}
			batch := *p.ready[0]
			p.running = true
			b.running++
			go func() {
				err := b.writer.FlushLive(b.ctx, batch)
				b.completed <- flushCompletion{project: batch.projectID, id: batch.id, err: err}
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
func (b *bufferedAcceptance) complete(result flushCompletion) {
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.projects[result.project]
	if p == nil || len(p.ready) == 0 || p.ready[0].id != result.id || !p.running {
		return
	}
	p.running = false
	b.running--
	if errors.Is(result.err, errHostedProjectFenced) {
		b.bytes -= p.pendingBytes + bufferedProjectOverhead + int64(len(result.project))
		b.events -= len(p.pending)
		for _, batch := range p.ready {
			b.bytes -= batch.bytes
			b.events -= len(batch.events)
		}
		delete(b.projects, result.project)
		return
	}
	if errors.Is(result.err, errHostedLiveBatchTooLarge) {
		if left, right, ok := splitBufferedLiveBatch(*p.ready[0]); ok {
			p.ready[0] = right
			p.ready = append([]*bufferedFlushBatch{left}, p.ready...)
			p.retryAt = time.Time{}
			return
		}
		count := len(p.ready[0].events)
		b.droppedRequests++
		b.droppedEvents += uint64(count)
		slog.Warn("hosted ingestion dropped an oversized request", "project_id", result.project,
			"batch_id", result.id, "events", count, "error", result.err)
	} else if result.err != nil {
		p.failures++
		delay := b.config.retryMin
		for n := 1; n < p.failures && delay < b.config.retryMax; n++ {
			delay = min(b.config.retryMax, delay+min(delay, b.config.retryMax-delay))
		}
		p.retryAt = b.config.clock().Add(delay)
		return
	}
	b.bytes -= p.ready[0].bytes
	b.events -= len(p.ready[0].events)
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
func splitBufferedLiveBatch(batch bufferedFlushBatch) (left, right *bufferedFlushBatch, ok bool) {
	mid := len(batch.events) / 2
	cut := 0
	for i, event := range batch.events[:len(batch.events)-1] {
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
	first.id, second.id = eventapi.NewID(), eventapi.NewID()
	first.events = batch.events[:cut:cut]
	// The right child must not keep references to payloads released with the left.
	second.events = append([]bufferedLiveEvent(nil), batch.events[cut:]...)
	clear(batch.events[cut:])
	second.oldest = second.events[0].admittedAt
	first.bytes = 0
	for _, event := range first.events {
		first.bytes += bufferedInputBytes(event.input)
	}
	second.bytes = batch.bytes - first.bytes
	return &first, &second, true
}

func (b *bufferedAcceptance) checkCapacityLocked(projectID string, size int64, count int) (*bufferedProject, int64, error) {
	if b.ctx.Err() != nil || b.draining {
		return nil, 0, &acceptanceFailure{message: "buffer is unavailable or draining", retry: true}
	}
	p := b.projects[projectID]
	newProject := p == nil
	projectSize, projectEvents := int64(0), 0
	if newProject {
		if len(b.projects) >= b.config.maxProjects {
			return nil, 0, &acceptanceFailure{message: "buffer is full", retry: true}
		}
	} else {
		projectSize = p.pendingBytes
		projectEvents = len(p.pending)
		if p.failures > 0 {
			return nil, 0, &acceptanceFailure{message: "project buffer is retrying a flush", retry: true}
		}
		for _, batch := range p.ready {
			projectSize += batch.bytes
			projectEvents += len(batch.events)
		}
	}
	overhead := int64(0)
	if newProject {
		overhead = bufferedProjectOverhead + int64(len(projectID))
	}
	if size > b.config.maxBytes-b.bytes-overhead || count > b.config.maxEvents-b.events || size > b.config.projectMaxBytes-projectSize || count > b.config.projectMaxEvents-projectEvents {
		return nil, 0, &acceptanceFailure{message: "buffer is full", retry: true}
	}
	return p, overhead, nil
}

func (b *bufferedAcceptance) stopAdmission() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	b.wake()
}

func (b *bufferedAcceptance) available() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ctx.Err() == nil && !b.draining && b.bytes < b.config.maxBytes && b.events < b.config.maxEvents
}
