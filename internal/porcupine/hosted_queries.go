package porcupine

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	eventapi "github.com/olup/porcupine/internal/event"
	lakeapi "github.com/olup/porcupine/internal/lake"
	"github.com/olup/porcupine/internal/usage"
)

type interactiveReader interface {
	Query(context.Context, string, string, map[string]string, lakeapi.QueryExecutionLimits) (lakeapi.AnalyticsResult, error)
	Catalog(context.Context, lakeapi.EventCatalogInput) (lakeapi.EventCatalog, error)
	Profile(context.Context, string, lakeapi.EventProfileOptions, time.Time) (lakeapi.EventProfileRead, error)
}

type hostedEventReader interface {
	interactiveReader
	ListEvents(context.Context, string, int, int) ([]eventapi.Event, error)
}

type hostedQueryRuntime struct {
	reader       hostedEventReader
	recorder     *usage.Recorder
	register     func(context.Context, *sql.Tx, string, string) error
	finish       func(context.Context, *sql.DB, string, string) error
	returnedHook func(string, string)
	mu           sync.Mutex
	returned     map[string]string
	inFlight     int
}

const hostedQueryUsageSource = "hosted_query_executions"
const hostedReaderCompletionLimit = 256

// initializeHostedQueries installs the hosted read dependencies before serving
// requests or starting usage observation. The caller owns the global profile gate.
// returned, when set, observes actual return before the acknowledgement.
func (s *Service) initializeHostedQueries(store *lakeapi.DuckLakeStore, register func(context.Context, *sql.Tx, string, string) error, finish func(context.Context, *sql.DB, string, string) error, returned func(project, id string)) error {
	if s.shared == nil || s.control == nil || s.usage == nil || register == nil || finish == nil {
		return errors.New("hosted reads require shared query admission, usage and project lifetime authority")
	}
	reader, err := lakeapi.NewHostedReader(store)
	if err != nil {
		return err
	}
	s.hostedQueries = &hostedQueryRuntime{
		reader: reader, recorder: usage.NewRecorder(s.control, hostedQueryUsageSource, s.clock),
		register: register, finish: finish, returned: make(map[string]string), returnedHook: returned,
	}
	return nil
}

func (s *Service) interactiveReader() interactiveReader {
	if s.hostedQueries != nil {
		return s.hostedQueries.reader
	}
	return s.lake
}

func (r *hostedQueryRuntime) reserveCompletion() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inFlight+len(r.returned) >= hostedReaderCompletionLimit {
		return false
	}
	r.inFlight++
	return true
}

func (r *hostedQueryRuntime) dropReservation() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
}

func (r *hostedQueryRuntime) noteReturn(id, project string) {
	if r.returnedHook != nil {
		r.returnedHook(project, id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inFlight--
	r.returned[id] = project
}

func (r *hostedQueryRuntime) acknowledgeReturn(ctx context.Context, db *sql.DB, id, project string) error {
	if err := r.finish(ctx, db, project, id); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.returned, id)
	return nil
}

func (r *hostedQueryRuntime) observeReturns(ctx context.Context, db *sql.DB) error {
	r.mu.Lock()
	pending := make(map[string]string, len(r.returned))
	for id, project := range r.returned {
		pending[id] = project
	}
	r.mu.Unlock()
	for id, project := range pending {
		if err := r.acknowledgeReturn(ctx, db, id, project); err != nil {
			return err
		}
	}
	return nil
}
